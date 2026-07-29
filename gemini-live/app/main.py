"""riley-gemini-live — Riley card-support voice agent over the Gemini Live API.

Listens on /voice for a single bidirectional
PCM16 stream from the Veris actor. For each connection, opens its own Gemini
Live session (server-to-server, via the `google-genai` SDK), registers the BCS
function tools, and bridges audio + tool calls. The agent speaks first. Tool
calls are dispatched against `BCSAPI`, which talks to postgres.

Sample-rate note: the Veris actor speaks/listens at 24 kHz PCM16. Gemini Live
*requires* 16 kHz PCM16 input and *emits* 24 kHz PCM16 output. So we downsample
24 kHz → 16 kHz on the way in (stdlib `audioop.ratecv`) and forward Gemini's
24 kHz output straight back to the actor unchanged.

Logging is intentionally chatty so it's obvious from agent.log alone whether
audio is flowing, how Gemini is responding, and where things stall.
"""

from __future__ import annotations

import asyncio
import audioop
import json
import logging
import os
import time
from contextlib import suppress
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket
from google import genai
from google.genai import types
from starlette.websockets import WebSocketDisconnect

from .db import BCSAPI
from .reporting import report_tool_call
from .tools import TOOLS, dispatch

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


GEMINI_LIVE_MODEL = os.environ.get(
    "GEMINI_LIVE_MODEL", "gemini-2.5-flash-native-audio-preview-12-2025"
)
GEMINI_VOICE = os.environ.get("GEMINI_VOICE", "Puck")

# The Veris actor speaks and listens at 24 kHz. Gemini Live is fixed at 16 kHz
# input / 24 kHz output, so only the input leg needs resampling.
ACTOR_RATE_HZ = 24000
GEMINI_INPUT_RATE_HZ = 16000

# How often to emit periodic frame-count log lines from the two pumps. At
# 50 fps (20 ms/frame) this is roughly one heartbeat per second.
LOG_EVERY_N_FRAMES = 50


def _load_agent_prompt() -> str:
    # Resolve from CWD first (matches the mini-bcs `/agent` working dir
    # entry_point), then fall back to a path next to this package.
    for candidate in (
        Path("agent_desc.txt"),
        Path(__file__).resolve().parent.parent / "agent_desc.txt",
    ):
        if candidate.is_file():
            return candidate.read_text()
    raise FileNotFoundError("agent_desc.txt not found")


AGENT_PROMPT = _load_agent_prompt()


def _build_config() -> types.LiveConnectConfig:
    return types.LiveConnectConfig(
        response_modalities=["AUDIO"],
        system_instruction=AGENT_PROMPT,
        tools=TOOLS,
        speech_config=types.SpeechConfig(
            voice_config=types.VoiceConfig(
                prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=GEMINI_VOICE)
            )
        ),
        # Benchmark turn-taking standard: 800 ms end-of-turn silence, set on
        # Gemini's own native automatic activity detection (its VAD model stays
        # Gemini's — only the threshold is matched across frameworks). Gemini's
        # unset server default is undocumented; pinning this makes the threshold
        # explicit and identical to the cohort. See METHODOLOGY §4.
        realtime_input_config=types.RealtimeInputConfig(
            automatic_activity_detection=types.AutomaticActivityDetection(
                silence_duration_ms=800,
                prefix_padding_ms=300,
            )
        ),
        # Transcripts for both legs so agent.log shows what was said.
        input_audio_transcription=types.AudioTranscriptionConfig(),
        output_audio_transcription=types.AudioTranscriptionConfig(),
    )


app = FastAPI(title="riley-gemini-live", version="0.1.0")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.websocket("/voice")
async def voice(actor_ws: WebSocket) -> None:
    """One actor connection ↔ one Gemini Live session with BCS tools.

    Logs every state transition so it's possible to debug stalls from
    agent.log without correlating with proxy / sandbox logs.
    """
    await actor_ws.accept()
    peer = f"{actor_ws.client.host}:{actor_ws.client.port}" if actor_ws.client else "?"
    logger.info("[voice] actor connected peer=%s", peer)
    api = BCSAPI()
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

    t_start = time.monotonic()
    try:
        async with client.aio.live.connect(
            model=GEMINI_LIVE_MODEL, config=_build_config()
        ) as session:
            logger.info(
                "[voice] Gemini Live connected model=%s voice=%s in=%dHz out=%dHz",
                GEMINI_LIVE_MODEL, GEMINI_VOICE, GEMINI_INPUT_RATE_HZ, ACTOR_RATE_HZ,
            )
            # Agent speaks first — call etiquette. Nudge the model to greet now;
            # the system prompt owns the actual greeting wording.
            await session.send_client_content(
                turns=types.Content(
                    role="user",
                    parts=[types.Part(text="<call connected — greet the caller now>")],
                ),
                turn_complete=True,
            )
            logger.info("[voice] sent greeting trigger (agent greets first)")

            # Run both pumps. If either raises (or returns), cancel the other so
            # we don't leak a half-dead session.
            t1 = asyncio.create_task(
                _pump_actor_to_gemini(actor_ws, session), name="actor→gemini"
            )
            t2 = asyncio.create_task(
                _pump_gemini_to_actor(session, actor_ws, api), name="gemini→actor"
            )
            done, pending = await asyncio.wait(
                {t1, t2}, return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await task
            for task in done:
                exc = task.exception()
                if exc is not None:
                    logger.error("[voice] pump %s failed: %s", task.get_name(), exc, exc_info=exc)
    except Exception as exc:
        logger.exception("[voice] handler failed: %s", exc)
    finally:
        elapsed = time.monotonic() - t_start
        logger.info("[voice] handler exit duration=%.1fs", elapsed)


async def _pump_actor_to_gemini(actor_ws: WebSocket, session) -> None:
    """Binary PCM16 frames from the actor (24 kHz) → Gemini Live input (16 kHz)."""
    n_frames = 0
    n_bytes = 0
    resample_state = None  # carried across audioop.ratecv calls for continuity
    try:
        while True:
            frame = await actor_ws.receive_bytes()
            n_frames += 1
            n_bytes += len(frame)
            pcm16, resample_state = audioop.ratecv(
                frame, 2, 1, ACTOR_RATE_HZ, GEMINI_INPUT_RATE_HZ, resample_state
            )
            if n_frames == 1:
                logger.info(
                    "[a→g] first frame received bytes=%d (→%d bytes @16kHz)",
                    len(frame), len(pcm16),
                )
            elif n_frames % LOG_EVERY_N_FRAMES == 0:
                logger.info("[a→g] forwarded %d frames (%d bytes @24kHz)", n_frames, n_bytes)
            await session.send_realtime_input(
                audio=types.Blob(data=pcm16, mime_type=f"audio/pcm;rate={GEMINI_INPUT_RATE_HZ}")
            )
    except WebSocketDisconnect as exc:
        logger.info(
            "[a→g] actor disconnected after %d frames (%d bytes): code=%s",
            n_frames, n_bytes, getattr(exc, "code", "?"),
        )
    except KeyError as exc:
        # Starlette raises KeyError('bytes') when it received a text frame
        # instead of a binary one — i.e. protocol mismatch on the actor side.
        logger.error(
            "[a→g] received non-binary frame after %d binary frames — "
            "actor protocol mismatch? (%s)", n_frames, exc,
        )
        raise
    except Exception as exc:
        logger.exception(
            "[a→g] pump died after %d frames (%d bytes): %s", n_frames, n_bytes, exc,
        )
        raise


async def _pump_gemini_to_actor(session, actor_ws: WebSocket, api: BCSAPI) -> None:
    """Gemini Live events → 24 kHz audio bytes back to the actor; dispatch tools.

    Gemini emits 24 kHz PCM16 output, which matches the actor's rate, so audio
    is streamed straight through as it arrives. After a tool call we send the
    function result back with ``send_tool_response``; Gemini resumes the turn
    on its own — no explicit re-trigger needed (unlike OpenAI Realtime).

    ``session.receive()`` yields one *complete model turn* and then returns
    (it breaks internally on ``turn_complete``). So we wrap it in an outer loop
    and re-enter it for each subsequent turn — otherwise the pump would exit
    after the opening greeting, closing the WS and dropping the call.
    """
    n_audio_frames = 0
    n_audio_bytes = 0
    n_turns = 0

    try:
        while True:
            audio_bytes_this_turn = 0
            agent_transcript = ""
            got_any = False
            async for response in session.receive():
                got_any = True
                # Audio output — forward straight to the actor (24 kHz, no resample).
                audio = response.data
                if audio:
                    n_audio_frames += 1
                    n_audio_bytes += len(audio)
                    audio_bytes_this_turn += len(audio)
                    if n_audio_frames == 1:
                        logger.info("[g→a] first audio chunk bytes=%d", len(audio))
                    elif n_audio_frames % LOG_EVERY_N_FRAMES == 0:
                        logger.info(
                            "[g→a] streamed %d audio chunks (%d bytes) so far this session",
                            n_audio_frames, n_audio_bytes,
                        )
                    await actor_ws.send_bytes(audio)

                sc = response.server_content
                if sc is not None:
                    if sc.output_transcription and sc.output_transcription.text:
                        agent_transcript += sc.output_transcription.text
                    if sc.input_transcription and sc.input_transcription.text:
                        logger.info("[g→a] actor_said: %s", sc.input_transcription.text[:200])
                    if sc.interrupted:
                        logger.info("[g→a] turn interrupted (barge-in)")
                    if sc.turn_complete:
                        n_turns += 1
                        if agent_transcript.strip():
                            logger.info(
                                "[g→a] agent_said (turn #%d): %s",
                                n_turns, agent_transcript.strip()[:300],
                            )
                        logger.info(
                            "[g→a] turn_complete #%d (%d audio bytes this turn, %d total)",
                            n_turns, audio_bytes_this_turn, n_audio_bytes,
                        )
                        audio_bytes_this_turn = 0
                        agent_transcript = ""

                # Tool calls — dispatch each against BCSAPI and return the results.
                if response.tool_call and response.tool_call.function_calls:
                    function_responses = []
                    for fc in response.tool_call.function_calls:
                        args = dict(fc.args or {})
                        logger.info("[g→a] tool_call: %s args=%s", fc.name, str(args)[:200])
                        try:
                            output = dispatch(api, fc.name, args)
                            logger.info(
                                "[g→a] tool_result %s: %s",
                                fc.name, json.dumps(output, default=str)[:200],
                            )
                        except Exception as exc:  # surface errors to the model
                            logger.exception("[g→a] tool %s failed", fc.name)
                            output = {"error": str(exc)}

                        report_tool_call(fc.name, args, output)
                        function_responses.append(
                            types.FunctionResponse(id=fc.id, name=fc.name, response=output)
                        )
                    await session.send_tool_response(function_responses=function_responses)

            # receive() returned with no messages → the Gemini session closed.
            if not got_any:
                logger.info(
                    "[g→a] session closed (receive yielded no messages); %d turns, %d audio bytes",
                    n_turns, n_audio_bytes,
                )
                break
    except WebSocketDisconnect:
        logger.info("[g→a] actor WS closed; ending Gemini receive loop")
    except Exception as exc:
        logger.exception(
            "[g→a] pump died after %d audio chunks (%d bytes), %d turns: %s",
            n_audio_frames, n_audio_bytes, n_turns, exc,
        )
        raise

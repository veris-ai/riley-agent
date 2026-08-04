"""riley-gradbot — Riley card-ops voice agent on the gradbot framework.

Listens on /voice for a single bidirectional PCM16 stream from the Veris
actor. For each connection, starts its own gradbot session: gradbot's Rust
multiplexer runs Gradium streaming STT, an OpenAI-compatible LLM, and Gradium
streaming TTS concurrently, handling turn-taking and barge-in, and hands tool
calls back here. Tool calls are dispatched against `BCSAPI`, which talks to
postgres.

Unlike the hosted speech-to-speech implementations, nothing about the session
lives in a vendor's cloud beyond the individual STT/LLM/TTS calls — the event
loop, the conversation state, and the tools are all in this process.

Logging is intentionally chatty so it's obvious from agent.log alone whether
audio is flowing, how the multiplexer is stepping, and where things stall.
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

import gradbot
from fastapi import FastAPI, WebSocket
from starlette.websockets import WebSocketDisconnect

from .db import BCSAPI
from .reporting import report_tool_call
from .tools import TOOLS, dispatch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Veris's voice_ws actor speaks raw PCM16 mono at 24 kHz in both directions.
# gradbot's Pcm audio format is asymmetric: it decodes input at 24 kHz (so the
# actor's frames go straight in) but encodes output at 48 kHz, which is halved
# back down on the way out.
ACTOR_RATE_HZ = 24000
GRADBOT_INPUT_RATE_HZ = 24000
GRADBOT_OUTPUT_RATE_HZ = 48000

# Gradium flagship voice. "Harper" — modern, confident, friendly, standard
# American accent. Any voice id from the Gradium catalog works.
VOICE_ID = os.environ.get("GRADBOT_VOICE_ID", "4SZHfMpw-p46Ywgs")

# gradbot re-engages the caller with "..." after this many seconds of silence,
# and hangs up after three unanswered prompts. None of the other Riley
# implementations nudge the caller, so it's disabled (0.0) to keep the
# comparison like-for-like — Riley waits as long as the caller needs.
SILENCE_TIMEOUT_S = 0.0

# How often to emit periodic frame-count log lines from the two pumps. At
# 50 fps (20 ms/frame) this is roughly one heartbeat per second.
LOG_EVERY_N_FRAMES = 50


def _load_agent_prompt() -> str:
    # Resolve from CWD first (matches the `/agent` working dir entry_point),
    # then fall back to a path next to this package.
    for candidate in (
        Path("agent_desc.txt"),
        Path(__file__).resolve().parent.parent / "agent_desc.txt",
    ):
        if candidate.is_file():
            return candidate.read_text()
    raise FileNotFoundError("agent_desc.txt not found")


GREETING = "Thanks for calling Acme Bank, this is Riley — how can I help?"

# gradbot opens the call by sending the LLM a literal "[start]" message, and
# its own system-prompt scaffold tells the model to greet and start the
# conversation. agent_desc.txt — shared verbatim with the other Riley
# implementations — says the opposite, that the greeting already happened.
# Resolve the contradiction here rather than forking the shared prompt, and
# pin the same opening words the other implementations speak.
INSTRUCTIONS = f"""{_load_agent_prompt()}

# Opening line
Disregard the note above about having already greeted the caller: on this
platform your first turn *is* the greeting. The call has just connected and
nobody has spoken. Open with exactly: "{GREETING}" Then stop and wait for the
caller to respond. Do not greet again after that."""


app = FastAPI(title="riley-gradbot", version="0.1.0")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


def _session_config() -> gradbot.SessionConfig:
    return gradbot.SessionConfig(
        voice_id=VOICE_ID,
        instructions=INSTRUCTIONS,
        language=gradbot.Lang.En,
        tools=TOOLS,
        assistant_speaks_first=True,
        silence_timeout_s=SILENCE_TIMEOUT_S,
    )


@app.websocket("/voice")
async def voice(actor_ws: WebSocket) -> None:
    """One actor connection ↔ one gradbot session with BCS tools.

    Logs every state transition so it's possible to debug stalls from
    agent.log without correlating with proxy / sandbox logs.
    """
    await actor_ws.accept()
    peer = f"{actor_ws.client.host}:{actor_ws.client.port}" if actor_ws.client else "?"
    logger.info("[voice] actor connected peer=%s", peer)
    api = BCSAPI()

    t_start = time.monotonic()
    try:
        # gradbot reads its LLM and Gradium credentials from the environment
        # (LLM_API_KEY / LLM_BASE_URL / LLM_MODEL, GRADIUM_API_KEY) via
        # `config.from_env()`; `client_kwargs` drops anything unset so
        # gradbot's own defaults apply.
        cfg = gradbot.config.from_env()
        input_handle, output_handle = await gradbot.run(
            **cfg.client_kwargs,
            session_config=_session_config(),
            input_format=gradbot.AudioFormat.Pcm,
            output_format=gradbot.AudioFormat.Pcm,
        )
        logger.info(
            "[voice] gradbot session started voice=%s model=%s tools=%d "
            "instructions=%d chars in=%d Hz out=%d Hz",
            VOICE_ID,
            cfg.llm.model or "(gradbot default)",
            len(TOOLS),
            len(INSTRUCTIONS),
            GRADBOT_INPUT_RATE_HZ,
            GRADBOT_OUTPUT_RATE_HZ,
        )

        # Run both pumps. If either raises (or returns), cancel the other so
        # we don't leak a half-dead session.
        t1 = asyncio.create_task(
            _pump_actor_to_gradbot(actor_ws, input_handle), name="actor→gradbot"
        )
        t2 = asyncio.create_task(
            _pump_gradbot_to_actor(output_handle, actor_ws, api), name="gradbot→actor"
        )
        done, pending = await asyncio.wait(
            {t1, t2}, return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await task
        # Surface any exception from the first task to finish so it shows up
        # in the log instead of being silently swallowed.
        for task in done:
            exc = task.exception()
            if exc is not None:
                logger.error("[voice] pump %s failed: %s", task.get_name(), exc, exc_info=exc)
    except Exception as exc:
        logger.exception("[voice] handler failed: %s", exc)
    finally:
        elapsed = time.monotonic() - t_start
        logger.info("[voice] handler exit duration=%.1fs", elapsed)


async def _pump_actor_to_gradbot(
    actor_ws: WebSocket, input_handle: gradbot.SessionInputHandle
) -> None:
    """Binary PCM16 frames from the actor → gradbot's STT input.

    No resampling: the actor's 24 kHz matches gradbot's Pcm input rate.
    """
    n_frames = 0
    n_bytes = 0
    try:
        while True:
            frame = await actor_ws.receive_bytes()
            n_frames += 1
            n_bytes += len(frame)
            if n_frames == 1:
                logger.info("[a→gb] first frame received bytes=%d", len(frame))
            elif n_frames % LOG_EVERY_N_FRAMES == 0:
                logger.info("[a→gb] forwarded %d frames (%d bytes)", n_frames, n_bytes)
            await input_handle.send_audio(frame)
    except WebSocketDisconnect as exc:
        logger.info(
            "[a→gb] actor disconnected after %d frames (%d bytes): code=%s",
            n_frames, n_bytes, getattr(exc, "code", "?"),
        )
        await input_handle.close()
    except KeyError as exc:
        # Starlette raises KeyError('bytes') when it received a text frame
        # instead of a binary one — i.e. protocol mismatch on the actor side.
        logger.error(
            "[a→gb] received non-binary frame after %d binary frames — "
            "actor protocol mismatch? (%s)", n_frames, exc,
        )
        raise
    except Exception as exc:
        logger.exception(
            "[a→gb] pump died after %d frames (%d bytes): %s",
            n_frames, n_bytes, exc,
        )
        raise


async def _pump_gradbot_to_actor(
    output_handle: gradbot.SessionOutputHandle, actor_ws: WebSocket, api: BCSAPI
) -> None:
    """gradbot messages → audio back to the actor; dispatch tool calls.

    Every audio chunk streams straight to the actor as the multiplexer emits
    it — no buffering — so the actor hears the reply onset immediately. On
    barge-in gradbot simply stops emitting, so there is nothing to flush here.
    """
    # audioop.ratecv carries fractional-sample state between calls; keeping it
    # across chunks is what stops a click at every chunk boundary.
    resample_state = None
    n_audio_chunks = 0
    n_audio_bytes = 0
    n_tool_calls = 0

    while True:
        msg = await output_handle.receive()
        if msg is None:
            logger.info(
                "[gb→a] session ended — totals %d audio chunks / %d bytes / %d tool calls",
                n_audio_chunks, n_audio_bytes, n_tool_calls,
            )
            return

        if msg.msg_type == "audio":
            pcm48 = msg.data
            if not pcm48:
                continue
            pcm24, resample_state = audioop.ratecv(
                pcm48, 2, 1, GRADBOT_OUTPUT_RATE_HZ, ACTOR_RATE_HZ, resample_state
            )
            n_audio_chunks += 1
            n_audio_bytes += len(pcm24)
            if n_audio_chunks == 1:
                logger.info(
                    "[gb→a] first audio chunk in=%d bytes out=%d bytes",
                    len(pcm48), len(pcm24),
                )
            elif n_audio_chunks % LOG_EVERY_N_FRAMES == 0:
                logger.info(
                    "[gb→a] streamed %d audio chunks (%d bytes) so far this session",
                    n_audio_chunks, n_audio_bytes,
                )
            await actor_ws.send_bytes(pcm24)

        elif msg.msg_type == "tts_text":
            logger.info("[gb→a] agent_said: %s", (msg.text or "")[:200])

        elif msg.msg_type == "stt_text":
            logger.info("[gb→a] actor_said: %s", (msg.text or "")[:200])

        elif msg.msg_type == "event":
            # Listening → Flushing → EndOfTurn → Processing, plus interruptions.
            logger.info("[gb→a] event: %s", msg.event.event_type)

        elif msg.msg_type == "tool_call":
            n_tool_calls += 1
            await _handle_tool_call(msg, api)


async def _handle_tool_call(msg: gradbot.MsgOut, api: BCSAPI) -> None:
    """Dispatch one gradbot tool call and hand the result back.

    Dispatch runs inline in the output pump rather than in a spawned task:
    `BCSAPI` is synchronous psycopg2 and every card-ops query is a
    single-row lookup or update, so the pump stalls for a millisecond or two
    at most. gradbot's own deferred-tool machinery (keep talking while a slow
    tool runs) is there if a tool ever gets slow enough to need it.
    """
    call = msg.tool_call
    name = call.tool_name
    logger.info("[gb→a] tool_call: %s args=%s", name, call.args_json[:200])

    args: dict = {}
    try:
        args = json.loads(call.args_json) if call.args_json else {}
        output = dispatch(api, name, args)
        logger.info(
            "[gb→a] tool_result %s: %s", name, json.dumps(output, default=str)[:200]
        )
    except Exception as exc:  # surface errors to the model
        logger.exception("[gb→a] tool %s failed", name)
        output = {"error": str(exc)}

    report_tool_call(name, args, output)
    await msg.tool_call_handle.send(json.dumps(output, default=str))

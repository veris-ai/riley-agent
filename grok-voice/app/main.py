"""riley-grok-voice — Riley card-ops voice agent over xAI Grok realtime WS.

Listens on /voice for a single bidirectional PCM16 stream from the Veris
actor. For each connection, opens its own Grok speech-to-speech session
(OpenAI Realtime-compatible wire protocol), registers the BCS function tools,
and bridges audio + tool calls. The agent speaks first. Tool calls are
dispatched against `BCSAPI`, which talks to postgres.

Logging is intentionally chatty so it's obvious from agent.log alone whether
audio is flowing, how Grok is responding, and where things stall.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
from contextlib import suppress
from pathlib import Path

import websockets
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


GROK_MODEL = os.environ.get("GROK_VOICE_MODEL", "grok-voice-think-fast-2.0")
GROK_URL = f"wss://api.x.ai/v1/realtime?model={GROK_MODEL}"
GROK_VOICE = os.environ.get("GROK_VOICE", "eve")
SAMPLE_RATE_HZ = 24000

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


AGENT_PROMPT = _load_agent_prompt()


app = FastAPI(title="riley-grok-voice", version="0.1.0")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.websocket("/voice")
async def voice(actor_ws: WebSocket) -> None:
    """One actor connection ↔ one Grok realtime session with BCS tools.

    Logs every state transition so it's possible to debug stalls from
    agent.log without correlating with proxy / sandbox logs.
    """
    await actor_ws.accept()
    peer = f"{actor_ws.client.host}:{actor_ws.client.port}" if actor_ws.client else "?"
    logger.info("[voice] actor connected peer=%s", peer)
    api = BCSAPI()
    gk_headers = {"Authorization": f"Bearer {os.environ['XAI_API_KEY']}"}

    t_start = time.monotonic()
    try:
        async with websockets.connect(GROK_URL, additional_headers=gk_headers) as gk_ws:
            logger.info(
                "[voice] Grok realtime connected model=%s voice=%s rate=%d Hz",
                GROK_MODEL, GROK_VOICE, SAMPLE_RATE_HZ,
            )
            await _configure_session(gk_ws)
            # Agent speaks first — call etiquette. The system prompt tells the
            # model the greeting already happened, so force it on this opening
            # turn with per-response instructions, which override the session
            # instructions for that one response.
            await gk_ws.send(json.dumps({
                "type": "response.create",
                "response": {
                    "instructions": (
                        "Open the call by greeting the caller now. Say exactly: "
                        "\"Thanks for calling Acme Bank, this is Riley, how can I "
                        "help?\" Then stop and wait for the caller to respond."
                    ),
                },
            }))
            logger.info("[voice] sent greeting trigger (agent greets first)")

            # Run both pumps. If either raises (or returns), cancel the
            # other so we don't leak a half-dead WS pair.
            t1 = asyncio.create_task(
                _pump_actor_to_grok(actor_ws, gk_ws), name="actor→grok"
            )
            t2 = asyncio.create_task(
                _pump_grok_to_actor(gk_ws, actor_ws, api), name="grok→actor"
            )
            done, pending = await asyncio.wait(
                {t1, t2}, return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await task
            # Surface any exception from the first task to finish so it shows
            # up in the log instead of being silently swallowed.
            for task in done:
                exc = task.exception()
                if exc is not None:
                    logger.error("[voice] pump %s failed: %s", task.get_name(), exc, exc_info=exc)
    except Exception as exc:
        logger.exception("[voice] handler failed: %s", exc)
    finally:
        elapsed = time.monotonic() - t_start
        logger.info("[voice] handler exit duration=%.1fs", elapsed)


async def _configure_session(gk_ws) -> None:
    # Unlike OpenAI Realtime, Grok keeps voice / instructions / turn_detection
    # at the session's top level rather than nested under audio.
    payload = {
        "type": "session.update",
        "session": {
            "voice": GROK_VOICE,
            "instructions": AGENT_PROMPT,
            # 800 ms end-of-turn silence, matching the other Riley
            # implementations' endpointing. threshold and prefix_padding_ms
            # stay on xAI's tuned defaults.
            "turn_detection": {
                "type": "server_vad",
                "silence_duration_ms": 800,
            },
            "audio": {
                "input": {
                    "format": {"type": "audio/pcm", "rate": SAMPLE_RATE_HZ},
                    # Grok only emits input transcription events when this
                    # model is set; without it actor speech never shows up
                    # in agent.log.
                    "transcription": {"model": "grok-transcribe"},
                },
                "output": {
                    "format": {"type": "audio/pcm", "rate": SAMPLE_RATE_HZ},
                },
            },
            "tools": TOOLS,
        },
    }
    await gk_ws.send(json.dumps(payload))
    logger.info(
        "[voice] session.update sent: %d tools, instructions=%d chars",
        len(TOOLS), len(AGENT_PROMPT),
    )


async def _pump_actor_to_grok(actor_ws: WebSocket, gk_ws) -> None:
    """Binary PCM16 frames from the actor → Grok input buffer."""
    n_frames = 0
    n_bytes = 0
    try:
        while True:
            frame = await actor_ws.receive_bytes()
            n_frames += 1
            n_bytes += len(frame)
            if n_frames == 1:
                logger.info("[a→gk] first frame received bytes=%d", len(frame))
            elif n_frames % LOG_EVERY_N_FRAMES == 0:
                logger.info("[a→gk] forwarded %d frames (%d bytes)", n_frames, n_bytes)
            await gk_ws.send(json.dumps({
                "type": "input_audio_buffer.append",
                "audio": base64.b64encode(frame).decode(),
            }))
    except WebSocketDisconnect as exc:
        logger.info(
            "[a→gk] actor disconnected after %d frames (%d bytes): code=%s",
            n_frames, n_bytes, getattr(exc, "code", "?"),
        )
    except KeyError as exc:
        # Starlette raises KeyError('bytes') when it received a text frame
        # instead of a binary one — i.e. protocol mismatch on the actor side.
        logger.error(
            "[a→gk] received non-binary frame after %d binary frames — "
            "actor protocol mismatch? (%s)", n_frames, exc,
        )
        raise
    except Exception as exc:
        logger.exception(
            "[a→gk] pump died after %d frames (%d bytes): %s",
            n_frames, n_bytes, exc,
        )
        raise


async def _pump_grok_to_actor(gk_ws, actor_ws: WebSocket, api: BCSAPI) -> None:
    """Grok events → audio bytes back to the actor; dispatch tool calls.

    Every audio delta streams straight to the actor as Grok generates it — no
    buffering — so the actor hears the reply onset immediately.
    """
    pending_tool_response = False
    n_audio_frames = 0
    n_audio_bytes = 0
    n_responses = 0

    try:
        async for raw in gk_ws:
            evt = json.loads(raw)
            etype = evt.get("type", "")

            if etype == "response.output_audio.delta":
                audio = base64.b64decode(evt.get("delta", ""))
                n_audio_frames += 1
                n_audio_bytes += len(audio)
                if n_audio_frames == 1:
                    logger.info("[gk→a] first audio delta bytes=%d", len(audio))
                elif n_audio_frames % LOG_EVERY_N_FRAMES == 0:
                    logger.info(
                        "[gk→a] streamed %d audio deltas (%d bytes) so far this session",
                        n_audio_frames, n_audio_bytes,
                    )
                if audio:
                    await actor_ws.send_bytes(audio)

            elif etype == "response.output_audio_transcript.done":
                text = (evt.get("transcript") or "").strip()
                if text:
                    logger.info("[gk→a] agent_said: %s", text[:200])

            elif etype == "conversation.item.input_audio_transcription.completed":
                # Cumulative transcript of the actor's audio; Grok may emit it
                # more than once per utterance as the transcript refines. If
                # this never fires after the actor speaks, our VAD didn't
                # commit and the actor's silence trailer is missing.
                text = (evt.get("transcript") or "").strip()
                if text:
                    logger.info("[gk→a] actor_said: %s", text[:200])

            elif etype == "input_audio_buffer.speech_started":
                logger.info("[gk→a] vad: speech_started")

            elif etype == "input_audio_buffer.speech_stopped":
                logger.info("[gk→a] vad: speech_stopped")

            elif etype == "input_audio_buffer.committed":
                logger.info("[gk→a] vad: buffer committed → response generation")

            elif etype == "response.created":
                n_responses += 1
                logger.info("[gk→a] response.created #%d", n_responses)

            elif etype == "response.function_call_arguments.done":
                pending_tool_response = True
                name = evt.get("name", "")
                call_id = evt.get("call_id", "")
                args_raw = evt.get("arguments") or "{}"
                logger.info("[gk→a] tool_call: %s args=%s", name, str(args_raw)[:200])
                args: dict = {}
                try:
                    args = json.loads(args_raw) if isinstance(args_raw, str) else dict(args_raw)
                    output = dispatch(api, name, args)
                    logger.info(
                        "[gk→a] tool_result %s: %s",
                        name, json.dumps(output, default=str)[:200],
                    )
                except Exception as exc:  # surface errors to the model
                    logger.exception("[gk→a] tool %s failed", name)
                    output = {"error": str(exc)}

                report_tool_call(name, args, output)

                await gk_ws.send(json.dumps({
                    "type": "conversation.item.create",
                    "item": {
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": json.dumps(output, default=str),
                    },
                }))

            elif etype == "response.done":
                if pending_tool_response:
                    pending_tool_response = False
                    logger.info("[gk→a] response.done after tool — kicking next response.create")
                    # Continue the turn now that tool outputs are in context.
                    await gk_ws.send(json.dumps({"type": "response.create"}))
                else:
                    logger.info(
                        "[gk→a] response.done — session totals %d audio deltas / %d bytes",
                        n_audio_frames, n_audio_bytes,
                    )

            elif etype == "error":
                logger.error("[gk→a] Grok realtime error: %s", evt.get("error"))
                raise RuntimeError(f"Grok realtime error: {evt.get('error')}")
    except websockets.ConnectionClosed as exc:
        logger.info("[gk→a] Grok WS closed code=%s reason=%s", exc.code, exc.reason)
    except Exception as exc:
        logger.exception(
            "[gk→a] pump died after %d audio deltas (%d bytes), %d responses: %s",
            n_audio_frames, n_audio_bytes, n_responses, exc,
        )
        raise

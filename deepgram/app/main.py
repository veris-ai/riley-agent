"""riley-deepgram — Riley card-support voice agent over Deepgram's Voice Agent API.

Listens on /voice for a single bidirectional PCM16 stream from the Veris actor.
For each connection it opens one Voice Agent WebSocket session
(wss://agent.deepgram.com/v1/agent/converse), sends the Settings message
carrying the Riley prompt, tools, STT/LLM/TTS providers and greeting, and
bridges audio + function calls. The agent speaks first via `agent.greeting`.
Function calls are dispatched against `BCSAPI`, which talks to postgres.

Audio is raw binary in both directions — the Voice Agent API takes PCM16 on the
socket with no JSON envelope and no base64, and emits its TTS the same way, so
configuring both legs at 24 kHz makes this a byte passthrough with no
resampling and no re-framing.

Logging is intentionally chatty so it's obvious from agent.log alone whether
audio is flowing, how the session is responding, and where things stall.
"""

from __future__ import annotations

import asyncio
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


DEEPGRAM_AGENT_URL = "wss://agent.deepgram.com/v1/agent/converse"
SAMPLE_RATE_HZ = 24000

# Speech-to-text. nova-3 on the v1 provider: the same model the cascaded Riley
# implementations (pipecat, livekit, vapi) run for STT. Note that v1 exposes no
# endpointing control through Voice Agent Settings — turn-taking is Deepgram's
# built-in, not the 800 ms end-of-turn silence the cascaded agents configure.
DEEPGRAM_LISTEN_MODEL = os.environ.get("DEEPGRAM_LISTEN_MODEL", "nova-3")
# Text-to-speech. Aura-2 is Deepgram's current-generation voice line.
DEEPGRAM_VOICE = os.environ.get("DEEPGRAM_VOICE", "aura-2-thalia-en")
# The LLM runs on Deepgram's managed OpenAI access — `think.provider.type:
# open_ai` needs no key of our own — so gpt-4.1-mini matches the rest of the
# riley-* fleet while DEEPGRAM_API_KEY stays the only credential this agent needs.
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4.1-mini")

GREETING = "Thanks for calling Acme Bank, this is Riley — how can I help?"

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


def _settings() -> dict:
    """The Settings message — the whole session config, sent once on connect."""
    return {
        "type": "Settings",
        "audio": {
            "input": {"encoding": "linear16", "sample_rate": SAMPLE_RATE_HZ},
            # container "none" keeps the downstream bytes raw PCM16, which is
            # exactly what the actor's voice_ws expects; anything else would
            # wrap them in a header the actor would play as noise.
            "output": {
                "encoding": "linear16",
                "sample_rate": SAMPLE_RATE_HZ,
                "container": "none",
            },
        },
        "agent": {
            "listen": {
                "provider": {
                    "type": "deepgram",
                    "model": DEEPGRAM_LISTEN_MODEL,
                }
            },
            "think": {
                "provider": {"type": "open_ai", "model": LLM_MODEL},
                "prompt": AGENT_PROMPT,
                # No `endpoint` on any entry — that is what makes these
                # client-side, so calls come back here as FunctionCallRequest
                # instead of Deepgram POSTing an HTTP endpoint of its own.
                "functions": TOOLS,
            },
            "speak": {"provider": {"type": "deepgram", "model": DEEPGRAM_VOICE}},
            # Spoken at session start — the agent greets first, straight to TTS
            # without a round trip through the LLM.
            "greeting": GREETING,
        },
    }


app = FastAPI(title="riley-deepgram", version="0.1.0")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.websocket("/voice")
async def voice(actor_ws: WebSocket) -> None:
    """One actor connection ↔ one Deepgram Voice Agent session with BCS tools.

    Logs every state transition so it's possible to debug stalls from
    agent.log without correlating with proxy / sandbox logs.
    """
    await actor_ws.accept()
    peer = f"{actor_ws.client.host}:{actor_ws.client.port}" if actor_ws.client else "?"
    logger.info("[voice] actor connected peer=%s", peer)
    api = BCSAPI()
    headers = {"Authorization": f"Token {os.environ['DEEPGRAM_API_KEY']}"}

    t_start = time.monotonic()
    try:
        async with websockets.connect(
            DEEPGRAM_AGENT_URL, additional_headers=headers
        ) as dg_ws:
            logger.info(
                "[voice] Deepgram Voice Agent connected listen=%s think=%s speak=%s rate=%d Hz",
                DEEPGRAM_LISTEN_MODEL, LLM_MODEL, DEEPGRAM_VOICE, SAMPLE_RATE_HZ,
            )
            await dg_ws.send(json.dumps(_settings()))
            logger.info(
                "[voice] Settings sent: %d functions, prompt=%d chars, greeting=%r",
                len(TOOLS), len(AGENT_PROMPT), GREETING,
            )

            # Gate actor audio on SettingsApplied — audio sent before the
            # server has applied Settings is discarded.
            ready = asyncio.Event()

            # Run both pumps. If either raises (or returns), cancel the
            # other so we don't leak a half-dead WS pair.
            t1 = asyncio.create_task(
                _pump_actor_to_dg(actor_ws, dg_ws, ready), name="actor→dg"
            )
            t2 = asyncio.create_task(
                _pump_dg_to_actor(dg_ws, actor_ws, api, ready), name="dg→actor"
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


async def _pump_actor_to_dg(actor_ws: WebSocket, dg_ws, ready: asyncio.Event) -> None:
    """Binary PCM16 frames from the actor → the Voice Agent socket, verbatim.

    Frames that arrive before SettingsApplied are dropped (they're silence —
    the actor only speaks after it hears the greeting).
    """
    n_frames = 0
    n_bytes = 0
    n_dropped = 0
    try:
        while True:
            frame = await actor_ws.receive_bytes()
            if not ready.is_set():
                n_dropped += 1
                continue
            n_frames += 1
            n_bytes += len(frame)
            if n_frames == 1:
                logger.info(
                    "[a→dg] first frame forwarded bytes=%d (%d pre-ready frames dropped)",
                    len(frame), n_dropped,
                )
            elif n_frames % LOG_EVERY_N_FRAMES == 0:
                logger.info("[a→dg] forwarded %d frames (%d bytes)", n_frames, n_bytes)
            await dg_ws.send(frame)
    except WebSocketDisconnect as exc:
        logger.info(
            "[a→dg] actor disconnected after %d frames (%d bytes): code=%s",
            n_frames, n_bytes, getattr(exc, "code", "?"),
        )
    except KeyError as exc:
        # Starlette raises KeyError('bytes') when it received a text frame
        # instead of a binary one — i.e. protocol mismatch on the actor side.
        logger.error(
            "[a→dg] received non-binary frame after %d binary frames — "
            "actor protocol mismatch? (%s)", n_frames, exc,
        )
        raise
    except Exception as exc:
        logger.exception(
            "[a→dg] pump died after %d frames (%d bytes): %s",
            n_frames, n_bytes, exc,
        )
        raise


async def _pump_dg_to_actor(dg_ws, actor_ws: WebSocket, api: BCSAPI, ready: asyncio.Event) -> None:
    """Voice Agent messages → audio bytes back to the actor; run function calls.

    The socket carries both kinds of frame: binary frames are TTS audio and go
    straight through to the actor as they arrive, text frames are the JSON
    event stream.
    """
    n_audio_frames = 0
    n_audio_bytes = 0
    n_turns = 0
    latency: dict = {}  # partial LatencyReport fields for the in-flight turn
    try:
        async for raw in dg_ws:
            if isinstance(raw, bytes):
                n_audio_frames += 1
                n_audio_bytes += len(raw)
                if n_audio_frames == 1:
                    logger.info("[dg→a] first audio chunk bytes=%d", len(raw))
                elif n_audio_frames % LOG_EVERY_N_FRAMES == 0:
                    logger.info(
                        "[dg→a] forwarded %d audio chunks (%d bytes) so far this session",
                        n_audio_frames, n_audio_bytes,
                    )
                await actor_ws.send_bytes(raw)
                continue

            evt = json.loads(raw)
            etype = evt.get("type", "")

            if etype == "Welcome":
                logger.info("[dg→a] Welcome request_id=%s", evt.get("request_id"))

            elif etype == "SettingsApplied":
                ready.set()
                logger.info("[dg→a] SettingsApplied — actor audio now forwarding")

            elif etype == "ConversationText":
                role = evt.get("role", "?")
                text = (evt.get("content") or "").strip()
                logger.info(
                    "[dg→a] %s: %s",
                    "agent_said" if role == "assistant" else "actor_said", text[:200],
                )

            elif etype == "UserStartedSpeaking":
                # Barge-in. Audio is forwarded frame-by-frame with no buffer
                # here, so there is nothing of ours to flush — Deepgram simply
                # stops producing.
                logger.info("[dg→a] UserStartedSpeaking (barge-in)")

            elif etype == "LatencyReport":
                # Deepgram's own timing for the turn. Each report carries a
                # single field, so they accumulate until total_latency arrives
                # and closes the turn out. (This is where the numbers live:
                # AgentStartedSpeaking carries the same breakdown in one
                # message but is only emitted under `experimental: true`, and
                # AgentThinking never fires at all.)
                latency.update({k: v for k, v in evt.items() if k != "type"})
                if "total_latency" in latency:
                    n_turns += 1
                    logger.info(
                        "[dg→a] turn #%d latency total=%.2fs ttt=%.2fs tts=%.2fs",
                        n_turns, latency["total_latency"],
                        latency.get("ttt_text_latency", 0.0),
                        latency.get("tts_latency", 0.0),
                    )
                    latency = {}

            elif etype == "AgentAudioDone":
                logger.info(
                    "[dg→a] AgentAudioDone (%d audio chunks / %d bytes this session)",
                    n_audio_frames, n_audio_bytes,
                )

            elif etype == "FunctionCallRequest":
                await _handle_function_calls(dg_ws, api, evt.get("functions") or [])

            elif etype == "Error":
                logger.error(
                    "[dg→a] Error code=%s description=%s",
                    evt.get("code"), evt.get("description"),
                )
                raise RuntimeError(f"Deepgram Voice Agent error: {evt.get('description')}")

            elif etype == "Warning":
                logger.warning(
                    "[dg→a] Warning code=%s description=%s",
                    evt.get("code"), evt.get("description"),
                )
    except websockets.ConnectionClosed as exc:
        logger.info("[dg→a] Deepgram WS closed code=%s reason=%s", exc.code, exc.reason)
    except Exception as exc:
        logger.exception(
            "[dg→a] pump died after %d audio chunks (%d bytes), %d turns: %s",
            n_audio_frames, n_audio_bytes, n_turns, exc,
        )
        raise


async def _handle_function_calls(dg_ws, api: BCSAPI, functions: list[dict]) -> None:
    """Run each client-side call and send its FunctionCallResponse back.

    One request can carry several calls. Server-side entries (`client_side:
    false`) are informational — Deepgram runs those itself — so they are logged
    and skipped; every function this agent declares is client-side.
    """
    for fn in functions:
        name = fn.get("name", "")
        call_id = fn.get("id", "")
        if not fn.get("client_side", True):
            logger.info("[dg→a] server-side call %s — Deepgram runs it, skipping", name)
            continue
        args = json.loads(fn.get("arguments") or "{}")
        logger.info("[dg→a] tool_call: %s args=%s", name, json.dumps(args)[:200])
        # Off the event loop — synchronous postgres would otherwise freeze both
        # audio pumps for its duration and seed overlap collisions with the actor.
        try:
            output = await asyncio.to_thread(dispatch, api, name, args)
            logger.info(
                "[dg→a] tool_result %s: %s", name, json.dumps(output, default=str)[:200]
            )
        except Exception as exc:  # surface errors to the model
            logger.exception("[dg→a] tool %s failed", name)
            output = {"error": str(exc)}
        report_tool_call(name, args, output)
        await dg_ws.send(json.dumps({
            "type": "FunctionCallResponse",
            "id": call_id,
            "name": name,
            # `content` is a string — the model reads it as the call's result.
            "content": json.dumps(output, default=str),
        }))

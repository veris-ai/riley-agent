"""riley-vapi — Riley card-ops voice agent over Vapi WebSocket transport.

Listens on ``/voice`` for a
single bidirectional PCM16 stream from the Veris actor. For each
connection, the agent creates a new Vapi call (``transport.provider =
vapi.websocket``), opens its own WebSocket to the URL Vapi returns, and
bridges audio in both directions.

Tools fire as HTTP webhooks from Vapi to ``/tool`` on this same server.
Vapi cloud needs a publicly-reachable URL: the Veris platform exposes
this app through its shared webhook gateway (``agent.public_endpoint``
in veris.yaml) and injects ``PUBLIC_BASE_URL`` — a unique
``/hooks/{sim_id}`` URL per simulation, so concurrent sims never share
an endpoint.

Logging is intentionally chatty so it's obvious from agent.log alone
whether audio is flowing, how Vapi is responding, and where things stall.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import suppress
from pathlib import Path

import httpx
import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import JSONResponse
from starlette.websockets import WebSocketDisconnect

from .db import BCSAPI
from .reporting import report_tool_call
from .tools import build_tools, dispatch

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


SAMPLE_RATE_HZ = 24000

VAPI_API_URL = "https://api.vapi.ai/call"

VAPI_MODEL_PROVIDER = os.environ.get("VAPI_MODEL_PROVIDER", "openai")
VAPI_MODEL = os.environ.get("VAPI_MODEL", "gpt-4.1-mini")
VAPI_VOICE_PROVIDER = os.environ.get("VAPI_VOICE_PROVIDER", "11labs")
VAPI_VOICE_ID = os.environ.get("VAPI_VOICE_ID", "EXAVITQu4vr4xnSDxMaL")  # ElevenLabs "Sarah"
# Pin the ElevenLabs TTS model so it matches the other pipelines (apples-to-apples).
# Only meaningful for the 11labs provider; ignored otherwise.
VAPI_VOICE_MODEL = os.environ.get("VAPI_VOICE_MODEL", "eleven_flash_v2")
VAPI_TRANSCRIBER_PROVIDER = os.environ.get("VAPI_TRANSCRIBER_PROVIDER", "deepgram")
VAPI_TRANSCRIBER_MODEL = os.environ.get("VAPI_TRANSCRIBER_MODEL", "nova-2")

# Heartbeat log cadence — at ~50 fps that's one line/sec.
LOG_EVERY_N_FRAMES = 50


def _load_agent_prompt() -> str:
    for candidate in (
        Path("agent_desc.txt"),
        Path(__file__).resolve().parent.parent / "agent_desc.txt",
    ):
        if candidate.is_file():
            return candidate.read_text()
    raise FileNotFoundError("agent_desc.txt not found")


AGENT_PROMPT = _load_agent_prompt()


# Vapi POSTs tool calls to this URL. The Veris platform routes it through
# the shared webhook gateway (agent.public_endpoint in veris.yaml) and
# injects PUBLIC_BASE_URL; a missing or malformed value is a config error,
# so fail at import — there is no tunnel fallback.
_public_base_url = os.environ["PUBLIC_BASE_URL"].strip().rstrip("/")
if not _public_base_url.startswith("https://"):
    raise RuntimeError(
        f"PUBLIC_BASE_URL must be an absolute https:// URL, got {_public_base_url!r}"
    )
TOOL_WEBHOOK_URL = _public_base_url + "/tool"
logger.info("[startup] tool webhook URL=%s", TOOL_WEBHOOK_URL)

app = FastAPI(title="riley-vapi", version="0.1.0")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Tool webhook — Vapi POSTs `tool-calls` here, expects sync JSON response.
# Each call is also reported to the Veris engine via report_tool_call
# (app/reporting.py) so it lands in the graded trace.
# ---------------------------------------------------------------------------

_TOOL_API = BCSAPI()


def _extract_tool_call(call: dict) -> tuple[str, str, dict]:
    """Pull (call_id, name, args) from a Vapi toolCallList entry.

    Vapi's HTTP webhook envelope uses OpenAI-tool-call shape: the call's
    name and arguments live under ``function``, not flat on the call. The
    older docs showed flat ``{id, name, arguments}`` but the live payload
    is ``{id, type:"function", function:{name, arguments}, ...}``. Support
    both so the dispatcher works regardless.
    """
    call_id = call.get("id") or ""
    fn = call.get("function") or {}
    name = fn.get("name") or call.get("name") or ""
    raw_args = fn.get("arguments")
    if raw_args is None:
        raw_args = call.get("arguments")
    if isinstance(raw_args, str):
        args = json.loads(raw_args) if raw_args else {}
    else:
        args = dict(raw_args or {})
    return call_id, name, args


@app.post("/tool")
async def tool_webhook(request: Request) -> JSONResponse:
    body = await request.json()
    msg = body.get("message") or {}
    tool_call_list = msg.get("toolCallList") or []

    results: list[dict] = []
    names: list[str] = []
    for call in tool_call_list:
        try:
            call_id, name, args = _extract_tool_call(call)
        except json.JSONDecodeError as exc:
            cid = call.get("id") or ""
            logger.error("[tool] bad JSON args for %s: %s", cid, exc)
            results.append({"toolCallId": cid, "error": f"bad arguments: {exc}"})
            continue
        names.append(name)

        try:
            output = dispatch(_TOOL_API, name, args)
            logger.info("[tool] %s result: %s", name, json.dumps(output, default=str)[:200])
            results.append({"toolCallId": call_id, "result": json.dumps(output, default=str)})
            report_tool_call(name, args, output)
        except Exception as exc:
            logger.exception("[tool] %s failed", name)
            results.append({"toolCallId": call_id, "error": str(exc)})
            report_tool_call(name, args, {"error": str(exc)})

    logger.info("[tool] received %d tool call(s): %s", len(tool_call_list), names)

    return JSONResponse({"results": results})


# ---------------------------------------------------------------------------
# Voice bridge — actor /voice WS <-> Vapi call WS.
# ---------------------------------------------------------------------------

@app.websocket("/voice")
async def voice(actor_ws: WebSocket) -> None:
    """One actor connection ↔ one Vapi WebSocket call with BCS tools."""
    await actor_ws.accept()
    peer = f"{actor_ws.client.host}:{actor_ws.client.port}" if actor_ws.client else "?"
    logger.info("[voice] actor connected peer=%s", peer)

    t_start = time.monotonic()
    try:
        call = await _create_vapi_call()
    except Exception as exc:
        logger.exception("[voice] failed to set up Vapi call: %s", exc)
        await actor_ws.close(code=1011)
        return

    vapi_ws_url = call["transport"]["websocketCallUrl"]
    call_id = call.get("id", "?")
    logger.info("[voice] Vapi call %s ready url=%s", call_id, vapi_ws_url)

    try:
        async with websockets.connect(vapi_ws_url, max_size=None) as vapi_ws:
            logger.info("[voice] Vapi WS connected (call %s)", call_id)
            t1 = asyncio.create_task(_pump_actor_to_vapi(actor_ws, vapi_ws), name="actor→vapi")
            t2 = asyncio.create_task(_pump_vapi_to_actor(vapi_ws, actor_ws), name="vapi→actor")
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
        logger.info("[voice] handler exit call=%s duration=%.1fs", call_id, elapsed)


async def _create_vapi_call() -> dict:
    """Create a Vapi call with an inline assistant + WebSocket transport."""
    api_key = os.environ["VAPI_API_KEY"]
    payload = {
        "transport": {
            "provider": "vapi.websocket",
            "audioFormat": {
                "format": "pcm_s16le",
                "container": "raw",
                "sampleRate": SAMPLE_RATE_HZ,
            },
        },
        "assistant": {
            "firstMessage": "Thanks for calling Acme Bank, this is Riley — how can I help?",
            "firstMessageMode": "assistant-speaks-first",
            "model": {
                "provider": VAPI_MODEL_PROVIDER,
                "model": VAPI_MODEL,
                # Vapi's default of 250 truncates longer tool-call results and
                # model turns.
                "maxTokens": 500,
                "messages": [{"role": "system", "content": AGENT_PROMPT}],
                "tools": build_tools(TOOL_WEBHOOK_URL),
            },
            "voice": {"provider": VAPI_VOICE_PROVIDER, "voiceId": VAPI_VOICE_ID, "model": VAPI_VOICE_MODEL},
            "transcriber": {
                "provider": VAPI_TRANSCRIBER_PROVIDER,
                "model": VAPI_TRANSCRIBER_MODEL,
                "language": "en",
            },
            # Benchmark turn-taking standard: ~0.8 s end-of-turn silence.
            # Per Vapi's config review: transcriptionEndpointingPlan is what
            # controls turn commit, and its countdown starts once the
            # transcript is received (~250 ms after speech stops), so 0.6 s on
            # punctuated turns keeps total endpointing at or above ~0.8 s.
            # waitSeconds is NOT the limiter — it is only the minimum time
            # before reply audio may flow, measured from the caller's last
            # detected speech; transcript receipt + endpointing + LLM + TTS
            # normally exceeds it. This path ONLY holds with a transcriber
            # that has NO built-in end-of-turn model (nova-2, the default
            # here) — an EOT-capable transcriber would make Vapi ignore
            # transcriptionEndpointingPlan.
            "startSpeakingPlan": {
                "waitSeconds": 0.8,
                "transcriptionEndpointingPlan": {
                    "onPunctuationSeconds": 0.6,
                    "onNoPunctuationSeconds": 1.2,
                    "onNumberSeconds": 0.7,
                },
            },
            # Barge-in needs 2 confidently transcribed words, except the
            # listed phrases, which interrupt immediately.
            "stopSpeakingPlan": {
                "numWords": 2,
                "voiceSeconds": 0.2,
                "backoffSeconds": 1,
                "interruptionPhrases": ["stop", "hold on"],
            },
            # Vapi denoises by default; off here for parity with the other
            # Riley transports — background noise is not part of the benchmark.
            "backgroundSpeechDenoisingPlan": {
                "smartDenoisingPlan": {"enabled": False},
                "fourierDenoisingPlan": {"enabled": False},
            },
            "silenceTimeoutSeconds": 60,
            "maxDurationSeconds": 1800,
            "clientMessages": [
                "transcript",
                "speech-update",
                "status-update",
                "conversation-update",
                "tool-calls",
                "tool-calls-result",
            ],
        },
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            VAPI_API_URL,
            json=payload,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
    if r.status_code >= 400:
        logger.error("[voice] Vapi /call failed status=%d body=%s", r.status_code, r.text[:1000])
        r.raise_for_status()
    return r.json()


async def _pump_actor_to_vapi(actor_ws: WebSocket, vapi_ws) -> None:
    """Binary PCM16 frames from the actor → Vapi WebSocket."""
    n_frames = 0
    n_bytes = 0
    try:
        while True:
            frame = await actor_ws.receive_bytes()
            n_frames += 1
            n_bytes += len(frame)
            if n_frames == 1:
                logger.info("[a→v] first frame received bytes=%d", len(frame))
            elif n_frames % LOG_EVERY_N_FRAMES == 0:
                logger.info("[a→v] forwarded %d frames (%d bytes)", n_frames, n_bytes)
            await vapi_ws.send(frame)
    except WebSocketDisconnect as exc:
        logger.info(
            "[a→v] actor disconnected after %d frames (%d bytes): code=%s",
            n_frames, n_bytes, getattr(exc, "code", "?"),
        )
    except KeyError as exc:
        logger.error(
            "[a→v] received non-binary frame after %d binary frames — "
            "actor protocol mismatch? (%s)", n_frames, exc,
        )
        raise
    except Exception as exc:
        logger.exception("[a→v] pump died after %d frames (%d bytes): %s", n_frames, n_bytes, exc)
        raise


async def _pump_vapi_to_actor(vapi_ws, actor_ws: WebSocket) -> None:
    """Vapi audio + control messages → audio bytes back to the actor.

    Vapi sends two kinds of frames over the WebSocket:
      - Binary: PCM16 LE TTS output at the configured sample rate.
      - Text (JSON): control/event messages — transcript, speech-update,
        status-update, conversation-update, tool-calls (informational —
        the actual tool dispatch happens via the HTTP /tool webhook).
    """
    n_audio_frames = 0
    n_audio_bytes = 0

    try:
        async for raw in vapi_ws:
            if isinstance(raw, (bytes, bytearray)):
                n_audio_frames += 1
                n_audio_bytes += len(raw)
                if n_audio_frames == 1:
                    logger.info("[v→a] first audio frame bytes=%d", len(raw))
                elif n_audio_frames % LOG_EVERY_N_FRAMES == 0:
                    logger.info("[v→a] forwarded %d audio frames (%d bytes)", n_audio_frames, n_audio_bytes)
                await actor_ws.send_bytes(bytes(raw))
                continue

            # JSON control / event message.
            try:
                evt = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("[v→a] non-JSON text frame: %.200s", raw)
                continue

            msg = evt.get("message") if isinstance(evt, dict) else None
            if msg is None and isinstance(evt, dict):
                msg = evt  # some Vapi messages omit the outer envelope
            mtype = (msg or {}).get("type", "?")

            if mtype == "transcript":
                role = (msg or {}).get("role", "?")
                status = (msg or {}).get("transcriptType") or (msg or {}).get("status", "?")
                text = (msg or {}).get("transcript") or ""
                if status == "final":
                    logger.info("[v→a] transcript %s (final): %s", role, text[:200])

            elif mtype == "speech-update":
                role = (msg or {}).get("role", "?")
                status = (msg or {}).get("status", "?")
                logger.info("[v→a] speech-update role=%s status=%s", role, status)

            elif mtype == "status-update":
                status = (msg or {}).get("status", "?")
                logger.info("[v→a] status-update: %s", status)
                if status in ("ended", "completed", "failed"):
                    logger.info("[v→a] Vapi call ended — closing")
                    return

            elif mtype == "tool-calls":
                names = [
                    (t.get("function") or {}).get("name") or t.get("name")
                    for t in (msg.get("toolCallList") or [])
                ]
                logger.info("[v→a] tool-calls notification: %s (dispatched via /tool webhook)", names)

            elif mtype == "tool-calls-result":
                logger.info("[v→a] tool-calls-result: %.200s", json.dumps(msg, default=str))

            elif mtype == "conversation-update":
                # Conversation-update is verbose; just note it fired.
                logger.debug("[v→a] conversation-update")

            elif mtype == "error":
                logger.error("[v→a] Vapi error: %s", json.dumps(msg, default=str))

            else:
                logger.info("[v→a] event %s: %.200s", mtype, json.dumps(msg, default=str))

    except websockets.ConnectionClosed as exc:
        logger.info("[v→a] Vapi WS closed code=%s reason=%s", exc.code, exc.reason)
    except Exception as exc:
        logger.exception(
            "[v→a] pump died after %d audio frames (%d bytes): %s",
            n_audio_frames, n_audio_bytes, exc,
        )
        raise

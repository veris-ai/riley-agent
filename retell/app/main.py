"""riley-retell — Riley, the BCS card-ops voice agent, over a Retell AI web call.

Listens on ``/voice`` for a single
bidirectional PCM16 stream from the Veris actor. For each connection,
the agent creates a Retell web call and joins its LiveKit room from this
process — the same room the browser `retell-client-js-sdk` would join
(the SDK is a thin wrapper that connects ``livekit-client`` to
``wss://retell-ai-4ihahnq7.livekit.cloud`` with the web call's
``access_token``) — and bridges audio in both directions.

Tools fire as HTTP webhooks ("custom functions") from Retell cloud to
``/tool`` on this same server. Retell cloud needs a publicly-reachable
URL: the Veris platform exposes this app through its shared webhook
gateway (``agent.public_endpoint`` in veris.yaml) and injects
``PUBLIC_BASE_URL`` — a unique ``/hooks/{sim_id}`` URL per simulation,
so concurrent sims never share an endpoint.

Unlike Vapi, Retell has no per-call inline assistant config: the prompt
and tools live on a persistent Retell LLM object and the voice config on
a Retell agent. On the first ``/voice`` connection per boot the app
provisions both with the boot's fresh tool webhook URL (or, when
``RETELL_LLM_ID``/``RETELL_AGENT_ID`` are set, updates the existing pair
in place).

Logging is intentionally chatty so it's obvious from agent.log alone
whether audio is flowing, how Retell is responding, and where things stall.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request, Response, WebSocket
from fastapi.responses import JSONResponse
from livekit import rtc
from retell import AsyncRetell
from retell.lib.webhook_auth import verify as verify_signature
from starlette.websockets import WebSocketDisconnect

from .db import BCSAPI
from .reporting import report_tool_call
from .tools import build_tools, dispatch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


SAMPLE_RATE_HZ = 24000

# Hardcoded in retell-client-js-sdk (src/index.ts) — web calls are rooms on
# Retell's LiveKit Cloud, and the create-web-call access_token is the room JWT.
RETELL_LIVEKIT_URL = "wss://retell-ai-4ihahnq7.livekit.cloud"

# gpt-4o-mini (the family default) is not in Retell's model enum; gpt-4.1-mini
# is the closest available analogue.
RETELL_MODEL = os.environ.get("RETELL_MODEL", "gpt-4.1-mini")
RETELL_VOICE_ID = os.environ.get("RETELL_VOICE_ID", "11labs-Adrian")
# Pin the ElevenLabs TTS model to match the other pipelines (apples-to-apples).
RETELL_VOICE_MODEL = os.environ.get("RETELL_VOICE_MODEL", "eleven_flash_v2")

# Retell signs /tool webhooks (X-Retell-Signature) with the account's webhook
# signing key. When set, requests with a bad signature are rejected.
RETELL_WEBHOOK_KEY = os.environ.get("RETELL_WEBHOOK_KEY", "")

GREETING = "Thanks for calling Acme Bank, this is Riley — how can I help?"

# Heartbeat log cadence — at ~100 fps (10 ms LiveKit frames) that's one line/sec.
LOG_EVERY_N_FRAMES = 100


def _load_agent_prompt() -> str:
    for candidate in (
        Path("agent_desc.txt"),
        Path(__file__).resolve().parent.parent / "agent_desc.txt",
    ):
        if candidate.is_file():
            return candidate.read_text()
    raise FileNotFoundError("agent_desc.txt not found")


AGENT_PROMPT = _load_agent_prompt()


# Retell POSTs custom-function calls to this URL. The Veris platform routes it
# through the shared webhook gateway (agent.public_endpoint in veris.yaml) and
# injects PUBLIC_BASE_URL; a missing or malformed value is a config error,
# so fail at import — there is no tunnel fallback.
_public_base_url = os.environ["PUBLIC_BASE_URL"].strip().rstrip("/")
if not _public_base_url.startswith("https://"):
    raise RuntimeError(
        f"PUBLIC_BASE_URL must be an absolute https:// URL, got {_public_base_url!r}"
    )
TOOL_WEBHOOK_URL = _public_base_url + "/tool"
logger.info("[startup] tool webhook URL=%s", TOOL_WEBHOOK_URL)


# ---------------------------------------------------------------------------
# Retell provisioning — one LLM + agent per boot (tool URLs must match the
# boot's per-sim PUBLIC_BASE_URL), reused across /voice connections within
# the process.
# ---------------------------------------------------------------------------

_RETELL: Optional[AsyncRetell] = None
_AGENT_ID: str = ""
# IDs created (not reused) this boot — best-effort deleted on shutdown so sim
# runs don't accumulate orphaned pairs in the Retell account.
_CREATED_LLM_ID: str = ""
_CREATED_AGENT_ID: str = ""
_PROVISION_LOCK = asyncio.Lock()


def _retell() -> AsyncRetell:
    global _RETELL
    if _RETELL is None:
        _RETELL = AsyncRetell(api_key=os.environ["RETELL_API_KEY"])
    return _RETELL


async def _ensure_retell_agent() -> str:
    """Provision (or refresh) the Retell LLM + agent pair, once per boot.

    The tool webhook URL is baked into the Retell LLM's custom functions,
    and PUBLIC_BASE_URL is unique per simulation — so a pinned
    RETELL_LLM_ID / RETELL_AGENT_ID pair is updated in place rather than
    trusted as-is.
    """
    global _AGENT_ID, _CREATED_LLM_ID, _CREATED_AGENT_ID
    if _AGENT_ID:
        return _AGENT_ID
    async with _PROVISION_LOCK:
        if _AGENT_ID:
            return _AGENT_ID
        client = _retell()
        tools = build_tools(TOOL_WEBHOOK_URL)
        llm_id = os.environ.get("RETELL_LLM_ID", "")
        agent_id = os.environ.get("RETELL_AGENT_ID", "")
        if llm_id and agent_id:
            await client.llm.update(llm_id, general_tools=tools)
            # Re-bind so the agent's draft tracks the freshly-updated LLM.
            await client.agent.update(
                agent_id, response_engine={"type": "retell-llm", "llm_id": llm_id}
            )
            logger.info(
                "[startup] reusing Retell agent %s (llm %s) — tool URLs updated to %s",
                agent_id, llm_id, TOOL_WEBHOOK_URL,
            )
        else:
            llm = await client.llm.create(
                model=RETELL_MODEL,
                general_prompt=AGENT_PROMPT,
                begin_message=GREETING,
                start_speaker="agent",
                general_tools=tools,
            )
            agent = await client.agent.create(
                response_engine={"type": "retell-llm", "llm_id": llm.llm_id},
                voice_id=RETELL_VOICE_ID,
                voice_model=RETELL_VOICE_MODEL,
                agent_name="Riley (Retell)",
                max_call_duration_ms=1_800_000,
                end_call_after_silence_ms=60_000,
            )
            agent_id = agent.agent_id
            _CREATED_LLM_ID = llm.llm_id
            _CREATED_AGENT_ID = agent_id
            logger.info(
                "[startup] created Retell agent %s + llm %s — set RETELL_AGENT_ID and "
                "RETELL_LLM_ID in .env to reuse them (local dev only)",
                agent_id, llm.llm_id,
            )
        _AGENT_ID = agent_id
    return _AGENT_ID


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Retell provisioning is deferred to the first /voice connection (see
    # _ensure_retell_agent): it creates real cloud resources, which scenario
    # generation — where the agent is only introspected — should not do.
    logger.info("[startup] agent ready; Retell agent provisions lazily on first /voice")
    try:
        yield
    finally:
        if _CREATED_AGENT_ID:
            logger.info(
                "[shutdown] deleting boot-created Retell agent %s + llm %s",
                _CREATED_AGENT_ID, _CREATED_LLM_ID,
            )
            with suppress(Exception):
                await _retell().agent.delete(_CREATED_AGENT_ID)
                await _retell().llm.delete(_CREATED_LLM_ID)


app = FastAPI(title="riley-retell", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Tool webhook — Retell POSTs custom-function calls here, expects sync JSON.
# Each call is reported to the Veris engine via report_tool_call (see
# app/reporting.py) so it lands in the graded trace.
# ---------------------------------------------------------------------------

_TOOL_API = BCSAPI()


@app.post("/tool")
async def tool_webhook(request: Request) -> Response:
    raw = await request.body()

    if RETELL_WEBHOOK_KEY:
        signature = request.headers.get("X-Retell-Signature", "")
        if not verify_signature(raw.decode(), RETELL_WEBHOOK_KEY, signature):
            logger.warning("[tool] rejected request with bad X-Retell-Signature")
            return JSONResponse({"error": "invalid signature"}, status_code=401)

    body = json.loads(raw)
    name = body["name"]
    args = body.get("args") or {}

    # Business-rule failures (e.g. "cannot replace a cancelled card") come back
    # as 2xx with an error payload: Retell retries non-2xx responses up to 2
    # times, and retrying a side-effecting tool like request_card_replacement
    # must not happen. The LLM voices the error content instead.
    try:
        output = dispatch(_TOOL_API, name, args)
        logger.info("[tool] %s result: %s", name, json.dumps(output, default=str)[:200])
        report_tool_call(name, args, output)
    except Exception as exc:
        logger.exception("[tool] %s failed", name)
        report_tool_call(name, args, {"error": str(exc)})
        output = {"error": str(exc)}

    return Response(content=json.dumps(output, default=str), media_type="application/json")


# ---------------------------------------------------------------------------
# Voice bridge — actor /voice WS <-> Retell web call (LiveKit room).
# ---------------------------------------------------------------------------

@app.websocket("/voice")
async def voice(actor_ws: WebSocket) -> None:
    """One actor connection ↔ one Retell web call with BCS tools."""
    await actor_ws.accept()
    peer = f"{actor_ws.client.host}:{actor_ws.client.port}" if actor_ws.client else "?"
    logger.info("[voice] actor connected peer=%s", peer)

    t_start = time.monotonic()
    try:
        agent_id = await _ensure_retell_agent()
        call = await _retell().call.create_web_call(agent_id=agent_id)
    except Exception as exc:
        logger.exception("[voice] failed to set up Retell call: %s", exc)
        await actor_ws.close(code=1011)
        return

    call_id = call.call_id
    logger.info("[voice] Retell web call %s ready (agent %s)", call_id, agent_id)

    room = rtc.Room()
    loop = asyncio.get_running_loop()
    agent_track: asyncio.Future[rtc.RemoteTrack] = loop.create_future()
    room_closed = asyncio.Event()
    last_logged_utterance = ""

    @room.on("track_subscribed")
    def on_track_subscribed(
        track: rtc.Track, pub: rtc.RemoteTrackPublication, participant: rtc.RemoteParticipant
    ) -> None:
        logger.info(
            "[voice] track_subscribed name=%r kind=%s from=%r",
            pub.name, track.kind, participant.identity,
        )
        if pub.name == "agent_audio" and not agent_track.done():
            agent_track.set_result(track)

    @room.on("data_received")
    def on_data_received(packet: rtc.DataPacket) -> None:
        # Retell's server participant pushes JSON events over the data channel:
        # update (running transcript), agent_start_talking, agent_stop_talking.
        nonlocal last_logged_utterance
        try:
            evt = json.loads(bytes(packet.data))
        except json.JSONDecodeError:
            logger.warning("[r→a] non-JSON data packet: %.200s", bytes(packet.data))
            return
        etype = evt.get("event_type", "?")
        if etype == "update":
            transcript = evt.get("transcript") or []
            if transcript:
                last = transcript[-1]
                utterance = f"{last.get('role', '?')}: {last.get('content', '')}"
                if utterance != last_logged_utterance:
                    logger.info("[r→a] transcript %s", utterance[:200])
                    last_logged_utterance = utterance
        else:
            logger.info("[r→a] event %s", etype)

    @room.on("disconnected")
    def on_disconnected(*args) -> None:
        logger.info("[voice] LiveKit room disconnected args=%s", args)
        room_closed.set()

    try:
        await room.connect(RETELL_LIVEKIT_URL, call.access_token)
        logger.info(
            "[voice] LiveKit room connected as %r (call %s)",
            room.local_participant.identity, call_id,
        )

        source = rtc.AudioSource(SAMPLE_RATE_HZ, 1)
        mic = rtc.LocalAudioTrack.create_audio_track("mic", source)
        await room.local_participant.publish_track(
            mic, rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE)
        )
        logger.info("[voice] mic track published")

        t1 = asyncio.create_task(_pump_actor_to_retell(actor_ws, source), name="actor→retell")
        t2 = asyncio.create_task(_pump_retell_to_actor(agent_track, actor_ws), name="retell→actor")
        t3 = asyncio.create_task(room_closed.wait(), name="room-closed")
        done, pending = await asyncio.wait(
            {t1, t2, t3}, return_when=asyncio.FIRST_COMPLETED,
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
        if not agent_track.done():
            agent_track.cancel()
        with suppress(Exception):
            await room.disconnect()
        elapsed = time.monotonic() - t_start
        logger.info("[voice] handler exit call=%s duration=%.1fs", call_id, elapsed)


async def _pump_actor_to_retell(actor_ws: WebSocket, source: rtc.AudioSource) -> None:
    """Binary PCM16 frames from the actor → the published LiveKit mic track."""
    n_frames = 0
    n_bytes = 0
    try:
        while True:
            frame = await actor_ws.receive_bytes()
            n_frames += 1
            n_bytes += len(frame)
            if n_frames == 1:
                logger.info("[a→r] first frame received bytes=%d", len(frame))
            elif n_frames % LOG_EVERY_N_FRAMES == 0:
                logger.info("[a→r] forwarded %d frames (%d bytes)", n_frames, n_bytes)
            await source.capture_frame(
                rtc.AudioFrame(
                    data=frame,
                    sample_rate=SAMPLE_RATE_HZ,
                    num_channels=1,
                    samples_per_channel=len(frame) // 2,
                )
            )
    except WebSocketDisconnect as exc:
        logger.info(
            "[a→r] actor disconnected after %d frames (%d bytes): code=%s",
            n_frames, n_bytes, getattr(exc, "code", "?"),
        )
    except RuntimeError as exc:
        # starlette raises RuntimeError instead of WebSocketDisconnect when the
        # peer send-pump already observed the close — same normal hangup.
        logger.info(
            "[a→r] actor WS already closed after %d frames (%d bytes): %s",
            n_frames, n_bytes, exc,
        )
    except KeyError as exc:
        logger.error(
            "[a→r] received non-binary frame after %d binary frames — "
            "actor protocol mismatch? (%s)", n_frames, exc,
        )
        raise
    except Exception as exc:
        logger.exception("[a→r] pump died after %d frames (%d bytes): %s", n_frames, n_bytes, exc)
        raise


async def _pump_retell_to_actor(
    agent_track: "asyncio.Future[rtc.RemoteTrack]", actor_ws: WebSocket
) -> None:
    """Riley's audio track → PCM16 bytes back to the actor.

    The agent_audio track emits frames continuously (comfort noise between
    turns, like a real phone line), so the actor's server_vad sees an
    unbroken stream and commits turns on its own — no end-of-turn silence
    trailer is needed, same as the LiveKit variants and unlike Vapi/ElevenLabs
    whose platforms only emit audio while TTS is active.
    """
    track = await agent_track
    stream = rtc.AudioStream(track, sample_rate=SAMPLE_RATE_HZ, num_channels=1)
    n_frames = 0
    n_bytes = 0
    try:
        async for ev in stream:
            data = bytes(ev.frame.data)
            n_frames += 1
            n_bytes += len(data)
            if n_frames == 1:
                logger.info("[r→a] first audio frame bytes=%d", len(data))
            elif n_frames % LOG_EVERY_N_FRAMES == 0:
                logger.info("[r→a] forwarded %d audio frames (%d bytes)", n_frames, n_bytes)
            try:
                await actor_ws.send_bytes(data)
            except Exception as exc:
                # The agent track streams continuously, so a normal actor hangup
                # almost always lands mid-send — that's the end of the call, not
                # a pump failure.
                logger.info(
                    "[r→a] actor WS closed mid-stream after %d frames (%d bytes) — ending call (%s)",
                    n_frames, n_bytes, exc,
                )
                return
    except Exception as exc:
        logger.exception(
            "[r→a] pump died after %d audio frames (%d bytes): %s",
            n_frames, n_bytes, exc,
        )
        raise

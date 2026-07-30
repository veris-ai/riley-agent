"""riley-assemblyai — Riley card-support voice agent over AssemblyAI's Voice Agent API.

On startup it creates one AssemblyAI *stored agent* (POST /v1/agents) carrying
the Riley prompt (loaded verbatim from agent_desc.txt), tools, voice, turn
detection, and a bring-your-own LLM — OpenAI's gpt-4.1-mini, matching the other
riley-* voice agents (BYO LLM only takes effect on a stored agent; AssemblyAI
rejects an inline `llm` block on session.update). The agent is deleted on
shutdown.

Listens on /voice for a single bidirectional PCM16 stream from the Veris actor.
For each connection it opens a Voice Agent session (speech-to-speech over one
WebSocket), attaches it to the stored agent by id, and bridges audio + tool
calls. The agent speaks first via the agent `greeting`. Tool calls are
dispatched against `BCSAPI`, which talks to postgres.

The Voice Agent API natively speaks PCM16 mono at 24 kHz — the same wire
format as the actor's voice_ws — so audio passes through with no resampling,
only base64 framing on the AssemblyAI side.

Logging is intentionally chatty so it's obvious from agent.log alone whether
audio is flowing, how the session is responding, and where things stall.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import random
import time
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import httpx
import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import Response, StreamingResponse
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


ASSEMBLYAI_AGENT_URL = "wss://agents.assemblyai.com/v1/ws"
ASSEMBLYAI_AGENTS_REST = "https://agents.assemblyai.com/v1/agents"
AAI_HEADERS = {"Authorization": os.environ["ASSEMBLYAI_API_KEY"]}
ASSEMBLYAI_VOICE = os.environ.get("ASSEMBLYAI_VOICE", "ivy")
SAMPLE_RATE_HZ = 24000

# BYO LLM. AssemblyAI rejects an inline `llm` block on session.update
# ("invalid_value: ... define it on a stored agent via POST /v1/agents"), so the
# model is set on a stored agent at create time (see _agent_definition). This
# agent runs gpt-4.1-mini like the other riley-* voice agents — AssemblyAI's own
# LLM Gateway carries gpt-4.1 but not the mini variant, so the OpenAI endpoint
# it is. The stored agent does NOT point at OpenAI directly, though: it points
# at this app's /llm proxy (exposed via the Veris webhook gateway), which
# forces parallel_tool_calls=false on every request. A reply containing a
# parallel tool-call batch wedges the Voice Agent session — the next generation
# never completes and the server streams realtime silence until the call times
# out — even when tool.results are drained per the documented sequence
# (observed consistently in simulation runs). Single calls per
# reply work, so single calls it is.
OPENAI_BASE_URL = "https://api.openai.com/v1"
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4.1-mini")

# AssemblyAI's servers POST BYO-LLM chat completions to this app's /llm proxy.
# The Veris platform routes it through the shared webhook gateway
# (agent.public_endpoint in veris.yaml) and injects PUBLIC_BASE_URL; a missing
# or malformed value is a config error, so fail at import — there is no tunnel
# fallback.
PUBLIC_BASE_URL = os.environ["PUBLIC_BASE_URL"].strip().rstrip("/")
if not PUBLIC_BASE_URL.startswith("https://"):
    raise RuntimeError(
        f"PUBLIC_BASE_URL must be an absolute https:// URL, got {PUBLIC_BASE_URL!r}"
    )

GREETING = "Thanks for calling Acme Bank, this is Riley — how can I help?"

# How often to emit periodic frame-count log lines from the two pumps. At
# 50 fps (20 ms/frame) this is roughly one heartbeat per second.
LOG_EVERY_N_FRAMES = 50


async def _connect_aai(url: str):
    """Connect to the Voice Agent API, retrying with jittered backoff.

    AssemblyAI caps concurrent sessions per account; past the cap the
    handshake hangs until the client's open timeout instead of failing
    fast. Under a 25-sim burst that killed every over-cap call: all sims
    retried on the actor's fixed schedule, so the collisions repeated
    (observed: 12/25 sims failed with 3 synchronized timeouts
    each). Sessions free up as earlier calls end, so spread retries over
    ~2 minutes with jitter before giving up.
    """
    delays = [0, 2, 4, 8, 16, 32, 64]
    for attempt, delay in enumerate(delays, start=1):
        if delay:
            await asyncio.sleep(delay + random.uniform(0, delay / 2))
        try:
            ws = await websockets.connect(url)
        except (OSError, TimeoutError, websockets.exceptions.InvalidHandshake) as exc:
            logger.warning(
                "[voice] AAI connect attempt %d/%d failed: %s",
                attempt, len(delays), exc,
            )
        else:
            if attempt > 1:
                logger.info("[voice] AAI connected on attempt %d", attempt)
            return ws
    raise RuntimeError(f"could not connect to AssemblyAI after {len(delays)} attempts")


def _load_agent_prompt() -> str:
    # Resolve from CWD first (matches the `/agent` working dir the sandbox
    # entry_point runs in), then fall back to a path next to this package.
    for candidate in (
        Path("agent_desc.txt"),
        Path(__file__).resolve().parent.parent / "agent_desc.txt",
    ):
        if candidate.is_file():
            return candidate.read_text()
    raise FileNotFoundError("agent_desc.txt not found")


AGENT_PROMPT = _load_agent_prompt()

# Benchmark turn-taking standard: 800 ms end-of-turn silence, applied via
# AssemblyAI's native min_silence (the VAD/endpointer model itself is
# AssemblyAI's own and can't be swapped — only the threshold is matched across
# frameworks). max_silence is the hard cap that forces a turn end.
TURN_DETECTION = {
    "vad_threshold": 0.5,
    "min_silence": 800,
    "max_silence": 4000,
    "interrupt_response": True,
}


def _agent_definition() -> dict:
    """Full stored-agent config POSTed to /v1/agents.

    Everything that used to ride on the first session.update lives here, because
    BYO LLM only takes effect on a stored agent — the WS just attaches by id.
    """
    return {
        "name": "riley-assemblyai",
        "system_prompt": AGENT_PROMPT,
        # Spoken by the agent at session start — the agent greets first. Goes
        # straight to TTS, not the LLM.
        "greeting": GREETING,
        "voice": {"voice_id": ASSEMBLYAI_VOICE},
        "tools": TOOLS,
        "input": {"turn_detection": TURN_DETECTION},
        # BYO LLM: a list of OpenAI-schema endpoints (one here). AssemblyAI runs
        # the conversation against this instead of its built-in model. The llm
        # entry schema is a strict whitelist (base_url/model/api_key) — extra
        # generation params like parallel_tool_calls are silently dropped, so
        # the endpoint is this app's /llm proxy, which injects the param.
        "llm": [
            {
                "base_url": f"{PUBLIC_BASE_URL}/llm",
                "model": LLM_MODEL,
                "api_key": os.environ["OPENAI_API_KEY"],
            }
        ],
    }


async def _create_stored_agent() -> str:
    """Create the BYO-LLM stored agent, confirm the schema stuck, return its id."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            ASSEMBLYAI_AGENTS_REST, headers=AAI_HEADERS, json=_agent_definition()
        )
        if resp.status_code >= 400:
            # The BYO-LLM create schema isn't in the public docs yet, so the API's
            # response body is the source of truth — but it can reflect the
            # submitted OPENAI_API_KEY, so scrub it before logging.
            safe = resp.text.replace(os.environ["OPENAI_API_KEY"], "***REDACTED***")
            logger.error("[startup] create agent failed %d: %s", resp.status_code, safe)
            resp.raise_for_status()
        agent_id = resp.json()["id"]
        # Round-trip GET. The happy path can't tell whether `llm` and
        # `input.turn_detection` were honored or silently dropped (the create
        # schema is undocumented and `voice` already needed reshaping), so read
        # the stored agent back and warn loudly if either didn't survive.
        got = (
            await client.get(f"{ASSEMBLYAI_AGENTS_REST}/{agent_id}", headers=AAI_HEADERS)
        ).json()
    td = (got.get("input") or {}).get("turn_detection") or {}
    llm_ok = bool(got.get("llm"))
    if td.get("min_silence") != TURN_DETECTION["min_silence"] or not llm_ok:
        logger.warning(
            "[startup] stored-agent schema did NOT round-trip: turn_detection=%s llm_present=%s",
            td, llm_ok,
        )
    logger.info(
        "[startup] stored agent created id=%s llm=%s @ %s/llm voice=%s tools=%d turn_detection=%s",
        agent_id, LLM_MODEL, PUBLIC_BASE_URL, ASSEMBLYAI_VOICE, len(TOOLS), td or None,
    )
    return agent_id


async def _delete_stored_agent(agent_id: str) -> None:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.delete(f"{ASSEMBLYAI_AGENTS_REST}/{agent_id}", headers=AAI_HEADERS)
    logger.info("[shutdown] deleted stored agent id=%s status=%d", agent_id, resp.status_code)


AGENT_ID: str = ""


@asynccontextmanager
async def lifespan(app: FastAPI):
    global AGENT_ID
    AGENT_ID = await _create_stored_agent()
    try:
        yield
    finally:
        # Best-effort cleanup, but make a failed teardown loud rather than
        # silently leaking the stored agent (e.g. delete times out).
        try:
            await _delete_stored_agent(AGENT_ID)
        except Exception as exc:
            logger.error("[shutdown] failed to delete stored agent id=%s: %s", AGENT_ID, exc)


app = FastAPI(title="riley-assemblyai", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/llm/chat/completions")
async def llm_proxy(request: Request):
    """OpenAI chat-completions passthrough that forces parallel_tool_calls=false.

    The stored agent's BYO-LLM base_url points here (via the webhook gateway).
    AssemblyAI authenticates with the api_key registered on the stored agent,
    OpenAI-style — the Authorization header is forwarded verbatim, so this
    proxy holds no credentials and an invalid key fails at OpenAI, loudly.
    """
    body = await request.json()
    if body.get("tools"):
        body["parallel_tool_calls"] = False
    stream = bool(body.get("stream"))
    logger.info(
        "[llm] chat.completions model=%s messages=%d tools=%s stream=%s",
        body.get("model"), len(body.get("messages") or []), bool(body.get("tools")), stream,
    )
    headers = {
        "Authorization": request.headers.get("authorization", ""),
        "Content-Type": "application/json",
    }
    url = f"{OPENAI_BASE_URL}/chat/completions"

    if not stream:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(url, json=body, headers=headers)
        if resp.status_code >= 400:
            logger.error("[llm] upstream %d: %s", resp.status_code, resp.text[:300])
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            media_type=resp.headers.get("content-type", "application/json"),
        )

    client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=120.0))
    upstream = await client.send(
        client.build_request("POST", url, json=body, headers=headers), stream=True
    )
    if upstream.status_code >= 400:
        err = await upstream.aread()
        await upstream.aclose()
        await client.aclose()
        logger.error("[llm] upstream %d: %s", upstream.status_code, err[:300])
        return Response(content=err, status_code=upstream.status_code,
                        media_type=upstream.headers.get("content-type", "application/json"))

    async def pipe():
        try:
            async for chunk in upstream.aiter_bytes():
                yield chunk
        finally:
            await upstream.aclose()
            await client.aclose()

    return StreamingResponse(
        pipe(),
        status_code=upstream.status_code,
        media_type=upstream.headers.get("content-type", "text/event-stream"),
    )


@app.websocket("/voice")
async def voice(actor_ws: WebSocket) -> None:
    """One actor connection ↔ one AssemblyAI Voice Agent session with BCS tools.

    Logs every state transition so it's possible to debug stalls from
    agent.log without correlating with proxy / sandbox logs.
    """
    await actor_ws.accept()
    peer = f"{actor_ws.client.host}:{actor_ws.client.port}" if actor_ws.client else "?"
    logger.info("[voice] actor connected peer=%s", peer)
    api = BCSAPI()
    url = f"{ASSEMBLYAI_AGENT_URL}?token={os.environ['ASSEMBLYAI_API_KEY']}"

    t_start = time.monotonic()
    try:
        async with await _connect_aai(url) as aai_ws:
            logger.info(
                "[voice] AAI Voice Agent connected voice=%s rate=%d Hz",
                ASSEMBLYAI_VOICE, SAMPLE_RATE_HZ,
            )
            await _configure_session(aai_ws)

            # Gate actor audio on session.ready — input.audio sent before the
            # session is ready is rejected by the API.
            ready = asyncio.Event()

            # Run both pumps. If either raises (or returns), cancel the
            # other so we don't leak a half-dead WS pair.
            t1 = asyncio.create_task(
                _pump_actor_to_aai(actor_ws, aai_ws, ready), name="actor→aai"
            )
            t2 = asyncio.create_task(
                _pump_aai_to_actor(aai_ws, actor_ws, api, ready), name="aai→actor"
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

            # Clean teardown — without session.end the server keeps the
            # session resumable (and billable) for 30 seconds.
            with suppress(Exception):
                await aai_ws.send(json.dumps({"type": "session.end"}))
                logger.info("[voice] sent session.end")
    except Exception as exc:
        logger.exception("[voice] handler failed: %s", exc)
    finally:
        elapsed = time.monotonic() - t_start
        logger.info("[voice] handler exit duration=%.1fs", elapsed)


async def _configure_session(aai_ws) -> None:
    # With a stored agent, the first session.update must carry agent_id as its
    # ONLY field — inline config is mutually exclusive with it. Prompt, greeting,
    # voice, tools, BYO LLM, and turn detection all live on the agent itself.
    await aai_ws.send(
        json.dumps({"type": "session.update", "session": {"agent_id": AGENT_ID}})
    )
    logger.info("[voice] session.update sent: agent_id=%s", AGENT_ID)


async def _pump_actor_to_aai(actor_ws: WebSocket, aai_ws, ready: asyncio.Event) -> None:
    """Binary PCM16 frames from the actor → base64 `input.audio` messages.

    Frames that arrive before session.ready are dropped (they're silence —
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
                    "[a→aai] first frame forwarded bytes=%d (%d pre-ready frames dropped)",
                    len(frame), n_dropped,
                )
            elif n_frames % LOG_EVERY_N_FRAMES == 0:
                logger.info("[a→aai] forwarded %d frames (%d bytes)", n_frames, n_bytes)
            await aai_ws.send(json.dumps({
                "type": "input.audio",
                "audio": base64.b64encode(frame).decode(),
            }))
    except WebSocketDisconnect as exc:
        logger.info(
            "[a→aai] actor disconnected after %d frames (%d bytes): code=%s",
            n_frames, n_bytes, getattr(exc, "code", "?"),
        )
    except KeyError as exc:
        # Starlette raises KeyError('bytes') when it received a text frame
        # instead of a binary one — i.e. protocol mismatch on the actor side.
        logger.error(
            "[a→aai] received non-binary frame after %d binary frames — "
            "actor protocol mismatch? (%s)", n_frames, exc,
        )
        raise
    except Exception as exc:
        logger.exception(
            "[a→aai] pump died after %d frames (%d bytes): %s",
            n_frames, n_bytes, exc,
        )
        raise


async def _pump_aai_to_actor(aai_ws, actor_ws: WebSocket, api: BCSAPI, ready: asyncio.Event) -> None:
    """Voice Agent events → audio bytes back to the actor; dispatch tool calls.

    Reply audio passes through as it arrives — `reply.audio` carries base64
    PCM16 at 24 kHz, the exact format the actor expects.

    Tool calls dispatch against postgres on a worker thread (the pumps stay
    live), but their results are NOT shipped mid-reply: the protocol requires
    `tool.result` only after `reply.done` ("accumulate on tool.call and drain
    inside the reply.done handler"). A result sent while the reply is still
    streaming works for a single call but corrupts the session on a parallel
    tool-call batch — the next BYO-LLM generation never completes and the
    server streams realtime silence until the sim times out
    (observed: every sim that hit a 2-tool batch stalled).
    """
    n_audio_frames = 0
    n_audio_bytes = 0
    n_replies = 0
    reply_bytes = 0  # audio forwarded for the in-flight reply
    pending_results: list[tuple[str, object]] = []  # (call_id, output) — drained on reply.done
    in_reply = False  # a reply is streaming (reply.started seen, reply.done not yet)
    try:
        async for raw in aai_ws:
            evt = json.loads(raw)
            etype = evt.get("type", "")

            if etype == "session.ready":
                ready.set()
                logger.info("[aai→a] session.ready id=%s", evt.get("session_id"))

            elif etype == "reply.started":
                n_replies += 1
                reply_bytes = 0
                in_reply = True
                logger.info("[aai→a] reply.started #%d", n_replies)

            elif etype == "reply.audio":
                audio = base64.b64decode(evt["data"])
                n_audio_frames += 1
                n_audio_bytes += len(audio)
                reply_bytes += len(audio)
                if n_audio_frames == 1:
                    logger.info("[aai→a] first audio chunk bytes=%d", len(audio))
                elif n_audio_frames % LOG_EVERY_N_FRAMES == 0:
                    logger.info(
                        "[aai→a] forwarded %d audio chunks (%d bytes) so far this session",
                        n_audio_frames, n_audio_bytes,
                    )
                await actor_ws.send_bytes(audio)

            elif etype == "reply.done":
                status = evt.get("status", "completed")
                in_reply = False
                logger.info(
                    "[aai→a] reply.done status=%s (%d reply bytes, %d total bytes this session)",
                    status, reply_bytes, n_audio_bytes,
                )
                reply_bytes = 0

                # Drain accumulated tool results now that the reply is done —
                # the only point the protocol allows tool.result. `result`
                # must be a JSON string, not an object.
                for call_id, output in pending_results:
                    await aai_ws.send(json.dumps({
                        "type": "tool.result",
                        "call_id": call_id,
                        "result": json.dumps(output, default=str),
                    }))
                if pending_results:
                    logger.info("[aai→a] drained %d tool result(s)", len(pending_results))
                    pending_results = []

            elif etype == "transcript.agent":
                logger.info(
                    "[aai→a] agent_said%s: %s",
                    " (interrupted)" if evt.get("interrupted") else "",
                    (evt.get("text") or "")[:200],
                )

            elif etype == "transcript.user":
                logger.info("[aai→a] actor_said: %s", (evt.get("text") or "")[:200])

            elif etype == "input.speech.started":
                logger.info("[aai→a] vad: speech_started")

            elif etype == "input.speech.stopped":
                logger.info("[aai→a] vad: speech_stopped")

            elif etype == "tool.call":
                name = evt.get("name", "")
                call_id = evt.get("call_id", "")
                args = evt.get("arguments") or {}  # already a parsed object
                logger.info("[aai→a] tool_call: %s args=%s", name, json.dumps(args)[:200])
                # Off the event loop — synchronous postgres would otherwise
                # freeze both audio pumps for its duration and seed overlap
                # collisions with the actor.
                try:
                    output = await asyncio.to_thread(dispatch, api, name, args)
                    logger.info(
                        "[aai→a] tool_result %s: %s",
                        name, json.dumps(output, default=str)[:200],
                    )
                except Exception as exc:  # surface errors to the model
                    logger.exception("[aai→a] tool %s failed", name)
                    output = {"error": str(exc)}
                report_tool_call(name, args, output)
                if in_reply:
                    pending_results.append((call_id, output))
                else:
                    # reply.done is already the latest event — the one state
                    # where tool.result may be sent right away.
                    await aai_ws.send(json.dumps({
                        "type": "tool.result",
                        "call_id": call_id,
                        "result": json.dumps(output, default=str),
                    }))

            elif etype == "session.ended":
                logger.info(
                    "[aai→a] session.ended duration=%ss audio=%ss",
                    evt.get("session_duration_seconds"), evt.get("audio_duration_seconds"),
                )
                return

            elif etype == "session.error":
                logger.error(
                    "[aai→a] session.error code=%s message=%s",
                    evt.get("code"), evt.get("message"),
                )
                raise RuntimeError(f"AssemblyAI session.error: {evt.get('message')}")
    except websockets.ConnectionClosed as exc:
        logger.info("[aai→a] AAI WS closed code=%s reason=%s", exc.code, exc.reason)
    except Exception as exc:
        logger.exception(
            "[aai→a] pump died after %d audio chunks (%d bytes), %d replies: %s",
            n_audio_frames, n_audio_bytes, n_replies, exc,
        )
        raise

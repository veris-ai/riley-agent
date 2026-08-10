"""riley-fluxions-realtime — Riley card-ops voice agent on the Fluxions realtime API.

Listens on /voice for a single bidirectional PCM16 stream from the Veris actor
and bridges it to one Fluxions realtime session. Unlike the `fluxions` row —
which assembles akro batch STT, an OpenAI-compatible LLM and VUI TTS into a
pipeline this process drives — the realtime API carries the whole agent behind
one socket: ASR, routing, TTS, endpointing and barge-in all run server-side.

So this process is a bridge, not a pipeline. It owns exactly three things:

  * sample rate — the Veris actor speaks 24 kHz both ways, Fluxions wants
    16 kHz up and returns 24 kHz down, so caller audio is resampled on the
    way up and agent audio passes through untouched on the way down;
  * playout — agent audio arrives several times faster than realtime, so it
    is queued and written to the actor at 1x, which is what makes the
    server's `audio.flush` on barge-in mean anything (see _playout);
  * tools — `tool.call` is dispatched against `BCSAPI`, which talks to postgres.

Logging is intentionally chatty so it's obvious from agent.log alone whether
audio is flowing, whether the router is actually calling tools, and where a
turn stalls.
"""

from __future__ import annotations

import asyncio
import audioop
import json
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager, suppress
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


REALTIME_URL = os.environ["FLUXIONS_REALTIME_URL"]

# Bring-your-own tool definitions are only registered when the `tools` list in
# session.update also names at least one of the server's built-in tools. A list
# of pure definitions is accepted without error and no tool is ever called, so
# one built-in is named alongside our definitions purely to anchor them. Pick
# the most inert one the deployment offers: it is a capability the agent would
# not otherwise have, and it should never be reachable from a card-support call.
ANCHOR_TOOL = os.environ["FLUXIONS_ANCHOR_TOOL"]

# Veris's voice_ws actor speaks raw PCM16 mono at 24 kHz in both directions.
# Fluxions takes 16 kHz up and returns 24 kHz down, so only the upstream leg
# is ever resampled.
ACTOR_RATE_HZ = 24000
UP_RATE_HZ = 16000

FRAME_MS = 20
DOWN_FRAME_BYTES = ACTOR_RATE_HZ * FRAME_MS // 1000 * 2

# Playback position is reported every 250 ms; the server's VAD and echo
# canceller use it to tell agent audio apart from the caller.
POS_INTERVAL_S = 0.25

# How often to emit periodic frame-count log lines from the audio pump. At
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


# Sent as the session's `soul`, and byte-identical to every other Riley row.
# Sent explicitly on every connect rather than relying on any server-side
# default, so the persona this row is measured on lives in this repo.
AGENT_PROMPT = _load_agent_prompt()


def _session_update() -> str:
    return json.dumps({
        "type": "session.update",
        "tools": [ANCHOR_TOOL] + TOOLS,
        "soul": AGENT_PROMPT,
    })


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open one throwaway session so a cold fleet warms before the first call.

    This doubles as a connectivity check, so a misconfigured endpoint fails at
    startup rather than first surfacing as a dead call.
    """
    t0 = time.monotonic()
    async with websockets.connect(REALTIME_URL, max_size=None) as ws:
        await ws.send(_session_update())
        while True:
            msg = await ws.recv()
            if isinstance(msg, bytes):
                continue
            event = json.loads(msg)
            if event["type"] == "session.created":
                logger.info(
                    "[startup] session.created in %.2fs model=%s voice=%s builtins=%s",
                    time.monotonic() - t0, event.get("model"), event.get("voice"),
                    event.get("tools"),
                )
                break
    yield


app = FastAPI(title="riley-fluxions-realtime", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.websocket("/voice")
async def voice(actor_ws: WebSocket) -> None:
    """One actor connection ↔ one Fluxions realtime session with BCS tools."""
    await actor_ws.accept()
    peer = f"{actor_ws.client.host}:{actor_ws.client.port}" if actor_ws.client else "?"
    logger.info("[voice] actor connected peer=%s", peer)

    t_start = time.monotonic()
    call = _Call(actor_ws)
    try:
        await call.run()
    except Exception as exc:
        logger.exception("[voice] handler failed: %s", exc)
    finally:
        logger.info("[voice] handler exit duration=%.1fs", time.monotonic() - t_start)


class _Call:
    """The bridge for one call: actor audio up, agent audio and events down."""

    def __init__(self, actor_ws: WebSocket) -> None:
        self.actor_ws = actor_ws
        self.api = BCSAPI()
        self.up: websockets.ClientConnection | None = None

        self._resample: tuple | None = None   # audioop.ratecv carry-over state
        self._queue: deque[bytes] = deque()   # agent audio awaiting playout
        self._played_s = 0.0                  # audio actually written to the actor
        self._n_tools = 0

    async def run(self) -> None:
        async with websockets.connect(REALTIME_URL, max_size=None) as up:
            self.up = up
            await up.send(_session_update())
            logger.info(
                "[session] session.update sent: %d tools + anchor %r, soul %d bytes",
                len(TOOLS), ANCHOR_TOOL, len(AGENT_PROMPT),
            )

            tasks = [
                asyncio.create_task(self._pump_up(), name="actor→fluxions"),
                asyncio.create_task(self._read_down(), name="fluxions→actor"),
                asyncio.create_task(self._playout(), name="playout"),
                asyncio.create_task(self._report_pos(), name="playback.pos"),
            ]
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await task
            # Surface any exception from the first task to finish so it shows up
            # in the log instead of being silently swallowed.
            for task in done:
                exc = task.exception()
                if exc is not None:
                    logger.error(
                        "[voice] task %s failed: %s", task.get_name(), exc, exc_info=exc
                    )
        logger.info("[voice] session closed after %d tool calls", self._n_tools)

    async def _pump_up(self) -> None:
        """Caller audio: actor 24 kHz → resample to 16 kHz → Fluxions.

        The actor streams continuously, silence included, which is exactly what
        Fluxions' endpointing is tuned for — so frames are forwarded as they
        arrive rather than buffered into utterances.
        """
        n_frames = 0
        n_bytes = 0
        try:
            while True:
                frame = await self.actor_ws.receive_bytes()
                n_frames += 1
                n_bytes += len(frame)
                if n_frames == 1:
                    logger.info("[audio] first frame received bytes=%d", len(frame))
                elif n_frames % LOG_EVERY_N_FRAMES == 0:
                    logger.info("[audio] received %d frames (%d bytes)", n_frames, n_bytes)

                resampled, self._resample = audioop.ratecv(
                    frame, 2, 1, ACTOR_RATE_HZ, UP_RATE_HZ, self._resample
                )
                await self.up.send(resampled)
        except WebSocketDisconnect as exc:
            logger.info(
                "[audio] actor disconnected after %d frames (%d bytes): code=%s",
                n_frames, n_bytes, getattr(exc, "code", "?"),
            )
        except KeyError as exc:
            # Starlette raises KeyError('bytes') when it received a text frame
            # instead of a binary one — i.e. protocol mismatch on the actor side.
            logger.error(
                "[audio] received non-binary frame after %d binary frames — "
                "actor protocol mismatch? (%s)", n_frames, exc,
            )
            raise

    async def _read_down(self) -> None:
        """Agent audio and events: Fluxions → playout queue / tool dispatch."""
        n_chunks = 0
        async for msg in self.up:
            if isinstance(msg, bytes):
                n_chunks += 1
                if n_chunks == 1:
                    logger.info("[audio] first agent chunk (%d bytes)", len(msg))
                for i in range(0, len(msg), DOWN_FRAME_BYTES):
                    self._queue.append(msg[i:i + DOWN_FRAME_BYTES])
                continue

            event = json.loads(msg)
            kind = event["type"]

            if kind == "audio.flush":
                # Barge-in. Agent audio runs several seconds ahead of what the
                # caller has heard, so everything still queued is speech they
                # interrupted and must never hear.
                dropped = len(self._queue)
                self._queue.clear()
                logger.info("[vad] audio.flush — dropped %.2fs of queued playback",
                            dropped * FRAME_MS / 1000)
            elif kind == "tool.call":
                await self._run_tool(event)
            elif kind == "user.transcript":
                logger.info("[stt] actor_said: %s", str(event.get("text"))[:200])
            elif kind == "agent.sentence":
                logger.info("[agent] agent_said: %s", str(event.get("text"))[:200])
            elif kind == "agent.done":
                logger.info("[agent] turn done cancelled=%s", event.get("cancelled"))
            elif kind == "agent.hangup":
                logger.info("[agent] agent ended the call")
                return
            elif kind == "session.created":
                logger.info(
                    "[session] created id=%s model=%s voice=%s aec=%s builtins=%s",
                    event.get("session_id"), event.get("model"), event.get("voice"),
                    event.get("server_aec"), event.get("tools"),
                )
            elif kind == "error":
                raise RuntimeError(f"fluxions session error: {msg}")

    async def _playout(self) -> None:
        """Write queued agent audio to the actor at 1x realtime.

        Fluxions pushes a whole turn's audio far faster than it is spoken. If
        that were forwarded as it arrived, the actor would already hold the
        rest of the sentence by the time the caller interrupted, and
        `audio.flush` would have nothing left to drop — the agent would talk
        over the person interrupting it. Pacing here is what keeps barge-in
        honest, and it is why the queue is the only thing flush has to clear.
        """
        next_t = time.monotonic()
        while True:
            if self._queue:
                frame = self._queue.popleft()
                await self.actor_ws.send_bytes(frame)
                self._played_s += len(frame) / 2 / ACTOR_RATE_HZ
            next_t += FRAME_MS / 1000
            await asyncio.sleep(max(0, next_t - time.monotonic()))

    async def _report_pos(self) -> None:
        """Tell Fluxions how much agent audio the caller has actually heard."""
        while True:
            await asyncio.sleep(POS_INTERVAL_S)
            await self.up.send(json.dumps(
                {"type": "playback.pos", "played_s": self._played_s}
            ))

    async def _run_tool(self, event: dict) -> None:
        """Dispatch one tool call and answer it.

        Runs inline: `BCSAPI` is synchronous psycopg2 and every card-ops query
        is a single-row lookup or update, so this costs a millisecond or two —
        far inside the 12 s the agent waits before telling the caller the
        action did not happen.
        """
        name = event["name"]
        raw = event.get("arguments")
        logger.info("[tool] %s args=%s", name, str(raw)[:200])
        self._n_tools += 1

        args: dict = {}
        try:
            args = json.loads(raw) if isinstance(raw, str) else dict(raw or {})
            output = dispatch(self.api, name, args)
            logger.info("[tool] %s -> %s", name, json.dumps(output, default=str)[:200])
        except Exception as exc:  # surface errors to the agent, which says so honestly
            logger.exception("[tool] %s failed", name)
            output = {"error": str(exc)}

        report_tool_call(name, args, output)
        await self.up.send(json.dumps({
            "type": "tool.result",
            "call_id": event["call_id"],
            "output": json.dumps(output, default=str),
        }))

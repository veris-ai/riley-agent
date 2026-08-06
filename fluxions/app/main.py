"""riley-fluxions — Riley card-ops voice agent on Fluxions STT/TTS.

Listens on /voice for a single bidirectional PCM16 stream from the Veris actor.
Fluxions ships two of the three legs of a voice agent — `akro` batch
transcription and `VUI` streaming TTS (its realtime conversation API is not
released yet) — so this process *is* the pipeline: each caller turn is wrapped
in a WAV and submitted to akro, an OpenAI-compatible chat completion reasons
over the transcript and calls the BCS tools, and VUI speaks the reply over a
warm WebSocket. Turn-taking, barge-in, and conversation state live here — no
vendor holds the session.

Tool calls are dispatched against `BCSAPI`, which talks to postgres.

Logging is intentionally chatty so it's obvious from agent.log alone whether
audio is flowing, how the turn state is stepping, and where things stall.
"""

from __future__ import annotations

import asyncio
import audioop
import json
import logging
import os
import struct
import time
from collections import deque
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import httpx
import websockets
from fastapi import FastAPI, WebSocket
from openai import AsyncOpenAI
from starlette.websockets import WebSocketDisconnect

from .db import BCSAPI
from .reporting import report_tool_call
from .tools import TOOLS, dispatch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


FLUXIONS_API = "https://api.fluxions.ai"
TTS_WS_URL = "wss://api.fluxions.ai/vui/v1/tts/ws"

LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4.1-mini")
LLM_BASE_URL = os.environ.get("LLM_BASE_URL")

# A Fluxions voice slug (the stable `id` from GET /vui/voices). The full
# `voice_id` carries the serving checkpoint's suffix, which changes when
# Fluxions ships a new model, so it's resolved at startup rather than pinned.
VOICE_SLUG = os.environ.get("FLUXIONS_VOICE", "maeve")

# Veris's voice_ws actor speaks raw PCM16 mono at 24 kHz in both directions —
# which is also VUI's native output format, so no audio is ever resampled or
# converted in this process. akro accepts the caller's 24 kHz WAV as-is.
ACTOR_RATE_HZ = 24000

# End of the caller's turn: this much silence, matching the 800 ms server-VAD
# the hosted implementations use.
#
# It is measured in *audio* time — silent bytes seen — not on the wall clock.
# Frames do not arrive at a steady 50 fps: anything that stalls the pump lets
# the socket backlog and then delivers a burst, and a wall-clock timer reads
# that burst as a long silence and endpoints in the middle of a sentence.
END_OF_TURN_S = 0.8

# RMS of a 20 ms PCM16 frame above which the caller counts as speaking. The
# actor's line is digitally silent between utterances (RMS 0) and its speech
# sits in the thousands, so anything clear of the noise floor separates them.
VAD_RMS_THRESHOLD = 500

# Audio kept from just before the VAD trips, prepended to the turn. Speech
# onset ramps up through the threshold, so the first frames of an utterance
# read as silence and would otherwise be clipped from the submitted WAV.
PREROLL_S = 0.4

# akro is submit-and-poll, not streaming: this is the poll cadence while a
# turn's transcription job runs.
STT_POLL_S = 0.2

# A turn that still wants tools after this many round trips is looping.
MAX_TOOL_ROUNDS = 5

# How often to emit periodic frame-count log lines from the audio pump. At
# 50 fps (20 ms/frame) this is roughly one heartbeat per second.
LOG_EVERY_N_FRAMES = 50

GREETING = "Thanks for calling Acme Bank, this is Riley — how can I help?"


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


# Used verbatim. Unlike the hosted implementations, nothing here contradicts the
# shared prompt's "you have already greeted the caller" — the greeting below is
# spoken straight through TTS before the first LLM turn, so by the time the model
# sees anything, it really has happened.
AGENT_PROMPT = _load_agent_prompt()

_http: httpx.AsyncClient
_llm: AsyncOpenAI
_voice_id: str


async def _fluxions(method: str, path: str, **kwargs) -> dict:
    """One Fluxions REST call, JSON out.

    429 clears within seconds and 502/503 mean a model server is restarting —
    both documented as retry-after-a-pause — so those back off and retry.
    Anything else non-2xx raises.
    """
    for delay in (1, 2, 4, 8, 0):
        resp = await _http.request(method, path, **kwargs)
        if resp.status_code not in (429, 502, 503) or delay == 0:
            resp.raise_for_status()
            return resp.json()
        logger.warning(
            "[fluxions] %s %s -> %d, retrying in %ds",
            method, path, resp.status_code, delay,
        )
        await asyncio.sleep(delay)


def _wav(pcm: bytes) -> bytes:
    """Wrap raw s16le mono 24 kHz PCM in a WAV header for /akro/submit."""
    return b"".join((
        b"RIFF", struct.pack("<I", 36 + len(pcm)), b"WAVEfmt ",
        struct.pack("<IHHIIHH", 16, 1, 1, ACTOR_RATE_HZ, ACTOR_RATE_HZ * 2, 2, 16),
        b"data", struct.pack("<I", len(pcm)), pcm,
    ))


async def _warmup() -> None:
    """Wake both Fluxions services before the first call can arrive.

    The GPU fleet powers down when idle, and the first request after a quiet
    period is held at the gateway while a machine comes up — documented as up
    to ~30 s, measured at over a minute. Absorb that during container startup
    instead of on the greeting and the caller's first turn; these two requests
    get their own generous timeout since startup patience is free. akro gets
    `cache=false` because the warmup bytes are identical every start, and a
    cached result would skip the GPU entirely.
    """

    async def akro() -> None:
        t0 = time.monotonic()
        job = await _fluxions(
            "POST", "/akro/submit",
            content=_wav(b"\x00" * ACTOR_RATE_HZ),  # 0.5 s of silence
            headers={"Content-Type": "audio/wav"},
            params={"cache": "false"},
            timeout=180.0,
        )
        while (await _fluxions("GET", f"/transcriptions/{job['id']}"))["status"] not in (
            "completed", "failed",
        ):
            await asyncio.sleep(STT_POLL_S)
        logger.info("[startup] akro warm in %.1fs", time.monotonic() - t0)

    async def vui() -> None:
        t0 = time.monotonic()
        resp = await _http.post(
            "/vui/v1/tts",
            json={"voice": _voice_id, "input": "hi", "response_format": "pcm",
                  "verify_chunks": False},
            timeout=180.0,
        )
        resp.raise_for_status()
        logger.info("[startup] vui warm in %.1fs", time.monotonic() - t0)

    await asyncio.gather(akro(), vui())


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build the shared clients, resolve the voice slug, and warm the fleet."""
    global _http, _llm, _voice_id
    _http = httpx.AsyncClient(
        base_url=FLUXIONS_API,
        headers={"Authorization": os.environ["FLUXIONS_API_KEY"]},
        # Generous enough to sit through a cold start being held at the gateway.
        timeout=60.0,
    )
    _llm = AsyncOpenAI(api_key=os.environ["LLM_API_KEY"], base_url=LLM_BASE_URL)

    voices = (await _fluxions("GET", "/vui/voices"))["voices"]
    matches = [v for v in voices if v["id"] == VOICE_SLUG]
    if not matches:
        raise RuntimeError(
            f"voice {VOICE_SLUG!r} is not a Fluxions voice; available: "
            + ", ".join(sorted(v["id"] for v in voices))
        )
    _voice_id = matches[0]["voice_id"]
    logger.info(
        "[startup] stt=akro llm=%s (%s) tts=vui voice=%s (%s)",
        LLM_MODEL, LLM_BASE_URL or "api.openai.com", VOICE_SLUG, _voice_id,
    )
    await _warmup()
    yield
    await _http.aclose()


app = FastAPI(title="riley-fluxions", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.websocket("/voice")
async def voice(actor_ws: WebSocket) -> None:
    """One actor connection ↔ one Fluxions STT/LLM/TTS pipeline with BCS tools."""
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
    """The pipeline for one call: turn audio in, STT + LLM turn, TTS stream out.

    Two tasks run concurrently against this state — the actor audio pump
    (which does VAD, endpointing, and turn capture) and a turn worker. Turns
    are processed one at a time off a queue, so the message list is only ever
    touched by the worker even when the caller talks over a reply.
    """

    def __init__(self, actor_ws: WebSocket) -> None:
        self.actor_ws = actor_ws
        self.api = BCSAPI()
        self.messages: list = [{"role": "system", "content": AGENT_PROMPT}]

        self._voiced = False               # caller is mid-utterance
        self._silence_s = 0.0              # audio seconds since the last voiced frame
        self._turn_pcm = bytearray()       # the utterance being captured
        self._preroll: deque[bytes] = deque(maxlen=int(PREROLL_S / 0.02))
        self._turns: asyncio.Queue[bytes] = asyncio.Queue()
        self._speaking: asyncio.Task | None = None
        self._tts: websockets.ClientConnection | None = None

    async def run(self) -> None:
        tasks = [
            asyncio.create_task(self._pump_actor(), name="actor→turns"),
            asyncio.create_task(self._turn_worker(), name="turns"),
        ]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await task
        # Surface any exception from the first task to finish so it shows up in
        # the log instead of being silently swallowed.
        for task in done:
            exc = task.exception()
            if exc is not None:
                logger.error("[voice] task %s failed: %s", task.get_name(), exc, exc_info=exc)
        if self._tts is not None:
            with suppress(Exception):
                await self._tts.close()

    async def _pump_actor(self) -> None:
        """Binary PCM16 frames from the actor → VAD → turn capture.

        Every frame's RMS decides whether the caller is speaking, which drives
        barge-in, the turn endpoint, and what audio lands in the turn's WAV:
        the utterance itself plus a short pre-roll, so onset ramping up through
        the threshold isn't clipped.
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

                if audioop.rms(frame, 2) >= VAD_RMS_THRESHOLD:
                    self._on_voice()
                    self._turn_pcm += frame
                elif self._voiced:
                    self._turn_pcm += frame  # silence inside the utterance
                    self._silence_s += len(frame) / 2 / ACTOR_RATE_HZ
                    if self._silence_s >= END_OF_TURN_S:
                        self._endpoint()
                else:
                    self._preroll.append(frame)
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

    def _on_voice(self) -> None:
        """A voiced frame arrived: open the turn, and cut off any reply in progress."""
        self._silence_s = 0.0
        if self._voiced:
            return
        self._voiced = True
        logger.info("[vad] caller started speaking")
        self._turn_pcm = bytearray(b"".join(self._preroll))
        self._preroll.clear()
        if self._speaking is not None and not self._speaking.done():
            logger.info("[vad] barge-in — cutting the reply short")
            self._speaking.cancel()

    def _endpoint(self) -> None:
        """The caller has stopped: hand the captured utterance to the worker."""
        logger.info(
            "[turn] endpoint after %.2fs of silence (%.1fs audio captured)",
            self._silence_s, len(self._turn_pcm) / 2 / ACTOR_RATE_HZ,
        )
        self._voiced = False
        self._silence_s = 0.0
        self._turns.put_nowait(bytes(self._turn_pcm))
        self._turn_pcm = bytearray()

    async def _turn_worker(self) -> None:
        """Greet, then process caller turns strictly one at a time.

        The greeting runs here rather than before the pump starts so that actor
        audio is being consumed from the first frame — speaking it inline would
        let seconds of caller audio pile up in the socket.
        """
        # Call etiquette: Riley speaks first. Spoken directly rather than asked
        # of the LLM, so the opening words match the other implementations
        # exactly and the first turn costs no completion.
        await self._speak(GREETING)
        self.messages.append({"role": "assistant", "content": GREETING})

        while True:
            pcm = await self._turns.get()
            text = await self._transcribe(pcm)
            if not text:
                logger.info("[stt] turn had no words — skipping")
                continue
            await self._take_turn(text)

    async def _transcribe(self, pcm: bytes) -> str:
        """One utterance through akro: submit the WAV, poll until it lands.

        `word_level_timestamps=true` is what makes the poll response carry the
        `segments` array — without it the endpoint returns only job metadata
        (the docs show inline `text`, but the live API doesn't send it). The
        turn text is the segment texts joined; speaker labels are meaningless
        here since every submitted WAV is one caller utterance.
        """
        t0 = time.monotonic()
        job = await _fluxions(
            "POST", "/akro/submit",
            content=_wav(pcm),
            headers={"Content-Type": "audio/wav"},
        )
        while True:
            res = await _fluxions(
                "GET", f"/transcriptions/{job['id']}",
                params={"word_level_timestamps": "true"},
            )
            if res["status"] in ("completed", "failed"):
                break
            await asyncio.sleep(STT_POLL_S)
        if res["status"] == "failed":
            raise RuntimeError(
                f"transcription {job['id']} failed: {res.get('error_message')}"
            )
        text = " ".join(s["text"] for s in res.get("segments") or []).strip()
        logger.info(
            "[stt] actor_said (%.1fs audio in %.2fs): %s",
            len(pcm) / 2 / ACTOR_RATE_HZ, time.monotonic() - t0, text[:200],
        )
        return text

    async def _take_turn(self, text: str) -> None:
        """One caller turn: LLM (with tools) → spoken reply."""
        self.messages.append({"role": "user", "content": text})

        for _ in range(MAX_TOOL_ROUNDS):
            t0 = time.monotonic()
            resp = await _llm.chat.completions.create(
                model=LLM_MODEL,
                messages=self.messages,
                tools=TOOLS,
                tool_choice="auto",
            )
            msg = resp.choices[0].message
            self.messages.append(msg)
            logger.info(
                "[llm] %s replied in %.2fs (%d tool calls)",
                LLM_MODEL, time.monotonic() - t0, len(msg.tool_calls or []),
            )
            if not msg.tool_calls:
                break
            for call in msg.tool_calls:
                self.messages.append(self._run_tool(call))
        else:
            raise RuntimeError(f"tool loop did not settle in {MAX_TOOL_ROUNDS} rounds")

        if msg.content:
            logger.info("[llm] agent_said: %s", str(msg.content)[:200])
            await self._speak(str(msg.content))

    def _run_tool(self, call) -> dict:
        """Dispatch one tool call and build the tool message for the next round.

        Runs inline: `BCSAPI` is synchronous psycopg2 and every card-ops query
        is a single-row lookup or update, so this costs a millisecond or two.
        """
        name = call.function.name
        raw = call.function.arguments
        logger.info("[tool] %s args=%s", name, str(raw)[:200])

        args: dict = {}
        try:
            args = json.loads(raw) if isinstance(raw, str) else dict(raw)
            output = dispatch(self.api, name, args)
            logger.info("[tool] %s -> %s", name, json.dumps(output, default=str)[:200])
        except Exception as exc:  # surface errors to the model
            logger.exception("[tool] %s failed", name)
            output = {"error": str(exc)}

        report_tool_call(name, args, output)
        return {
            "role": "tool",
            "tool_call_id": call.id,
            "content": json.dumps(output, default=str),
        }

    async def _speak(self, text: str) -> None:
        """Speak a reply, cancellable by barge-in."""
        self._speaking = asyncio.create_task(self._speak_now(text))
        try:
            await self._speaking
        except asyncio.CancelledError:
            if not self._speaking.cancelled():
                # The worker itself is being torn down, not barged in on.
                self._speaking.cancel()
                raise
            # Barge-in cut the render mid-stream. The server is still pushing
            # the dead render's frames at this socket, so drop it — the next
            # reply reconnects.
            with suppress(Exception):
                await self._tts.close()
            self._tts = None
        finally:
            self._speaking = None

    async def _speak_now(self, text: str) -> None:
        """Stream a VUI render straight to the actor as it arrives.

        The socket is opened on first use and stays open across renders, so
        every reply after the greeting skips the TLS handshake and goes
        straight to synthesis. VUI's binary frames are s16le PCM at 24 kHz —
        already the actor's wire format, so bytes pass through untouched.
        `verify_chunks` is off for the lowest-latency stream: the re-check
        would hold back first audio for ~1 s per sentence.
        """
        t0 = time.monotonic()
        if self._tts is None:
            self._tts = await websockets.connect(
                TTS_WS_URL,
                additional_headers={"Authorization": os.environ["FLUXIONS_API_KEY"]},
            )
            logger.info("[tts] socket opened in %.2fs", time.monotonic() - t0)
        await self._tts.send(json.dumps({
            "type": "speak",
            "voice": _voice_id,
            "input": text,
            "verify_chunks": False,
        }))
        n_chunks = 0
        n_bytes = 0
        async for msg in self._tts:
            if isinstance(msg, bytes):
                n_chunks += 1
                n_bytes += len(msg)
                if n_chunks == 1:
                    logger.info(
                        "[tts] first chunk after %.2fs (%d bytes)",
                        time.monotonic() - t0, len(msg),
                    )
                await self.actor_ws.send_bytes(msg)
                continue
            event = json.loads(msg)
            if event["type"] == "done":
                break
            if event["type"] == "error":
                raise RuntimeError(f"VUI render failed: {event.get('message')}")
            # "start" — the worker stream opened; audio frames follow.
        logger.info(
            "[tts] spoke %d chunks (%d bytes, %.1fs audio) in %.2fs",
            n_chunks, n_bytes, n_bytes / 2 / ACTOR_RATE_HZ, time.monotonic() - t0,
        )

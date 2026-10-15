"""riley-smallest — Riley card-ops voice agent on Smallest AI's voice stack.

Listens on /voice for a single bidirectional PCM16 stream from the Veris actor.
Smallest ships the three legs of a voice agent as separate products rather
than one session, so this process *is* the pipeline: Pulse transcribes the
caller over a WebSocket, Electron — Smallest's OpenAI-compatible chat model —
reasons over the transcript and calls the BCS tools, and Lightning speaks the
reply back over a second WebSocket, one connection per utterance. Turn-taking,
barge-in, and conversation state live here — no vendor holds the session. One
SMALLEST_API_KEY covers all three legs.

Tool calls are dispatched against `BCSAPI`, which talks to postgres.

Logging is intentionally chatty so it's obvious from agent.log alone whether
audio is flowing, how the turn state is stepping, and where things stall.
"""

from __future__ import annotations

import asyncio
import audioop
import base64
import json
import logging
import os
import time
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from urllib.parse import urlencode

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


# Electron is the only chat model this transport asks for — there is no
# second tier to pick between, so this is not an environment override.
LLM_MODEL = "electron"
TTS_MODEL = os.environ.get("SMALLEST_TTS_MODEL", "lightning_v3.1_pro")
# The Pro pool: broadcast-quality voices, dedicated inference capacity, no
# cloning. Pro is picky about voice/model pairing — a standard-pool voice is
# rejected outright by the API rather than silently mispairing, so VOICE must
# name a voice actually in the Pro pool. `kelsey` is also what `TTS_MS_PER_WORD`
# below is measured on.
VOICE = os.environ.get("SMALLEST_VOICE", "kelsey")
LANGUAGE = os.environ.get("SMALLEST_LANGUAGE", "en")

LLM_BASE_URL = "https://api.smallest.ai/waves/v1"

# Veris's voice_ws actor speaks raw PCM16 mono at 24 kHz in both directions.
# Pulse's live endpoint and Lightning both accept 24 kHz directly, so no audio
# is resampled anywhere in this file.
ACTOR_RATE_HZ = 24000
PCM16_BYTES_PER_MS = ACTOR_RATE_HZ * 2 // 1000

STT_URL = "wss://api.smallest.ai/waves/v1/pulse/get_text?" + urlencode(
    {
        "language": LANGUAGE,
        "encoding": "linear16",
        "sample_rate": ACTOR_RATE_HZ,
        "word_timestamps": "false",
        "full_transcript": "false",
        "sentence_timestamps": "false",
        "redact_pii": "false",
        "redact_pci": "false",
        "numerals": "auto",
        "diarize": "false",
    }
)

# The unified streaming route — not the older per-model path
# (`/waves/v1/{model}/get_speech/stream`), which Smallest's own docs flag as
# deprecated. The model is a body field here rather than part of the URL.
TTS_URL = "wss://api.smallest.ai/waves/v1/tts/live"

# End of the caller's turn: this much silence, matching the 800 ms server-VAD
# the hosted implementations use.
#
# Measured in *audio* time — silent bytes seen — not the wall clock. Frames do
# not arrive at a steady 50 fps: anything that stalls the pump lets the socket
# backlog and then deliver a burst, and a wall-clock timer reads that burst as
# a long pause and endpoints mid-sentence.
END_OF_TURN_S = 0.8

# RMS of a 20 ms PCM16 frame above which the caller counts as speaking: the
# actor's line is digitally silent between utterances (RMS 0) and its speech
# sits in the thousands, so anything clear of the noise floor separates them.
VAD_RMS_THRESHOLD = 500

# How long a word of the reply takes Lightning to say, measured on `kelsey`
# (338 ms/word over four replies, 288-381 each). Needed because a barge-in
# cancels the TTS connection mid-reply, and Lightning's `chunk` frames carry
# no text alignment of their own, so the cut turns audio sent into words.
TTS_MS_PER_WORD = 340

# Lightning streams a reply faster than it plays, so sent as it lands the
# whole reply sits in the actor's buffer within a second or two and a
# barge-in has nothing left to cut. Replies go out in slices this long, paced
# against playback.
PLAYBACK_CHUNK_BYTES = 500 * PCM16_BYTES_PER_MS

# How far ahead of real-time playback the pacer is allowed to run. One slice
# in the actor's buffer keeps the line gapless; more just widens the slice of
# already-delivered audio a barge-in can't claw back.
PLAYBACK_LEAD_S = 1.0

# Appended to a reply the caller talked over, in place of the words that were
# never sent, so the model reads its own history as cut off rather than said.
INTERRUPTED_MARKER = " … [interrupted by the caller]"

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

_llm: AsyncOpenAI


def _auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {os.environ['SMALLEST_API_KEY']}"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build the shared Electron client once per process."""
    global _llm
    _llm = AsyncOpenAI(base_url=LLM_BASE_URL, api_key=os.environ["SMALLEST_API_KEY"])
    logger.info("[startup] stt=pulse llm=%s tts=%s voice=%s", LLM_MODEL, TTS_MODEL, VOICE)
    yield


app = FastAPI(title="riley-smallest", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.websocket("/voice")
async def voice(actor_ws: WebSocket) -> None:
    """One actor connection ↔ one Pulse session with BCS tools."""
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
    """The pipeline for one call: STT stream in, LLM turn, TTS stream out.

    Three tasks run concurrently against this state — the actor→STT audio pump
    (which also does VAD and endpointing), the STT event loop, and a turn
    worker. Turns are processed one at a time off a queue, so the message list
    is only ever touched by the worker even when the caller talks over a reply.
    """

    def __init__(self, actor_ws: WebSocket) -> None:
        self.actor_ws = actor_ws
        self.api = BCSAPI()
        self.messages: list = [{"role": "system", "content": AGENT_PROMPT}]

        self._stt = None
        self._voiced = False               # caller is mid-utterance
        self._silence_s = 0.0              # audio seconds since the last voiced frame
        self._segments: list[str] = []     # `is_final` texts of the turn in progress
        self._turns: asyncio.Queue[str] = asyncio.Queue()
        self._speaking: asyncio.Task | None = None
        self._sent_bytes = 0  # PCM16 of the in-flight reply handed to the actor

    async def run(self) -> None:
        self._stt = await websockets.connect(STT_URL, additional_headers=_auth_headers())
        logger.info("[voice] Pulse connected rate=%dHz", ACTOR_RATE_HZ)

        tasks = [
            asyncio.create_task(self._pump_actor_to_stt(), name="actor→stt"),
            asyncio.create_task(self._pump_stt_events(), name="stt→turns"),
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
        with suppress(Exception):
            await self._stt.close()

    async def _pump_actor_to_stt(self) -> None:
        """Binary PCM16 frames from the actor → Pulse, unconverted (both run at 24 kHz).

        Doubles as the VAD: every frame's RMS decides whether the caller is
        speaking, which drives both barge-in and the turn endpoint.
        """
        n_frames = 0
        n_bytes = 0
        try:
            while True:
                frame = await self.actor_ws.receive_bytes()
                n_frames += 1
                n_bytes += len(frame)
                if n_frames == 1:
                    logger.info("[a→stt] first frame received bytes=%d", len(frame))
                elif n_frames % LOG_EVERY_N_FRAMES == 0:
                    logger.info("[a→stt] forwarded %d frames (%d bytes)", n_frames, n_bytes)

                if audioop.rms(frame, 2) >= VAD_RMS_THRESHOLD:
                    self._on_voice()
                elif self._voiced:
                    self._silence_s += len(frame) / 2 / ACTOR_RATE_HZ
                    if self._silence_s >= END_OF_TURN_S:
                        await self._endpoint()

                await self._stt.send(frame)
        except WebSocketDisconnect as exc:
            logger.info(
                "[a→stt] actor disconnected after %d frames (%d bytes): code=%s",
                n_frames, n_bytes, getattr(exc, "code", "?"),
            )
        except KeyError as exc:
            # Starlette raises KeyError('bytes') when it received a text frame
            # instead of a binary one — i.e. protocol mismatch on the actor side.
            logger.error(
                "[a→stt] received non-binary frame after %d binary frames — "
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
        if self._speaking is not None and not self._speaking.done():
            logger.info("[vad] barge-in — cutting the reply short")
            self._speaking.cancel()

    async def _endpoint(self) -> None:
        """The caller has stopped: ask Pulse to finalize the turn.

        Nothing waits here for the answer — the pump goes straight back to
        forwarding audio, and `_pump_stt_events` dispatches the turn when
        Pulse's `from_finalize` final arrives.
        """
        logger.info("[turn] endpoint — finalizing")
        self._voiced = False
        self._silence_s = 0.0
        await self._stt.send(json.dumps({"type": "finalize"}))

    async def _pump_stt_events(self) -> None:
        """Transcription events → turn text.

        An `is_final` transcript covers only the audio since the previous
        final — confirmed live against Pulse (looping Lightning's own TTS
        output back into it): a caller who reads an account number, pauses
        half a second and gives a name arrives as two finals inside one turn,
        and the turn is their concatenation, not the latest final alone. The
        final answering our `finalize` carries `from_finalize: true` (with
        whatever text was still pending, often none) and is what closes the
        turn. Interim (`is_final: false`) events are not acted on; they exist
        for live captions, which this transport does not render.
        """
        async for raw in self._stt:
            data = json.loads(raw)
            if not data.get("is_final"):
                continue
            text = data["transcript"].strip()
            if text:
                self._segments.append(text)
            if data.get("from_finalize"):
                turn = " ".join(self._segments)
                self._segments.clear()
                logger.info("[stt] actor_said: %s", turn[:200])
                if turn:
                    await self._turns.put(turn)

        raise RuntimeError("Pulse STT closed the stream")

    async def _turn_worker(self) -> None:
        """Greet, then process caller turns strictly one at a time.

        The greeting runs here rather than before the pumps start so that actor
        audio is being consumed from the first frame — speaking it inline would
        let three seconds of caller audio pile up in the socket.
        """
        # Call etiquette: Riley speaks first. Spoken straight through TTS
        # rather than asked of the LLM, so the opening words match the other
        # implementations exactly and the first turn costs no completion.
        # Recorded as the first assistant turn so the model knows it has
        # already greeted and doesn't repeat it.
        #
        # The greeting is not interruptible: it is spoken by `_speak_now`
        # directly, so `_speaking` stays unset and `_on_voice` has nothing to
        # cancel. Background noise at the start of a call firing Silero-style
        # VAD this early would otherwise cancel the greeting before any audio
        # went out, leaving the caller with silence. A caller who does talk
        # over it has their turn taken right after.
        logger.info("[turn] speaking greeting (agent greets first)")
        await self._speak_now(GREETING)
        self.messages.append({"role": "assistant", "content": GREETING})

        while True:
            text = await self._turns.get()
            await self._take_turn(text)

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
            assistant_message: dict = {"role": "assistant", "content": msg.content}
            if msg.tool_calls:
                assistant_message["tool_calls"] = [
                    {
                        "id": call.id,
                        "type": "function",
                        "function": {
                            "name": call.function.name,
                            "arguments": call.function.arguments,
                        },
                    }
                    for call in msg.tool_calls
                ]
            self.messages.append(assistant_message)
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
            "name": name,
            "tool_call_id": call.id,
            "content": json.dumps(output, default=str),
        }

    async def _speak(self, text: str) -> None:
        """Speak the last assistant message, cancellable by barge-in.

        ``text`` is that message's content, already on ``messages`` so a
        barge-in can cut it down to what the caller heard.
        """
        self._sent_bytes = 0
        self._speaking = asyncio.create_task(self._speak_now(text))
        try:
            await self._speaking
        except asyncio.CancelledError:
            # Barge-in cancels only the TTS task, and the worker carries on to
            # the caller's next turn. A cancel aimed at the worker itself (the
            # call ending mid-reply) lands here too, and swallowing that would
            # leave the worker parked on the queue forever.
            if asyncio.current_task().cancelling():
                raise
            self._trim_interrupted_reply(text)
        finally:
            self._speaking = None

    def _trim_interrupted_reply(self, text: str) -> None:
        """Rewrite the last assistant message to the part of ``text`` that went out.

        Without this the history says Riley told the caller things they never
        heard, and the next turn reasons from that. Lightning's `chunk`
        messages carry no sentence boundaries, so the cut is proportional over
        the reply's words: the PCM sent to the actor, in ms, against the
        reply's length at TTS_MS_PER_WORD. Bytes handed to the actor count as
        heard; the pacer runs at most PLAYBACK_LEAD_S ahead, so that
        overstates it by no more.
        """
        if self._sent_bytes == 0:
            self.messages.pop()
            logger.info("[voice] barge-in: dropped reply, nothing sent before the cut")
            return
        heard_ms = self._sent_bytes // PCM16_BYTES_PER_MS
        words = text.split()
        total_ms = len(words) * TTS_MS_PER_WORD
        kept = " ".join(words[: heard_ms // TTS_MS_PER_WORD])
        self.messages[-1] = {"role": "assistant", "content": f"{kept}{INTERRUPTED_MARKER}"}
        logger.info(
            '[voice] barge-in: trimmed reply to %d ms / %d ms ("%s")',
            heard_ms, total_ms, kept[:60],
        )

    async def _speak_now(self, text: str) -> None:
        """One Lightning utterance: a fresh connection, closed when it completes.

        Unlike Pulse, Lightning's WebSocket takes one utterance per connection
        rather than a session held for the whole call — there is no documented
        way to abandon an utterance mid-stream and reuse the socket cleanly, so
        a barge-in cancelling this coroutine simply closes the connection
        (the ``async with`` below) rather than leaving stale audio for the next
        reply to collide with.

        The first slice goes out as soon as it lands, so the actor hears the
        reply onset immediately; after that slices go out no more than
        PLAYBACK_LEAD_S ahead of real-time playback, and cancelling this task
        strands the rest unsent.
        """
        t0 = time.monotonic()
        # close_timeout=0: Lightning does not answer a close frame while it is
        # still streaming, so a barge-in's close would otherwise wait out the
        # default 10 s handshake timeout — ten seconds of dead air before the
        # caller's turn. The utterance is being abandoned; there is nothing to
        # wait for.
        async with websockets.connect(
            TTS_URL, additional_headers=_auth_headers(), close_timeout=0
        ) as ws:
            await ws.send(json.dumps({
                "text": text,
                "voice_id": VOICE,
                "model": TTS_MODEL,
                "language": LANGUAGE,
                "sample_rate": ACTOR_RATE_HZ,
            }))
            n_chunks = 0
            n_bytes = 0
            t_play = None  # playback clock, started at the first byte sent
            async for raw in ws:
                msg = json.loads(raw)
                status = msg.get("status")
                if status == "chunk":
                    pcm16 = base64.b64decode(msg["data"]["audio"])
                    n_chunks += 1
                    n_bytes += len(pcm16)
                    if n_chunks == 1:
                        logger.info(
                            "[tts] first chunk after %.2fs (%d bytes)",
                            time.monotonic() - t0, len(pcm16),
                        )
                        t_play = time.monotonic()
                    for i in range(0, len(pcm16), PLAYBACK_CHUNK_BYTES):
                        sent_s = self._sent_bytes / 2 / ACTOR_RATE_HZ
                        ahead = sent_s - (time.monotonic() - t_play)
                        if ahead > PLAYBACK_LEAD_S:
                            await asyncio.sleep(ahead - PLAYBACK_LEAD_S)
                        piece = pcm16[i : i + PLAYBACK_CHUNK_BYTES]
                        await self.actor_ws.send_bytes(piece)
                        self._sent_bytes += len(piece)
                elif status == "complete":
                    break
                elif status == "error":
                    raise RuntimeError(f"Lightning TTS error: {msg.get('error', msg)}")
                else:
                    logger.warning("[tts] unknown message status: %s", msg)
            else:
                raise RuntimeError("Lightning TTS closed the stream before `complete`")
            logger.info(
                "[tts] spoke %d chunks (%d bytes, %.1fs audio) in %.2fs",
                n_chunks, n_bytes, n_bytes / 2 / ACTOR_RATE_HZ, time.monotonic() - t0,
            )

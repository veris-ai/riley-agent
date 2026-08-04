"""riley-mistral — Riley card-ops voice agent on Mistral's speech-to-speech pipeline.

Listens on /voice for a single bidirectional PCM16 stream from the Veris actor.
Mistral ships the three legs of a voice agent as separate services rather than
one session, so this process *is* the pipeline: Voxtral Realtime transcribes the
caller over a WebSocket, a Mistral chat completion reasons over the transcript
and calls the BCS tools, and Voxtral TTS speaks the reply back. Turn-taking,
barge-in, and conversation state live here — no vendor holds the session.

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

import numpy as np
from fastapi import FastAPI, WebSocket
from mistralai.client import Mistral
from mistralai.client.models import AudioFormat
from mistralai.client.utils import BackoffStrategy, RetryConfig
from starlette.websockets import WebSocketDisconnect

from .db import BCSAPI
from .reporting import report_tool_call
from .tools import TOOLS, dispatch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


STT_MODEL = os.environ.get("MISTRAL_STT_MODEL", "voxtral-mini-transcribe-realtime-2602")
TTS_MODEL = os.environ.get("MISTRAL_TTS_MODEL", "voxtral-mini-tts-2603")
LLM_MODEL = os.environ.get("MISTRAL_LLM_MODEL", "mistral-large-latest")

# Voxtral's US-English presets are the `en_paul_*` family; the other 22 presets
# are en_gb (Oliver / Jane) or fr_fr (Marie). Resolved to a UUID at startup —
# `voice_id` takes the id, not the slug.
VOICE_SLUG = os.environ.get("MISTRAL_VOICE", "en_paul_neutral")

# Veris's voice_ws actor speaks raw PCM16 mono at 24 kHz in both directions.
# Voxtral Realtime wants 16 kHz, so caller audio is downsampled on the way in.
# Voxtral TTS emits 24 kHz already, so replies only need float32 → int16.
ACTOR_RATE_HZ = 24000
STT_RATE_HZ = 16000

# End of the caller's turn: this much silence, matching the 800 ms server-VAD
# the hosted implementations use.
#
# Endpointing runs on the caller's audio, not on transcription timing. Voxtral
# emits deltas in bursts — it holds words back until it has enough right
# context, and mid-sentence gaps of two seconds happen — so delta silence says
# nothing about whether the caller stopped talking. Measuring the audio keeps
# the endpoint honest and leaves `flush_audio()` for what it is good at:
# forcing out the tail the deltas are still sitting on.
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

# A turn that still wants tools after this many round trips is looping.
MAX_TOOL_ROUNDS = 5

# How often to emit periodic frame-count log lines from the audio pumps. At
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

_client: Mistral
_voice_id: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build the shared client and resolve the voice slug once per process."""
    global _client, _voice_id
    # A turn spends 2–3 completions walking the tool loop, which is enough to
    # trip Mistral's per-second rate limit. The SDK knows how to retry a 429 but
    # ships that switched off, so a single throttled request would otherwise
    # take the call down mid-sentence. Backoff is capped well under a caller's
    # patience — past that, failing is the honest outcome.
    _client = Mistral(
        api_key=os.environ["MISTRAL_API_KEY"],
        retry_config=RetryConfig(
            "backoff",
            BackoffStrategy(
                initial_interval=200,
                max_interval=2_000,
                exponent=1.5,
                max_elapsed_time=8_000,
            ),
            retry_connection_errors=True,
        ),
    )
    voices = await _client.audio.voices.list_async(type_="preset", limit=100)
    matches = [v for v in voices.items if v.slug == VOICE_SLUG]
    if not matches:
        raise RuntimeError(
            f"voice {VOICE_SLUG!r} is not a Voxtral preset; available: "
            + ", ".join(sorted(v.slug for v in voices.items if v.slug))
        )
    _voice_id = matches[0].id
    logger.info(
        "[startup] stt=%s llm=%s tts=%s voice=%s (%s)",
        STT_MODEL, LLM_MODEL, TTS_MODEL, VOICE_SLUG, _voice_id,
    )
    yield


app = FastAPI(title="riley-mistral", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.websocket("/voice")
async def voice(actor_ws: WebSocket) -> None:
    """One actor connection ↔ one Voxtral Realtime session with BCS tools."""
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
        self._partial: list[str] = []      # deltas since the last flush, for the log
        self._turns: asyncio.Queue[str] = asyncio.Queue()
        self._speaking: asyncio.Task | None = None

    async def run(self) -> None:
        self._stt = await _client.audio.realtime.connect(
            model=STT_MODEL,
            audio_format=AudioFormat(encoding="pcm_s16le", sample_rate=STT_RATE_HZ),
        )
        logger.info(
            "[voice] Voxtral Realtime connected request_id=%s in=%d Hz stt=%d Hz",
            self._stt.request_id, ACTOR_RATE_HZ, STT_RATE_HZ,
        )

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
        """Binary PCM16 frames from the actor → Voxtral Realtime, 24k → 16k.

        Doubles as the VAD: every frame's RMS decides whether the caller is
        speaking, which drives both barge-in and the turn endpoint.

        The actor streams continuously, silence included, which is also what
        keeps the session alive: Voxtral drops the connection after ~30 s with
        no audio at all.
        """
        state = None  # ratecv carries fractional-sample state between frames
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

                pcm16k, state = audioop.ratecv(
                    frame, 2, 1, ACTOR_RATE_HZ, STT_RATE_HZ, state
                )
                await self._stt.send_audio(pcm16k)
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
        """The caller has stopped: flush so Voxtral finalizes the turn."""
        logger.info(
            "[turn] endpoint after %.2fs of silence — flushing (partial: %s)",
            self._silence_s, "".join(self._partial).strip()[:120] or "(none yet)",
        )
        self._voiced = False
        self._silence_s = 0.0
        await self._stt.flush_audio()

    async def _pump_stt_events(self) -> None:
        """Transcription events → turn text.

        Deltas are incremental word fragments, collected only so agent.log
        shows the transcript building in real time. The turn text itself comes
        from `transcription.done`, which the flush triggers.
        """
        async for ev in self._stt.events():
            etype = getattr(ev, "type", None)

            if etype == "transcription.text.delta":
                self._partial.append(ev.text)

            elif etype == "transcription.done":
                # Authoritative text for the turn: the flush pushes out the last
                # word or two, which the deltas hold back waiting for context.
                text = ev.text.strip()
                self._partial.clear()
                logger.info("[stt] actor_said: %s", text[:200])
                if text:
                    await self._turns.put(text)

            elif etype == "error":
                logger.error("[stt] Voxtral Realtime error: %s", ev.error)
                raise RuntimeError(f"Voxtral Realtime error: {ev.error}")

            else:
                logger.info("[stt] %s", etype)

    async def _turn_worker(self) -> None:
        """Greet, then process caller turns strictly one at a time.

        The greeting runs here rather than before the pumps start so that actor
        audio is being consumed from the first frame — speaking it inline would
        let three seconds of caller audio pile up in the socket.
        """
        # Call etiquette: Riley speaks first. Spoken directly rather than asked
        # of the LLM, so the opening words match the other implementations
        # exactly and the first turn costs no completion.
        await self._speak(GREETING)
        self.messages.append({"role": "assistant", "content": GREETING})

        while True:
            text = await self._turns.get()
            await self._take_turn(text)

    async def _take_turn(self, text: str) -> None:
        """One caller turn: LLM (with tools) → spoken reply."""
        self.messages.append({"role": "user", "content": text})

        for _ in range(MAX_TOOL_ROUNDS):
            t0 = time.monotonic()
            resp = await _client.chat.complete_async(
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
            "name": name,
            "tool_call_id": call.id,
            "content": json.dumps(output, default=str),
        }

    async def _speak(self, text: str) -> None:
        """Speak a reply, cancellable by barge-in."""
        self._speaking = asyncio.create_task(self._speak_now(text))
        with suppress(asyncio.CancelledError):
            await self._speaking
        self._speaking = None

    async def _speak_now(self, text: str) -> None:
        """Stream Voxtral TTS straight to the actor as it arrives.

        Voxtral's `pcm` is raw float32 LE at 24 kHz — already the actor's rate,
        so the only conversion is float32 → int16. Chunks go out as they land
        rather than being buffered, so the actor hears the reply onset
        immediately; each is ~0.4 s of audio, which is also the granularity at
        which barge-in can cut the reply.
        """
        t0 = time.monotonic()
        stream = await _client.audio.speech.complete_async(
            model=TTS_MODEL,
            input=text,
            voice_id=_voice_id,
            response_format="pcm",
            stream=True,
        )
        n_chunks = 0
        n_bytes = 0
        async for ev in stream:
            if getattr(ev.data, "type", None) != "speech.audio.delta":
                continue
            f32 = np.frombuffer(base64.b64decode(ev.data.audio_data), dtype="<f4")
            pcm16 = (np.clip(f32, -1.0, 1.0) * 32767.0).astype("<i2").tobytes()
            n_chunks += 1
            n_bytes += len(pcm16)
            if n_chunks == 1:
                logger.info("[tts] first chunk after %.2fs (%d bytes)", time.monotonic() - t0, len(pcm16))
            await self.actor_ws.send_bytes(pcm16)
        logger.info(
            "[tts] spoke %d chunks (%d bytes, %.1fs audio) in %.2fs",
            n_chunks, n_bytes, n_bytes / 2 / ACTOR_RATE_HZ, time.monotonic() - t0,
        )

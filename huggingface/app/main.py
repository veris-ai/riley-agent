"""riley-huggingface — Riley card-ops voice agent on Hugging Face's hosted inference.

Listens on /voice for a single bidirectional PCM16 stream from the Veris actor.
The pipeline is the huggingface/speech-to-speech cascade — VAD, Whisper STT, an
open LLM, Kokoro TTS — with every model leg replaced by a Hugging Face
Inference Providers call, so one HF token bills all three and nothing runs
locally. HF's hosted legs are plain request/response HTTP (no streaming STT,
no streaming TTS), so the pipeline buffers each caller utterance, transcribes
it on endpoint, and paces the synthesized reply back out. Turn-taking,
barge-in, and conversation state live here — no vendor holds the session.

The three legs go through the router with plain httpx rather than
`huggingface_hub`'s AsyncInferenceClient: the client's fal-ai text-to-speech
path downloads the result with a *blocking* requests call inside the event
loop, which would stall the 20 ms audio pumps. The router URLs it would build
are constructed the same way here (verified against huggingface_hub 1.26).

Tool calls are dispatched against `BCSAPI`, which talks to postgres.

Logging is intentionally chatty so it's obvious from agent.log alone whether
audio is flowing, how the turn state is stepping, and where things stall.
"""

from __future__ import annotations

import asyncio
import audioop
import base64
import io
import json
import logging
import os
import time
import wave
from collections import deque
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import httpx
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


ROUTER = "https://router.huggingface.co"

# The three legs of the cascade, each a Hugging Face Inference Providers model.
# Whisper large-v3 and Kokoro are the huggingface/speech-to-speech repo's own
# STT and TTS choices; gpt-oss-120b is an open LLM with dependable function
# calling. The LLM id may carry a `:provider` suffix (e.g. `:groq`) to pin
# which partner serves it — without one the router picks.
STT_MODEL = os.environ.get("HF_STT_MODEL", "openai/whisper-large-v3")
LLM_MODEL = os.environ.get("HF_LLM_MODEL", "openai/gpt-oss-120b:groq")
TTS_MODEL = os.environ.get("HF_TTS_MODEL", "hexgrad/Kokoro-82M")

# A Kokoro voice id: `af_*`/`am_*` are American female/male. Passed through to
# the provider verbatim; an unknown voice is the provider's error to raise.
TTS_VOICE = os.environ.get("HF_TTS_VOICE", "af_heart")

# Dedicated Inference Endpoints — HF's other hosting product. When set, the
# leg posts to the deployment instead of the serverless router; the same HF
# token authorizes both. HF_STT_URL is an ASR endpoint (default engine, same
# raw-bytes shape as hf-inference), used as-is. HF_LLM_URL is the endpoint's
# base URL as copied from the console; a vLLM engine serves the OpenAI API
# under /v1, and HF_LLM_MODEL must then be the bare served model id — the
# `:provider` routing suffix is a router concept.
STT_URL = os.environ.get("HF_STT_URL", f"{ROUTER}/hf-inference/models/{STT_MODEL}")
LLM_CHAT_URL = (
    f"{os.environ['HF_LLM_URL'].rstrip('/')}/v1/chat/completions"
    if "HF_LLM_URL" in os.environ
    else f"{ROUTER}/v1/chat/completions"
)

# HF_TTS_URL is a custom-handler TTS endpoint speaking this repo's own
# contract — POST {"inputs", "parameters": {"voice"}} → {"audio_b64": WAV} —
# rather than fal-ai's two-step URL shape. HF_TTS_VOICE must then name a
# voice the deployed model actually has (XTTS-v2 studio speakers are e.g.
# "Ana Florence"; Kokoro's are `af_*`/`am_*`).
TTS_URL_OVERRIDE = os.environ.get("HF_TTS_URL")

# Veris's voice_ws actor speaks raw PCM16 mono at 24 kHz in both directions.
# Utterances are downsampled to 16 kHz before upload — Whisper resamples to
# 16 kHz anyway, so the extra bytes buy nothing. Kokoro synthesizes at 24 kHz,
# the actor's rate already.
ACTOR_RATE_HZ = 24000
STT_RATE_HZ = 16000

# End of the caller's turn: this much silence, matching the 800 ms server-VAD
# the hosted implementations use. Measured in *audio* time — silent bytes seen
# — not on the wall clock: frames arrive in bursts when anything stalls the
# pump, and a wall-clock timer reads a burst as a long silence and endpoints
# mid-sentence.
END_OF_TURN_S = 0.8

# RMS of a 20 ms PCM16 frame above which the caller counts as speaking. The
# actor's line is digitally silent between utterances (RMS 0) and its speech
# sits in the thousands, so anything clear of the noise floor separates them.
VAD_RMS_THRESHOLD = 500

# Audio kept from just before the first voiced frame. The RMS gate trips a
# frame or two after the true onset — a soft "h" sits under any threshold —
# and unlike the streaming-STT implementations nothing else hears that audio,
# so without a preroll the transcript loses word onsets.
PREROLL_S = 0.24

# The reply is sent in slices this long, paced against playback (see
# _speak_now) so barge-in can cut the tail of a reply that Kokoro returned as
# one complete file.
PLAYBACK_CHUNK_S = 0.5

# How far ahead of real-time playback the pacer is allowed to run. One chunk
# in the actor's buffer keeps the line gapless; more just widens the slice of
# already-delivered audio barge-in can't claw back.
PLAYBACK_LEAD_S = 1.0

# A turn that still wants tools after this many round trips is looping.
MAX_TOOL_ROUNDS = 5

# Bounded retry for the three HTTP legs. 429s arrive in bursts that outlast
# any single retry (the mistral implementation logged seven in one call and
# still finished), and hf-inference answers 503 while a cold model loads —
# both are worth waiting out mid-call, since giving up kills the call.
RETRY_STATUSES = {429, 503}
RETRY_MAX_ELAPSED_S = 25.0
RETRY_INITIAL_S = 0.5

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


# Used verbatim. The greeting below is spoken straight through TTS before the
# first LLM turn, so the prompt's "you have already greeted the caller" is true
# as written by the time the model sees anything.
AGENT_PROMPT = _load_agent_prompt()

_http: httpx.AsyncClient       # HF router + hub API, carries the HF token
_download: httpx.AsyncClient   # bare client for signed audio URLs — no token leaves HF
_tts_url: str


async def _resolve_tts_route(http: httpx.AsyncClient) -> str:
    """Resolve the TTS model id to its provider route on the router.

    Text-to-speech is not served on HF's own hf-inference infra, so the hub's
    provider mapping says which partner serves the model and under what id —
    for Kokoro that is fal-ai's `fal-ai/kokoro/american-english`. Resolved at
    startup so an unservable model fails the boot with the live mapping
    instead of 404ing mid-call.
    """
    resp = await http.get(
        f"https://huggingface.co/api/models/{TTS_MODEL}",
        params={"expand": "inferenceProviderMapping"},
    )
    resp.raise_for_status()
    mapping = resp.json().get("inferenceProviderMapping", {})
    live = {
        provider: entry["providerId"]
        for provider, entry in mapping.items()
        if entry["task"] == "text-to-speech" and entry["status"] == "live"
    }
    # The request/response shape below (`{"text": ...}` in, `audio.url` out)
    # is fal-ai's; other providers speak other shapes, so only fal-ai counts.
    if "fal-ai" not in live:
        raise RuntimeError(
            f"{TTS_MODEL!r} has no live fal-ai text-to-speech route; "
            f"live providers: {sorted(live) or 'none'}"
        )
    return f"{ROUTER}/fal-ai/{live['fal-ai']}"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build the shared HTTP client and resolve the TTS route once per process."""
    global _http, _download, _tts_url
    token = os.environ["HF_TOKEN"]
    # The read timeout is sized for provider cold starts, not for a healthy
    # request: fal spins Kokoro's worker down when idle, and the first
    # synthesis after that took 20–60 s measured (1–2 s warm). A timeout is
    # not a 429/503 — the retry loop can't save it — so the ceiling has to
    # clear the cold start or the first call of a run dies at the greeting.
    _http = httpx.AsyncClient(
        headers={"Authorization": f"Bearer {token}"},
        timeout=httpx.Timeout(120.0, connect=10.0),
    )
    _download = httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=10.0))
    _tts_url = TTS_URL_OVERRIDE or await _resolve_tts_route(_http)
    logger.info(
        "[startup] stt=%s llm=%s tts=%s voice=%s stt_url=%s llm_url=%s tts_url=%s",
        STT_MODEL, LLM_MODEL, TTS_MODEL, TTS_VOICE, STT_URL, LLM_CHAT_URL, _tts_url,
    )
    yield
    await _http.aclose()
    await _download.aclose()


app = FastAPI(title="riley-huggingface", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


async def _post_with_retry(url: str, *, label: str, **kwargs) -> httpx.Response:
    """POST, waiting out 429 bursts and hf-inference cold starts (503)."""
    delay = RETRY_INITIAL_S
    t0 = time.monotonic()
    while True:
        resp = await _http.post(url, **kwargs)
        if resp.status_code not in RETRY_STATUSES:
            if resp.is_error:
                # raise_for_status omits the body, which is where providers
                # put the actual reason.
                logger.error("[%s] HTTP %d: %s", label, resp.status_code, resp.text[:500])
            resp.raise_for_status()
            return resp
        elapsed = time.monotonic() - t0
        if elapsed + delay > RETRY_MAX_ELAPSED_S:
            logger.error("[%s] still HTTP %d after %.1fs — giving up: %s",
                         label, resp.status_code, elapsed, resp.text[:500])
            resp.raise_for_status()
        logger.warning(
            "[%s] HTTP %d, retrying in %.1fs (%.1fs elapsed): %s",
            label, resp.status_code, delay, elapsed, resp.text[:200],
        )
        await asyncio.sleep(delay)
        delay *= 2


def _wav_bytes(pcm: bytes, rate_hz: int) -> bytes:
    """Wrap raw PCM16 mono in a WAV container."""
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate_hz)
        w.writeframes(pcm)
    return buf.getvalue()


def _wav_to_actor_pcm(wav: bytes) -> bytes:
    """Decode a WAV file to the actor's raw format: PCM16 mono at 24 kHz."""
    with wave.open(io.BytesIO(wav), "rb") as w:
        pcm = w.readframes(w.getnframes())
        width, channels, rate = w.getsampwidth(), w.getnchannels(), w.getframerate()
    if width != 2:
        pcm = audioop.lin2lin(pcm, width, 2)
    if channels == 2:
        pcm = audioop.tomono(pcm, 2, 0.5, 0.5)
    if rate != ACTOR_RATE_HZ:
        pcm, _ = audioop.ratecv(pcm, 2, 1, rate, ACTOR_RATE_HZ, None)
    return pcm


@app.websocket("/voice")
async def voice(actor_ws: WebSocket) -> None:
    """One actor connection ↔ one VAD → Whisper → LLM → Kokoro pipeline."""
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
    """The pipeline for one call: utterance in, LLM turn, paced reply out.

    Two tasks run concurrently against this state — the actor-audio pump
    (which does VAD, utterance buffering, and endpointing) and a turn worker.
    Turns are processed one at a time off a queue, so the message list is only
    ever touched by the worker even when the caller talks over a reply.
    """

    def __init__(self, actor_ws: WebSocket) -> None:
        self.actor_ws = actor_ws
        self.api = BCSAPI()
        self.messages: list = [{"role": "system", "content": AGENT_PROMPT}]

        self._voiced = False               # caller is mid-utterance
        self._silence_s = 0.0              # audio seconds since the last voiced frame
        self._utterance: list[bytes] = []  # voiced frames of the open utterance
        self._preroll: deque[bytes] = deque(
            maxlen=max(1, int(PREROLL_S * 1000 / 20))
        )
        self._turns: asyncio.Queue[bytes] = asyncio.Queue()
        self._speaking: asyncio.Task | None = None

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

    async def _pump_actor(self) -> None:
        """Binary PCM16 frames from the actor → VAD → buffered utterances.

        Whisper is request/response, so unlike the streaming-STT
        implementations nothing consumes audio continuously: voiced frames
        collect in `_utterance`, and the 800 ms endpoint closes it and hands
        it to the turn worker.
        """
        n_frames = 0
        n_bytes = 0
        try:
            while True:
                frame = await self.actor_ws.receive_bytes()
                n_frames += 1
                n_bytes += len(frame)
                if n_frames == 1:
                    logger.info("[pump] first frame received bytes=%d", len(frame))
                elif n_frames % LOG_EVERY_N_FRAMES == 0:
                    logger.info("[pump] %d frames (%d bytes)", n_frames, n_bytes)

                if audioop.rms(frame, 2) >= VAD_RMS_THRESHOLD:
                    self._on_voice()
                    self._utterance.append(frame)
                elif self._voiced:
                    # Trailing frames belong to the utterance — the endpoint
                    # threshold is silence *within* it, and Whisper is happy
                    # to see the pause.
                    self._utterance.append(frame)
                    self._silence_s += len(frame) / 2 / ACTOR_RATE_HZ
                    if self._silence_s >= END_OF_TURN_S:
                        self._endpoint()
                else:
                    self._preroll.append(frame)
        except WebSocketDisconnect as exc:
            logger.info(
                "[pump] actor disconnected after %d frames (%d bytes): code=%s",
                n_frames, n_bytes, getattr(exc, "code", "?"),
            )
        except KeyError as exc:
            # Starlette raises KeyError('bytes') when it received a text frame
            # instead of a binary one — i.e. protocol mismatch on the actor side.
            logger.error(
                "[pump] received non-binary frame after %d binary frames — "
                "actor protocol mismatch? (%s)", n_frames, exc,
            )
            raise

    def _on_voice(self) -> None:
        """A voiced frame arrived: open the utterance, cut off any reply in progress."""
        self._silence_s = 0.0
        if self._voiced:
            return
        self._voiced = True
        self._utterance = list(self._preroll)
        self._preroll.clear()
        logger.info("[vad] caller started speaking")
        if self._speaking is not None and not self._speaking.done():
            logger.info("[vad] barge-in — cutting the reply short")
            self._speaking.cancel()

    def _endpoint(self) -> None:
        """The caller has stopped: close the utterance and queue it for the worker."""
        pcm = b"".join(self._utterance)
        logger.info(
            "[turn] endpoint after %.2fs of silence (%.1fs audio)",
            self._silence_s, len(pcm) / 2 / ACTOR_RATE_HZ,
        )
        self._voiced = False
        self._silence_s = 0.0
        self._utterance = []
        self._turns.put_nowait(pcm)

    async def _turn_worker(self) -> None:
        """Greet, then process caller utterances strictly one at a time.

        The greeting runs here rather than before the pumps start so that
        actor audio is being consumed from the first frame — speaking it
        inline would let seconds of caller audio pile up in the socket.
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
                continue
            await self._take_turn(text)

    async def _transcribe(self, pcm: bytes) -> str:
        """One utterance → Whisper on hf-inference → turn text."""
        pcm16k, _ = audioop.ratecv(pcm, 2, 1, ACTOR_RATE_HZ, STT_RATE_HZ, None)
        wav = _wav_bytes(pcm16k, STT_RATE_HZ)
        t0 = time.monotonic()
        resp = await _post_with_retry(
            STT_URL,
            label="stt",
            content=wav,
            headers={"Content-Type": "audio/wav"},
        )
        text = resp.json()["text"].strip()
        logger.info(
            "[stt] %.1fs audio → %.2fs latency, actor_said: %s",
            len(wav) / 2 / STT_RATE_HZ, time.monotonic() - t0, text[:200],
        )
        return text

    async def _take_turn(self, text: str) -> None:
        """One caller turn: LLM (with tools) → spoken reply."""
        self.messages.append({"role": "user", "content": text})

        for _ in range(MAX_TOOL_ROUNDS):
            t0 = time.monotonic()
            body = {
                "model": LLM_MODEL,
                "messages": self.messages,
                "tools": TOOLS,
                "tool_choice": "auto",
            }
            if "HF_LLM_URL" in os.environ:
                # Reasoning-capable models behind vLLM think when they judge a
                # turn hard — measured 28 s on the verification turn, against
                # sub-second everywhere else. A caller cannot wait on that.
                # Templates without a `thinking` flag simply ignore the kwarg.
                body["chat_template_kwargs"] = {"thinking": False}
            resp = await _post_with_retry(LLM_CHAT_URL, label="llm", json=body)
            msg = resp.json()["choices"][0]["message"]
            # Keep only the portable OpenAI-schema fields. gpt-oss on Groq
            # returns extra reasoning fields in the message, and replaying
            # those must not break when HF_LLM_MODEL pins another provider.
            self.messages.append(
                {k: msg[k] for k in ("role", "content", "tool_calls") if msg.get(k) is not None}
            )
            tool_calls = msg.get("tool_calls") or []
            logger.info(
                "[llm] %s replied in %.2fs (%d tool calls)",
                LLM_MODEL, time.monotonic() - t0, len(tool_calls),
            )
            if not tool_calls:
                break
            for call in tool_calls:
                self.messages.append(self._run_tool(call))
        else:
            raise RuntimeError(f"tool loop did not settle in {MAX_TOOL_ROUNDS} rounds")

        if msg.get("content"):
            logger.info("[llm] agent_said: %s", msg["content"][:200])
            await self._speak(msg["content"])

    def _run_tool(self, call: dict) -> dict:
        """Dispatch one tool call and build the tool message for the next round.

        Runs inline: `BCSAPI` is synchronous psycopg2 and every card-ops query
        is a single-row lookup or update, so this costs a millisecond or two.
        """
        name = call["function"]["name"]
        raw = call["function"]["arguments"]
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
            "tool_call_id": call["id"],
            "content": json.dumps(output, default=str),
        }

    async def _speak(self, text: str) -> None:
        """Speak a reply, cancellable by barge-in."""
        self._speaking = asyncio.create_task(self._speak_now(text))
        with suppress(asyncio.CancelledError):
            await self._speaking
        self._speaking = None

    async def _speak_now(self, text: str) -> None:
        """Synthesize the reply, then pace the audio out against playback.

        A custom TTS endpoint returns the WAV in one JSON hop (base64);
        fal-ai's shape is two round trips — the POST returns JSON carrying a
        signed URL, the GET fetches the finished WAV. Either way the whole
        reply arrives at once, so pacing is what preserves barge-in — blast
        it and the actor's buffer already holds the full reply by the time
        the caller interrupts. Chunks go out no more than PLAYBACK_LEAD_S
        ahead of real-time playback; cancelling this task strands the rest
        unsent.
        """
        t0 = time.monotonic()
        if TTS_URL_OVERRIDE:
            resp = await _post_with_retry(
                _tts_url, label="tts",
                json={"inputs": text, "parameters": {"voice": TTS_VOICE}},
            )
            wav_bytes = base64.b64decode(resp.json()["audio_b64"])
        else:
            resp = await _post_with_retry(
                _tts_url, label="tts", json={"text": text, "voice": TTS_VOICE},
            )
            audio_url = resp.json()["audio"]["url"]
            wav = await _download.get(audio_url)
            wav.raise_for_status()
            wav_bytes = wav.content
        pcm = _wav_to_actor_pcm(wav_bytes)
        total_s = len(pcm) / 2 / ACTOR_RATE_HZ
        logger.info(
            "[tts] %.1fs audio for %d chars in %.2fs", total_s, len(text), time.monotonic() - t0,
        )

        chunk_bytes = int(PLAYBACK_CHUNK_S * ACTOR_RATE_HZ) * 2
        t_play = time.monotonic()  # playback clock starts at first byte sent
        sent_s = 0.0
        for i in range(0, len(pcm), chunk_bytes):
            ahead = sent_s - (time.monotonic() - t_play)
            if ahead > PLAYBACK_LEAD_S:
                await asyncio.sleep(ahead - PLAYBACK_LEAD_S)
            chunk = pcm[i : i + chunk_bytes]
            await self.actor_ws.send_bytes(chunk)
            sent_s += len(chunk) / 2 / ACTOR_RATE_HZ
        logger.info("[tts] spoke %.1fs in %.2fs", total_s, time.monotonic() - t0)

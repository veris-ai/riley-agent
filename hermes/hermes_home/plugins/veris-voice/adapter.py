"""veris-voice — a Hermes Agent platform adapter for the Veris voice_ws channel.

One WebSocket per call: the Veris actor speaks raw PCM16 mono at 24 kHz in both
directions on ``/voice``. Inbound, this adapter endpoints the caller's turns
with an RMS VAD on the audio stream, transcribes each utterance through
Hermes's own STT stack (``tools.transcription_tools``), and injects the
transcript into the gateway as a ``MessageType.VOICE`` message — the same entry
point Hermes's Discord voice-channel loop uses. Outbound, it implements the
gateway's streaming-TTS adapter contract (``supports_streaming_tts`` et al.),
so Hermes's ``StreamingTTSConsumer`` synthesises the reply sentence-by-sentence
off the live LLM delta stream and this adapter forwards the PCM straight onto
the socket.

The plugin also registers the five BCS card tools in a ``bcs`` toolset;
``platform_toolsets`` in config.yaml restricts this platform to exactly that
set.
"""

from __future__ import annotations

import asyncio
import audioop
import contextlib
import json
import logging
import os
import tempfile
import uuid
import wave
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from gateway.config import Platform
from gateway.platforms.base import (
    AudioFormat,
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    StreamingTTSHandle,
)
from tools.transcription_tools import transcribe_audio
from tools.tts_streaming import mark_speech_interrupted, resolve_streaming_provider
from tools.tts_tool import _load_tts_config

from app.db import BCSAPI
from app.reporting import report_tool_call
from app.tools import _SCHEMAS, dispatch

logger = logging.getLogger("riley-hermes")

PLATFORM_NAME = "veris_voice"
TOOLSET = "bcs"

# Veris's voice_ws actor speaks raw PCM16 mono at 24 kHz in both directions.
# Hermes's streaming TTS providers emit exactly this format (int16 mono at
# 24 kHz), so outbound audio is a byte passthrough; inbound utterances go to
# Deepgram as 24 kHz WAVs, which it accepts directly.
ACTOR_RATE_HZ = 24000

# End of the caller's turn: this much silence, matching the 800 ms end-of-turn
# convention the other cascaded Riley implementations use. Measured in *audio*
# time — silent bytes seen — not on the wall clock (a stalled pump delivers a
# burst that a wall-clock timer would misread as a long silence).
END_OF_TURN_S = 0.8

# RMS of a PCM16 frame above which the caller counts as speaking. The actor's
# line is digitally silent between utterances (RMS 0) and its speech sits in
# the thousands, so anything clear of the noise floor separates them.
VAD_RMS_THRESHOLD = 500

# Utterances shorter than this are dropped before STT — mirrors the
# MIN_SPEECH_DURATION guard in Hermes's Discord voice-channel receiver, and
# keeps sub-word noise blips from burning an STT pass.
MIN_SPEECH_S = 0.5

# Frames kept while the line is quiet and prepended when speech starts, so the
# RMS gate doesn't clip the first phoneme. 15 frames of the actor's 20 ms
# framing = 300 ms of pre-roll.
PREROLL_FRAMES = 15

LOG_EVERY_N_FRAMES = 50

GREETING = "Thanks for calling Acme Bank, this is Riley — how can I help?"

PORT = int(os.environ.get("VERIS_VOICE_PORT", "8008"))


@dataclass
class _Call:
    """Per-connection state: one Veris actor call."""

    ws: WebSocket
    chat_id: str
    voiced: bool = False
    silence_s: float = 0.0
    utterance: bytearray = field(default_factory=bytearray)
    preroll: deque = field(default_factory=lambda: deque(maxlen=PREROLL_FRAMES))
    greeting: Optional[asyncio.Task] = None
    tts_handle: Optional[StreamingTTSHandle] = None
    stt_tasks: set = field(default_factory=set)
    n_frames: int = 0
    n_bytes: int = 0


class VerisVoiceAdapter(BasePlatformAdapter):
    """Bridges the Veris voice_ws actor protocol to the Hermes gateway."""

    # Nothing can be delivered after the socket closes; refuse background
    # delivery promises (see BasePlatformAdapter.supports_async_delivery).
    supports_async_delivery = False

    def __init__(self, config):
        super().__init__(config, Platform(PLATFORM_NAME))
        self._calls: Dict[str, _Call] = {}
        self._server: Optional[uvicorn.Server] = None
        self._serve_task: Optional[asyncio.Task] = None
        self._greeting_streamer = None
        self._session_seeded = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        api = FastAPI()

        # LLM repair proxy for the Portal's SSE tool-call bug (see llm_shim.py);
        # config.yaml's nous-key provider points at http://127.0.0.1:8008/v1.
        from .llm_shim import router as llm_router

        api.include_router(llm_router)

        @api.get("/health")
        async def health():
            return {"status": "ok"}

        @api.websocket("/voice")
        async def voice(ws: WebSocket) -> None:
            await self._handle_call(ws)

        # The server must share the gateway's event loop: the streaming-TTS
        # consumer runs there and writes to these WebSockets directly.
        self._server = uvicorn.Server(
            uvicorn.Config(api, host="0.0.0.0", port=PORT, log_level="warning")
        )
        self._serve_task = asyncio.get_running_loop().create_task(self._server.serve())
        self._mark_connected()
        logger.info("[voice] serving voice_ws on :%d/voice", PORT)
        return True

    async def disconnect(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._serve_task is not None:
            with contextlib.suppress(Exception):
                await self._serve_task
        self._mark_disconnected()

    # ------------------------------------------------------------------
    # One call: actor socket → VAD → STT → gateway
    # ------------------------------------------------------------------

    async def _handle_call(self, ws: WebSocket) -> None:
        await ws.accept()
        peer = f"{ws.client.host}:{ws.client.port}" if ws.client else "?"
        chat_id = f"call_{uuid.uuid4().hex[:8]}"
        call = _Call(ws=ws, chat_id=chat_id)
        self._calls[chat_id] = call
        # Voice replies for this chat go out through the streaming-TTS seam;
        # marking the chat here is what makes the gateway construct the
        # StreamingTTSConsumer for its turns (same gate `/voice on` flips).
        self._auto_tts_enabled_chats.add(chat_id)
        logger.info("[voice] actor connected peer=%s chat=%s", peer, chat_id)

        # HERMES_HOME is rebuilt per boot, so without this every container's
        # first call would trip the gateway's first-install onboarding (an
        # injected "introduce yourself and mention /help" note — nonsense on a
        # phone line). A steady-state deployment has prior sessions; seeding
        # one restores that condition (`has_any_sessions` wants a row besides
        # the current call's).
        if not self._session_seeded:
            self._session_store.get_or_create_session(
                self.build_source(chat_id="boot-seed", chat_type="dm", user_id="seed")
            )
            self._session_seeded = True

        # The greeting runs as a task rather than inline so that actor audio
        # is being consumed from the first frame — speaking it inline would
        # let seconds of caller audio pile up in the socket.
        call.greeting = asyncio.create_task(self._speak_greeting(call))
        call.greeting.add_done_callback(_log_task_failure)

        try:
            while True:
                frame = await ws.receive_bytes()
                call.n_frames += 1
                call.n_bytes += len(frame)
                if call.n_frames == 1:
                    logger.info("[a→stt] first frame received bytes=%d", len(frame))
                elif call.n_frames % LOG_EVERY_N_FRAMES == 0:
                    logger.info(
                        "[a→stt] received %d frames (%d bytes)",
                        call.n_frames,
                        call.n_bytes,
                    )

                if audioop.rms(frame, 2) >= VAD_RMS_THRESHOLD:
                    self._on_voice(call)
                elif call.voiced:
                    call.silence_s += len(frame) / 2 / ACTOR_RATE_HZ
                    if call.silence_s >= END_OF_TURN_S:
                        self._endpoint(call)

                if call.voiced:
                    call.utterance += frame
                else:
                    call.preroll.append(frame)
        except WebSocketDisconnect as exc:
            logger.info(
                "[a→stt] actor disconnected after %d frames (%d bytes): code=%s",
                call.n_frames,
                call.n_bytes,
                getattr(exc, "code", "?"),
            )
        except KeyError as exc:
            # Starlette raises KeyError('bytes') when it received a text frame
            # instead of a binary one — i.e. protocol mismatch on the actor side.
            logger.error(
                "[a→stt] received non-binary frame after %d binary frames — "
                "actor protocol mismatch? (%s)",
                call.n_frames,
                exc,
            )
            raise
        finally:
            if call.greeting is not None and not call.greeting.done():
                call.greeting.cancel()
            if call.tts_handle is not None:
                call.tts_handle.aborted = True
            for task in list(call.stt_tasks):
                task.cancel()
            del self._calls[chat_id]
            self._auto_tts_enabled_chats.discard(chat_id)
            logger.info("[voice] call %s ended", chat_id)

    def _on_voice(self, call: _Call) -> None:
        """A voiced frame arrived: open the turn, and cut off any reply in progress."""
        call.silence_s = 0.0
        if call.voiced:
            return
        call.voiced = True
        logger.info("[vad] caller started speaking")
        call.utterance += b"".join(call.preroll)
        call.preroll.clear()
        if call.greeting is not None and not call.greeting.done():
            logger.info("[vad] barge-in — cutting the greeting short")
            call.greeting.cancel()
        if call.tts_handle is not None and not call.tts_handle.aborted:
            logger.info("[vad] barge-in — cutting the reply short")
            # Late chunks are dropped in write_streaming_tts; Hermes's own
            # interruption note mechanism tells the next turn it was cut off.
            call.tts_handle.aborted = True
            mark_speech_interrupted()

    def _endpoint(self, call: _Call) -> None:
        """The caller has stopped: hand the buffered utterance to STT."""
        call.voiced = False
        call.silence_s = 0.0
        pcm = bytes(call.utterance)
        call.utterance.clear()
        duration_s = len(pcm) / 2 / ACTOR_RATE_HZ
        if duration_s < MIN_SPEECH_S:
            logger.info("[vad] utterance too short (%.2fs) — dropped", duration_s)
            return
        logger.info("[vad] end of turn — %.2fs of audio to STT", duration_s)
        task = asyncio.create_task(self._transcribe_and_dispatch(call, pcm))
        call.stt_tasks.add(task)
        task.add_done_callback(call.stt_tasks.discard)
        task.add_done_callback(_log_task_failure)

    async def _transcribe_and_dispatch(self, call: _Call, pcm: bytes) -> None:
        fd, wav_path = tempfile.mkstemp(prefix="veris-utterance-", suffix=".wav")
        try:
            with os.fdopen(fd, "wb") as f, wave.open(f, "wb") as w:
                w.setnchannels(1)
                w.setsampwidth(2)
                w.setframerate(ACTOR_RATE_HZ)
                w.writeframes(pcm)
            result = await asyncio.to_thread(transcribe_audio, wav_path)
        finally:
            Path(wav_path).unlink(missing_ok=True)

        if not result.get("success"):
            raise RuntimeError(f"STT failed: {result.get('error')}")
        transcript = (result.get("transcript") or "").strip()
        if not transcript:
            logger.info("[stt] dropped empty transcript")
            return

        logger.info("actor_said: %s", transcript)
        event = MessageEvent(
            text=transcript,
            message_type=MessageType.VOICE,
            source=self.build_source(
                chat_id=call.chat_id,
                chat_type="dm",
                user_id="caller",
                user_name="Caller",
            ),
        )
        await self.handle_message(event)

    # ------------------------------------------------------------------
    # Outbound audio
    # ------------------------------------------------------------------

    def _streamer(self):
        """The configured streaming TTS provider (used only for the greeting;
        agent replies go through the gateway's StreamingTTSConsumer)."""
        if self._greeting_streamer is None:
            streamer = resolve_streaming_provider(_load_tts_config())
            if streamer is None:
                raise RuntimeError(
                    "no streaming TTS provider is usable — this bridge requires "
                    "one (set tts.provider to a streaming-capable provider in "
                    "config.yaml and export its API key)"
                )
            _require_actor_format(
                AudioFormat(streamer.sample_rate, streamer.channels, streamer.sample_width)
            )
            self._greeting_streamer = streamer
        return self._greeting_streamer

    async def _speak_greeting(self, call: _Call) -> None:
        streamer = self._streamer()
        logger.info("[tts] speaking greeting")
        iterator = iter(streamer.stream(GREETING))
        while True:
            has_chunk, chunk = await asyncio.to_thread(_next_chunk, iterator)
            if not has_chunk:
                break
            if chunk:
                await call.ws.send_bytes(chunk)
        logger.info("agent_said: %s", GREETING)

    def supports_streaming_tts(self, chat_id: str, audio_format: AudioFormat) -> bool:
        if chat_id not in self._calls:
            return False
        _require_actor_format(audio_format)
        return True

    async def begin_streaming_tts(
        self,
        chat_id: str,
        audio_format: AudioFormat,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[StreamingTTSHandle]:
        call = self._calls.get(chat_id)
        if call is None:
            return None  # call ended while the turn was still processing
        handle = StreamingTTSHandle(chat_id=chat_id, audio_format=audio_format)
        call.tts_handle = handle
        logger.info("[tts] reply stream opened chat=%s", chat_id)
        return handle

    async def write_streaming_tts(self, handle: StreamingTTSHandle, chunk: bytes) -> None:
        if handle.aborted:
            return
        call = self._calls.get(handle.chat_id)
        if call is None:
            handle.aborted = True
            return
        await call.ws.send_bytes(chunk)

    async def finish_streaming_tts(
        self, handle: StreamingTTSHandle, *, interrupted: bool = False
    ) -> None:
        call = self._calls.get(handle.chat_id)
        if call is not None and call.tts_handle is handle:
            call.tts_handle = None
        logger.info(
            "[tts] reply stream %s",
            "interrupted" if (interrupted or handle.aborted) else "finished",
        )

    async def abort_streaming_tts(
        self, handle: StreamingTTSHandle, error: Optional[str] = None
    ) -> None:
        handle.aborted = True
        call = self._calls.get(handle.chat_id)
        if call is not None and call.tts_handle is handle:
            call.tts_handle = None
        if error:
            logger.warning("[tts] reply stream aborted: %s", error)

    async def play_tts(self, chat_id: str, audio_path: str, **kwargs) -> SendResult:
        # The gateway falls back here when streaming TTS could not deliver a
        # turn. There is no whole-file path to the actor — fail loudly so a
        # misconfigured TTS provider shows up as a silent, failing call, not a
        # quietly degraded one.
        logger.error(
            "[tts] whole-file TTS fallback invoked (chat=%s) — streaming TTS "
            "failed and no audio was delivered for this turn",
            chat_id,
        )
        return SendResult(success=False, error="no whole-file audio path on veris_voice")

    # ------------------------------------------------------------------
    # Text delivery (transcript log only)
    # ------------------------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        # The reply reaches the caller as audio through the streaming-TTS
        # seam; the gateway's text delivery lands here for the log.
        logger.info("agent_said: %s", content)
        return SendResult(success=True, message_id=uuid.uuid4().hex)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"id": chat_id, "type": "dm", "name": "Veris voice call"}


def _require_actor_format(audio_format: AudioFormat) -> None:
    got = (audio_format.sample_rate, audio_format.channels, audio_format.sample_width)
    if got != (ACTOR_RATE_HZ, 1, 2):
        raise RuntimeError(
            f"TTS stream format {got} is not PCM16 mono at {ACTOR_RATE_HZ} Hz — "
            "the actor protocol is fixed; configure a 24 kHz streaming TTS provider"
        )


def _next_chunk(iterator) -> tuple:
    try:
        return True, next(iterator)
    except StopIteration:
        return False, None


def _log_task_failure(task: asyncio.Task) -> None:
    if not task.cancelled() and task.exception() is not None:
        logger.error("[voice] background task failed", exc_info=task.exception())


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------

_api_instance: Optional[BCSAPI] = None


def _api() -> BCSAPI:
    global _api_instance
    if _api_instance is None:
        _api_instance = BCSAPI()
    return _api_instance


def _make_handler(name: str):
    def handler(args: dict, **kwargs):
        args = args or {}
        try:
            result = dispatch(_api(), name, args)
        except Exception as exc:
            report_tool_call(name, args, {"error": str(exc)})
            raise
        report_tool_call(name, args, result)
        # Hermes's registry result contract takes strings, not dicts.
        return json.dumps(result, default=str)

    return handler


def register(ctx) -> None:
    """Plugin entry point: called by the Hermes plugin system."""
    from .stt_deepgram import DeepgramSTT

    ctx.register_transcription_provider(DeepgramSTT())
    for schema in _SCHEMAS:
        ctx.register_tool(
            name=schema["name"],
            toolset=TOOLSET,
            schema=schema,
            handler=_make_handler(schema["name"]),
        )
    ctx.register_platform(
        name=PLATFORM_NAME,
        label="Veris Voice",
        adapter_factory=lambda cfg: VerisVoiceAdapter(cfg),
        check_fn=lambda: True,
        install_hint="Installed with riley-hermes (fastapi + uvicorn).",
    )

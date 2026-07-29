"""voice_ws bridge for the LiveKit cascaded agent.

Tiny FastAPI app that bridges a raw PCM16/24 kHz WebSocket at ``/voice`` into a
LiveKit room, so a ``voice_ws`` actor channel can talk to the LiveKit-based
agent. The voice agent itself is a separate ``AgentServer`` worker
(``app.agent``) that auto-dispatches into every room; this bridge joins a room
as the caller participant and the worker is dispatched in alongside it.
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
import time
from contextlib import suppress

from fastapi import FastAPI, WebSocket
from livekit import api, rtc
from starlette.websockets import WebSocketDisconnect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("riley-livekit-bridge")

# The /voice bridge dials the in-container LiveKit server.
INTERNAL_LIVEKIT_URL = os.environ.get("LIVEKIT_URL", "ws://localhost:7880")

# voice_ws wire format: PCM16 / 24 kHz / mono, 20 ms frames.
SAMPLE_RATE_HZ = 24000
NUM_CHANNELS = 1
FRAME_DURATION_MS = 20
FRAME_SAMPLES = SAMPLE_RATE_HZ * FRAME_DURATION_MS // 1000  # 480
FRAME_BYTES = FRAME_SAMPLES * 2 * NUM_CHANNELS  # 960
LOG_EVERY_N_FRAMES = 50

app = FastAPI(title="riley-livekit", version="0.1.0")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


def _mint_bridge_token(room_name: str, identity: str) -> str:
    grant = api.VideoGrants(
        room_join=True,
        room=room_name,
        can_publish=True,
        can_subscribe=True,
        can_publish_data=True,
    )
    return (
        api.AccessToken(os.environ["LIVEKIT_API_KEY"], os.environ["LIVEKIT_API_SECRET"])
        .with_identity(identity)
        .with_name(identity)
        .with_grants(grant)
        .to_jwt()
    )


@app.websocket("/voice")
async def voice_bridge(actor_ws: WebSocket) -> None:
    """Bridge a raw PCM16/24 kHz WS into a LiveKit room.

    Each connection spins up its own room (``veris-<rand>``), joins as
    ``veris-actor``, publishes the incoming actor audio as a mic track, and
    forwards the agent's audio back to the actor — re-sliced to 20 ms frames.
    """
    await actor_ws.accept()
    peer = f"{actor_ws.client.host}:{actor_ws.client.port}" if actor_ws.client else "?"
    room_name = f"veris-{secrets.token_hex(4)}"
    identity = "veris-actor"
    logger.info("[voice] actor connected peer=%s room=%s url=%s", peer, room_name, INTERNAL_LIVEKIT_URL)

    room = rtc.Room()
    t_start = time.monotonic()
    agent_audio_ready = asyncio.Event()
    agent_audio = {"track": None}

    @room.on("participant_connected")
    def _on_participant(participant: rtc.RemoteParticipant) -> None:
        logger.info("[voice] agent participant joined identity=%s", participant.identity)

    @room.on("track_subscribed")
    def _on_track(track: rtc.Track, publication: rtc.RemoteTrackPublication, participant: rtc.RemoteParticipant) -> None:
        logger.info("[voice] track_subscribed kind=%s from=%s", track.kind, participant.identity)
        if track.kind == rtc.TrackKind.KIND_AUDIO:
            agent_audio["track"] = track
            agent_audio_ready.set()

    token = _mint_bridge_token(room_name, identity)
    try:
        await room.connect(INTERNAL_LIVEKIT_URL, token)
        logger.info("[voice] connected to room=%s", room_name)

        source = rtc.AudioSource(SAMPLE_RATE_HZ, NUM_CHANNELS)
        mic_track = rtc.LocalAudioTrack.create_audio_track("veris-mic", source)
        publication = await room.local_participant.publish_track(
            mic_track,
            rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE),
        )
        logger.info("[voice] published mic track sid=%s", publication.sid)

        t_actor = asyncio.create_task(_pump_actor_to_room(actor_ws, source), name="actor→room")
        t_agent = asyncio.create_task(
            _pump_room_to_actor(actor_ws, agent_audio_ready, lambda: agent_audio["track"]),
            name="room→actor",
        )
        done, pending = await asyncio.wait({t_actor, t_agent}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        for task in done:
            exc = task.exception()
            if exc is not None:
                logger.error("[voice] pump %s failed: %s", task.get_name(), exc)
    except Exception:
        logger.exception("[voice] bridge handler failed")
    finally:
        with suppress(Exception):
            await room.disconnect()
        with suppress(Exception):
            await actor_ws.close()
        elapsed = time.monotonic() - t_start
        logger.info("[voice] bridge exit room=%s duration=%.1fs", room_name, elapsed)


async def _pump_actor_to_room(actor_ws: WebSocket, source: rtc.AudioSource) -> None:
    """Forward PCM16 frames from the actor WS into the LiveKit room.

    Frames are passed through as-is — LiveKit's ``AudioSource`` accepts
    variable-size frames.
    """
    n_frames = 0
    n_bytes = 0
    try:
        while True:
            data = await actor_ws.receive_bytes()
            samples = len(data) // (2 * NUM_CHANNELS)
            frame = rtc.AudioFrame(data, SAMPLE_RATE_HZ, NUM_CHANNELS, samples)
            await source.capture_frame(frame)
            if n_frames == 0:
                logger.info("[a→r] first frame bytes=%d", len(data))
            n_frames += 1
            n_bytes += len(data)
            if n_frames % LOG_EVERY_N_FRAMES == 0:
                logger.info("[a→r] forwarded %d frames (%d bytes)", n_frames, n_bytes)
    except WebSocketDisconnect as exc:
        logger.info(
            "[a→r] actor disconnected after %d frames (%d bytes): code=%s",
            n_frames,
            n_bytes,
            getattr(exc, "code", "?"),
        )
    except KeyError as exc:
        logger.error(
            "[a→r] received non-binary frame after %d binary frames — actor protocol mismatch? (%s)",
            n_frames,
            exc,
        )


async def _pump_room_to_actor(actor_ws: WebSocket, agent_audio_ready: asyncio.Event, get_track) -> None:
    """Forward the agent's audio track back to the actor as PCM16 frames.

    LiveKit's ``AudioStream`` typically emits 10 ms frames (480 bytes at
    24 kHz mono). The actor expects 20 ms frames, so we buffer and re-slice
    to ``FRAME_BYTES`` boundaries before sending.
    """
    try:
        await asyncio.wait_for(agent_audio_ready.wait(), 30)
    except asyncio.TimeoutError:
        logger.error("[r→a] agent audio track never subscribed within 30s")
        return
    track = get_track()
    if track is None:
        logger.error("[r→a] agent_audio_ready set but track is None")
        return

    stream = rtc.AudioStream(track, sample_rate=SAMPLE_RATE_HZ, num_channels=NUM_CHANNELS)
    buf = bytearray()
    n_frames = 0
    n_bytes = 0
    in_frames = 0
    try:
        async for ev in stream:
            data = bytes(ev.frame.data)
            if in_frames == 0:
                logger.info(
                    "[r→a] first AudioStream frame bytes=%d (will re-slice to %d-byte frames)",
                    len(data),
                    FRAME_BYTES,
                )
            in_frames += 1
            buf.extend(data)
            while len(buf) >= FRAME_BYTES:
                chunk = bytes(buf[:FRAME_BYTES])
                del buf[:FRAME_BYTES]
                await actor_ws.send_bytes(chunk)
                if n_frames == 0:
                    logger.info("[r→a] emitted first 20 ms frame to actor")
                n_frames += 1
                n_bytes += len(chunk)
                if n_frames % LOG_EVERY_N_FRAMES == 0:
                    logger.info(
                        "[r→a] forwarded %d 20-ms frames (%d bytes) from %d AudioStream frames",
                        n_frames,
                        n_bytes,
                        in_frames,
                    )
    except WebSocketDisconnect:
        logger.info("[r→a] actor WS closed during pump (%d frames, %d bytes)", n_frames, n_bytes)
    finally:
        if buf:
            pad = FRAME_BYTES - len(buf)
            chunk = bytes(buf) + bytes(pad)
            with suppress(Exception):
                await actor_ws.send_bytes(chunk)
            logger.info("[r→a] flushed final partial frame (%d real bytes + %d silence)", len(buf), pad)
        with suppress(Exception):
            await stream.aclose()

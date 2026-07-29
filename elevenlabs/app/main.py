"""riley-elevenlabs — Riley card-support voice agent over an ElevenLabs voice WS.

Listens on /voice for a single bidirectional PCM16 stream from the Veris
actor. For each connection, opens its own `AsyncConversation` against the
ElevenLabs Agents platform, registers the five BCS client tools, and bridges
audio in/out via a custom AsyncAudioInterface. The agent speaks first
(configured via `first_message` on the agent at creation time). Tool calls are
dispatched against `BCSAPI`, which talks to postgres.

Logging is intentionally chatty so it's obvious from agent.log alone whether
audio is flowing, how the agent is responding, and where things stall.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import suppress
from typing import Awaitable, Callable, Optional

from dotenv import load_dotenv
from elevenlabs import ElevenLabs
from elevenlabs.conversational_ai.conversation import (
    AsyncAudioInterface,
    AsyncConversation,
    ClientTools,
)
from fastapi import FastAPI, WebSocket
from starlette.websockets import WebSocketDisconnect

from .agent_setup import ensure_agent
from .db import BCSAPI
from .reporting import report_tool_call
from .tools import dispatch


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# How often to emit periodic frame-count log lines from the two pumps. At
# 50 fps (20 ms/frame) this is roughly one heartbeat per second.
LOG_EVERY_N_FRAMES = 50


app = FastAPI(title="riley-elevenlabs", version="0.1.0")


@app.on_event("startup")
async def _startup() -> None:
    """Build the shared ElevenLabs client and (if pinned) record the agent ID.

    Agent provisioning is deferred to the first /voice call so the server
    can boot for /health checks even without an API key in the environment.
    """
    api_key = os.environ.get("ELEVENLABS_API_KEY")
    app.state.eleven = ElevenLabs(api_key=api_key) if api_key else None
    app.state.agent_id = os.environ.get("AGENT_ID")
    if not api_key:
        logger.warning("[startup] ELEVENLABS_API_KEY not set — /voice will fail until you set it")
    elif app.state.agent_id:
        logger.info("[startup] ready — pinned AGENT_ID=%s", app.state.agent_id)
    else:
        logger.info("[startup] ready — agent will be provisioned on first /voice call")


def _resolve_agent(app_state) -> tuple[ElevenLabs, str]:
    """Return (client, agent_id), provisioning the agent lazily if needed."""
    if app_state.eleven is None:
        raise RuntimeError("ELEVENLABS_API_KEY is not set")
    if app_state.agent_id is None:
        app_state.agent_id = ensure_agent(app_state.eleven)
    return app_state.eleven, app_state.agent_id


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


class WebSocketAudioInterface(AsyncAudioInterface):
    """Bridges audio between the actor WebSocket and ElevenLabs.

    `output()` is called by the SDK with PCM16 bytes from ElevenLabs — we
    forward them straight to the actor. `start()` hands us back the SDK's
    `input_callback`, which we then drive from the FastAPI handler each time
    a frame arrives on the actor WS.
    """

    def __init__(self, actor_ws: WebSocket) -> None:
        self.actor_ws = actor_ws
        self.input_callback: Optional[Callable[[bytes], Awaitable[None]]] = None
        self._stopped = False
        self.n_in = 0
        self.n_out = 0
        self.bytes_in = 0
        self.bytes_out = 0

    async def start(self, input_callback: Callable[[bytes], Awaitable[None]]) -> None:
        self.input_callback = input_callback
        logger.info("[audio] interface started — waiting for actor frames")

    async def stop(self) -> None:
        self._stopped = True
        logger.info(
            "[audio] interface stopped — in=%d frames/%d bytes, out=%d frames/%d bytes",
            self.n_in, self.bytes_in, self.n_out, self.bytes_out,
        )

    async def output(self, audio: bytes) -> None:
        if self._stopped:
            return
        self.n_out += 1
        self.bytes_out += len(audio)
        if self.n_out == 1:
            logger.info("[el→a] first audio chunk bytes=%d", len(audio))
        elif self.n_out % LOG_EVERY_N_FRAMES == 0:
            logger.info(
                "[el→a] streamed %d chunks (%d bytes) so far this session",
                self.n_out, self.bytes_out,
            )

        try:
            await self.actor_ws.send_bytes(audio)
        except Exception as exc:
            logger.warning("[el→a] actor send failed (%s); stopping output", exc)
            self._stopped = True

    async def interrupt(self) -> None:
        # User barge-in. ElevenLabs has already told us to stop voicing —
        # nothing is buffered on our side (we forward straight through).
        logger.info("[el→a] interrupt")

    async def push_actor_audio(self, frame: bytes) -> None:
        """Forward a PCM16 frame from the actor → ElevenLabs."""
        if self.input_callback is None or self._stopped:
            return
        self.n_in += 1
        self.bytes_in += len(frame)
        if self.n_in == 1:
            logger.info("[a→el] first actor frame bytes=%d", len(frame))
        elif self.n_in % LOG_EVERY_N_FRAMES == 0:
            logger.info(
                "[a→el] forwarded %d frames (%d bytes) so far this session",
                self.n_in, self.bytes_in,
            )
        await self.input_callback(frame)


def _build_client_tools(api: BCSAPI, loop: asyncio.AbstractEventLoop) -> ClientTools:
    """Register the five BCS tools against a ClientTools instance.

    Each handler is a sync callable. The SDK runs sync handlers in a thread
    pool, so blocking on psycopg2 is fine. We strip the `tool_call_id` ElevenLabs
    injects into the parameters before passing them to `dispatch`.
    """
    tools = ClientTools(loop=loop)

    def _wrap(name: str):
        def _handler(parameters: dict):
            args = {k: v for k, v in parameters.items() if k != "tool_call_id"}
            logger.info("[tool] %s args=%s", name, args)
            try:
                result = dispatch(api, name, args)
                logger.info("[tool] %s result=%s", name, str(result)[:200])
            except Exception as exc:
                logger.exception("[tool] %s failed", name)
                result = {"error": str(exc)}
            report_tool_call(name, args, result)
            # ElevenLabs's `client_tool_result.result` is a string field; if we
            # return the dict directly the orchestrator rejects the frame with
            # `1008 policy violation`. `default=str` handles enums + datetimes
            # that aren't natively JSON-serializable.
            return json.dumps(result, default=str)
        return _handler

    for tool_name in (
        "display_user_info",
        "display_card_info_by_last4",
        "change_card_status",
        "request_card_replacement",
        "update_card_replacement_status",
    ):
        tools.register(tool_name, _wrap(tool_name))
    return tools


@app.websocket("/voice")
async def voice(actor_ws: WebSocket) -> None:
    """One actor connection ↔ one ElevenLabs Conversation with BCS tools."""
    await actor_ws.accept()
    peer = f"{actor_ws.client.host}:{actor_ws.client.port}" if actor_ws.client else "?"
    logger.info("[voice] actor connected peer=%s", peer)

    try:
        eleven, agent_id = _resolve_agent(app.state)
    except RuntimeError as exc:
        logger.error("[voice] cannot accept call: %s", exc)
        await actor_ws.close(code=1011, reason=str(exc))
        return
    api = BCSAPI()

    loop = asyncio.get_running_loop()
    audio_interface = WebSocketAudioInterface(actor_ws)
    client_tools = _build_client_tools(api, loop)

    async def _on_agent_response(text: str) -> None:
        logger.info("[el→a] agent_said: %s", text[:200])

    async def _on_user_transcript(text: str) -> None:
        logger.info("[a→el] actor_said: %s", text[:200])

    async def _on_latency(ms: int) -> None:
        logger.info("[el] latency=%d ms", ms)

    conversation = AsyncConversation(
        client=eleven,
        agent_id=agent_id,
        requires_auth=True,
        audio_interface=audio_interface,
        client_tools=client_tools,
        callback_agent_response=_on_agent_response,
        callback_user_transcript=_on_user_transcript,
        callback_latency_measurement=_on_latency,
    )

    t_start = time.monotonic()
    try:
        await conversation.start_session()
        logger.info("[voice] ElevenLabs session started agent_id=%s", agent_id)
        try:
            while True:
                frame = await actor_ws.receive_bytes()
                await audio_interface.push_actor_audio(frame)
        except WebSocketDisconnect as exc:
            logger.info(
                "[voice] actor disconnected after %d frames (%d bytes): code=%s",
                audio_interface.n_in, audio_interface.bytes_in,
                getattr(exc, "code", "?"),
            )
        except KeyError as exc:
            # Starlette raises KeyError('bytes') when it received a text frame
            # instead of a binary one — i.e. protocol mismatch on the actor side.
            logger.error(
                "[voice] received non-binary frame after %d binary frames — "
                "actor protocol mismatch? (%s)", audio_interface.n_in, exc,
            )
    except Exception as exc:
        logger.exception("[voice] handler failed: %s", exc)
    finally:
        with suppress(Exception):
            await conversation.end_session()
        with suppress(Exception):
            await conversation.wait_for_session_end()
        elapsed = time.monotonic() - t_start
        logger.info("[voice] handler exit duration=%.1fs", elapsed)

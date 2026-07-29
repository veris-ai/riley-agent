"""Web/transport server for riley-nemotron.

FastAPI app exposing the Veris ``voice_ws`` transport onto the Pipecat
pipeline: ``WS /voice`` accepts raw PCM16/24 kHz mono binary frames (the wire
protocol Veris's ``voice_ws`` actor channel speaks). The handler hands the
WebSocket to ``app.agent.run_voice_ws_bot`` which wraps it in a
``FastAPIWebsocketTransport`` + ``RawPCM16Serializer`` and runs the pipeline.

No SFU, no WebRTC — audio terminates inside this process.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI, WebSocket

from .agent import run_voice_ws_bot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("riley-nemotron-web")


app = FastAPI(title="riley-nemotron", version="0.1.0")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.websocket("/voice")
async def voice(websocket: WebSocket) -> None:
    """Veris ``voice_ws`` actor channel.

    Raw PCM16/24 kHz mono binary in both directions. The FastAPI WS handler
    is already its own task per connection, and ``run_voice_ws_bot`` blocks
    until the WS closes or the runner exits.
    """
    peer = (
        f"{websocket.client.host}:{websocket.client.port}"
        if websocket.client
        else "?"
    )
    logger.info("[web] /voice connection peer=%s", peer)
    await websocket.accept()
    try:
        await run_voice_ws_bot(websocket)
    finally:
        logger.info("[web] /voice connection closed peer=%s", peer)

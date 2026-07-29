"""riley-openai-realtime — Riley card-ops voice agent over OpenAI Realtime WS.

Listens on /voice for a single bidirectional PCM16 stream from the Veris
actor. For each connection, opens its own OpenAI Realtime session, registers
the BCS function tools, and bridges audio + tool calls. The agent speaks
first. Tool calls are dispatched against `BCSAPI`, which talks to postgres.

Logging is intentionally chatty so it's obvious from agent.log alone whether
audio is flowing, how Realtime is responding, and where things stall.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
from contextlib import suppress
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


REALTIME_MODEL = os.environ.get("REALTIME_MODEL", "gpt-realtime-2.1-mini")
REALTIME_URL = f"wss://api.openai.com/v1/realtime?model={REALTIME_MODEL}"
REALTIME_VOICE = os.environ.get("REALTIME_VOICE", "alloy")
SAMPLE_RATE_HZ = 24000

# How often to emit periodic frame-count log lines from the two pumps. At
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


AGENT_PROMPT = _load_agent_prompt()


app = FastAPI(title="riley-openai-realtime", version="0.1.0")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.websocket("/voice")
async def voice(actor_ws: WebSocket) -> None:
    """One actor connection ↔ one OpenAI Realtime session with BCS tools.

    Logs every state transition so it's possible to debug stalls from
    agent.log without correlating with proxy / sandbox logs.
    """
    await actor_ws.accept()
    peer = f"{actor_ws.client.host}:{actor_ws.client.port}" if actor_ws.client else "?"
    logger.info("[voice] actor connected peer=%s", peer)
    api = BCSAPI()
    oa_headers = {"Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}"}

    t_start = time.monotonic()
    try:
        async with websockets.connect(REALTIME_URL, additional_headers=oa_headers) as oa_ws:
            logger.info(
                "[voice] OA Realtime connected model=%s voice=%s rate=%d Hz",
                REALTIME_MODEL, REALTIME_VOICE, SAMPLE_RATE_HZ,
            )
            await _configure_session(oa_ws)
            # Agent speaks first — call etiquette. The system prompt tells the
            # model the greeting already happened, so force it on this opening
            # turn with per-response instructions, which override the session
            # instructions for that one response.
            await oa_ws.send(json.dumps({
                "type": "response.create",
                "response": {
                    "instructions": (
                        "Open the call by greeting the caller now. Say exactly: "
                        "\"Thanks for calling Acme Bank, this is Riley, how can I "
                        "help?\" Then stop and wait for the caller to respond."
                    ),
                },
            }))
            logger.info("[voice] sent greeting trigger (agent greets first)")

            # Run both pumps. If either raises (or returns), cancel the
            # other so we don't leak a half-dead WS pair.
            t1 = asyncio.create_task(
                _pump_actor_to_oa(actor_ws, oa_ws), name="actor→oa"
            )
            t2 = asyncio.create_task(
                _pump_oa_to_actor(oa_ws, actor_ws, api), name="oa→actor"
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
    except Exception as exc:
        logger.exception("[voice] handler failed: %s", exc)
    finally:
        elapsed = time.monotonic() - t_start
        logger.info("[voice] handler exit duration=%.1fs", elapsed)


async def _configure_session(oa_ws) -> None:
    payload = {
        "type": "session.update",
        "session": {
            "type": "realtime",
            "output_modalities": ["audio"],
            "instructions": AGENT_PROMPT,
            "audio": {
                "input": {
                    "format": {"type": "audio/pcm", "rate": SAMPLE_RATE_HZ},
                    # 800 ms end-of-turn silence via OpenAI's native server_vad.
                    # threshold/prefix_padding_ms are pinned explicitly so they
                    # are not silently defaulted. server_vad is pure silence-
                    # duration (not semantic).
                    "turn_detection": {
                        "type": "server_vad",
                        "silence_duration_ms": 800,
                        "threshold": 0.5,
                        "prefix_padding_ms": 300,
                    },
                },
                "output": {
                    "format": {"type": "audio/pcm", "rate": SAMPLE_RATE_HZ},
                    "voice": REALTIME_VOICE,
                },
            },
            "tools": TOOLS,
        },
    }
    await oa_ws.send(json.dumps(payload))
    logger.info(
        "[voice] session.update sent: %d tools, instructions=%d chars",
        len(TOOLS), len(AGENT_PROMPT),
    )


async def _pump_actor_to_oa(actor_ws: WebSocket, oa_ws) -> None:
    """Binary PCM16 frames from the actor → Realtime input buffer."""
    n_frames = 0
    n_bytes = 0
    try:
        while True:
            frame = await actor_ws.receive_bytes()
            n_frames += 1
            n_bytes += len(frame)
            if n_frames == 1:
                logger.info("[a→oa] first frame received bytes=%d", len(frame))
            elif n_frames % LOG_EVERY_N_FRAMES == 0:
                logger.info("[a→oa] forwarded %d frames (%d bytes)", n_frames, n_bytes)
            await oa_ws.send(json.dumps({
                "type": "input_audio_buffer.append",
                "audio": base64.b64encode(frame).decode(),
            }))
    except WebSocketDisconnect as exc:
        logger.info(
            "[a→oa] actor disconnected after %d frames (%d bytes): code=%s",
            n_frames, n_bytes, getattr(exc, "code", "?"),
        )
    except KeyError as exc:
        # Starlette raises KeyError('bytes') when it received a text frame
        # instead of a binary one — i.e. protocol mismatch on the actor side.
        logger.error(
            "[a→oa] received non-binary frame after %d binary frames — "
            "actor protocol mismatch? (%s)", n_frames, exc,
        )
        raise
    except Exception as exc:
        logger.exception(
            "[a→oa] pump died after %d frames (%d bytes): %s",
            n_frames, n_bytes, exc,
        )
        raise


async def _pump_oa_to_actor(oa_ws, actor_ws: WebSocket, api: BCSAPI) -> None:
    """Realtime events → audio bytes back to the actor; dispatch tool calls.

    STREAMING: the first output item of each response is forwarded to the actor
    delta-by-delta as OA generates it — no whole-turn buffering — so the actor
    hears the reply onset immediately. This is how OA Realtime is meant to be
    consumed, and keeps response latency reflecting the model rather than this
    harness (whole-turn buffering previously added ~3 s).

    Any *additional* items in the same response are buffered and transcript-
    deduped before forwarding, preserving the fix for OA occasionally emitting
    the same item twice within one response (observed in
    sim_2vjfg1r5awp4mphrgy5d0: greeting audio played twice). The common case is
    one item per response, which streams with zero buffering.
    """
    pending_tool_response = False
    n_audio_frames = 0
    n_audio_bytes = 0
    n_responses = 0

    # Per-response state — reset on response.created.
    live_item_id: str | None = None        # first audio item this response; streamed live
    items: dict[str, dict] = {}            # buffered ADDITIONAL items (dedup path)
    forwarded_transcripts: set[str] = set()
    forwarded_count = 0

    def _new_item() -> dict:
        return {
            "audio": bytearray(),
            "transcript": "",
            "audio_done": False,
            "transcript_done": False,
            "decision": None,
        }

    async def _maybe_forward(item_id: str) -> None:
        nonlocal forwarded_count
        item = items[item_id]
        if not (item["audio_done"] and item["transcript_done"]):
            return
        if item["decision"] is not None:
            return
        text = item["transcript"]
        if text and text in forwarded_transcripts:
            item["decision"] = "drop"
            logger.warning(
                "[oa→a] dropped duplicate item %s (same transcript as earlier item this response): %s",
                item_id, text[:120],
            )
            return
        item["decision"] = "forward"
        forwarded_transcripts.add(text)
        forwarded_count += 1
        audio_bytes = bytes(item["audio"])
        if audio_bytes:
            await actor_ws.send_bytes(audio_bytes)
        logger.info(
            "[oa→a] agent_said (item %s, %d bytes): %s",
            item_id, len(audio_bytes), text[:200],
        )

    try:
        async for raw in oa_ws:
            evt = json.loads(raw)
            etype = evt.get("type", "")

            if etype == "response.output_audio.delta":
                item_id = evt.get("item_id", "default")
                audio = base64.b64decode(evt.get("delta", ""))
                n_audio_frames += 1
                n_audio_bytes += len(audio)
                if n_audio_frames == 1:
                    logger.info("[oa→a] first audio delta bytes=%d (item %s)", len(audio), item_id)
                elif n_audio_frames % LOG_EVERY_N_FRAMES == 0:
                    logger.info(
                        "[oa→a] streamed %d audio deltas (%d bytes) so far this session",
                        n_audio_frames, n_audio_bytes,
                    )
                if live_item_id is None:
                    live_item_id = item_id
                    forwarded_count += 1
                    logger.info("[oa→a] streaming live item %s", item_id)
                if item_id == live_item_id:
                    # Stream straight to the actor — no buffering.
                    if audio:
                        await actor_ws.send_bytes(audio)
                else:
                    # A second item in the same response — buffer for dedup.
                    items.setdefault(item_id, _new_item())["audio"].extend(audio)

            elif etype == "response.output_audio.done":
                item_id = evt.get("item_id", "default")
                if item_id == live_item_id:
                    continue  # already streamed delta-by-delta
                item = items.setdefault(item_id, _new_item())
                item["audio_done"] = True
                await _maybe_forward(item_id)

            elif etype == "response.output_audio_transcript.done":
                item_id = evt.get("item_id", "default")
                text = (evt.get("transcript") or "").strip()
                if item_id == live_item_id:
                    # Record the streamed transcript so a duplicate later item
                    # in this response is dropped by _maybe_forward.
                    if text:
                        forwarded_transcripts.add(text)
                        logger.info("[oa→a] agent_said (live item %s): %s", item_id, text[:200])
                    continue
                item = items.setdefault(item_id, _new_item())
                item["transcript"] = text
                item["transcript_done"] = True
                await _maybe_forward(item_id)

            elif etype == "conversation.item.input_audio_transcription.completed":
                # What our OA Realtime transcribed from the actor's audio.
                # If this never fires after the actor speaks, our VAD didn't
                # commit and the actor's silence trailer is missing.
                text = (evt.get("transcript") or "").strip()
                if text:
                    logger.info("[oa→a] actor_said: %s", text[:200])

            elif etype == "input_audio_buffer.speech_started":
                logger.info("[oa→a] vad: speech_started")

            elif etype == "input_audio_buffer.speech_stopped":
                logger.info("[oa→a] vad: speech_stopped")

            elif etype == "input_audio_buffer.committed":
                logger.info("[oa→a] vad: buffer committed → response generation")

            elif etype == "response.created":
                n_responses += 1
                # Reset per-response streaming + dedupe state.
                live_item_id = None
                items.clear()
                forwarded_transcripts.clear()
                forwarded_count = 0
                logger.info("[oa→a] response.created #%d", n_responses)

            elif etype == "response.function_call_arguments.done":
                pending_tool_response = True
                name = evt.get("name", "")
                call_id = evt.get("call_id", "")
                args_raw = evt.get("arguments") or "{}"
                logger.info("[oa→a] tool_call: %s args=%s", name, str(args_raw)[:200])
                args: dict = {}
                try:
                    args = json.loads(args_raw) if isinstance(args_raw, str) else dict(args_raw)
                    output = dispatch(api, name, args)
                    logger.info(
                        "[oa→a] tool_result %s: %s",
                        name, json.dumps(output, default=str)[:200],
                    )
                except Exception as exc:  # surface errors to the model
                    logger.exception("[oa→a] tool %s failed", name)
                    output = {"error": str(exc)}

                report_tool_call(name, args, output)

                await oa_ws.send(json.dumps({
                    "type": "conversation.item.create",
                    "item": {
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": json.dumps(output, default=str),
                    },
                }))

            elif etype == "response.done":
                if pending_tool_response:
                    pending_tool_response = False
                    logger.info("[oa→a] response.done after tool — kicking next response.create")
                    # Continue the turn now that tool outputs are in context.
                    await oa_ws.send(json.dumps({"type": "response.create"}))
                else:
                    logger.info(
                        "[oa→a] response.done — %d item(s) forwarded; session totals %d frames / %d bytes",
                        forwarded_count, n_audio_frames, n_audio_bytes,
                    )

            elif etype == "error":
                logger.error("[oa→a] OpenAI Realtime error: %s", evt.get("error"))
                raise RuntimeError(f"OpenAI Realtime error: {evt.get('error')}")
    except websockets.ConnectionClosed as exc:
        logger.info("[oa→a] OA WS closed code=%s reason=%s", exc.code, exc.reason)
    except Exception as exc:
        logger.exception(
            "[oa→a] pump died after %d audio frames (%d bytes), %d responses: %s",
            n_audio_frames, n_audio_bytes, n_responses, exc,
        )
        raise

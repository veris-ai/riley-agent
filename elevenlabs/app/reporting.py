"""Veris simulation event reporting.

When running inside a Veris simulation, report each tool call to the engine so
it lands in the graded trace. ElevenLabs client tools execute in-process (the
SDK round-trips them over the conversation WebSocket to this app) and never
reach the actor transcript, so without this the grader can't see them and may
flag real, completed actions as fabricated. ``SIMULATION_ID`` and
``ENGINE_URL`` are set by the sandbox; outside a simulation this no-ops.
"""

from __future__ import annotations

import json
import logging
import os

import httpx

logger = logging.getLogger("riley-elevenlabs")

_ENGINE_URL = os.environ.get("ENGINE_URL", "http://localhost:6100")
_SIMULATION_ID = os.environ.get("SIMULATION_ID")


def _emit_tool_event(name: str, args: dict, result: object) -> None:
    body = json.dumps(
        {
            "service": "agent",
            "event_type": "agent_tool_call",
            "data": {"name": name, "arguments": args, "result": result},
        },
        default=str,  # enums/datetimes — same handling as the tool result
    )
    try:
        httpx.post(
            f"{_ENGINE_URL}/simulations/{_SIMULATION_ID}/events",
            content=body,
            headers={"Content-Type": "application/json"},
            timeout=2.0,
        )
    except Exception as exc:
        logger.warning("[tool] could not report %s to engine: %s", name, exc)


def report_tool_call(name: str, args: dict, result: object) -> None:
    """Report a client-tool call to the Veris engine.

    No-op outside a simulation. The ElevenLabs SDK runs client-tool handlers in
    a worker thread — off the event loop — so the blocking POST is safe inline:
    it can only delay this tool's own response, never the audio path.
    """
    if not _SIMULATION_ID:
        return
    _emit_tool_event(name, args, result)

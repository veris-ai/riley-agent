"""riley-livekit — Riley card-support agent on LiveKit Agents.

A cascaded LiveKit ``AgentServer`` pipeline: Deepgram STT, a gpt-4.1-mini chat
LLM, and ElevenLabs TTS. Riley handles Acme Bank card-replacement and
status-update calls with five Postgres-backed tools.

The worker connects out to a LiveKit server (``LIVEKIT_URL``) and auto-
dispatches into every room that gets created. The caller joins the same room
through the ``/voice`` bridge in ``app/bridge.py``.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from livekit.agents import Agent, AgentServer, AgentSession, JobContext, RunContext, cli
from livekit.agents.llm import ToolError, function_tool
from livekit.plugins import deepgram, elevenlabs, openai, silero

from .db import BCSAPI, CardReplacementStatus, CardStatus
from .reporting import report_tool_call

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("riley-livekit")


def _load_agent_prompt() -> str:
    for candidate in (
        Path("agent_desc.txt"),
        Path(__file__).resolve().parent.parent / "agent_desc.txt",
    ):
        if candidate.is_file():
            return candidate.read_text()
    raise FileNotFoundError("agent_desc.txt not found")


AGENT_PROMPT = _load_agent_prompt()


class RileyAgent(Agent):
    """Card-support agent for Acme Bank.

    Holds a ``BCSAPI`` instance so each tool call goes through the same
    validated facade. Tool methods are async (a LiveKit requirement) but the
    BCSAPI calls themselves are synchronous — psycopg2 is sync, and tool calls
    are infrequent enough that blocking the loop for a quick query is fine.
    """

    def __init__(self) -> None:
        super().__init__(instructions=AGENT_PROMPT)
        self._api = BCSAPI()

    async def on_enter(self) -> None:
        await self.session.generate_reply(
            instructions="Greet the caller as Riley from Acme Bank's credit card team and ask how you can help, in one short sentence."
        )

    @function_tool()
    async def display_user_info(self, context: RunContext, user_id: str) -> dict:
        """Retrieve user account information including name, email, phone,
        address, and list of card IDs. Returns {} if not found.

        Args:
            user_id: The user's unique identifier (e.g. 'u_alice_johnson').
        """
        logger.info("tool display_user_info user_id=%s", user_id)
        user = self._api.get_user_info(user_id)
        result = user.model_dump() if user else {}
        report_tool_call("display_user_info", {"user_id": user_id}, result)
        return result

    @function_tool()
    async def display_card_info_by_last4(self, context: RunContext, last4: str) -> dict:
        """Find a card by the last 4 digits of the card number and return
        its details. Returns {} if not found.

        Args:
            last4: The last 4 digits of the card number.
        """
        logger.info("tool display_card_info_by_last4 last4=%s", last4)
        card = self._api.find_card_by_last4(last4)
        result = card.model_dump() if card else {}
        report_tool_call("display_card_info_by_last4", {"last4": last4}, result)
        return result

    @function_tool()
    async def change_card_status(self, context: RunContext, card_id: str, new_status: str) -> dict:
        """Update a card's status. A cancelled card cannot be changed to
        any other status.

        Args:
            card_id: The card's unique identifier (e.g. 'c_1234abcd').
            new_status: One of 'active', 'frozen', 'cancelled'.
        """
        logger.info("tool change_card_status card_id=%s new_status=%s", card_id, new_status)
        args = {"card_id": card_id, "new_status": new_status}
        try:
            card = self._api.update_card_status(card_id, CardStatus(new_status))
            result = card.model_dump() if card else {}
            report_tool_call("change_card_status", args, result)
            return result
        except ValueError as exc:
            report_tool_call("change_card_status", args, {"error": str(exc)})
            raise ToolError(str(exc))

    @function_tool()
    async def request_card_replacement(self, context: RunContext, card_id: str) -> dict:
        """Cancel the given card and issue a replacement. Returns the new
        card. Cannot replace an already cancelled card.

        Args:
            card_id: The card's unique identifier to replace.
        """
        logger.info("tool request_card_replacement card_id=%s", card_id)
        args = {"card_id": card_id}
        try:
            card = self._api.request_card_replacement(card_id)
            result = card.model_dump() if card else {}
            report_tool_call("request_card_replacement", args, result)
            return result
        except ValueError as exc:
            report_tool_call("request_card_replacement", args, {"error": str(exc)})
            raise ToolError(str(exc))

    @function_tool()
    async def update_card_replacement_status(self, context: RunContext, card_id: str, new_status: str) -> dict:
        """Update a replacement's delivery status (requested/mailed/delivered).

        Args:
            card_id: The card's unique identifier.
            new_status: One of 'requested', 'mailed', 'delivered'.
        """
        logger.info(
            "tool update_card_replacement_status card_id=%s new_status=%s",
            card_id,
            new_status,
        )
        args = {"card_id": card_id, "new_status": new_status}
        try:
            rep = self._api.update_card_replacement_status(card_id, CardReplacementStatus(new_status))
            result = rep.model_dump() if rep else {}
            report_tool_call("update_card_replacement_status", args, result)
            return result
        except ValueError as exc:
            report_tool_call("update_card_replacement_status", args, {"error": str(exc)})
            raise ToolError(str(exc))


# load_fnc always reports 0 so this dedicated, one-call-per-container worker
# never self-throttles. By default a `start` (prod-mode) worker uses a CPU-based
# load function with a 0.7 threshold; when the SFU, worker, and bridge share one
# CPU-bound container under load, that threshold trips and the SFU reports "no
# workers with sufficient capacity", so the agent never joins the room and the
# caller hears no answer. (dev mode defaults the threshold to inf for this same
# reason.)
server = AgentServer(load_fnc=lambda *_: 0.0)


@server.rtc_session()
async def entrypoint(ctx: JobContext) -> None:
    ctx.log_context_fields = {"room": ctx.room.name}
    logger.info("[entrypoint] joining room=%s", ctx.room.name)

    session = AgentSession(
        # Plain Silero VAD with an 800 ms end-of-turn silence threshold. No
        # turn-detector plugin, so endpointing is pure silence. The effective
        # endpoint is max(VAD silence, min_endpointing_delay), so both are
        # pinned to 0.8 s.
        min_endpointing_delay=0.8,
        stt=deepgram.STT(model=os.environ.get("DEEPGRAM_MODEL", "nova-3-general")),
        llm=openai.LLM(model=os.environ.get("LLM_MODEL", "gpt-4.1-mini")),
        tts=elevenlabs.TTS(
            # The LiveKit ElevenLabs plugin reads ELEVEN_API_KEY by default; also
            # accept ELEVENLABS_API_KEY so either key name works.
            api_key=os.environ.get("ELEVENLABS_API_KEY") or os.environ.get("ELEVEN_API_KEY"),
            voice_id=os.environ.get("ELEVENLABS_VOICE_ID", "EXAVITQu4vr4xnSDxMaL"),
            model=os.environ.get("ELEVENLABS_TTS_MODEL", "eleven_flash_v2"),
        ),
        vad=silero.VAD.load(min_silence_duration=0.8, activation_threshold=0.5),
    )
    await session.start(agent=RileyAgent(), room=ctx.room)
    logger.info("[entrypoint] session started room=%s", ctx.room.name)


if __name__ == "__main__":
    cli.run_app(server)

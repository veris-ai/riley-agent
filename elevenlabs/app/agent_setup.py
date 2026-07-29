"""Provisions the ElevenLabs agent that backs this app.

If `AGENT_ID` is set, we reuse it. Otherwise we call
`client.conversational_ai.agents.create()` with the BCS prompt, our five
`client`-type tools, and PCM16/24 kHz audio formats on both sides.

The first message ("Thanks for calling Acme Bank...") is baked into the agent
config so ElevenLabs voices it as soon as the conversation starts.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from elevenlabs import ElevenLabs

from .tools import TOOLS


logger = logging.getLogger(__name__)


FIRST_MESSAGE = "Thanks for calling Acme Bank, this is Riley — how can I help?"

DEFAULT_VOICE_ID = "EXAVITQu4vr4xnSDxMaL"  # Sarah — warm, friendly female
DEFAULT_LLM = "gpt-4.1-mini"
DEFAULT_TTS_MODEL = "eleven_flash_v2"  # English ConvAI agents reject v2_5/v3 variants
SAMPLE_RATE_HZ = 24000
AUDIO_FORMAT = "pcm_24000"


def _load_agent_prompt() -> str:
    # Resolve from CWD first (matches the /agent working dir set by code_path
    # in .veris/veris.yaml), then fall back to a path next to this package.
    for candidate in (
        Path("agent_desc.txt"),
        Path(__file__).resolve().parent.parent / "agent_desc.txt",
    ):
        if candidate.is_file():
            return candidate.read_text()
    raise FileNotFoundError("agent_desc.txt not found")


def _build_conversation_config() -> dict:
    """Build the dict we pass as `conversation_config` to `agents.create()`.

    Pydantic accepts dicts that match the nested schema, and the ElevenLabs
    SDK's UncheckedBaseModel ignores unknown fields, so we don't have to
    import a dozen submodels.
    """
    voice_id = os.environ.get("ELEVENLABS_VOICE_ID", DEFAULT_VOICE_ID)
    llm = os.environ.get("ELEVENLABS_LLM", DEFAULT_LLM)
    tts_model = os.environ.get("ELEVENLABS_TTS_MODEL", DEFAULT_TTS_MODEL)

    return {
        "agent": {
            "first_message": FIRST_MESSAGE,
            "language": "en",
            "prompt": {
                "prompt": _load_agent_prompt(),
                "llm": llm,
                "temperature": 0.3,
                "tools": TOOLS,
                # The platform otherwise prepends generic agent boilerplate
                # that conflicts with the Riley persona.
                "ignore_default_personality": True,
            },
        },
        "tts": {
            "model_id": tts_model,
            "voice_id": voice_id,
            "agent_output_audio_format": AUDIO_FORMAT,
        },
        "asr": {
            "quality": "high",
            "user_input_audio_format": AUDIO_FORMAT,
        },
        # ElevenLabs' end-of-turn is its proprietary learned turn model and
        # cannot be pinned to a silence threshold. turn_timeout is a
        # re-prompt/inactivity timer, NOT the per-utterance endpoint. We pin
        # turn_eagerness to the default so runs are explicit/consistent; the
        # turn model itself is left to the platform default (no silence-ms
        # threshold exists to set).
        "turn": {
            "turn_eagerness": "normal",
        },
    }


def ensure_agent(client: ElevenLabs) -> str:
    """Return the agent_id we should use for conversations.

    If `AGENT_ID` is set, returns it unchanged. Otherwise creates a fresh
    agent on the ElevenLabs platform and logs the new ID.
    """
    pinned = os.environ.get("AGENT_ID")
    if pinned:
        logger.info("[startup] Using pinned AGENT_ID=%s", pinned)
        return pinned

    config = _build_conversation_config()
    response = client.conversational_ai.agents.create(
        name="riley-elevenlabs (Riley @ Acme Bank)",
        conversation_config=config,
        tags=["riley", "voice"],
    )
    agent_id = response.agent_id
    logger.warning(
        "[startup] Created ElevenLabs agent %s — set AGENT_ID=%s in .env to reuse it",
        agent_id, agent_id,
    )
    return agent_id

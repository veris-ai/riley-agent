"""Deepgram STT for Hermes, via the transcription-provider plugin API.

The benchmark's other cascaded framework rows (livekit, pipecat) hold their
voice legs constant at Deepgram ``nova-3-general`` + ElevenLabs; registering
the same STT here makes the Hermes rows like-for-like — the agent loop and
LLM are the only variables. Hermes ships no Deepgram backend, but
``PluginContext.register_transcription_provider`` is its sanctioned extension
surface for exactly this (the ABC's docstring uses ``deepgram`` as its
example name).

Batch, not streaming: the adapter endpoints turns itself and hands whole
utterance WAVs to ``transcribe_audio``, so one pre-recorded API call per turn
is the natural shape.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

import httpx

from agent.transcription_provider import TranscriptionProvider

DEEPGRAM_URL = "https://api.deepgram.com/v1/listen"
DEFAULT_MODEL = os.environ.get("DEEPGRAM_MODEL", "nova-3-general")


class DeepgramSTT(TranscriptionProvider):
    @property
    def name(self) -> str:
        return "deepgram"

    @property
    def display_name(self) -> str:
        return "Deepgram"

    def is_available(self) -> bool:
        return bool(os.environ.get("DEEPGRAM_API_KEY"))

    def list_models(self) -> list:
        return [{"id": DEFAULT_MODEL}]

    def transcribe(
        self,
        file_path: str,
        *,
        model: Optional[str] = None,
        language: Optional[str] = None,
        **extra: Any,
    ) -> Dict[str, Any]:
        params = {"model": model or DEFAULT_MODEL, "smart_format": "true"}
        if language:
            params["language"] = language
        try:
            with open(file_path, "rb") as f:
                audio = f.read()
            response = httpx.post(
                DEEPGRAM_URL,
                params=params,
                content=audio,
                headers={
                    "Authorization": f"Token {os.environ['DEEPGRAM_API_KEY']}",
                    "Content-Type": "audio/wav",
                },
                timeout=30.0,
            )
            response.raise_for_status()
            transcript = (
                response.json()["results"]["channels"][0]["alternatives"][0]["transcript"]
            )
            return {"success": True, "transcript": transcript, "provider": self.name}
        except Exception as exc:
            return {
                "success": False,
                "transcript": "",
                "provider": self.name,
                "error": str(exc),
            }

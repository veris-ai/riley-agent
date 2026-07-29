# Adapted from NVIDIA-AI-Blueprints/nemotron-voice-agent
# (src/examples/shared/nemotron_speech_text_filter.py).
# SPDX-FileCopyrightText: Copyright (c) 2024-2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: BSD-2-Clause

"""Magpie TTS text cleaning filter.

Magpie's text preprocessor reserves ``{...}`` for ARPAbet phoneme notation,
``<tag>`` for SSML, and treats ``*`` as a Markdown emphasis marker. Any of
these in LLM output is either spoken literally or breaks synthesis, so the
TTS service runs this filter over every sentence before it is sent.
"""

import re

from pipecat.utils.text.base_text_filter import BaseTextFilter

_TTS_RESERVED_CHARACTERS = re.compile(
    r"<(?=[A-Za-z/!])"  # < that starts a tag: <b>, </em>, <!--
    r"|[*{}]"  # Markdown asterisks and ARPAbet phoneme delimiters: *, {, }
)


class NemotronSpeechTextFilter(BaseTextFilter):
    """Strips characters reserved by the NVIDIA TTS text preprocessor."""

    async def filter(self, text: str) -> str:
        return _TTS_RESERVED_CHARACTERS.sub("", text)

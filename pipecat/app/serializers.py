"""Frame serializer for Veris's ``voice_ws`` channel.

Veris's actor speaks raw PCM16/24 kHz mono binary frames over a WebSocket
— no telephony envelope, no JSON wrapping. This serializer is the trivial
bytes ↔ frame mapping that lets ``FastAPIWebsocketTransport`` shuttle that
straight into the same Pipecat pipeline used by the WebRTC path.
"""

from __future__ import annotations

from pipecat.frames.frames import Frame, InputAudioRawFrame, OutputAudioRawFrame
from pipecat.serializers.base_serializer import FrameSerializer

VERIS_SAMPLE_RATE_HZ = 24000
VERIS_NUM_CHANNELS = 1


class RawPCM16Serializer(FrameSerializer):
    """Pass-through serializer for raw PCM16/24 kHz/mono WebSocket audio.

    Inbound (``deserialize``): bytes → ``InputAudioRawFrame``.
    Outbound (``serialize``): ``OutputAudioRawFrame`` → bytes.
    All other frames are dropped (None) so the WS only carries audio.
    """

    def __init__(
        self,
        sample_rate: int = VERIS_SAMPLE_RATE_HZ,
        num_channels: int = VERIS_NUM_CHANNELS,
        params: FrameSerializer.InputParams | None = None,
    ) -> None:
        super().__init__(params=params)
        self._sample_rate = sample_rate
        self._num_channels = num_channels

    async def serialize(self, frame: Frame) -> bytes | None:
        if isinstance(frame, OutputAudioRawFrame):
            return frame.audio
        return None

    async def deserialize(self, data: str | bytes) -> Frame | None:
        if isinstance(data, bytes):
            return InputAudioRawFrame(
                audio=data,
                sample_rate=self._sample_rate,
                num_channels=self._num_channels,
            )
        return None

"""riley-nemotron — Riley card-support agent on Pipecat, end-to-end NVIDIA.

A cascaded Pipecat pipeline built entirely on NVIDIA-hosted models — Nemotron
ASR Streaming, a Nemotron 3 Nano chat LLM, and Magpie TTS, all served from
NVIDIA's cloud API (one ``NVIDIA_API_KEY``, no GPU). Riley handles Acme Bank
card-replacement and status-update calls with five Postgres-backed tools:

    transport.input() -> stt -> user_aggregator -> llm -> tts
    -> transport.output() -> assistant_aggregator

The transport is Veris's ``voice_ws`` channel: ``run_voice_ws_bot(websocket)``
wraps a FastAPI ``WebSocket`` (accepted by ``app/web.py:/voice``) in a
``FastAPIWebsocketTransport`` with ``RawPCM16Serializer`` so the actor's
PCM16/24 kHz binary frames flow straight through the pipeline.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import riva.client
from loguru import logger

from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import TTSSpeakFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.services.llm_service import FunctionCallParams
from pipecat.services.nvidia.llm import NvidiaLLMService, NvidiaLLMSettings
from pipecat.services.nvidia.stt import NvidiaSTTService
from pipecat.services.nvidia.tts import NvidiaTTSService, NvidiaTTSSettings
from pipecat.transports.base_transport import BaseTransport
from pipecat.turns.user_start import (
    TranscriptionUserTurnStartStrategy,
    VADUserTurnStartStrategy,
)
from pipecat.turns.user_stop import SpeechTimeoutUserTurnStopStrategy
from pipecat.turns.user_turn_strategies import UserTurnStrategies
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)

from .db import BCSAPI, CardReplacementStatus, CardStatus
from .reporting import report_tool_call
from .serializers import (
    VERIS_NUM_CHANNELS,
    VERIS_SAMPLE_RATE_HZ,
    RawPCM16Serializer,
)
from .text_filter import NemotronSpeechTextFilter


# The whole stack runs on NVIDIA-hosted endpoints, authenticated by a single
# NVIDIA_API_KEY (an nvapi-... key from build.nvidia.com). Speech models are
# NVCF cloud functions reached over gRPC and addressed by function id; the LLM
# is plain OpenAI-compatible HTTPS. These pin the exact models this agent is
# benchmarked on (they also match pipecat's built-in defaults).
NVIDIA_SPEECH_SERVER = "grpc.nvcf.nvidia.com:443"
NEMOTRON_ASR = {
    "function_id": "bb0837de-8c7b-481f-9ec8-ef5663e9c1fa",
    "model_name": "nemotron-asr-streaming",
}
MAGPIE_TTS = {
    "function_id": "877104f7-e885-42b9-8de8-f6e4c6303969",
    "model_name": "magpie-tts-multilingual",
}
MAGPIE_VOICE = "Magpie-Multilingual.EN-US.Aria"
NEMOTRON_LLM = "nvidia/nemotron-3-nano-30b-a3b"


def _load_agent_prompt() -> str:
    for candidate in (
        Path("agent_desc.txt"),
        Path(__file__).resolve().parent.parent / "agent_desc.txt",
    ):
        if candidate.is_file():
            return candidate.read_text()
    raise FileNotFoundError("agent_desc.txt not found")


AGENT_PROMPT = _load_agent_prompt()


# The fixed opening line — spoken on connect AND seeded as the first assistant
# turn in the LLM context. That keeps it visible to the trace-based grader (a bare
# TTSSpeakFrame greeting bypasses the LLM and never appears in the OTel trace) and
# stops the model from greeting a second time.
GREETING = "Thanks for calling Acme Bank, this is Riley — how can I help?"


# Tool schemas mirror riley-livekit's five @function_tool methods exactly.
# Names and descriptions match so the prompt's "Tools you can use" list is valid.
display_user_info = FunctionSchema(
    name="display_user_info",
    description=(
        "Retrieve user account information including name, email, phone, "
        "address, and list of card IDs. Returns an error if not found."
    ),
    properties={
        "user_id": {
            "type": "string",
            "description": "The user's unique identifier (e.g. u_alice_johnson).",
        },
    },
    required=["user_id"],
)

display_card_info_by_last4 = FunctionSchema(
    name="display_card_info_by_last4",
    description=(
        "Find a card by the last 4 digits of the card number and return its "
        "details. Returns an error if not found."
    ),
    properties={
        "last4": {
            "type": "string",
            "description": "The last 4 digits of the card number (e.g. '1234').",
        },
    },
    required=["last4"],
)

change_card_status = FunctionSchema(
    name="change_card_status",
    description=(
        "Update a card's status. A cancelled card cannot be changed to any "
        "other status."
    ),
    properties={
        "card_id": {
            "type": "string",
            "description": "The card's unique identifier (e.g. c_alice_debit).",
        },
        "new_status": {
            "type": "string",
            "enum": ["active", "frozen", "cancelled"],
            "description": "The new card status.",
        },
    },
    required=["card_id", "new_status"],
)

request_card_replacement = FunctionSchema(
    name="request_card_replacement",
    description=(
        "Cancel the given card and issue a replacement. Returns the new card. "
        "Cannot replace an already cancelled card."
    ),
    properties={
        "card_id": {
            "type": "string",
            "description": "The card's unique identifier to replace.",
        },
    },
    required=["card_id"],
)

update_card_replacement_status = FunctionSchema(
    name="update_card_replacement_status",
    description=(
        "Update the delivery status (requested/mailed/delivered) of a card's "
        "replacement. Accepts the original card's id or the replacement card's id."
    ),
    properties={
        "card_id": {
            "type": "string",
            "description": "The card's unique identifier.",
        },
        "new_status": {
            "type": "string",
            "enum": ["requested", "mailed", "delivered"],
            "description": "The new replacement status.",
        },
    },
    required=["card_id", "new_status"],
)


BCS_TOOLS = ToolsSchema(
    standard_tools=[
        display_user_info,
        display_card_info_by_last4,
        change_card_status,
        request_card_replacement,
        update_card_replacement_status,
    ]
)


# Single shared API instance — psycopg2 connections are opened per-call
# inside BCSAPI/Database, so this is safe to share across pipelines.
_api = BCSAPI()


# Silero VAD — the end-of-turn detector — constructed ONCE at import.
# uvicorn imports this module before it binds :8008, so the platform's TCP
# readiness probe only passes after it has loaded. Building it inside
# _build_pipeline_task instead cold-loads the ONNX session on the first
# /voice connection (~4 s warm, but 25 s+ under concurrent cluster load),
# which races — and loses to — the actor's 10 s voice_ws connect timeout,
# dropping the call before it starts. pipecat has no global model cache, so we
# share the instance; the sandbox runs one call per pod, and per-connection
# turn state lives in fresh strategy wrappers built in _build_pipeline_task.
_SHARED_VAD = SileroVADAnalyzer()


def _prewarm_magpie_tts() -> None:
    """One throwaway Magpie synthesis at import.

    The first request on a fresh channel to NVCF can take 10-20 s (NVIDIA's
    voice blueprint pre-warms for the same reason), and the sandbox runs one
    call per pod — so without this, every call's greeting pays the cold start.
    Running it at import moves the wait to pod boot, before uvicorn binds
    :8008, where the readiness probe absorbs it (same trick as _SHARED_VAD
    above). Best-effort: a failure or timeout only means the first call pays
    the cold start — or fails loudly itself if the key is actually bad. The
    call is deadline-bounded because a stalled NVCF stream would otherwise
    hang boot forever (gRPC unary calls have no default deadline), turning a
    warm-up nicety into a pod that never passes readiness.
    """
    api_key = os.environ.get("NVIDIA_API_KEY")
    if not api_key:
        logger.warning("NVIDIA_API_KEY not set — skipping Magpie TTS prewarm")
        return
    started = time.monotonic()
    try:
        auth = riva.client.Auth(
            None,
            True,
            NVIDIA_SPEECH_SERVER,
            [
                ["function-id", MAGPIE_TTS["function_id"]],
                ["authorization", f"Bearer {api_key}"],
            ],
        )
        riva.client.SpeechSynthesisService(auth).synthesize(
            "Warm up.",
            voice_name=MAGPIE_VOICE,
            language_code="en-US",
            sample_rate_hz=VERIS_SAMPLE_RATE_HZ,
            future=True,
        ).result(timeout=60)
        logger.info(f"Magpie TTS prewarm done in {time.monotonic() - started:.1f}s")
    except Exception as exc:
        logger.warning(
            f"Magpie TTS prewarm failed after {time.monotonic() - started:.1f}s "
            f"(the first call will pay the cold start): {exc}"
        )


_prewarm_magpie_tts()


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------
# Pipecat tools execute in-process, so they never appear in the voice_ws actor
# transcript the grader reads. Each handler reports its call — success and
# error — to the Veris engine via report_tool_call so real, completed actions
# land in the graded trace. Domain errors surface as ``{"error": "..."}``
# rather than raising, since Pipecat has no LiveKit-style ToolError channel.
#
# Every result must be a NON-EMPTY dict: pipecat's aggregator only re-runs the
# LLM after a tool call `if frame.result:`, and that gate also guards the
# FunctionCallResultProperties(run_llm=...) escape hatch — a falsy result
# (`{}`) means the agent never speaks again and the call deadlocks to the sim
# timeout. So not-found returns `{"error": "not found"}` instead of `{}`.
_NOT_FOUND = {"error": "not found"}


async def handle_display_user_info(params: FunctionCallParams) -> None:
    user_id = params.arguments["user_id"]
    logger.info(f"tool display_user_info user_id={user_id}")
    user = _api.get_user_info(user_id)
    result = user.model_dump() if user else _NOT_FOUND
    report_tool_call("display_user_info", {"user_id": user_id}, result)
    await params.result_callback(result)


async def handle_display_card_info_by_last4(params: FunctionCallParams) -> None:
    last4 = params.arguments["last4"]
    logger.info(f"tool display_card_info_by_last4 last4={last4}")
    card = _api.find_card_by_last4(last4)
    result = card.model_dump() if card else _NOT_FOUND
    report_tool_call("display_card_info_by_last4", {"last4": last4}, result)
    await params.result_callback(result)


async def handle_change_card_status(params: FunctionCallParams) -> None:
    card_id = params.arguments["card_id"]
    new_status = params.arguments["new_status"]
    logger.info(f"tool change_card_status card_id={card_id} new_status={new_status}")
    args = {"card_id": card_id, "new_status": new_status}
    try:
        card = _api.update_card_status(card_id, CardStatus(new_status))
    except ValueError as exc:
        report_tool_call("change_card_status", args, {"error": str(exc)})
        await params.result_callback({"error": str(exc)})
        return
    result = card.model_dump() if card else _NOT_FOUND
    report_tool_call("change_card_status", args, result)
    await params.result_callback(result)


async def handle_request_card_replacement(params: FunctionCallParams) -> None:
    card_id = params.arguments["card_id"]
    logger.info(f"tool request_card_replacement card_id={card_id}")
    args = {"card_id": card_id}
    try:
        card = _api.request_card_replacement(card_id)
    except ValueError as exc:
        report_tool_call("request_card_replacement", args, {"error": str(exc)})
        await params.result_callback({"error": str(exc)})
        return
    result = card.model_dump() if card else _NOT_FOUND
    report_tool_call("request_card_replacement", args, result)
    await params.result_callback(result)


async def handle_update_card_replacement_status(params: FunctionCallParams) -> None:
    card_id = params.arguments["card_id"]
    new_status = params.arguments["new_status"]
    logger.info(
        f"tool update_card_replacement_status card_id={card_id} new_status={new_status}"
    )
    args = {"card_id": card_id, "new_status": new_status}
    try:
        replacement = _api.update_card_replacement_status(
            card_id, CardReplacementStatus(new_status)
        )
    except ValueError as exc:
        report_tool_call("update_card_replacement_status", args, {"error": str(exc)})
        await params.result_callback({"error": str(exc)})
        return
    result = replacement.model_dump() if replacement else _NOT_FOUND
    report_tool_call("update_card_replacement_status", args, result)
    await params.result_callback(result)


def _build_llm() -> NvidiaLLMService:
    """Nemotron 3 Nano chat LLM (OpenAI-compatible) with the BCS tools wired up."""
    llm = NvidiaLLMService(
        api_key=os.environ["NVIDIA_API_KEY"],
        settings=NvidiaLLMSettings(
            model=os.environ.get("LLM_MODEL", NEMOTRON_LLM),
            # Nemotron 3 is a hybrid-reasoning model; thinking tokens delay the
            # first spoken word by seconds, so turn reasoning off per-request.
            # Same settings NVIDIA's voice blueprint ships for this model.
            extra={
                "extra_body": {
                    "chat_template_kwargs": {"enable_thinking": False},
                    "repetition_penalty": 1.05,
                }
            },
        ),
    )
    llm.register_function("display_user_info", handle_display_user_info)
    llm.register_function("display_card_info_by_last4", handle_display_card_info_by_last4)
    llm.register_function("change_card_status", handle_change_card_status)
    llm.register_function("request_card_replacement", handle_request_card_replacement)
    llm.register_function(
        "update_card_replacement_status", handle_update_card_replacement_status
    )
    return llm


def _build_stt() -> NvidiaSTTService:
    """Nemotron ASR Streaming over NVCF gRPC, fed the Veris voice_ws PCM16 audio.

    Riva accepts the declared sample rate and resamples server-side, so the
    pipeline's 24 kHz audio streams straight through. Riva-side endpointing
    (stop_history, default 320 ms) only controls when final transcripts are
    emitted — turn-taking itself is pipeline-side, see _build_pipeline_task.
    """
    return NvidiaSTTService(
        api_key=os.environ["NVIDIA_API_KEY"],
        server=NVIDIA_SPEECH_SERVER,
        model_function_map=NEMOTRON_ASR,
        sample_rate=VERIS_SAMPLE_RATE_HZ,
    )


def _build_tts() -> NvidiaTTSService:
    """Magpie TTS over NVCF gRPC, emitting PCM16 at the Veris voice_ws sample rate."""
    return NvidiaTTSService(
        api_key=os.environ["NVIDIA_API_KEY"],
        server=NVIDIA_SPEECH_SERVER,
        model_function_map=MAGPIE_TTS,
        settings=NvidiaTTSSettings(
            voice=os.environ.get("NVIDIA_TTS_VOICE", MAGPIE_VOICE),
        ),
        sample_rate=VERIS_SAMPLE_RATE_HZ,
        text_filters=[NemotronSpeechTextFilter()],
    )


def _build_pipeline_task(transport: BaseTransport) -> PipelineTask:
    """Assemble the Pipecat pipeline for a voice_ws transport."""
    stt = _build_stt()
    llm = _build_llm()
    tts = _build_tts()

    # System prompt drives Riley. The opening greeting is seeded as the first
    # assistant turn (and also spoken on connect, see _run_with_transport) so it
    # is visible to the trace-based grader and the model doesn't repeat it.
    context = LLMContext(
        [
            {"role": "system", "content": AGENT_PROMPT},
            {"role": "assistant", "content": GREETING},
        ],
        BCS_TOOLS,
    )

    # Reuse the import-time analyzers (no per-connection ONNX cold-load); only
    # the lightweight per-call strategy wrappers are fresh.
    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(
            vad_analyzer=_SHARED_VAD,
            user_turn_strategies=UserTurnStrategies(
                # Start a user turn on VAD and on FINAL transcripts only. The
                # pipecat default (default_user_turn_start_strategies) also lets
                # INTERIM transcripts start a turn, and every turn-start
                # broadcasts an interruption that cancels the agent's in-flight
                # reply. Nemotron ASR streams interim hypotheses while the
                # caller talks (interim_results defaults on), so a partial
                # transcript landing mid-reply cancels the generation; the turn
                # then re-settles via a no-strategy ("None") stop that fires
                # on_user_turn_stopped but NOT on_user_turn_inference_triggered,
                # so the LLM is never invoked and the call dies in silence (idle
                # timeout). riley-pipecat lost ~30-50% of calls this way to
                # Deepgram interim churn. use_interim=False keeps the
                # soft-speech fallback (final transcripts) without the churn.
                start=[
                    VADUserTurnStartStrategy(),
                    TranscriptionUserTurnStartStrategy(use_interim=False),
                ],
                # ~0.8 s effective end-of-turn silence = 0.2 s Silero VAD
                # stop_secs + max(0.6 s user_speech_timeout, STT-finalization
                # safety net) — the same pure silence-timer endpointing as
                # riley-pipecat and riley-livekit. Nemotron ASR has no measured
                # TTFS in pipecat (it publishes the conservative 1.0 s default),
                # so the safety net is max(0, 1.0 − 0.2) = 0.8 s from VAD stop
                # and a turn can stretch to ~1.0 s after speech end when a
                # final transcript is late; Riva-side endpointing finalizes
                # after ~320 ms of silence, so finals normally land inside the
                # 0.6 s window and the safety net short-circuits.
                stop=[SpeechTimeoutUserTurnStopStrategy(user_speech_timeout=0.6)],
            ),
        ),
    )

    pipeline = Pipeline(
        [
            transport.input(),
            stt,
            user_aggregator,
            llm,
            tts,
            transport.output(),
            assistant_aggregator,
        ]
    )

    return PipelineTask(
        pipeline,
        params=PipelineParams(
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
    )


async def _run_with_transport(transport: BaseTransport, label: str) -> None:
    """Spin up the pipeline for ``transport``, wire connect/disconnect
    handlers that greet then cancel, and block until the runner exits."""
    logger.info(f"[{label}] starting pipeline")
    task = _build_pipeline_task(transport)

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client) -> None:
        logger.info(f"[{label}] client connected — speaking greeting")
        await task.queue_frames([TTSSpeakFrame(GREETING)])

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client) -> None:
        logger.info(f"[{label}] client disconnected")
        await task.cancel()

    runner = PipelineRunner(handle_sigint=False)
    await runner.run(task)
    logger.info(f"[{label}] pipeline finished")


async def run_voice_ws_bot(websocket) -> None:
    """Run a Pipecat pipeline for a single Veris ``voice_ws`` connection.

    The WebSocket is already accepted by the FastAPI route. Frames are
    raw PCM16/24 kHz/mono in both directions — see ``RawPCM16Serializer``.
    """
    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_in_sample_rate=VERIS_SAMPLE_RATE_HZ,
            audio_out_sample_rate=VERIS_SAMPLE_RATE_HZ,
            serializer=RawPCM16Serializer(
                sample_rate=VERIS_SAMPLE_RATE_HZ,
                num_channels=VERIS_NUM_CHANNELS,
            ),
        ),
    )
    await _run_with_transport(transport, label="voice_ws")

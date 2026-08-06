"""SSE repair shim for the Nous Portal's tool-call double-encoding bug.

The Portal's OpenAI-compatible endpoint (inference-api.nousresearch.com)
returns tool-call arguments correctly in non-streaming responses, but in SSE
mode it emits the argument deltas as fragments of a JSON-*string*-encoded
object — concatenated they yield ``"{\\"user_id\\": ...}"`` (a string) instead
of ``{"user_id": ...}``. Hermes's tool executor deliberately refuses to repair
malformed arguments, so every streamed tool call dies with "Invalid tool
arguments" (verified against the raw endpoint with curl, 2026-08-06).

Hermes must stream (the delta stream is what feeds sentence-streaming TTS), so
the provider block in config.yaml points at this shim instead of the Portal
directly. It proxies ``POST /v1/chat/completions`` verbatim — the only
intervention is on streamed tool-call argument deltas, which are buffered per
tool call and re-emitted in one repaired chunk just before the finish chunk.
Arguments that already decode to an object pass through byte-identical, so the
shim stays correct the day Nous fixes the bug. Text deltas are never buffered.
"""

from __future__ import annotations

import json
import logging
from typing import Any, AsyncIterator, Dict, Tuple

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import Response, StreamingResponse

logger = logging.getLogger("riley-hermes")

NOUS_BASE = "https://inference-api.nousresearch.com/v1"

router = APIRouter()


def _repair_arguments(buffered: str) -> str:
    """Undo one level of JSON-string encoding when (and only when) present."""
    try:
        outer = json.loads(buffered)
    except json.JSONDecodeError:
        return buffered
    if isinstance(outer, str):
        try:
            inner = json.loads(outer)
        except json.JSONDecodeError:
            return buffered
        if isinstance(inner, dict):
            logger.info("[llm-shim] repaired double-encoded tool arguments")
            return outer
    return buffered


def _synthetic_args_chunk(template: Dict[str, Any], choice_index: int,
                          tool_index: int, arguments: str) -> Dict[str, Any]:
    return {
        "id": template.get("id"),
        "object": template.get("object"),
        "created": template.get("created"),
        "model": template.get("model"),
        "choices": [
            {
                "index": choice_index,
                "delta": {
                    "tool_calls": [
                        {"index": tool_index, "function": {"arguments": arguments}}
                    ]
                },
                "finish_reason": None,
            }
        ],
    }


async def _repaired_sse(upstream: httpx.Response, client: httpx.AsyncClient) -> AsyncIterator[bytes]:
    # (choice_index, tool_index) -> accumulated argument fragments
    args_buf: Dict[Tuple[int, int], str] = {}

    def flush(template: Dict[str, Any]) -> list:
        chunks = []
        for (ci, ti), buffered in sorted(args_buf.items()):
            chunks.append(_synthetic_args_chunk(template, ci, ti, _repair_arguments(buffered)))
        args_buf.clear()
        return chunks

    try:
        async for line in upstream.aiter_lines():
            if not line.startswith("data:"):
                if line.strip():
                    yield (line + "\n\n").encode()
                continue
            payload = line[len("data:"):].strip()
            if payload == "[DONE]":
                # Abnormal stream end without a finish chunk: don't swallow
                # the buffered arguments.
                for chunk in flush({}):
                    yield f"data: {json.dumps(chunk)}\n\n".encode()
                yield b"data: [DONE]\n\n"
                continue

            obj = json.loads(payload)
            emit = False
            pre_chunks: list = []
            for choice in obj.get("choices") or []:
                ci = int(choice.get("index") or 0)
                delta = choice.get("delta") or {}
                kept_tcs = []
                for tc in delta.get("tool_calls") or []:
                    ti = int(tc.get("index") or 0)
                    fn = tc.get("function") or {}
                    if "arguments" in fn:
                        args_buf[(ci, ti)] = args_buf.get((ci, ti), "") + (fn.pop("arguments") or "")
                    # Keep chunks that still announce something (id / name).
                    if tc.get("id") or fn.get("name"):
                        kept_tcs.append(tc)
                if delta.get("tool_calls") is not None:
                    if kept_tcs:
                        delta["tool_calls"] = kept_tcs
                    else:
                        delta.pop("tool_calls")
                if delta and (delta.get("content") is not None or delta.get("tool_calls")
                              or delta.get("role") or delta.get("reasoning") is not None):
                    emit = True
                if choice.get("finish_reason"):
                    pre_chunks.extend(flush(obj))
                    emit = True
            if obj.get("usage") is not None:
                emit = True
            for chunk in pre_chunks:
                yield f"data: {json.dumps(chunk)}\n\n".encode()
            if emit:
                yield f"data: {json.dumps(obj)}\n\n".encode()
    finally:
        await upstream.aclose()
        await client.aclose()


@router.post("/v1/chat/completions")
async def chat_completions(request: Request):
    body = await request.body()
    headers = {
        "authorization": request.headers.get("authorization", ""),
        "content-type": "application/json",
    }
    if not json.loads(body).get("stream"):
        async with httpx.AsyncClient(timeout=300.0) as client:
            r = await client.post(f"{NOUS_BASE}/chat/completions", content=body, headers=headers)
            return Response(
                content=r.content,
                status_code=r.status_code,
                media_type=r.headers.get("content-type", "application/json"),
            )

    client = httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=15.0))
    upstream = await client.send(
        client.build_request("POST", f"{NOUS_BASE}/chat/completions", content=body, headers=headers),
        stream=True,
    )
    if upstream.status_code != 200:
        content = await upstream.aread()
        await upstream.aclose()
        await client.aclose()
        return Response(
            content=content,
            status_code=upstream.status_code,
            media_type=upstream.headers.get("content-type", "application/json"),
        )
    return StreamingResponse(_repaired_sse(upstream, client), media_type="text/event-stream")

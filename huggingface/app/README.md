# app

The Python package for Riley. Four modules: the FastAPI app that *is* the
agent process and hosts the whole speech-to-speech cascade, the tool schemas +
dispatcher, the Postgres-backed card-ops layer the tools call, and a small
Veris reporting shim. Container startup and how to run a simulation live in the
[top-level README](../README.md); this doc is about the code.

| Module | Role |
|--------|------|
| `main.py` | The whole agent process. A FastAPI app exposing `WS /voice` (and `/health`); each connection runs the cascade in `_Call` — VAD, utterance buffering, and endpointing on the caller's audio, Whisper on hf-inference per utterance, a router chat completion with the five tools, and Kokoro TTS via fal-ai, paced back out against playback. |
| `tools.py` | The five card-ops tools in the OpenAI function-calling shape the router speaks, and `dispatch()`, which maps a tool call to the corresponding `BCSAPI` method. |
| `reporting.py` | Veris integration shim. `report_tool_call` fire-and-forgets each tool call to the sandbox engine so it lands in the graded trace; a no-op outside a simulation. |
| `db.py` | The card-ops schema (pydantic models + enums), a psycopg2 `Database` wrapper, and `BCSAPI`, the validated facade the tools go through. See also [`db/README.md`](../db/README.md) for the data itself. |

`__init__.py` is empty — this is a plain namespace package, run as
`uvicorn app.main:app`.

## How one call flows through the package

1. The actor connects to `/voice` and `_Call.run()` starts two tasks: the
   audio pump and the turn worker.
2. The worker speaks the greeting through TTS and records it as the first
   assistant message.
3. The pump reads 20 ms PCM16 frames, gating them on RMS. Voiced frames (plus
   a short preroll) collect in the open utterance; 800 ms of in-utterance
   silence closes it and queues the PCM.
4. The worker downsamples the utterance to 16 kHz, wraps it in a WAV, and
   POSTs it to Whisper on hf-inference. A blank transcript ends the turn
   quietly.
5. The transcript joins `messages` and the worker walks the tool loop: chat
   completion on the router → `dispatch()` against `BCSAPI` for each tool
   call (reported via `reporting.py`) → repeat until the model answers in
   text.
6. The reply goes to Kokoro via fal-ai, which returns a WAV; the worker
   converts it to PCM16 @ 24 kHz and sends it in 0.5 s chunks, throttled to
   stay ~1 s ahead of real-time playback.
7. If the caller speaks while audio is going out, the pump cancels the
   speaking task — the unsent tail is dropped, and the caller's new utterance
   starts collecting immediately.

# riley-gradbot

Riley, a card-support voice agent for Acme Bank, built on **[gradbot](https://github.com/gradium-ai/gradbot)** — an open-source voice-agent framework whose core is a Rust multiplexer that runs streaming STT, LLM inference, and streaming TTS concurrently, handling turn-taking, barge-in, and conversation state. STT and TTS are [Gradium](https://gradium.ai); the LLM is any OpenAI-compatible endpoint (`gpt-4.1-mini` by default). Riley handles credit-card replacement and status-update calls end to end, backed by five Postgres tools. A single FastAPI process exposes the `voice_ws` endpoint (raw PCM16 over a plain WebSocket) and, per call, starts its own gradbot session and bridges audio and tool calls.

## What it does

One process runs inside the container (`uvicorn app.main:app` on `:8008`):

- **`app/main.py`** — a FastAPI app exposing `WS /voice` (plus `/health`). Each `/voice` connection calls `gradbot.run()` with the caller's PCM stream as input, the session configured from `agent_desc.txt` and the five card-ops tools, then runs two pumps: caller PCM16 → gradbot's STT input, and gradbot's output messages → caller audio + tool dispatch.
- The five tools (`display_user_info`, `display_card_info_by_last4`, `change_card_status`, `request_card_replacement`, `update_card_replacement_status`) are dispatched against `BCSAPI` (`app/db.py`), which talks to Postgres.

```mermaid
flowchart LR
    caller["caller<br/>(voice_ws actor)"]

    subgraph container ["one container"]
        direction LR
        bridge["app/main.py<br/>:8008 /voice"]
        mux["gradbot multiplexer<br/>(Rust, in-process)"]
        pg[("Postgres<br/>card-ops schema")]
    end

    gradium["Gradium<br/>streaming STT + TTS"]
    llm["OpenAI-compatible LLM<br/>gpt-4.1-mini"]

    caller <-->|"PCM16 WS"| bridge
    bridge <-->|"send_audio /<br/>receive()"| mux
    mux <--> gradium
    mux <--> llm
    mux -.->|"tool calls"| bridge --> pg
```

Unlike the hosted speech-to-speech implementations, no vendor holds the session: the event loop, the turn state, and the tools all live in this process, and only the individual STT/LLM/TTS calls leave it. Unlike the other cascaded implementations (`pipecat`, `livekit`), the cascade isn't assembled from Python services — gradbot ships it as one Rust engine behind `run()` / `send_audio()` / `receive()`.

Things worth knowing about how gradbot shapes the agent:

- **gradbot wraps your instructions in its own system prompt.** It adds guidance on speaking style, transcription errors, interruption markers, and pending tool results, and it injects an internal `reset_asr` tool that it handles itself (it never reaches `dispatch()`). `agent_desc.txt` — byte-identical to the other Riley implementations — lands in the `additional_instructions` slot.
- **The opening turn is a literal `[start]` message.** With `assistant_speaks_first=True`, gradbot sends the LLM `"[start]"` and its scaffold says to greet the caller. The shared `agent_desc.txt` says the opposite — that the greeting already happened — so `main.py` appends an "Opening line" section that resolves the contradiction and pins the same greeting the other implementations speak.
- **Endpointing is a flush, not a VAD timer.** Once STT text stops arriving for 0.5 s, gradbot pushes `flush_duration_s` (default 0.5 s) of silence into the STT buffer and waits that long before handing the turn to the LLM — so the model always sees a complete utterance without waiting on a VAD hangover.
- **Silence re-engagement is disabled.** By default gradbot nudges a quiet caller with `"..."` after `silence_timeout_s` and says goodbye after three unanswered nudges. No other Riley implementation does this, so `main.py` sets `silence_timeout_s=0.0` to keep the comparison like-for-like.

## Run a Veris simulation against it

Everything the simulator needs is in `.veris/`: a `voice_ws` actor channel pointed at `ws://localhost:8008/voice` and `Dockerfile.sandbox`. You need the `veris` CLI and an account — run `veris login` first. `curl` and `jq` are used for the two API calls below.

Export your profile's values from `~/.veris/config.yaml` (written by `veris login`, under your active profile `profiles.<name>`):

```bash
export VERIS_URL=...    # backend_url
export VERIS_KEY=...    # api_key
export VERIS_ORG=...    # organization_id
```

**1. Create an environment.** This repo already ships a complete `.veris/`, so create a bare environment and drive everything by its id:

```bash
ENV_ID=$(curl -sS -X POST "$VERIS_URL/v1/environments" \
  -H "Authorization: Bearer $VERIS_KEY" -H "Content-Type: application/json" \
  -d "{\"name\":\"riley-gradbot\",\"organization_id\":\"$VERIS_ORG\",\"skip_managed_onboarding\":true}" \
  | jq -r '.environment.id')
echo "$ENV_ID"
```

**2. Set the provider keys** (secret, one-time):

```bash
veris env vars set GRADIUM_API_KEY=gsk_... --secret --env-id "$ENV_ID"
veris env vars set LLM_API_KEY=sk-...      --secret --env-id "$ENV_ID"
```

**3. Build & push the image:**

```bash
veris env push --env-id "$ENV_ID"
```

This builds `.veris/Dockerfile.sandbox` — runs `uv sync --frozen` (needs the committed `uv.lock`) and copies `app/`, `agent_desc.txt`, and `db/init.sql`.

> [!NOTE]
> `gradbot` is a compiled PyO3 extension. It publishes wheels for CPython 3.12–3.14 on **manylinux/musllinux x86_64**, macOS arm64, and Windows — but not linux aarch64. On an arm64 build host pip falls back to the sdist, which needs a Rust toolchain and `maturin`. Build the sandbox image for `linux/amd64`.

**4. Generate a scenario set** (also creates the grader bound to the set):

```bash
veris scenarios create --num 5 --env-id "$ENV_ID"   # prints a scenset_… id
veris scenarios status <scenset_id> --watch         # wait for "ready"
```

**5. Run + grade:**

```bash
veris run --scenario-set-id <scenset_id> --env-id "$ENV_ID"
```

`veris run` simulates every scenario, grades it with the set's grader, and prints a report (pass `--grader-id` to pin one; list them with `veris scenarios list --env-id "$ENV_ID"`). The actor streams PCM16 from its own voice persona into `/voice`, and each call's recording lands at `/sessions/{session_id}/voice-recording.mp3`.

Runs execute up to 50 simulations in parallel by default. To run sequentially (or set any `N`), create the run through the API instead and poll it:

```bash
RUN_ID=$(curl -sS -X POST "$VERIS_URL/v1/runs" \
  -H "Authorization: Bearer $VERIS_KEY" -H "Content-Type: application/json" \
  -d "{\"scenario_set_id\":\"<scenset_id>\",\"environment_id\":\"$ENV_ID\",\"parallel_jobs\":1,\"auto_evaluate\":true}" \
  | jq -r '.id')
veris simulations status "$RUN_ID" --watch
```

## Run locally

Against a separately-running Postgres seeded with `db/init.sql`:

```bash
uv sync
export GRADIUM_API_KEY=gsk_...
export LLM_API_KEY=sk-...
export LLM_MODEL=gpt-4.1-mini
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/veris
uv run uvicorn app.main:app --host 0.0.0.0 --port 8008
```

Health check: `curl -s http://127.0.0.1:8008/health` → `{"status":"ok"}`.

Any OpenAI-compatible endpoint works — point `LLM_BASE_URL` at Groq, OpenRouter, xAI, Ollama, LM Studio, and set `LLM_MODEL` to match.

## Wire protocol (caller ↔ /voice)

Binary WebSocket frames carrying raw PCM16 audio:

| Property      | Value                                 |
|---------------|---------------------------------------|
| Sample rate   | 24,000 Hz                             |
| Sample format | signed 16-bit little-endian (`s16le`) |
| Channels      | mono                                  |
| Frame size    | 20 ms (960 bytes) in; out streams gradbot's audio chunks as they arrive |
| End of call   | either side closes the WS             |

gradbot's `AudioFormat.Pcm` is asymmetric: it decodes input at 24 kHz — the actor's rate, so inbound frames pass through untouched — but encodes output at 48 kHz, which `main.py` halves back to 24 kHz with `audioop.ratecv` before sending.

## Environment

| Variable            | Required | Default                     | Notes |
|---------------------|----------|-----------------------------|-------|
| `GRADIUM_API_KEY`   | yes      | —                           | Gradium streaming STT + TTS |
| `LLM_API_KEY`       | yes      | —                           | the OpenAI-compatible LLM |
| `DATABASE_URL`      | yes      | (set by Veris)              | Postgres for the card-ops tools |
| `LLM_MODEL`         | no       | `gpt-4.1-mini` (via `.veris/veris.yaml`) | any model the endpoint serves |
| `LLM_BASE_URL`      | no       | OpenAI                      | point at Groq, OpenRouter, xAI, Ollama, … |
| `GRADIUM_BASE_URL`  | no       | `https://api.gradium.ai/api/` | Gradium endpoint override |
| `GRADBOT_VOICE_ID`  | no       | `4SZHfMpw-p46Ywgs` (Harper) | any voice id from the Gradium catalog |

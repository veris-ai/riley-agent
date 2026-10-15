# riley-grok-voice

Riley, a card-support voice agent for Acme Bank, built directly on the **raw [xAI Grok speech-to-speech API](https://docs.x.ai/developers/model-capabilities/audio/speech-to-speech)** — a single speech-to-speech model (`grok-voice-think-fast-2.0`), no STT/LLM/TTS cascade. Riley handles credit-card replacement and status-update calls end to end, backed by five Postgres tools. A single FastAPI process exposes the `voice_ws` endpoint (raw PCM16 over a plain WebSocket) and, per call, opens its own Grok realtime session and bridges audio and tool calls between the caller and xAI.

## What it does

One process runs inside the container (`uvicorn app.main:app` on `:8008`):

- **`app/main.py`** — a FastAPI app exposing `WS /voice` (plus `/health`). Each `/voice` connection opens a dedicated Grok realtime WebSocket, configures the session (instructions from `agent_desc.txt`, the five card-ops tools, `server_vad` with 800 ms end-of-turn silence, `grok-transcribe` input transcription for logging), sends an initial `response.create` so Riley greets first, then runs two pumps: caller PCM16 → Grok input buffer, and Grok events → caller audio + tool dispatch.
- The five tools (`display_user_info`, `display_card_info_by_last4`, `change_card_status`, `request_card_replacement`, `update_card_replacement_status`) are dispatched against `BCSAPI` (`app/db.py`), which talks to Postgres.

```mermaid
flowchart LR
    caller["caller<br/>(voice_ws actor)"]

    subgraph container ["one container"]
        direction LR
        bridge["app/main.py<br/>:8008 /voice"]
        pg[("Postgres<br/>card-ops schema")]
    end

    grok["xAI Grok realtime<br/>grok-voice-think-fast-2.0"]

    caller <-->|"PCM16 WS"| bridge
    bridge <-->|"realtime WS<br/>(audio + tool events)"| grok
    bridge -->|"tool calls"| pg
```

Unlike a cascaded pipeline, the LLM, STT, and TTS all live inside xAI's realtime session — the container only moves audio bytes and executes tool calls locally against Postgres.

Grok's wire protocol is OpenAI Realtime-compatible (`session.update`, `input_audio_buffer.append`, `response.output_audio.delta`, `response.function_call_arguments.done`, …) with a different `session.update` shape: `voice`, `instructions`, and `turn_detection` sit at the session's top level rather than nested under `audio`, and input transcription events are only emitted when `audio.input.transcription.model` is set to `grok-transcribe`.

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
  -d "{\"name\":\"riley-grok-voice\",\"organization_id\":\"$VERIS_ORG\",\"skip_managed_onboarding\":true}" \
  | jq -r '.environment.id')
echo "$ENV_ID"
```

**2. Set the provider key** (secret, one-time):

```bash
veris env vars set XAI_API_KEY=xai-... --secret --env-id "$ENV_ID"
```

**3. Build & push the image:**

```bash
veris env push --env-id "$ENV_ID"
```

This builds `.veris/Dockerfile.sandbox` — runs `uv sync --frozen` (needs the committed `uv.lock`) and copies `app/`, `agent_desc.txt`, and `db/init.sql`.

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

### Run as a benchmark image candidate

The benchmark runner launches an arbitrary image directly rather than through
the full simulation entrypoint. `Dockerfile.bench` packages the same Riley app
with a local, freshly seeded Postgres so no application changes are required:

```bash
docker build --platform linux/amd64 -f Dockerfile.bench -t <registry>/riley-grok-voice:v1 .
docker push <registry>/riley-grok-voice:v1
```

Import `.veris/veris.yaml` as a managed image candidate, set `image.path` to
`/voice` and `image.health_path` to `/health`, and add the provider keys listed
above as secret candidate environment entries. The benchmark engine stamps the
candidate's in-cluster address into the `voice_ws` channel.

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
export XAI_API_KEY=xai-...
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/veris
uv run uvicorn app.main:app --host 0.0.0.0 --port 8008
```

Health check: `curl -s http://127.0.0.1:8008/health` → `{"status":"ok"}`.

## Wire protocol (caller ↔ /voice)

Binary WebSocket frames carrying raw PCM16 audio:

| Property      | Value                                 |
|---------------|---------------------------------------|
| Sample rate   | 24,000 Hz                             |
| Sample format | signed 16-bit little-endian (`s16le`) |
| Channels      | mono                                  |
| Frame size    | 20 ms (960 bytes) in; out streams Grok's audio deltas as they arrive |
| End of call   | either side closes the WS             |

## Environment

| Variable           | Required | Default                     | Notes |
|--------------------|----------|-----------------------------|-------|
| `XAI_API_KEY`      | yes      | —                           | the Grok realtime session |
| `DATABASE_URL`     | yes      | (set by Veris)              | Postgres for the card-ops tools |
| `GROK_VOICE_MODEL` | no       | `grok-voice-think-fast-2.0` | Grok voice model override |
| `GROK_VOICE`       | no       | `eve`                       | Grok voice (`eve`, `ara`, `rex`, `sal`, `leo`) |

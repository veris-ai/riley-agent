# riley-deepgram

Riley, a card-support voice agent for Acme Bank, built on the **[Deepgram Voice Agent API](https://developers.deepgram.com/reference/voice-agent/voice-agent)** — one WebSocket to `agent.deepgram.com` that carries Deepgram STT (`nova-3`), a managed OpenAI LLM (`gpt-4.1-mini`), and Deepgram TTS (`aura-2-thalia-en`) as a single session. Riley handles credit-card replacement and status-update calls end to end, backed by five Postgres tools. A single FastAPI process exposes the `voice_ws` endpoint (raw PCM16 over a plain WebSocket) and, per call, opens its own Voice Agent session and bridges audio and function calls between the caller and Deepgram.

## What it does

One process runs inside the container (`uvicorn app.main:app` on `:8008`):

- **`app/main.py`** — a FastAPI app exposing `WS /voice` (plus `/health`). Each `/voice` connection opens a dedicated Voice Agent WebSocket, sends one `Settings` message (prompt from `agent_desc.txt`, the five card-ops functions, the STT/LLM/TTS providers, PCM16 at 24 kHz both ways, and the greeting Riley opens with), then runs two pumps: caller PCM16 → Deepgram, and Deepgram's message stream → caller audio + function dispatch.
- The five tools (`display_user_info`, `display_card_info_by_last4`, `change_card_status`, `request_card_replacement`, `update_card_replacement_status`) are dispatched against `BCSAPI` (`app/db.py`), which talks to Postgres.

```mermaid
flowchart LR
    caller["caller<br/>(voice_ws actor)"]

    subgraph container ["one container"]
        direction LR
        bridge["app/main.py<br/>:8008 /voice"]
        pg[("Postgres<br/>card-ops schema")]
    end

    dg["Deepgram Voice Agent<br/>nova-3 · gpt-4.1-mini · aura-2"]

    caller <-->|"PCM16 WS"| bridge
    bridge <-->|"agent WS<br/>(binary audio + JSON events)"| dg
    bridge -->|"tool calls"| pg
```

Deepgram runs the whole cascade server-side inside one session — the container only moves audio bytes and executes function calls locally against Postgres.

Two properties of this API shape the bridge:

- **Audio is raw binary in both directions.** No JSON envelope, no base64. With `audio.input` and `audio.output` both set to `linear16` at 24 kHz — the actor's own format — and `container: "none"`, audio is a byte passthrough with no resampling or re-framing. The socket is mixed: binary frames are audio, text frames are the JSON event stream.
- **A function is client-side exactly when it declares no `endpoint`.** All five do, so Deepgram sends a `FunctionCallRequest` and waits for this process to answer with a `FunctionCallResponse` instead of calling an HTTP endpoint itself.

**Turn-taking is Deepgram's built-in.** The `v1` listen provider exposes no endpointing control through `Settings`, so unlike the cascaded Riley implementations — which pin 800 ms of end-of-turn silence — this agent runs whatever Deepgram's Voice Agent decides. That is a property of the product, not a configuration choice made here.

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
  -d "{\"name\":\"riley-deepgram\",\"organization_id\":\"$VERIS_ORG\",\"skip_managed_onboarding\":true}" \
  | jq -r '.environment.id')
echo "$ENV_ID"
```

**2. Set the provider key** (secret, one-time):

```bash
veris env vars set DEEPGRAM_API_KEY=... --secret --env-id "$ENV_ID"
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
export DEEPGRAM_API_KEY=...
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
| Frame size    | 20 ms (960 bytes) in; out streams Deepgram's TTS chunks as they arrive |
| End of call   | either side closes the WS             |

## Environment

| Variable                | Required | Default              | Notes |
|-------------------------|----------|----------------------|-------|
| `DEEPGRAM_API_KEY`      | yes      | —                    | the whole Voice Agent session: STT, TTS, and the managed LLM |
| `DATABASE_URL`          | yes      | (set by Veris)       | Postgres for the card-ops tools |
| `DEEPGRAM_LISTEN_MODEL` | no       | `nova-3`             | STT model override |
| `DEEPGRAM_VOICE`        | no       | `aura-2-thalia-en`   | TTS voice override |
| `LLM_MODEL`             | no       | `gpt-4.1-mini`       | model for Deepgram's managed OpenAI access |

# riley-gemini-live

Riley, a card-support voice agent for Acme Bank, built on the **Gemini Live API** (speech-to-speech, server-to-server via the [`google-genai`](https://pypi.org/project/google-genai/) SDK). Riley handles credit-card replacement and status-update calls end to end, backed by five Postgres tools. The agent listens on a single `voice_ws` WebSocket (`/voice`) carrying raw PCM16 — for each connection it opens its own Gemini Live session, registers the card-ops function tools, and bridges audio and tool calls in both directions.

Unlike the cascaded transports there is no STT/LLM/TTS pipeline to assemble: Gemini Live *is* the whole voice loop. What this repo adds is the `voice_ws` bridging (sample-rate conversion, end-of-turn silence), the Postgres card-ops layer, and Veris tool-call reporting.

## What it does

One uvicorn process runs `app/main.py`:

- **`WS /voice`** — each connection ↔ one Gemini Live session (`gemini-2.5-flash-native-audio-preview-12-2025`, voice `Puck` by default). Riley speaks first, uses `agent_desc.txt` as the system instruction, and exposes the five card-ops tools (`display_user_info`, `display_card_info_by_last4`, `change_card_status`, `request_card_replacement`, `update_card_replacement_status`).
- **`GET /health`** — liveness probe.

```mermaid
flowchart LR
    caller["caller<br/>(voice_ws actor)"]

    subgraph container ["one container"]
        direction LR
        main["app/main.py<br/>:8008 /voice"]
        pg[("Postgres<br/>card-ops schema")]
    end

    gemini["Gemini Live API<br/>(Google cloud)"]

    caller <-->|"PCM16 WS 24 kHz"| main
    main <-->|"16 kHz in / 24 kHz out,<br/>tool calls + responses"| gemini
    main -->|"BCSAPI"| pg
```

## Sample rates and turn-taking

The Veris actor speaks and listens at **24 kHz** PCM16. Gemini Live is fixed at **16 kHz input / 24 kHz output**, so the inbound leg is downsampled 24 kHz → 16 kHz (stdlib `audioop.ratecv`, with resample state carried across frames) and Gemini's output is forwarded back unchanged.

**Gemini's own VAD** is pinned to `silence_duration_ms=800` / `prefix_padding_ms=300` so the end-of-turn threshold is explicit rather than an undocumented server default.

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
  -d "{\"name\":\"riley-gemini-live\",\"organization_id\":\"$VERIS_ORG\",\"skip_managed_onboarding\":true}" \
  | jq -r '.environment.id')
echo "$ENV_ID"
```

**2. Set the provider key** (secret, one-time):

```bash
veris env vars set GEMINI_API_KEY=... --secret --env-id "$ENV_ID"
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
export GEMINI_API_KEY=...
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/veris
./start.sh
curl -s http://127.0.0.1:8008/health   # {"status":"ok"}
```

## Wire protocol (caller ↔ /voice)

Binary WebSocket frames carrying raw PCM16 audio:

| Property      | Value                                   |
|---------------|-----------------------------------------|
| Sample rate   | 24,000 Hz (actor side)                  |
| Sample format | signed 16-bit little-endian (`s16le`)   |
| Channels      | mono                                    |
| Frame size    | variable — pass through as Gemini emits |
| End of call   | either side closes the WS               |

After each agent turn (`turn_complete`), the bridge pumps ~1700 ms of 24 kHz PCM silence so the actor's VAD can detect end-of-speech.

## Tool call flow

The Gemini Live session is configured with the five card-ops function tools. When the model wants to call one:

1. Gemini emits a `tool_call` with one or more `function_calls` (`id`, `name`, `args`).
2. `dispatch()` (in `app/tools.py`) runs the matching `BCSAPI` method against Postgres.
3. The bridge replies with `send_tool_response([FunctionResponse(...)])`.
4. Gemini resumes the turn on its own and voices the result — no explicit re-trigger needed.

Each tool call (success or error) is also reported to the Veris engine as an `agent_tool_call` event (`app/reporting.py`), since the LLM runs in Google's cloud and tool calls never reach the actor transcript.

## Environment

| Variable            | Required | Default                         | Notes |
|---------------------|----------|---------------------------------|-------|
| `GEMINI_API_KEY`    | yes      | —                               | Gemini Live session per call |
| `DATABASE_URL`      | yes      | (set by Veris)                  | Postgres for the card-ops tools |
| `GEMINI_LIVE_MODEL` | no       | `gemini-2.5-flash-native-audio-preview-12-2025` | Live model override |
| `GEMINI_VOICE`      | no       | `Puck`                          | prebuilt voice |
| `PORT`              | no       | `8008`                          | voice_ws port (start.sh only) |

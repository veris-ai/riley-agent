# riley-elevenlabs

Riley, a card-support voice agent for Acme Bank, built on [ElevenLabs Conversational AI](https://elevenlabs.io/docs/agents-platform/overview) — a **managed platform: ASR, the `gpt-4.1-mini` LLM, `eleven_flash_v2` TTS, and turn-taking all run on ElevenLabs' side**. What runs here is a single FastAPI app that exposes a `voice_ws` channel (raw PCM16 over a plain WebSocket) at `WS /voice`, bridges each call into an ElevenLabs conversation, and executes the five Postgres-backed card-ops tools in-process as *client tools*.

Because ElevenLabs client tools round-trip over the app's own outbound WebSocket (`wss://api.elevenlabs.io`), no inbound tunnel or public webhook is needed — unlike Vapi/Retell-style server tools, everything stays inside the container. No `OPENAI_API_KEY` either: the LLM is billed and run through ElevenLabs.

## What it does

One process runs inside the container (launched by `start.sh`, or `veris.yaml`'s equivalent `entry_point`): `uvicorn app.main:app` on `:8008`, serving `WS /voice` and `GET /health`.

Per `/voice` connection, `app/main.py`:

1. Resolves the stored ElevenLabs agent — reuses `AGENT_ID` if set, otherwise `app/agent_setup.py:ensure_agent` creates one on the platform (prompt read verbatim from `agent_desc.txt`, `first_message` "Thanks for calling Acme Bank, this is Riley — how can I help?", `pcm_24000` audio both directions, `ignore_default_personality: true`). Provisioning is lazy — deferred to the first call — so the server boots for `/health` without an API key.
2. Opens an `AsyncConversation` against the platform and registers the five card-ops client tools (`display_user_info`, `display_card_info_by_last4`, `change_card_status`, `request_card_replacement`, `update_card_replacement_status`), each a thin wrapper over `BCSAPI` (`app/db.py`) → Postgres.
3. Bridges audio both ways through a custom `AsyncAudioInterface`: actor frames are forwarded to ElevenLabs as-is; ElevenLabs' reply chunks are forwarded straight back.
4. Reports every tool call — including error paths — to the Veris engine as an `agent_tool_call` event (`app/reporting.py`) so the grader sees them.

```mermaid
flowchart LR
    caller["caller<br/>(voice_ws actor)"]

    subgraph container ["one container"]
        direction LR
        main["app/main.py<br/>:8008 /voice"]
        pg[("Postgres<br/>card-ops schema")]
    end

    el["ElevenLabs ConvAI<br/>(hosted agent: ASR,<br/>gpt-4.1-mini, TTS)"]

    caller <-->|"PCM16 WS"| main
    main <-->|"one AsyncConversation per call:<br/>audio + client_tool_call/result"| el
    main -->|"client tools → BCSAPI"| pg
```

## Run it locally

```bash
uv sync
export ELEVENLABS_API_KEY=sk_...
export AGENT_ID=agent_...      # optional — omit to create a fresh agent on first call
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/veris
./start.sh
curl localhost:8008/health     # {"status":"ok"}
```

The first call without `AGENT_ID` logs the created agent's id — pin it in `.env` to reuse the stored agent instead of creating a new one per boot. Model/voice env vars (`ELEVENLABS_LLM`, `ELEVENLABS_VOICE_ID`, `ELEVENLABS_TTS_MODEL`) only take effect at creation time; a pinned agent keeps its platform config.

## Repo layout

| Path | What it is |
|------|------------|
| `app/` | The Python package — FastAPI app, agent provisioning, tool schemas + dispatch, Postgres layer, Veris reporting. See [`app/README.md`](app/README.md). |
| `agent_desc.txt` | Riley's system prompt, uploaded verbatim to the stored ElevenLabs agent at provisioning time. |
| `db/init.sql` | Card-ops schema + seed data; Veris seeds its Postgres from this. See [`db/README.md`](db/README.md). |
| `.veris/veris.yaml` | Simulation config: the `voice_ws` actor channel (`ws://localhost:8008/voice`), the postgres service, the agent `entry_point`. |
| `.veris/Dockerfile.sandbox` | Sandbox image: `uv sync --frozen` from the committed `uv.lock`, then copies `app/`, `agent_desc.txt`, `db/init.sql`, `start.sh`. |
| `start.sh` | Local launcher — `uvicorn app.main:app` on `$PORT` (default 8008). |
| `pyproject.toml` / `uv.lock` | Dependencies, pinned. `uv.lock` must stay committed — the sandbox build needs it. |

## Run a Veris simulation against it

### Run as a benchmark image candidate

The benchmark runner launches an arbitrary image directly rather than through
the full simulation entrypoint. `Dockerfile.bench` packages the same Riley app
with a local, freshly seeded Postgres so no application changes are required:

```bash
docker build --platform linux/amd64 -f Dockerfile.bench -t <registry>/riley-elevenlabs:v1 .
docker push <registry>/riley-elevenlabs:v1
```

Import `.veris/veris.yaml` as a managed image candidate, set `image.path` to
`/voice` and `image.health_path` to `/health`, and add the provider keys listed
above as secret candidate environment entries. The benchmark engine stamps the
candidate's in-cluster address into the `voice_ws` channel.

Everything the simulator needs is in `.veris/`: a `voice_ws` actor channel pointed at the app (`ws://localhost:8008/voice`) and `Dockerfile.sandbox`. You need the `veris` CLI and an account — run `veris login` first. `curl` and `jq` are used for the API calls below.

Export your profile's values from `~/.veris/config.yaml` (written by `veris login`, under your active profile `profiles.<name>`):

```bash
export VERIS_URL=...    # backend_url
export VERIS_KEY=...    # api_key
export VERIS_ORG=...    # organization_id
```

**1. Create an environment.** This repo already ships a complete `.veris/`, so create a bare environment and drive everything by its id (with CLI ≥ 2.27.0, `veris env create --self-serve` does the same):

```bash
ENV_ID=$(curl -sS -X POST "$VERIS_URL/v1/environments" \
  -H "Authorization: Bearer $VERIS_KEY" -H "Content-Type: application/json" \
  -d "{\"name\":\"riley-elevenlabs\",\"organization_id\":\"$VERIS_ORG\",\"skip_managed_onboarding\":true}" \
  | jq -r '.environment.id')
echo "$ENV_ID"
```

**2. Set the provider key** (secret, one-time). Optionally pin a stored agent too, so simulations reuse it instead of provisioning a fresh one per container:

```bash
veris env vars set ELEVENLABS_API_KEY=sk_... --secret --env-id "$ENV_ID"
veris env vars set AGENT_ID=agent_... --env-id "$ENV_ID"   # optional
```

**3. Build & push the image:**

```bash
veris env push --env-id "$ENV_ID"
```

This builds `.veris/Dockerfile.sandbox` — runs `uv sync --frozen` (needs the committed `uv.lock`) and copies `app/`, `agent_desc.txt`, `db/init.sql`, and `start.sh`.

**4. Generate a scenario set:**

```bash
veris scenarios create --num 5 --env-id "$ENV_ID"   # prints a scenset_… id
veris scenarios status <scenset_id> --watch         # wait for "ready"
```

CLI versions ≤ 2.27.1 silently skip grader generation (the set still reports "ready"), and `veris run` then aborts with "No grader found". Regenerate the graders via the API and re-watch:

```bash
curl -sS -X POST "$VERIS_URL/v1/scenario-sets/<scenset_id>/regenerate" \
  -H "Authorization: Bearer $VERIS_KEY" -H "Content-Type: application/json" \
  -d '{"from_step":"graders"}'
veris scenarios status <scenset_id> --watch
```

**5. Run + grade.** Bare `veris run` executes up to 50 simulations in parallel; create the run through the API to run sequentially (or set any `N`) and poll it:

```bash
RUN_ID=$(curl -sS -X POST "$VERIS_URL/v1/runs" \
  -H "Authorization: Bearer $VERIS_KEY" -H "Content-Type: application/json" \
  -d "{\"scenario_set_id\":\"<scenset_id>\",\"environment_id\":\"$ENV_ID\",\"parallel_jobs\":1,\"auto_evaluate\":true}" \
  | jq -r '.id')
veris simulations status "$RUN_ID" --watch
```

The actor streams PCM16 from its own voice persona into `/voice`, and each call's recording lands at `/sessions/{session_id}/voice-recording.mp3`.

## Wire protocol (caller ↔ /voice)

Binary WebSocket frames carrying raw PCM16 audio:

| Property      | Value                                 |
|---------------|---------------------------------------|
| Sample rate   | 24,000 Hz (`pcm_24000` on both ElevenLabs legs) |
| Sample format | signed 16-bit little-endian (`s16le`) |
| Channels      | mono                                  |
| Framing       | passthrough both ways — actor frames go to ElevenLabs as-is, ElevenLabs chunks come back as-is |
| End of call   | either side closes the WS             |

A text frame on this socket is a protocol error — the handler logs it as an actor-side mismatch (Starlette surfaces it as `KeyError('bytes')`).

## Environment

| Variable               | Required | Default                | Notes |
|------------------------|----------|------------------------|-------|
| `ELEVENLABS_API_KEY`   | yes      | —                      | the ConvAI platform (LLM, ASR, TTS all run behind it) |
| `AGENT_ID`             | no       | (created lazily)       | pin a stored ElevenLabs agent; unset → `ensure_agent` creates one on the first call |
| `ELEVENLABS_VOICE_ID`  | no       | `EXAVITQu4vr4xnSDxMaL` | Sarah; creation-time only |
| `ELEVENLABS_LLM`       | no       | `gpt-4.1-mini`         | creation-time only |
| `ELEVENLABS_TTS_MODEL` | no       | `eleven_flash_v2`      | creation-time only; English ConvAI agents reject v2_5/v3 variants |
| `DATABASE_URL`         | yes      | (set by Veris)         | Postgres for the card-ops tools |
| `PORT`                 | no       | `8008`                 | voice_ws port (`start.sh` only; `veris.yaml` pins 8008) |

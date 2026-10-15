# riley-fluxions

Riley, a card-support voice agent for Acme Bank, built on
**[Fluxions](https://fluxions.ai/docs)** — `akro` batch transcription turns
each caller utterance into text, an OpenAI-compatible chat completion
(`gpt-4.1-mini` by default) reasons over the transcript and calls the tools,
and `VUI` streaming TTS speaks the reply over a warm WebSocket. Riley handles
credit-card replacement and status-update calls end to end, backed by five
Postgres tools. A single FastAPI process exposes the `voice_ws` endpoint (raw
PCM16 over a plain WebSocket) and, per call, runs the whole pipeline:
turn-taking, barge-in, and conversation state included.

## What it does

One process runs inside the container (`uvicorn app.main:app` on `:8008`):

- **`app/main.py`** — a FastAPI app exposing `WS /voice` (plus `/health`). Each connection runs two tasks: the actor audio pump (which does VAD, endpointing, and turn capture) and a turn worker that transcribes the captured utterance, drives the LLM, and speaks the reply.
- The five tools (`display_user_info`, `display_card_info_by_last4`, `change_card_status`, `request_card_replacement`, `update_card_replacement_status`) are dispatched against `BCSAPI` (`app/db.py`), which talks to Postgres.

```mermaid
flowchart LR
    caller["caller<br/>(voice_ws actor)"]

    subgraph container ["one container"]
        direction LR
        agent["app/main.py<br/>:8008 /voice"]
        pg[("Postgres<br/>card-ops schema")]
    end

    stt["akro transcription<br/>(submit + poll)"]
    llm["OpenAI-compatible LLM<br/>gpt-4.1-mini"]
    tts["VUI TTS<br/>(warm WS)"]

    caller <-->|"PCM16 WS"| agent
    agent -->|"utterance WAV"| stt
    stt -->|"segments"| agent
    agent <-->|"tools"| llm
    agent --> tts -->|"s16le 24 kHz"| agent
    agent --> pg
```

Fluxions ships two of the three legs — its Realtime Voice conversation API is
"documentation coming soon" — so this process *is* the pipeline, like
`mistral`: a cascade of independent STT / LLM / TTS services with turn-taking,
barge-in, and the tool loop in ~300 lines of `main.py`, and no framework
assembling it. The LLM is bring-your-own, like `gradbot`. When the realtime
API ships, this row should probably move to it.

Things worth knowing about how the pipeline shapes the agent:

- **Riley greets without an LLM turn.** The pipeline is assembled here, so the opening line is simply spoken through TTS before the first completion. That makes the shared `agent_desc.txt` — byte-identical to the other Riley implementations — true as written: by the time the model sees anything, the greeting really has happened. No prompt override, unlike `grok-voice` and `gradbot`.
- **STT is batch, not streaming — the defining constraint of this row.** akro has no streaming endpoint, so each turn is captured whole (VAD + 800 ms endpoint, plus a 0.4 s pre-roll so onset ramping up through the RMS threshold isn't clipped), wrapped in a WAV, submitted to `/akro/submit`, and polled until the job lands. Warm, that costs ~3 s per turn before the LLM even starts — ~1.4 s in the submit round-trip plus ~1.7 s of processing and poll, and it's fixed cost rather than upload-bound: a 16 kHz submission saves a third of the bytes and none of the time, which is why nothing is downsampled. The streaming-STT implementations already have the transcript when the caller stops; this is what Fluxions offers today, and nothing in this process can hide it. (akro caches by audio bytes, so a repeated identical utterance comes back in ~1 s — real callers never repeat byte-identically, but test drivers do.)
- **The docs' transcription response is not what the live API sends.** `GET /transcriptions/{id}` returns job metadata only — no inline `text`. Passing `word_level_timestamps=true` is what materializes the `segments` array, so the poll always sends it and the turn text is the segment texts joined. (Speaker labels are meaningless here: every submitted WAV is one caller utterance.) akro also drops non-speech tags like `[pause]` into segment text even with `non_speech` unset; they ride into the LLM transcript, which copes.
- **No audio is ever resampled or converted.** The `voice_ws` wire format — PCM16 mono at 24 kHz — is also VUI's native output, and akro accepts the 24 kHz WAV as-is. Caller bytes go into the turn WAV untouched; VUI's binary frames go to the caller untouched. Compare `mistral`, which resamples inbound and converts float32 outbound.
- **Endpointing runs on the audio, in audio time.** An RMS check on each 20 ms frame decides the turn endpoint (800 ms of silence, matching the hosted implementations' `server_vad`) and barge-in. Silence is counted in audio seconds, not wall-clock: frames arrive in bursts whenever anything stalls the pump, and a wall-clock timer would read a burst as a long pause and endpoint mid-sentence.
- **One TTS socket per call, dropped on barge-in.** The VUI WebSocket stays open across renders, so every reply after the greeting skips the TLS handshake. `verify_chunks` is off — the re-check would hold first audio ~1 s per sentence. When the caller barges in, the render is cancelled mid-stream but the server keeps pushing the dead render's frames at the socket, so the socket is dropped with it; the next reply reconnects.
- **Cold starts are absorbed at startup, not mid-call.** The Fluxions GPU fleet powers down when idle and the first request after a quiet period is held at the gateway — documented as up to ~30 s, measured at 77 s after roughly fifteen minutes of quiet. `lifespan` wakes both services before uvicorn starts serving, so the actor never dials a cold agent. The akro warmup passes `cache=false`: the warmup bytes are identical on every start, and a cached result would skip the GPU entirely, defeating the wake. Back-to-back calls keep the fleet warm; a run that sits idle for tens of minutes re-exposes the hold mid-run, and a turn held past the 60 s client timeout fails the call.

## Run a Veris simulation against it

Everything the simulator needs is in `.veris/`: a `voice_ws` actor channel
pointed at `ws://localhost:8008/voice` and `Dockerfile.sandbox`. You need the
`veris` CLI and an account — run `veris login` first. `curl` and `jq` are used
for the two API calls below.

Export your profile's values from `~/.veris/config.yaml` (written by
`veris login`, under your active profile `profiles.<name>`):

```bash
export VERIS_URL=...    # backend_url
export VERIS_KEY=...    # api_key
export VERIS_ORG=...    # organization_id
```

**1. Create an environment.** This repo already ships a complete `.veris/`, so
create a bare environment and drive everything by its id:

```bash
ENV_ID=$(curl -sS -X POST "$VERIS_URL/v1/environments" \
  -H "Authorization: Bearer $VERIS_KEY" -H "Content-Type: application/json" \
  -d "{\"name\":\"riley-fluxions\",\"organization_id\":\"$VERIS_ORG\",\"skip_managed_onboarding\":true}" \
  | jq -r '.environment.id')
echo "$ENV_ID"
```

**2. Set the provider keys** (secret, one-time). Fluxions bills the STT and
TTS legs; the LLM bills its own:

```bash
veris env vars set FLUXIONS_API_KEY=... --secret --env-id "$ENV_ID"
veris env vars set LLM_API_KEY=sk-...   --secret --env-id "$ENV_ID"
```

**3. Build & push the image:**

```bash
veris env push --env-id "$ENV_ID"
```

This builds `.veris/Dockerfile.sandbox` — runs `uv sync --frozen` (needs the
committed `uv.lock`) and copies `app/`, `agent_desc.txt`, and `db/init.sql`.

**4. Generate a scenario set** (also creates the grader bound to the set):

```bash
veris scenarios create --num 5 --env-id "$ENV_ID"   # prints a scenset_… id
veris scenarios status <scenset_id> --watch         # wait for "ready"
```

**5. Run + grade:**

```bash
veris run --scenario-set-id <scenset_id> --env-id "$ENV_ID"
```

`veris run` simulates every scenario, grades it with the set's grader, and
prints a report (pass `--grader-id` to pin one; list them with
`veris scenarios list --env-id "$ENV_ID"`). The actor streams PCM16 from its
own voice persona into `/voice`, and each call's recording lands at
`/sessions/{session_id}/voice-recording.mp3`.

### Run as a benchmark image candidate

The benchmark runner launches an arbitrary image directly rather than through
the full simulation entrypoint. `Dockerfile.bench` packages the same Riley app
with a local, freshly seeded Postgres so no application changes are required:

```bash
docker build --platform linux/amd64 -f Dockerfile.bench -t <registry>/riley-fluxions:v1 .
docker push <registry>/riley-fluxions:v1
```

Import `.veris/veris.yaml` as a managed image candidate, set `image.path` to
`/voice` and `image.health_path` to `/health`, and add the provider keys listed
above as secret candidate environment entries. The benchmark engine stamps the
candidate's in-cluster address into the `voice_ws` channel.

Runs execute up to 50 simulations in parallel by default. To run sequentially
(or set any `N`), create the run through the API instead and poll it:

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
export FLUXIONS_API_KEY=...
export LLM_API_KEY=sk-...
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/veris
uv run uvicorn app.main:app --host 0.0.0.0 --port 8008
```

Health check: `curl -s http://127.0.0.1:8008/health` → `{"status":"ok"}`.
Startup blocks on the warmup, so from a cold Fluxions fleet the health
endpoint can take a minute or two to appear.

> [!NOTE]
> `pyproject.toml` pins Python to `>=3.12,<3.13`, and the ceiling is load-bearing
> rather than cautious. Python 3.13 enabled `ssl.VERIFY_X509_STRICT` by default,
> which rejects a CA carrying no `keyUsage` extension — including the one the
> Veris sandbox uses to intercept egress. On 3.13 every HTTPS call fails with
> `CA cert does not include key usage extension`, killing uvicorn startup, so the
> simulation hangs with nothing serving the actor. 3.12 verifies the same chain
> and still ships `audioop` in the stdlib, so no shim is needed.

## Wire protocol (caller ↔ /voice)

Binary WebSocket frames carrying raw PCM16 audio:

| Property      | Value                                 |
|---------------|---------------------------------------|
| Sample rate   | 24,000 Hz                             |
| Sample format | signed 16-bit little-endian (`s16le`) |
| Channels      | mono                                  |
| Frame size    | 20 ms (960 bytes) in; out streams VUI's chunks as they arrive |
| End of call   | either side closes the WS             |

Both Fluxions legs speak this format natively, so nothing is resampled or
converted anywhere in the process.

## Environment

| Variable           | Required | Default          | Notes |
|--------------------|----------|------------------|-------|
| `FLUXIONS_API_KEY` | yes      | —                | bills the akro STT and VUI TTS legs |
| `LLM_API_KEY`      | yes      | —                | any OpenAI-compatible endpoint |
| `DATABASE_URL`     | yes      | (set by Veris)   | Postgres for the card-ops tools |
| `LLM_BASE_URL`     | no       | `api.openai.com` | e.g. `https://api.x.ai/v1` |
| `LLM_MODEL`        | no       | `gpt-4.1-mini`   | held there to match the other cascade rows |
| `FLUXIONS_VOICE`   | no       | `maeve`          | a voice **slug** (the `id` from `GET /vui/voices`), resolved to the checkpoint-suffixed `voice_id` at startup |

Voice ids carry the serving checkpoint's suffix (`maeve.h8ff7e07da`), which
changes when Fluxions ships a new model — which is why the slug is resolved at
startup rather than pinned. An unknown slug fails at startup with the full
list.

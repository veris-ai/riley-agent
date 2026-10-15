# riley-smallest

Riley, a card-support voice agent for Acme Bank, built on **Smallest AI's**
voice stack — [Pulse](https://docs.smallest.ai/models/model-cards/speech-to-text/pulse)
transcribes the caller over a streaming WebSocket, a non-streaming chat
completion against **Electron** (Smallest's own OpenAI-compatible chat model)
reasons over the transcript and calls the tools, and **Lightning** speaks the
reply over a second WebSocket. Riley handles credit-card replacement and
status-update calls end to end, backed by five Postgres tools. A single
FastAPI process exposes the `voice_ws` endpoint (raw PCM16 over a plain
WebSocket) and, per call, runs the whole pipeline: turn-taking, barge-in, and
conversation state included.

## What it does

One process runs inside the container (`uvicorn app.main:app` on `:8008`):

- **`app/main.py`** — a FastAPI app exposing `WS /voice` (plus `/health`). Each connection opens a Pulse streaming-STT session, then runs three tasks: the caller-audio pump (which also does VAD and endpointing), the transcription event loop, and a turn worker that drives the LLM and speaks the reply.
- The five tools (`display_user_info`, `display_card_info_by_last4`, `change_card_status`, `request_card_replacement`, `update_card_replacement_status`) are dispatched against `BCSAPI` (`app/db.py`), which talks to Postgres.

```mermaid
flowchart LR
    caller["caller<br/>(voice_ws actor)"]

    subgraph container ["one container"]
        direction LR
        agent["app/main.py<br/>:8008 /voice"]
        pg[("Postgres<br/>card-ops schema")]
    end

    stt["Pulse<br/>streaming STT"]
    llm["Electron chat<br/>+ function calling"]
    tts["Lightning<br/>streaming TTS"]

    caller <-->|"PCM16 WS, 24 kHz"| agent
    agent -->|"24 kHz, unconverted"| stt
    stt -->|"transcript"| agent
    agent <-->|"tools"| llm
    agent --> tts -->|"PCM16 24 kHz"| agent
    agent --> pg
```

Smallest ships the three legs as separate products rather than one session, so
this process *is* the pipeline. That puts it between the two halves of the
repo: like `pipecat` and `livekit` it is a cascade of independent STT / LLM /
TTS services, but nothing here is assembled by a framework — turn-taking,
barge-in, and the tool loop are all in `main.py`. Unlike the hosted
speech-to-speech implementations, no vendor holds the session or decides when a
turn ends.

Things worth knowing about how the pipeline shapes the agent:

- **Riley greets without an LLM turn, and the greeting is not interruptible.** The pipeline is assembled here, so the opening line is spoken through TTS before the first completion, straight through `_speak_now` rather than the regular barge-in-aware `_speak` path. That makes the shared `agent_desc.txt` — byte-identical to the other Riley implementations — true as written: by the time the model sees anything, the greeting really has happened. Speaking it outside the barge-in path matters in practice: background noise at call start can fire the VAD before any greeting audio has gone out, and an interruptible greeting would be cancelled before the caller ever heard it, leaving them with silence.
- **No audio is resampled.** Pulse's live endpoint and Lightning both accept 24 kHz directly — the actor's own rate — so nothing here is downsampled or upsampled.
- **Pulse's `is_final` transcripts are deltas, not a restatement.** Verified live (looping Lightning's own TTS output back into Pulse): each `is_final` event's `transcript` covers only the audio since the previous final. Pulse finalizes on its own trailing-silence and word-count thresholds as well as on a client-sent `{"type": "finalize"}`, so a turn's text is every final since the last turn, joined — not just the latest one. The final answering our own `finalize` carries `from_finalize: true` and is what closes the turn; see `_endpoint` and `_pump_stt_events` in `app/main.py`.
- **Endpointing runs on the audio, not on the transcript**: frames do not arrive at a steady rate, so a wall-clock timer would endpoint mid-burst. Measured in audio time (silent bytes seen) via a per-frame RMS check, 800 ms of silence, rather than a frame-energy gate alone misreading background noise as speech.
- **TTS playback is paced, and barge-in actually has something to cut.** Lightning streams a reply faster than it plays, so unpaced the whole reply would land in the actor's buffer well before a barge-in could interrupt it. Replies go out in 500 ms slices, at most 1 s ahead of real-time playback. Because of that pacing, a barge-in's cut point is trustworthy: the interrupted reply is trimmed proportionally (PCM bytes sent, in ms, against the reply's word count at `TTS_MS_PER_WORD`) and the kept words are marked `… [interrupted by the caller]` in the model's history, so the next turn reasons from what the caller actually heard. `TTS_MS_PER_WORD` (340) is measured on `kelsey` (338 ms/word over four replies) — worth re-measuring if the deployed voice changes.
- **A barge-in's close doesn't wait on Lightning.** Lightning's WebSocket takes one utterance per connection and does not answer a close frame while still streaming, so cancelling mid-reply closes with `close_timeout=0` rather than waiting out the default 10 s handshake — ten seconds of dead air a caller would otherwise sit through before their turn is heard.

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
  -d "{\"name\":\"riley-smallest\",\"organization_id\":\"$VERIS_ORG\",\"skip_managed_onboarding\":true}" \
  | jq -r '.environment.id')
echo "$ENV_ID"
```

**2. Set the provider key** (secret, one-time). All three legs bill to it:

```bash
veris env vars set SMALLEST_API_KEY=... --secret --env-id "$ENV_ID"
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
`veris scenarios list --env-id "$ENV_ID"`). The actor streams PCM16 from its own
voice persona into `/voice`, and each call's recording lands at
`/sessions/{session_id}/voice-recording.mp3`.

## Run locally

Against a separately-running Postgres seeded with `db/init.sql`:

```bash
uv sync
export SMALLEST_API_KEY=...
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/veris
uv run uvicorn app.main:app --host 0.0.0.0 --port 8008
```

Health check: `curl -s http://127.0.0.1:8008/health` → `{"status":"ok"}`.

> [!NOTE]
> `pyproject.toml` pins Python to `>=3.12,<3.13`, and the ceiling is
> load-bearing rather than cautious: Python 3.13 enabled
> `ssl.VERIFY_X509_STRICT` by default, which rejects a CA carrying no
> `keyUsage` extension — including the one the Veris sandbox uses to intercept
> egress — and separately dropped `audioop` from the standard library, which
> the VAD here uses. 3.12 verifies the same chain and keeps `audioop`, so no
> shim is needed for either.

## Wire protocol (caller ↔ /voice)

Binary WebSocket frames carrying raw PCM16 audio:

| Property      | Value                                 |
|---------------|---------------------------------------|
| Sample rate   | 24,000 Hz                             |
| Sample format | signed 16-bit little-endian (`s16le`) |
| Channels      | mono                                  |
| Frame size    | 20 ms (960 bytes) in; out streams in 500 ms slices, paced at most 1 s ahead of playback |
| End of call   | either side closes the WS             |

## Environment

| Variable             | Required | Default                | Notes |
|----------------------|----------|-------------------------|-------|
| `SMALLEST_API_KEY`   | yes      | —                       | one key for Pulse, Electron, and Lightning |
| `DATABASE_URL`       | yes      | (set by Veris)          | Postgres for the card-ops tools |
| `SMALLEST_TTS_MODEL` | no       | `lightning_v3.1_pro`    | the premium pool: broadcast-quality voices, dedicated inference capacity, no cloning |
| `SMALLEST_VOICE`     | no       | `kelsey`                | must be a voice in the Pro pool — a standard-pool voice is rejected outright by the API rather than silently mispaired |
| `SMALLEST_LANGUAGE`  | no       | `en`                    | |

There is no `SMALLEST_LLM_MODEL` override: Electron is the only chat model
this transport asks for, so there is nothing to pick between.

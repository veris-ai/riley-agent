# riley-mistral

Riley, a card-support voice agent for Acme Bank, built on
**[Mistral's speech-to-speech pipeline](https://docs.mistral.ai/en/studio-api/audio/overview#speech-to-speech-pipeline)**
— [Voxtral Realtime](https://docs.mistral.ai/en/studio-api/audio/speech_to_text/realtime_transcription)
transcribes the caller over a WebSocket, a Mistral chat completion reasons over
the transcript and calls the tools, and
[Voxtral TTS](https://docs.mistral.ai/en/studio-api/audio/text_to_speech)
speaks the reply. Riley handles credit-card replacement and status-update calls
end to end, backed by five Postgres tools. A single FastAPI process exposes the
`voice_ws` endpoint (raw PCM16 over a plain WebSocket) and, per call, runs the
whole pipeline: turn-taking, barge-in, and conversation state included.

## What it does

One process runs inside the container (`uvicorn app.main:app` on `:8008`):

- **`app/main.py`** — a FastAPI app exposing `WS /voice` (plus `/health`). Each connection opens a Voxtral Realtime session, then runs three tasks: the caller-audio pump (which also does VAD and endpointing), the transcription event loop, and a turn worker that drives the LLM and speaks the reply.
- The five tools (`display_user_info`, `display_card_info_by_last4`, `change_card_status`, `request_card_replacement`, `update_card_replacement_status`) are dispatched against `BCSAPI` (`app/db.py`), which talks to Postgres.

```mermaid
flowchart LR
    caller["caller<br/>(voice_ws actor)"]

    subgraph container ["one container"]
        direction LR
        agent["app/main.py<br/>:8008 /voice"]
        pg[("Postgres<br/>card-ops schema")]
    end

    stt["Voxtral Realtime<br/>streaming STT"]
    llm["Mistral chat<br/>+ function calling"]
    tts["Voxtral TTS<br/>streaming"]

    caller <-->|"PCM16 WS"| agent
    agent -->|"24→16 kHz"| stt
    stt -->|"transcript"| agent
    agent <-->|"tools"| llm
    agent --> tts -->|"float32 24 kHz"| agent
    agent --> pg
```

Mistral ships the three legs as separate services rather than one session, so
this process *is* the pipeline. That puts it between the two halves of the
repo: like `pipecat` and `livekit` it is a cascade of independent STT / LLM /
TTS services, but nothing here is assembled by a framework — turn-taking,
barge-in, and the tool loop are ~300 lines of `main.py`. Unlike the hosted
speech-to-speech implementations, no vendor holds the session or decides when a
turn ends.

Things worth knowing about how the pipeline shapes the agent:

- **Riley greets without an LLM turn.** The pipeline is assembled here, so the opening line is simply spoken through TTS before the first completion. That makes the shared `agent_desc.txt` — byte-identical to the other Riley implementations — true as written: by the time the model sees anything, the greeting really has happened. No prompt override, unlike `grok-voice` and `gradbot`.
- **Endpointing runs on the audio, not on the transcript.** Voxtral emits transcription deltas in bursts — it holds words back until it has enough right context, and mid-sentence gaps of two seconds happen — so "no deltas lately" says nothing about whether the caller stopped talking. An RMS check on each 20 ms frame decides both the turn endpoint (800 ms of silence, matching the hosted implementations' `server_vad`) and barge-in.
- **Silence is measured in audio time, not wall-clock.** Frames do not arrive at a steady 50 fps; anything that stalls the pump lets the socket backlog and then deliver a burst, which a wall-clock timer reads as a long pause and endpoints on, mid-sentence.
- **The flush is what finishes a turn.** `flush_audio()` forces out the last word or two the deltas are still sitting on, and the `transcription.done` it triggers carries the authoritative turn text. The connection survives it, so one transcription session serves the whole call.
- **Voxtral drops an idle transcription session after ~30 s** with no audio at all. The actor streams continuously, silence included, so this only matters if you drive `/voice` yourself.
- **429s are retried.** A turn spends 2–3 completions walking the tool loop, which is enough to trip Mistral's rate limit; the SDK knows how to retry a 429 but ships that switched off, so `main.py` configures bounded backoff (see [Rate limits](#rate-limits)).

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
  -d "{\"name\":\"riley-mistral\",\"organization_id\":\"$VERIS_ORG\",\"skip_managed_onboarding\":true}" \
  | jq -r '.environment.id')
echo "$ENV_ID"
```

**2. Set the provider key** (secret, one-time). All three legs bill to it:

```bash
veris env vars set MISTRAL_API_KEY=... --secret --env-id "$ENV_ID"
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

Runs execute up to 50 simulations in parallel by default. To run sequentially
(or set any `N`), create the run through the API instead and poll it:

```bash
RUN_ID=$(curl -sS -X POST "$VERIS_URL/v1/runs" \
  -H "Authorization: Bearer $VERIS_KEY" -H "Content-Type: application/json" \
  -d "{\"scenario_set_id\":\"<scenset_id>\",\"environment_id\":\"$ENV_ID\",\"parallel_jobs\":1,\"auto_evaluate\":true}" \
  | jq -r '.id')
veris simulations status "$RUN_ID" --watch
```

> [!IMPORTANT]
> Parallel runs multiply the request rate against a single Mistral key. See
> [Rate limits](#rate-limits) before turning `parallel_jobs` up.

## Run locally

Against a separately-running Postgres seeded with `db/init.sql`:

```bash
uv sync
export MISTRAL_API_KEY=...
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/veris
uv run uvicorn app.main:app --host 0.0.0.0 --port 8008
```

Health check: `curl -s http://127.0.0.1:8008/health` → `{"status":"ok"}`.

## Rate limits

Each caller turn is 2–3 chat completions (one per tool round), on top of a TTS
request and the open transcription socket. On a low-tier key that is enough to
get throttled mid-call: Mistral answers `429` with `{"type":"rate_limited",
"code":"1300"}`, and the throttle can persist for several seconds.

`main.py` configures the SDK's retry (200 ms → 2 s backoff, 8 s ceiling) for
every request, which absorbs short bursts. A throttle that outlasts the ceiling
fails the call rather than leaving the caller listening to silence — if you see
that, the key is quota-limited, not the agent. Check your key's tier before a
large or parallel run.

## Wire protocol (caller ↔ /voice)

Binary WebSocket frames carrying raw PCM16 audio:

| Property      | Value                                 |
|---------------|---------------------------------------|
| Sample rate   | 24,000 Hz                             |
| Sample format | signed 16-bit little-endian (`s16le`) |
| Channels      | mono                                  |
| Frame size    | 20 ms (960 bytes) in; out streams Voxtral's TTS chunks (~0.4 s) as they arrive |
| End of call   | either side closes the WS             |

The two conversions this needs are on the provider side, not the caller's:
inbound audio is downsampled 24 → 16 kHz for Voxtral Realtime, and Voxtral TTS's
`pcm` — raw float32 LE, already at 24 kHz — is converted to int16 on the way
out. No resampling on the reply path.

## Environment

| Variable            | Required | Default                     | Notes |
|---------------------|----------|-----------------------------|-------|
| `MISTRAL_API_KEY`   | yes      | —                           | one key for STT, LLM, and TTS |
| `DATABASE_URL`      | yes      | (set by Veris)              | Postgres for the card-ops tools |
| `MISTRAL_LLM_MODEL` | no       | `mistral-large-latest`      | `mistral-small-latest` trades reasoning for latency |
| `MISTRAL_STT_MODEL` | no       | `voxtral-mini-transcribe-realtime-2602` | |
| `MISTRAL_TTS_MODEL` | no       | `voxtral-mini-tts-2603`     | |
| `MISTRAL_VOICE`     | no       | `en_paul_neutral`           | a Voxtral preset **slug**, resolved to an id at startup |

Voxtral's 30 presets are `en_paul_*` (en-US), `gb_oliver_*` / `gb_jane_*`
(en-GB), and `fr_marie_*` (fr-FR), each in eight or so deliveries — `neutral`,
`cheerful`, `confident`, and so on. `en_paul_*` is the only en-US family today,
which is why Riley sounds American and male here rather than matching the other
implementations' voices. An unknown slug fails at startup with the full list.

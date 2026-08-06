# riley-huggingface

Riley, a card-support voice agent for Acme Bank, built as
**[huggingface/speech-to-speech](https://github.com/huggingface/speech-to-speech)'s
cascade — VAD → STT → LLM → TTS — with every model leg hosted on Hugging
Face** and nothing running locally. One codebase, two hosting configurations,
flipped per leg by env vars:

| Configuration | STT | LLM | TTS | Caller-turn latency (measured) |
|---|---|---|---|---|
| **Dedicated Inference Endpoints** — the benchmarked one | [`openai/whisper-large-v3-turbo`](https://huggingface.co/openai/whisper-large-v3-turbo), 1× L4 | [`deepseek-ai/DeepSeek-V4-Flash-0731`](https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash-0731) on vLLM, 2× H200 | [`hexgrad/Kokoro-82M`](https://huggingface.co/hexgrad/Kokoro-82M) behind a [custom handler](https://huggingface.co/jrmeyer-veris/kokoro-82M-endpoint), 1× L4 | 1.5–3.1 s |
| **Serverless Inference Providers** — the zero-deploy fallback | Whisper large-v3 on `hf-inference` | [`gpt-oss-120b`](https://huggingface.co/openai/gpt-oss-120b) via Groq | Kokoro-82M via fal-ai | 3.4–5.3 s |

Dedicated endpoints are HF-managed GPU deployments billed per instance-hour:
you deploy the three models under your own account and point the agent at them
with `HF_STT_URL`/`HF_LLM_URL`/`HF_TTS_URL`. With no URLs set, the code falls
back to [Inference Providers](https://huggingface.co/docs/inference-providers) —
serverless partner routing, per-request billing against one HF token, nothing
to deploy. **The defaults baked into `main.py` (gpt-oss-120b, fal-ai) are that
serverless fallback; the benchmarked configuration overrides every one of
them.**

Riley handles credit-card replacement and status-update calls end to end,
backed by five Postgres tools. A single FastAPI process exposes the
`voice_ws` endpoint (raw PCM16 over a plain WebSocket) and, per call, runs the
whole pipeline: turn-taking, barge-in, and conversation state included.

## What it does

One process runs inside the container (`uvicorn app.main:app` on `:8008`):

- **`app/main.py`** — a FastAPI app exposing `WS /voice` (plus `/health`). Each connection runs two tasks: the caller-audio pump (VAD, utterance buffering, endpointing) and a turn worker that transcribes the utterance, drives the LLM, and paces the spoken reply back out.
- The five tools (`display_user_info`, `display_card_info_by_last4`, `change_card_status`, `request_card_replacement`, `update_card_replacement_status`) are dispatched against `BCSAPI` (`app/db.py`), which talks to Postgres.

```mermaid
flowchart LR
    caller["caller<br/>(voice_ws actor)"]

    subgraph container ["one container"]
        direction LR
        agent["app/main.py<br/>:8008 /voice"]
        pg[("Postgres<br/>card-ops schema")]
    end

    subgraph hf ["Hugging Face — dedicated endpoints (benchmarked) or serverless router"]
        stt["Whisper<br/>turbo on L4 · large-v3 on hf-inference"]
        llm["LLM<br/>DeepSeek V4 Flash on vLLM · gpt-oss-120b via Groq"]
        tts["Kokoro-82M<br/>custom handler · fal-ai"]
    end

    caller <-->|"PCM16 WS"| agent
    agent -->|"WAV per utterance"| stt
    stt -->|"transcript"| agent
    agent <-->|"tools"| llm
    agent --> tts -->|"WAV, paced out"| agent
    agent --> pg
```

The upstream [huggingface/speech-to-speech](https://github.com/huggingface/speech-to-speech)
project chains VAD, STT, LLM, and TTS handlers over queues, with local
inference for every leg (Whisper and Kokoro are both among its handler
options). This implementation keeps that cascade but swaps each leg for an
HF-hosted call, so the trade-offs change shape: HF's hosted STT and TTS are
plain request/response HTTP — no streaming ASR, no streaming synthesis — which
makes this the repo's only fully batch cascade. Like `mistral`, nothing is
assembled by a framework: turn-taking, barge-in, and the tool loop are ~300
lines of `main.py`, and the three legs are plain `httpx` calls.

Things worth knowing about how the pipeline shapes the agent:

- **Riley greets without an LLM turn.** The pipeline is assembled here, so the opening line is simply spoken through TTS before the first completion, which keeps the shared `agent_desc.txt` — byte-identical to the other Riley implementations — true as written.
- **Each utterance is one Whisper request.** Nothing transcribes while the caller talks: voiced frames (plus a 240 ms preroll — the RMS gate trips a frame or two after a soft onset, and unlike the streaming-STT implementations nothing else hears that audio) buffer locally, and the 800 ms endpoint closes the utterance and POSTs it as a WAV. Turn latency is therefore endpoint + full STT + LLM + full TTS, serial — the price of hosted batch legs.
- **Endpointing runs on the audio, in audio time.** An RMS check on each 20 ms frame decides voice/silence (the actor's line is digitally silent between utterances, so no Silero model is needed), and silence is counted in silent bytes seen, not wall-clock — frames arrive in bursts when anything stalls the pump, and a wall-clock timer endpoints mid-sentence on the next burst.
- **The reply is paced, not blasted.** The TTS leg returns the whole reply as one WAV. Sending it all at once would park the full reply in the actor's buffer and make barge-in meaningless, so chunks go out 0.5 s at a time, throttled to ~1 s ahead of real-time playback; barge-in cancels the pacer and strands the unsent tail.
- **`huggingface_hub` is not used at runtime.** Its `AsyncInferenceClient` downloads fal-ai TTS results with a *blocking* call inside the event loop, which would stall the audio pumps. The URLs it would build are constructed the same way here with async `httpx` (verified against `huggingface_hub` 1.26).
- **429s and cold starts are waited out.** All three legs retry on 429 and 503 with bounded backoff (25 s ceiling). A throttle that outlasts the ceiling fails the call — giving up kills the call, which is strictly worse than a slow reply.

## The benchmarked configuration: dedicated Inference Endpoints

`riley-huggingface-endpoints-env` in `.veris/veris.yaml`. Serverless
`hf-inference` serves no LLMs and no TTS at all anymore, so dedicated
endpoints are the only way to run the whole cascade on HF-managed metal — and
they are also the faster and more controllable half of this repo. Deploy the
three models, set the three URL variables, and each leg switches over:

| Leg | Deployment (validated) | Warm latency |
|-----|------------------------|--------------|
| STT | `openai/whisper-large-v3-turbo`, Default Engine, 1× L4 ($0.8/h) | 0.85 s for a 5.7 s utterance |
| LLM | `deepseek-ai/DeepSeek-V4-Flash-0731`, vLLM engine, 2× H200 ($10/h) | 0.7–0.9 s per completion, structured tool calls |
| TTS | `hexgrad/Kokoro-82M` behind a custom handler ([`jrmeyer-veris/kokoro-82M-endpoint`](https://huggingface.co/jrmeyer-veris/kokoro-82M-endpoint)), 1× L4 ($0.8/h) | ~1 s per reply |

Worth knowing before a run:

- **The ASR endpoint speaks the same raw-bytes shape as hf-inference**, so `HF_STT_URL` is used verbatim. `HF_LLM_URL` is the endpoint base URL; vLLM serves the OpenAI API under `/v1`, and `HF_LLM_MODEL` must then be the bare served-model id — the `:provider` suffix is a router concept.
- **The TTS endpoint speaks this repo's own contract** — `{"inputs", "parameters": {"voice"}}` in, base64 WAV out — defined by `handler.py` in the handler repo. `HF_TTS_VOICE` must name a voice the deployed model has (the same Kokoro ids as the serverless route).
- **DeepSeek's thinking mode is switched off** (`chat_template_kwargs: {"thinking": false}` on every endpoint-mode completion). Left on, a mid-call deliberative turn was observed reasoning for 28 s before answering — fatal in a voice conversation.
- **vLLM's default `--max-model-len` can serialize the endpoint.** At this model's native 524288, the KV-cache reservation admitted ~1–2 sequences and ten concurrent completions ran near-serially (~15 s each); at 32768 — ample for sub-4k-token voice calls — the same burst batches in 1.6 s.
- **First request after a vLLM boot is slow** (~30 s observed — engine warmup); everything after is sub-second.
- **Scale-to-zero is hostile to voice.** A scale-from-zero boot takes minutes (vLLM engine load; the TTS handler pip-installs at boot), far beyond the in-call retry budget, so the first call after an idle hour dies. Warm all three endpoints before a run, or set `min_replica: 1` while benching.
- **Match custom-handler wheels to the image's CUDA.** The toolkit image ships a CUDA-12 torch, and PyPI's default torchaudio/torchcodec wheels are CUDA-13 builds — mixing them fails at import (`libcudart.so.13` not found). An XTTS-v2 variant of this leg died on exactly this; if a handler needs the torch audio stack, install the `+cu126` builds from the PyTorch index and leave the image's torch alone. (Kokoro's dependency tree avoids the problem entirely.)

## The zero-deploy fallback: serverless Inference Providers

`riley-huggingface-env` in `.veris/veris.yaml`, and what `main.py` does with
no `HF_*_URL` set: Whisper large-v3 on `hf-inference` (HF's own infra),
`gpt-oss-120b` through the router's chat completions, Kokoro via `fal-ai` —
one HF token billing all three legs, nothing to deploy or warm.

- **The LLM pins its serving partner.** Router model ids take a `:provider` suffix — the default is `openai/gpt-oss-120b:groq`. Without one, the router's `:fastest` policy picks per request, which makes runs incomparable; `:cheapest` and `:preferred` also exist.
- **Cold starts are the provider's.** hf-inference answers 503 while a model loads; fal spins Kokoro's worker down when idle and the first synthesis after that took 20–60 s (the HTTP read timeout is sized for this — a timeout is not retryable). One ~10-minute fal saturation episode was also observed (sync route answering 504, queue submissions stuck in `IN_QUEUE`); no retry policy bridges that, so such calls fail loudly.
- **Measured latencies (validated live, Aug 2026).** Warm: STT ~1.3 s for a 5 s utterance, LLM 0.3–1.1 s per completion on Groq, TTS ~1–3 s per reply; caller-turn latency to first reply audio ran 3.4–5.3 s end to end.

> [!NOTE]
> Hosted TTS is the fragile leg of this variant. Inference Providers has
> removed text-to-speech from its documented task catalog, and exactly three
> TTS models still carry live provider mappings (Kokoro-82M on fal-ai,
> Chatterbox and Chatterbox-turbo on fal-ai/Replicate). The fal-ai route works
> with a plain HF token today — `main.py` resolves the mapping at startup and
> fails the boot with the live provider list if it disappears.

### Credits and rate limits (serverless)

Inference Providers bills through HF credits: the free tier includes $0.10 of
monthly credits — effectively a trial — PRO $2.00, and past the included
credits it is pay-as-you-go at provider list price. A voice call spends on all
three legs at once (one Whisper request per caller turn, 2–3 completions per
turn walking the tool loop, one Kokoro request per reply), so expect a
PAYG-enabled or PRO account to be necessary for any real run.

HF publishes no numeric rate limits; each pinned provider enforces its own.
`main.py` retries 429/503 with backoff (0.5 s → doubling, 25 s ceiling) on
every leg. A throttle that outlasts 25 s fails the call; if you see that, the
token or the pinned provider is the bottleneck, not the agent. Check both
before a large or parallel run. (Dedicated endpoints have no per-request
metering — the instances are yours — but each has its own concurrency
ceiling; see above.)

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
  -d "{\"name\":\"riley-huggingface\",\"organization_id\":\"$VERIS_ORG\",\"skip_managed_onboarding\":true}" \
  | jq -r '.environment.id')
echo "$ENV_ID"
```

**2. Set the variables** (one-time). Both configurations need the token — it
must carry the **Make calls to Inference Providers** permission — and the
endpoints configuration additionally needs the three deployment URLs (they are
account-specific, which is why they are injected rather than committed):

```bash
veris env vars set HF_TOKEN=... --secret --env-id "$ENV_ID"

# endpoints configuration only:
veris env vars set HF_STT_URL=https://<asr-endpoint>...    --env-id "$ENV_ID"
veris env vars set HF_LLM_URL=https://<vllm-endpoint>...   --env-id "$ENV_ID"
veris env vars set HF_TTS_URL=https://<kokoro-endpoint>... --env-id "$ENV_ID"
```

**3. Build & push the image**, picking the configuration as the build target:

```bash
veris env push --target riley-huggingface-endpoints-env --env-id "$ENV_ID"
# or: veris env push --target riley-huggingface-env --env-id "$ENV_ID"
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
> Parallelism multiplies the request rate against whichever legs you run.
> Serverless: HF credits run out fast — see
> [Credits and rate limits](#credits-and-rate-limits-serverless). Endpoints:
> each deployment has its own concurrency ceiling (the vLLM
> `--max-model-len` note above is the difference between 12 parallel calls
> batching and serializing).

## Run locally

Against a separately-running Postgres seeded with `db/init.sql`. With only the
token set this runs the serverless configuration; add the three URL vars for
the endpoints one:

```bash
uv sync
export HF_TOKEN=hf_...
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/veris
uv run uvicorn app.main:app --host 0.0.0.0 --port 8008
```

Health check: `curl -s http://127.0.0.1:8008/health` → `{"status":"ok"}`.

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
| Frame size    | 20 ms (960 bytes) in; out is 0.5 s chunks paced ~1 s ahead of playback |
| End of call   | either side closes the WS             |

Inbound audio is downsampled 24 → 16 kHz before the Whisper upload (Whisper
resamples to 16 kHz anyway, so the extra bytes buy nothing). Kokoro's WAV
comes back at 24 kHz — the actor's rate — and is converted from whatever
container header it carries to raw PCM16 on the way out.

## Environment

| Variable       | Required | Default                     | Notes |
|----------------|----------|-----------------------------|-------|
| `HF_TOKEN`     | yes      | —                           | one token for STT, LLM, and TTS in both configurations; needs the Inference Providers permission |
| `DATABASE_URL` | yes      | (set by Veris)              | Postgres for the card-ops tools |
| `HF_STT_URL`   | no       | (serverless router)         | dedicated ASR endpoint URL, used verbatim for the raw-bytes POST |
| `HF_LLM_URL`   | no       | (serverless router)         | dedicated vLLM endpoint base URL; `/v1/chat/completions` is appended |
| `HF_TTS_URL`   | no       | (serverless fal-ai route)   | custom TTS endpoint URL speaking the `audio_b64` contract |
| `HF_LLM_MODEL` | no       | `openai/gpt-oss-120b:groq`  | serverless router id (`:provider` pins the partner); with `HF_LLM_URL`, the bare served-model id — the endpoints config sets `deepseek-ai/DeepSeek-V4-Flash-0731` |
| `HF_STT_MODEL` | no       | `openai/whisper-large-v3`   | serverless: must be servable by `hf-inference` (the raw-bytes ASR shape is theirs); the endpoints config deploys `whisper-large-v3-turbo` |
| `HF_TTS_MODEL` | no       | `hexgrad/Kokoro-82M`        | serverless only: must have a live `fal-ai` text-to-speech mapping; with `HF_TTS_URL`, the model is whatever the handler serves |
| `HF_TTS_VOICE` | no       | `af_heart`                  | a Kokoro voice id: `af_*`/`am_*` are American female/male — same ids in both configurations (the endpoints handler serves Kokoro too) |

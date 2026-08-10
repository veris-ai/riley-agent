# Riley on the Fluxions realtime API

Riley card-support agent on Fluxions' realtime voice API: one WebSocket that
carries ASR, routing, TTS, endpointing and barge-in on the server side, with
the card-ops tools executed locally against Postgres.

This is a different stack from the [`fluxions`](../fluxions/) row, not a newer
version of it. That row exists because the pieces available at the time were
`akro` batch transcription and `VUI` streaming TTS, so the pipeline,
turn-taking and barge-in had to be assembled in the agent process. The realtime
API ships all of that behind one socket, so this row is a bridge instead of a
pipeline, and the two are worth measuring separately.

> [!NOTE]
> This repository ships no default endpoint. Supply your own via
> `FLUXIONS_REALTIME_URL`; the agent will not start without it.

## Architecture

```text
Veris actor ──24 kHz PCM16──▶ /voice ──16 kHz PCM16──▶ Fluxions realtime
            ◀─24 kHz PCM16──         ◀─24 kHz PCM16──
                                  │
                                  ├─ tool.call ──▶ BCSAPI ──▶ Postgres
                                  └─ tool.result ◀────────────┘
```

`app/main.py` owns exactly three things; the rest is server-side:

- **Sample rate.** The Veris actor speaks 24 kHz both directions, while the
  realtime API takes 16 kHz up and returns 24 kHz down, so only the upstream
  leg is resampled (`audioop.ratecv`, carry-over state kept across frames).
- **Playout pacing.** Agent audio arrives faster than realtime. It is queued
  and written to the actor at 1x so that an `audio.flush` on barge-in has
  something left to drop; forwarding it as it arrived would leave the actor
  holding speech the caller has already interrupted. `playback.pos` reports
  what has actually been written, which is what the server's VAD and echo
  canceller use to tell agent audio apart from the caller.
- **Tools.** `tool.call` is dispatched against `BCSAPI` and answered with
  `tool.result`, well inside the window the agent waits before telling the
  caller the action did not happen.

## Configuration

| Variable | Purpose |
| --- | --- |
| `FLUXIONS_REALTIME_URL` | The realtime endpoint, `wss://<host>/v1/realtime`. Required — no default, so a missing or wrong value fails at startup rather than on the first call. |
| `FLUXIONS_ANCHOR_TOOL` | Name of one server built-in tool, used to anchor our own tool definitions (see below). Required. |
| `DATABASE_URL` | Postgres, provisioned by Veris and seeded from `db/init.sql`. |

There is no `LLM_MODEL` knob — the model is whatever the endpoint serves. It is
reported back in `session.created` and logged at startup.

## Running

```bash
cd fluxions-realtime
uv sync
FLUXIONS_REALTIME_URL=wss://<host>/v1/realtime \
FLUXIONS_ANCHOR_TOOL=<built-in tool name> \
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/veris \
  uv run uvicorn app.main:app --host 0.0.0.0 --port 8008
```

Under Veris, `.veris/veris.yaml` does this for you once the two Fluxions
variables are set on the environment.

## How this row differs from the others

Two deviations, both forced by how the platform routes tools.

**Tool descriptions are rewritten.** `agent_desc.txt` is byte-identical to
every other row — that is benchmark policy — but `app/tools.py` is not.
Routing is JSON-mode prompting rather than the OpenAI tools API, so a
description is the only instruction the router ever reads. Each description
here leads with *when* to call the tool and names which identifiers come from
which earlier call. The tool *set* is unchanged — the same five operations
every other row gets — so the row is scored on the same surface as the rest.

**The tool list needs an anchor.** Bring-your-own tool definitions are only
registered when the `tools` list in `session.update` also names at least one of
the server's built-in tools; a list of pure definitions is accepted without
error and no tool is ever called. `FLUXIONS_ANCHOR_TOOL` supplies that name.
Choose the most
inert built-in the deployment offers — ideally read-only and unreachable from a
card-support conversation — since it is one capability no other row has.

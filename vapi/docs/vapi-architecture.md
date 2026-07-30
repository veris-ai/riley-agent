# How Vapi works: a thorough walkthrough of what runs where

This document exists because Vapi sits in an unusual architectural spot. If you're coming from "I write a Python agent that runs end-to-end on my laptop and only calls out to OpenAI for the LLM," or from "I drag boxes around in a Parloa/Decagon dashboard and they run the whole thing," Vapi will feel weird. It's neither. It's a *split runtime* — some of the agent lives at Vapi cloud, some lives in your infrastructure, and the wire between them runs in both directions during a single call.

This doc walks through that split carefully so you can predict what happens where, why the network shape is what it is, and how it compares to the architectures you already know.

## TL;DR

Vapi is **not a framework you embed**. There is no `pip install vapi-runtime` that gives you the conversation loop. There is no source repo for the orchestrator. Vapi's product is the **hosted orchestration runtime** — the thing that listens to audio, decides when the user is done talking, runs the LLM, decides when to call a tool, and speaks the result back.

Around that hosted core, Vapi exposes hooks so you can plug your own infrastructure in:

- Your own LLM endpoint (Vapi POSTs `/chat/completions`-shaped requests to it)
- Your own transcriber (Vapi streams audio to your WebSocket)
- Your own TTS (Vapi streams text to you and expects PCM audio back)
- Your own transport (Vapi accepts WebSocket or SIP audio from your side)
- Your own tool implementations (Vapi POSTs tool calls to a webhook URL you host)

So the runtime is fixed at Vapi cloud, but everything *attached* to the runtime can run in your infrastructure if you want. The runtime is the only thing you can't take in-house without an enterprise contract.

That's the whole picture in one paragraph. The rest of this doc unpacks each piece.

## Three deployment patterns to anchor your mental model

Voice agents in 2026 land in roughly three architectural buckets. It helps to name them.

### Pattern 1: Self-hosted runtime (most implementations in this repository)

Everything runs in your process. The agent loop, tool dispatch, conversation state, turn-taking — all in code you wrote. The only things crossing the wire are calls to *model APIs*: STT, LLM, TTS.

Examples from this repository:
- [`openai-realtime-mini`](../../openai-realtime-mini/) — opens an outbound WebSocket to OpenAI Realtime. OpenAI handles STT+LLM+TTS+VAD as one bundled service. The conversation loop and tool dispatch both run in your agent process. Tool calls arrive as events on the same outbound WebSocket and you respond on the same socket.
- [`livekit`](../../livekit/) — uses the LiveKit Agents Python framework. Same idea: the agent loop is `livekit-agents` code running in your container; LiveKit just owns the WebRTC media transport.

What's nice about this pattern: you can debug the loop, you can step through it, you own the latency budget, no part of the orchestration is opaque. What's annoying: you have to *write* the loop. Or pick a framework that wrote it for you.

### Pattern 2: Fully hosted no-code (Parloa, Decagon, Voiceflow, Synthflow)

You log into a web dashboard, draw boxes representing dialog states, configure each box's prompts and tools, and click "publish." Their servers do everything. You may not even have an SDK; you just have a phone number and a logged-out dashboard.

What's nice: zero infrastructure. What's annoying: you literally cannot run it locally; the boxes are the source code; integrating with anything outside their dashboard is constrained to whatever connectors they expose.

### Pattern 3: Split-runtime (Vapi, Retell)

This is the one we're trying to characterize. The conversational *runtime* — the agent loop and the things that make voice feel natural — runs on the vendor's servers, *always*. But the runtime exposes a wide surface of hooks back into your infrastructure: tool webhooks, custom LLM URLs, custom transcriber WebSockets, custom TTS endpoints, custom transport. You write code on both sides of the wire. The vendor's job is to glue your pieces together with their conversation engine.

The key word here is **hooks**. Vapi gives you a lot of them. You can replace almost any model in the pipeline with one you host. But the *thing that decides "the user has stopped talking, now run the LLM, the LLM picked tool X, now invoke tool X, now read the result back via TTS"* is theirs. You can't take that home.

That middle position is what feels weird if you've only worked in patterns 1 and 3.

## The pieces of a voice agent

To say "Vapi runs the orchestrator and you can swap the models" we need to be precise about what the pieces are. Below is a per-component breakdown of a typical voice agent.

### Transport (audio in/out)

The plumbing that carries raw audio bytes between the caller and the agent. Could be a phone-line (PSTN via Twilio/Telnyx/Vonage), SIP for traditional telephony stacks, WebRTC for browsers/mobile, or a raw WebSocket carrying PCM frames. This is the lowest layer — just bits on the wire, no understanding of speech.

### Transcriber (STT, speech-to-text)

Listens to inbound audio and produces a stream of text. Modern STT services are streaming: partial transcripts arrive while the user is still talking, finalized chunks arrive when the system thinks a phrase is complete. Deepgram, Gladia, AssemblyAI, Whisper variants.

### Orchestration (the conversation engine)

This is the part that's usually invisible in framework-based voice agents because it's split across the STT and the LLM and the application logic. Pulling it out as a named layer:

- **Endpointing**: deciding when the user finished a turn. Naive VAD says "the user paused for 500ms, they're done." Modern endpointing looks at audio + text + prosody together to detect natural turn boundaries.
- **Interruption detection**: deciding whether a sound from the user mid-assistant-utterance is a barge-in ("stop, I have a question") versus a backchannel ("uh-huh, go on"). Big quality-of-experience difference.
- **Background-noise / background-voice filtering**: distinguishing the caller's voice from TV in the background, a coworker talking nearby, road noise.
- **Backchanneling**: the assistant occasionally inserting "uh-huh", "got it", "yeah" while the user talks to feel natural.
- **Filler injection**: the assistant adding "um", "let me see…" during its own speech to mask LLM/TTS latency.
- **Turn management**: scheduling which side speaks when, what to do with overlap, when to commit a partial transcript.
- **Tool-call routing**: when the LLM emits a tool call, deciding what to do — invoke the tool synchronously, asynchronously, abort if conditions fail, etc.

This is the layer that's actually hard. STT and TTS and LLM are all commodity services you can rent. The orchestration is the differentiated work. It's also what Vapi explicitly calls "our core value proposition" in their own docs. It's what they don't let you replace.

### Language model (LLM)

The brain. Given the conversation history + system prompt + tool schemas, produces either the next message to speak or a tool call. Standard `/chat/completions`-shaped contract.

### Text-to-speech (TTS, voice)

Takes the LLM's text output and synthesizes streaming audio. ElevenLabs, PlayHT, Cartesia, OpenAI TTS. Often this is the dominant latency in a turn.

### Tool dispatcher

When the LLM emits a tool call (with a name and arguments), something has to actually execute the tool, get a result, and feed it back to the LLM. This is the layer where "I asked the bank to look up my card" turns into a SQL query and a return value.

### Conversation state / artifacts

Recording the call, storing the transcript, persisting structured outputs, billing. Usually post-processing.

That's the seven-layer stack. With that vocabulary, we can be precise about Vapi.

## Where each layer lives in Vapi

This is taken straight from Vapi's own [data-flow docs](https://docs.vapi.ai/security-and-privacy/data-flow). It's the most useful single table in their entire documentation.

| Layer | Default location | Bring Your Own Key (BYOK) | Bring Your Own Server (Custom Server) |
|---|---|---|---|
| Transport | Vapi | Twilio / Telnyx / Vonage etc. | WebSocket / SIP |
| Transcriber (STT) | Vapi (default Deepgram) | Most providers via your key | Custom WebSocket transcriber |
| **Orchestration** | **Vapi** | **❌ not customizable** | **❌ not customizable** |
| LLM | Vapi (default OpenAI) | Any provider via your key | Custom LLM URL (OpenAI-compatible `/chat/completions`) |
| Voice (TTS) | Vapi (default ElevenLabs/Cartesia/OpenAI) | Any provider via your key | Custom TTS endpoint |
| Storage | Vapi | S3 / GCS / R2 / Azure | Custom storage |

The single non-customizable row is what gives Vapi its shape. Every other row, you can move outside of Vapi's infrastructure. Orchestration stays.

This is the practical meaning of "split runtime": the orchestrator owns the *flow* of a conversation, and around the orchestrator you have a constellation of pluggable model endpoints — each of which can be (a) Vapi's default, (b) your provider account via BYOK, or (c) a server you wrote.

## What the orchestration layer actually does

It's worth lingering on this because if you don't understand what Vapi's orchestrator is doing, the architecture won't make sense. The orchestrator's job is everything between "raw audio in" and "raw audio out" *except* the model calls. So:

- It owns the realtime audio session — opening it, keeping it warm, closing it.
- It owns the timeline. It knows what time it is in the conversation, how long the user has been talking, when to commit a turn.
- It runs the endpointing model that decides "the user is done." This is not a fixed-timeout VAD; it's a learned model that combines audio cues, partial transcript content, prosody, and dialog context.
- It runs the interruption-detection model.
- It runs the backchannel and filler injection models.
- It calls the STT service, the LLM service, the TTS service. It strings their outputs together. It paces TTS playback so it doesn't go faster than realtime. It overlaps STT and TTS where useful (the user can speak while the assistant is mid-sentence; the orchestrator decides how to react).
- It maintains the per-call conversation state — the messages, the tool calls, the assistant config.
- It routes tool calls — when the LLM emits one, it executes whichever bound action is configured (HTTP webhook, internal code tool, default action, integration).

So when we say "the agent runs at Vapi," what we really mean is "the agent *loop* runs at Vapi." The loop is roughly:

```
while call is open:
    if user is speaking:
        stream audio to STT
        update partial transcript
        if endpointing model says "user done":
            commit transcript
            call LLM with history + tools
            if LLM returned a tool call:
                invoke tool (webhook | code | default | integration)
                feed tool result back into LLM
                continue
            else:
                stream LLM text to TTS
                stream TTS audio to caller
                while TTS playing:
                    if interruption detected:
                        stop TTS, jump back to STT
```

Every line of that loop runs on Vapi's servers. Your code shows up inside the `invoke tool` step (and only that step, by default — though with custom LLM/transcriber/TTS your code also runs inside those branches).

This is what a self-hosted "agent harness" is, and Vapi runs it for you.

## Lifecycle of a single conversational turn

Concrete walkthrough of what happens when the caller says "my card ending in 4519 was stolen" and Riley responds.

1. **Caller's mic → transport.** PCM frames arrive at Vapi's edge over whatever transport was negotiated (in our case, a WebSocket from the agent process forwarding the Veris actor's audio).
2. **Transport → STT.** Vapi sends the audio to its bound transcriber. Partial transcripts come back: "my", "my card", "my card ending in four five one nine"…
3. **Orchestrator updates the timeline.** Each partial is timestamped. The endpointing model evaluates the partials + audio for "is this a complete turn?"
4. **Endpoint detected.** The orchestrator decides the user finished. It commits the transcript: "my card ending in 4519 was stolen."
5. **Orchestrator calls the LLM.** Vapi POSTs to its bound LLM (default Vapi-managed OpenAI, or your custom URL) with the full message history, the system prompt, and the tools array. The LLM returns: "tool call: `display_card_info_by_last4`, arguments: `{last4: '4519'}`."
6. **Orchestrator looks up the tool.** It finds `display_card_info_by_last4` in this assistant's tools. The tool's type is `function` with a `server.url`.
7. **Orchestrator POSTs to `server.url`.** Vapi sends an HTTP request to your `/tool` endpoint. Body: `{"message": {"type": "tool-calls", "toolCallList": [{"id": "...", "function": {"name": "display_card_info_by_last4", "arguments": "..."}}]}}`.
8. **Your server runs the tool.** Your `/tool` handler dispatches via `BCSAPI.find_card_by_last4("4519")`, which queries postgres, gets the card row, returns a Pydantic model.
9. **Your server responds.** You return `{"results": [{"toolCallId": "...", "result": "<JSON of card>"}]}` over HTTP.
10. **Orchestrator feeds the result to the LLM.** Vapi appends a tool message to the conversation history and asks the LLM for a follow-up. The LLM produces text: "I found your card ending in 4519. Would you like me to freeze it?"
11. **Orchestrator calls TTS.** Vapi sends the text to its bound TTS service. Audio frames stream back.
12. **TTS audio → transport.** Vapi streams the audio to the caller over the WebSocket.
13. **Concurrently, the orchestrator's interruption-detection model is watching.** If the caller starts speaking, Vapi may cut TTS short and jump back to STT.

Out of those 13 steps, the parts on **your servers** are step 8 (and a fraction of 7 and 9 — the HTTP boundary). The rest is Vapi.

If you ran a custom LLM URL, you'd also own step 5 — Vapi would POST `/chat/completions` to your server, and your server would call OpenAI (or whoever) and stream back. Same for custom transcriber (step 2 over a WebSocket) and custom TTS (step 11). Same protocol shape: Vapi makes a request *out* of Vapi cloud to a URL or WS endpoint you provide.

The orchestrator (steps 1, 3, 4, 6, 10, 13, plus the connective tissue) is never on your side.

## Where tools live, in detail

Tools deserve their own section because there are four flavors and they sit at different points on the "in your infrastructure" axis. From most-yours to least-yours:

### 1. Custom function tools (webhook)

You define a tool with `type: "function"` and a `server.url`. When the LLM picks the tool, the orchestrator POSTs to that URL with the tool call payload and waits synchronously for a JSON response containing the result.

**What's at Vapi:** the schema (what the tool is called, what arguments it takes). At call-create time you send the schema inline (or you pre-register it via `POST /tool` and reference its id). Either way, Vapi cloud knows the tool's *interface* during the call.

**What's at your servers:** the implementation. Your `/tool` handler runs the actual work — query a database, call a vendor API, do arithmetic, whatever.

This is what riley-vapi uses for all five BCS tools.

### 2. MCP tools

You stand up a [Model Context Protocol](https://modelcontextprotocol.io) server. Vapi connects to it as an MCP client and lets the LLM call tools that the MCP server exposes. The protocol is richer than a single HTTP call — it supports tool discovery, server-initiated notifications, etc.

**What's at Vapi:** the MCP client + the binding into the conversation.

**What's at your servers:** the MCP server itself, plus all the tool implementations behind it.

Functionally close to the webhook pattern but with a different protocol. Same reachability constraints (Vapi cloud needs to reach your MCP server).

### 3. Code tools

You write a TypeScript snippet inline on the tool definition. Vapi runs it in a sandbox on Vapi's infrastructure when the LLM picks the tool. Your snippet has access to `args` (the LLM's tool-call arguments) and `env` (secrets you configured on the tool).

**What's at Vapi:** the snippet's source code (you uploaded it), the runtime that executes it, the secrets it can read.

**What's at your servers:** nothing. The tool's implementation lives at Vapi cloud, written in TypeScript.

This is useful if you don't want to host a webhook and the tool's work is something that can be done from Vapi's network (e.g., calling a public REST API, formatting data, doing arithmetic). It's *not* useful if the tool needs to talk to a database in your private network.

### 4. Default tools and integration tools

`transferCall`, `endCall`, `dtmf`, `voicemail`, `sms`, `slack-send-message`, GoHighLevel, Google Calendar, Make.com — these are pre-built tools where both the schema *and* the implementation live at Vapi. You configure them with the data they need (a phone number to transfer to, a Slack channel ID, etc.) and Vapi executes them itself.

**What's at Vapi:** the schema *and* the implementation.

**What's at your servers:** nothing for the tool itself. You might have to authenticate the integration (give Vapi a Slack token, OAuth your Google account, etc.) but the runtime is theirs.

These are typically call-control or integration-with-popular-SaaS operations. There's no built-in default tool for "query my bank's mainframe."

### Tabular summary

| Tool category | Schema lives at | Implementation lives at | Network direction |
|---|---|---|---|
| Custom function (webhook) | Vapi (inline or pre-registered) | Your servers | Vapi → your URL (inbound to you) |
| MCP | Vapi (discovered from your server) | Your MCP server | Vapi → your MCP server (inbound to you) |
| Code | Vapi (you uploaded the TS source) | Vapi (executes in their sandbox) | None — internal |
| Default / Integration | Vapi | Vapi (calls third-party APIs as you) | Vapi → third-party (outbound from Vapi) |

Note the rightmost column. For our agent (custom function tools), Vapi's traffic direction is *toward your servers*. That's the inbound-webhook requirement that drives the whole public-URL discussion.

## Customization hooks beyond tools

The same pattern (Vapi reaches out to a URL or WebSocket you host) recurs for the non-tool model layers:

**Custom LLM URL.** You expose an HTTP endpoint that speaks OpenAI's `/chat/completions` API. Vapi POSTs to it for every LLM turn. Streaming SSE is supported. Authentication via static API key or OAuth2.

**Custom transcriber.** You expose a WebSocket. Vapi opens it at call start, sends an initial JSON message with the audio format (`{"type": "start", "encoding": "linear16", ...}`), then streams binary PCM audio frames. You stream transcript JSON back: `{"type": "transcriber-response", "transcription": "...", "channel": "customer", "transcriptType": "final"}`.

**Custom TTS.** You expose an HTTP endpoint. Vapi POSTs text. You return PCM audio frames (matching the requested sample rate and format).

**Custom transport.** Same shape — Vapi can be the *client* of your audio WebSocket or SIP endpoint, or you can be the client of Vapi's.

The recurring pattern: Vapi cloud initiates the connection out to your URL/WS, you serve the request. It's never "Vapi runs an SDK in your process" or "Vapi exports a library you embed." It's always "Vapi makes a network call out and you respond."

## What crosses the wire

If you watch the network traffic between Vapi cloud and your servers during a call, you'll see roughly:

**Outbound from your side, one-time at call start:**
- `POST https://api.vapi.ai/call` — you tell Vapi "start a call with this assistant config." Returns a call object and (for WebSocket transport) a `websocketCallUrl`.

**Outbound from your side, ongoing during the call:**
- `wss://phone-call-websocket.aws-us-west-2-backend-production1.vapi.ai/{callId}/transport` — the realtime audio + event WebSocket. Binary audio frames flow both ways. JSON event messages flow from Vapi to you (transcript updates, speech-update lifecycle, tool-call notifications, status updates). JSON control messages flow from you to Vapi (add-message, control, say, end-call, transfer, send-transport-message).

**Inbound to your side, ongoing during the call (only for some setups):**
- `POST https://your.public.url/tool` — Vapi posts tool calls here, expects a JSON response. Synchronous.
- `POST https://your.public.url/server` — Vapi posts call lifecycle events here (status updates, conversation updates, end-of-call reports). These can also be sent over the WS as client messages instead, controlled by the `clientMessages` and `serverMessages` lists on the assistant.
- For custom LLM: `POST https://your.public.url/chat/completions`.
- For custom transcriber: `wss://your.public.url/transcribe`.
- For custom TTS: `POST https://your.public.url/tts`.

So the wire shape is a mix of outbound-from-you control + audio (the call create + the WS) and inbound-to-you per-event callbacks (tools, custom-model hooks, optional server URLs for lifecycle events). Both directions are TLS HTTPS / WSS.

The inbound requests are the ones that require your servers to be publicly reachable from Vapi's egress IPs. That's the constraint that drives "we need a public inbound path into the sandbox" in our agent.

## How this compares to peers

Putting Vapi next to other things you've seen:

### vs. LangGraph / OpenAI Agents SDK / CrewAI

These are **frameworks you embed**. You `pip install` them, they expose Python classes (`Agent`, `Tool`, `Graph`), and you write a Python program that constructs an agent and runs the loop. The "agent loop" is *your* code running on *your* machines. Tools are Python functions; the framework just routes the LLM's tool-call decisions to them in-process.

Vapi has no equivalent of this. You can't `pip install vapi` and get the loop. Their server-sdk-python is just a REST API client — it lets you create calls, register assistants, etc., but the loop is still at api.vapi.ai.

Vapi's analog of "your tool is a Python function" is "your tool is an HTTP endpoint." The shift from in-process call to HTTPS call is the architectural cost.

### vs. LiveKit Agents / Pipecat / Vocode

These are **voice-agent frameworks you embed**. You write a Python program that uses their classes (LiveKit's `Agent`, `AgentSession`, `function_tool` decorator; Pipecat's `Pipeline`), it runs in your container. They handle the realtime media plumbing for you (WebRTC for LiveKit, various transports for Pipecat). The voice equivalent of LangGraph.

These are what you should reach for if you want the "everything in my container except model APIs" architecture. They give you Vapi-ish capabilities (turn-taking, interruption handling, TTS pacing) as a library in your process rather than as a service on someone else's servers.

Note that the sibling [`livekit`](../../livekit/) implementation demonstrates this pattern. [`openai-realtime-mini`](../../openai-realtime-mini/) uses OpenAI Realtime, which is a hosted variant of the same idea (the loop runs in your process, but the voice-pipeline bundle runs at OpenAI).

### vs. Parloa / Decagon / Voiceflow / Synthflow

These are **fully hosted no-code platforms**. The agent is a config in their dashboard. You don't write code. The runtime *and* the configuration live at the vendor. You bring a phone number and a business problem.

Vapi sits one step below this. The agent is still a config (an "assistant"), but you can write code (custom LLM/TTS/transcriber/tools, scriptable webhooks) to override almost every step of the pipeline. You can also drive it programmatically (creating calls via API, defining assistants in code rather than the dashboard). It's more "API-first hosted runtime" than "dashboard-first no-code."

### A picture

```
            "I write the loop"           "Vendor writes the loop"
                  │                                    │
                  │                                    │
   LangGraph      │   LiveKit Agents,                  │
   OpenAI Agents  │   Pipecat, Vocode                  │
   SDK            │       │                            │
   ───────────    │   ───────────                      │
   text agents    │   voice agents                     │
                  │       │                            │
                  │       │       Vapi, Retell        Parloa, Decagon,
                  │       │           │               Voiceflow, Synthflow
                  │       │           │                    │
                  │       │           │                    │
        ◄─── you write everything ─── you write hooks ─── you write config in a dashboard ──►
        ◄────── code-first  ─────────────────────────────── no-code ──────────────────────►
        ◄────── runs in your process ──── runs at vendor with hooks ─── runs at vendor ──►
```

Vapi is closer to Parloa than to LangGraph on the runtime-location axis, but it's much closer to LangGraph than to Parloa on the code-first axis. That's the in-between feel.

## Practical implications of the split runtime

### Latency

Every model call in the pipeline either runs at Vapi or goes Vapi → you → back. If you keep everything at Vapi defaults, all hops are intra-cloud. If you set up a custom LLM URL, every LLM turn now goes Vapi → your-region → wherever-your-LLM-is → back through you → back to Vapi. Same for custom STT/TTS. Most of the network round-trips happen on Vapi's clock, but every customization layer you add adds latency.

For tool calls specifically, the round-trip is Vapi → your `/tool` → your DB → back → Vapi. In our setup, that means Vapi (us-west-2) → Veris webhook gateway → sandbox pod → your DB → back the same path. We saw 4.2s average response latency in an earlier ngrok-based run, of which a good chunk is this round-trip.

### Reachability and networking

Anything that takes a hook (tool webhooks, custom LLM URL, custom transcriber WS, custom TTS endpoint, server URL for events) requires your endpoint to be publicly reachable from Vapi's egress. If your code lives in a private VPC, you need a tunnel or a public ingress. There's no way around this within Vapi's architecture — they initiate the request, you serve it.

For local dev, ngrok / cloudflared / tailscale-funnel work fine. For production, you'd typically expose the endpoints on a public load balancer with auth (Vapi supports API key and OAuth2 client-credentials for auth on custom endpoints).

### Debuggability

The fact that the loop is at Vapi means you cannot step through it. You can read the call recording, the transcript, and the events Vapi exposes (which is a lot — server messages cover most lifecycle transitions). But "let me put a breakpoint in the turn-taking logic" is not a thing you can do.

For tools and custom-model hooks, you have full debuggability within your own code. The handoff is the HTTP boundary. You can log every Vapi-→-you request and every you-→-Vapi response and reconstruct most of the call from those + the recording.

### Vendor lock-in

The assistant config (model choice, voice choice, tools, prompt) is portable in concept — it's data — but the runtime semantics aren't. If you've come to rely on Vapi's endpointing model behaving a certain way, or its backchannel timing, you can't lift that to LiveKit Agents and expect the same conversation feel. You'd be rebuilding the orchestration layer.

The flip side is that switching the *models* underneath Vapi is easy. You can swap OpenAI for Anthropic for Cerebras with a config change. You can swap ElevenLabs for Cartesia. You can run your own LLM via the custom LLM URL. Pluggability of models is good. Pluggability of the orchestrator is zero.

### Sandbox-resident testing (i.e., Veris)

For an in-sandbox Veris simulation, your agent's process runs in the sandbox pod. The pod has outbound internet (per Veris's network policy) so it can reach api.vapi.ai. The pod is **not** publicly addressable, so anything Vapi tries to POST back to it needs an inbound path.

That's the entire architectural reason a public URL shows up in `riley-vapi`. It's not a quirk of our code; it's a property of *any* Vapi agent that wants its tool implementations to live inside a private network. Veris provides the path: `agent.public_endpoint: true` in veris.yaml makes the platform route a unique `https://voice.api.veris.ai/hooks/{sim_id}` URL through its shared webhook gateway to the pod's `:8008`, injected as `PUBLIC_BASE_URL` — per-sim URLs, so parallel sims never collide. (Earlier versions spawned a per-sim **cloudflared quick tunnel** instead — chosen over ngrok because free-tier ngrok pins one static domain per account (`ERR_NGROK_334`) — but Cloudflare throttles quick tunnels per egress IP, and the whole cluster shares one NAT IP, which capped parallelism for every framework.)

If you wanted to eliminate the inbound requirement entirely while keeping Vapi, the options are:
- Move tool implementations to code tools (TS at Vapi cloud) — possible if the tools don't need access to private data.
- Move tool implementations to default/integration tools — possible if Vapi already has a connector for the thing.
- Make the agent's data publicly reachable through a different channel (e.g., a public API gateway in front of your DB) — usually worse than tunneling /tool.

None of those preserve "everything runs in my sandbox and only model APIs are external." That property is incompatible with using Vapi at all, because Vapi *is* an external orchestrator.

## How riley-vapi maps onto all this

To make this concrete, here's exactly where every piece of our specific agent lives.

```
Vapi cloud (api.vapi.ai)
├── Orchestration runtime (always Vapi)
│   ├── Endpointing model
│   ├── Interruption detection
│   ├── Background noise/voice filtering
│   ├── Backchanneling
│   ├── Filler injection
│   └── Turn manager
├── Transcriber (Deepgram nova-3, BYOK via Vapi's Deepgram account by default)
├── LLM (OpenAI gpt-4.1-mini, BYOK via Vapi's OpenAI account by default)
├── TTS (ElevenLabs "Sarah", BYOK via Vapi's ElevenLabs account by default)
└── Transport endpoint (wss://phone-call-websocket.aws-us-west-2-backend-production1.vapi.ai/{callId}/transport)

Sandbox pod
├── FastAPI agent process (app.main)
│   ├── /voice           — actor (Veris persona) connects here over WS, PCM16 @ 24kHz
│   ├── /tool            — Vapi's tool-call webhooks land here
│   ├── /health          — liveness check
│   └── _create_vapi_call() — outbound POST to api.vapi.ai/call per /voice connection
├── BCSAPI (app.db)      — postgres-backed tool implementations
├── Tool schemas (app.tools)
│   ├── display_user_info
│   ├── display_card_info_by_last4
│   ├── change_card_status
│   ├── request_card_replacement
│   └── update_card_replacement_status
├── System prompt (agent_desc.txt, loaded at import time)
└── Postgres (in-pod, seeded from db/init.sql)

Veris webhook gateway (platform infrastructure, agent.public_endpoint opt-in)
└── Routes the public https://voice.api.veris.ai/hooks/{sim_id} URL
    (injected as PUBLIC_BASE_URL) to the in-pod :8008

Wire traffic during a call
├── Outbound from sandbox:
│   ├── api.vapi.ai/call (HTTPS, once per call)
│   └── wss://phone-call-websocket...vapi.ai (audio + events, full call duration)
└── Inbound to sandbox via the Veris webhook gateway:
    └── HTTPS POSTs from Vapi cloud to /tool, one per tool call
```

The agent loop, the LLM, STT, TTS, VAD, turn-taking — all at Vapi. The five tool implementations and the database — all in the sandbox. The audio bridge (a thin proxy that copies bytes from one WS to another) lives in our `/voice` handler and is the only piece of "voice plumbing" we wrote. The conversation engine itself is something we rented.

If you wanted to take this agent off Vapi entirely, you'd:
- Delete the Vapi call-create code in `_create_vapi_call()`.
- Replace the Vapi WebSocket bridge with either (a) a direct OpenAI Realtime WS (like the sibling `openai-realtime-mini` implementation does, gives you a hosted-bundle pipeline) or (b) a LiveKit Agents / Pipecat process (gives you an in-process loop you fully own).
- Move tool dispatch into the agent loop where it can be a direct Python call instead of an HTTP webhook.
- Drop `agent.public_endpoint` entirely; no inbound traffic needed.

That hypothetical version would no longer be a Vapi implementation — it'd be a different shape of voice agent, and this repository already has both of those shapes.

## Summary

Vapi is a hosted agent runtime, not a framework. Its product is the orchestration layer — endpointing, interruption detection, turn-taking, the conversation loop — and that layer runs on their servers, full stop. Around the orchestrator, you can plug in your own models (LLM, STT, TTS), your own transport, and your own tool implementations, all by exposing endpoints that Vapi calls out to.

So when you use Vapi:
- The **agent loop** runs at Vapi.
- The **system prompt**, **tool schemas**, **model selection**, and **per-call config** are *configured at Vapi* (either by inlining them in the `POST /call` body or by pre-registering an assistant) but the bytes describing them came from your code.
- The **tool implementations** can run anywhere — at Vapi (code tools, default tools, integration tools) or in your infrastructure (custom function tools via webhook, MCP servers).
- The **conversation dynamics** (VAD, endpointing, interruption detection, backchanneling) are Vapi's proprietary models and not customizable.

You're not running the agent. You're plugging tools and (optionally) models into a hosted agent. That's the architecture.

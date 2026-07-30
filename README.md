# Riley Agent

Riley Agent is a growing collection of voice-agent implementations for the
same card-support workflow. Each implementation gives Riley the same job,
tools, and synthetic Acme Bank data while using a different voice platform or
agent framework.

The repository is intended to make the implementations easy to study, run,
compare, and improve. Pull requests are welcome, including new provider
implementations.

> [!IMPORTANT]
> Riley is a simulation and reference project, not a production banking
> service. The included users, cards, and account data are synthetic. Review
> authentication, authorization, privacy, and provider-cost controls before
> adapting any implementation for real callers or data.

## Implementations

| Directory | Voice stack |
| --- | --- |
| [`assemblyai`](assemblyai/) | AssemblyAI Voice Agent API with a bring-your-own OpenAI LLM |
| [`elevenlabs`](elevenlabs/) | ElevenLabs Conversational AI |
| [`gemini-live`](gemini-live/) | Google Gemini Live API |
| [`livekit`](livekit/) | LiveKit Agents with Deepgram, OpenAI, and ElevenLabs |
| [`nemotron`](nemotron/) | Pipecat with NVIDIA Nemotron ASR, LLM, and TTS services |
| [`openai-realtime-mini`](openai-realtime-mini/) | OpenAI Realtime API |
| [`pipecat`](pipecat/) | Pipecat with Deepgram, OpenAI, and ElevenLabs |
| [`retell`](retell/) | Retell AI with a managed OpenAI LLM and ElevenLabs voice |
| [`vapi`](vapi/) | Vapi with Deepgram, OpenAI, and ElevenLabs |

More implementations may be added as the voice-agent ecosystem evolves.

## Repository structure

Each top-level implementation is an independent Python project. Work from the
implementation's directory rather than treating the repository as one Python
package:

```text
<implementation>/
├── README.md
├── agent_desc.txt
├── app/
├── db/
├── pyproject.toml
├── uv.lock
└── .veris/
```

Each implementation includes:

- A provider-specific voice runtime or bridge.
- The Riley system prompt and card-support tools.
- A synthetic Postgres schema and seed data.
- A locked Python environment.
- Veris simulation configuration.

Start with the implementation's own `README.md` for its architecture,
credentials, and run instructions. Vendor APIs may require separate accounts
and may incur usage charges.

## Contributing

Pull requests are welcome for new implementations, bug fixes, tests,
documentation, security improvements, and updates to existing integrations.

When adding an implementation:

1. Put it in a clearly named top-level directory.
2. Keep it independently installable and runnable.
3. Include a README, `pyproject.toml`, lockfile, and `.veris/` configuration.
4. Use only synthetic example data and never commit credentials.
5. Document the provider stack, required environment variables, and any
   behavior that differs from the other Riley implementations.

Please keep changes focused and explain how they were tested in the pull
request.

## License

Except where otherwise noted, this project is available under the
[MIT License](LICENSE). The NVIDIA-derived text filter is available under the
BSD 2-Clause License described in
[Third-Party Notices](THIRD_PARTY_NOTICES.md).

"""Launcher: run the Hermes Agent gateway with the veris-voice platform plugin.

Hermes Agent is not an ASGI app — it runs as a gateway process that hosts
platform adapters. This module is the single entry point (`python -m app.main`):
it prepares a fresh HERMES_HOME from the checked-in ``hermes_home/`` template,
installs Riley's persona, and hands control to the gateway. The veris-voice
plugin (loaded from that home) serves the ``voice_ws`` endpoint on :8008.

A fresh HERMES_HOME per process matters for the benchmark: Hermes persists
sessions, long-term memory, and learned skills under its home directory, and
copying the template to a temp dir on every boot guarantees each container
starts from the same cold state instead of inheriting artifacts from earlier
runs.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logger = logging.getLogger("riley-hermes")


def _load_agent_prompt() -> str:
    # Resolve from CWD first (matches the `/agent` working dir entry_point),
    # then fall back to a path next to this package.
    for candidate in (
        Path("agent_desc.txt"),
        Path(__file__).resolve().parent.parent / "agent_desc.txt",
    ):
        if candidate.is_file():
            return candidate.read_text()
    raise FileNotFoundError("agent_desc.txt not found")


def main() -> None:
    template = Path(__file__).resolve().parent.parent / "hermes_home"
    home = Path(tempfile.mkdtemp(prefix="riley-hermes-home-"))
    shutil.copytree(template, home, dirs_exist_ok=True)

    # Riley's persona goes in through SOUL.md — Hermes's own per-deployment
    # identity mechanism. It replaces the stock Hermes identity at the top of
    # the system prompt, so the shared agent_desc.txt is used verbatim with no
    # fork and no competing "you are Hermes" preamble.
    (home / "SOUL.md").write_text(_load_agent_prompt())

    # Model override (mirrors MISTRAL_LLM_MODEL in the mistral pair): the
    # Veris environment bakes the model in via this variable. Unset →
    # config.yaml's default (hermes-4-70b).
    model = os.environ.get("HERMES_LLM_MODEL")
    if model:
        import yaml

        cfg_path = home / "config.yaml"
        cfg = yaml.safe_load(cfg_path.read_text())
        cfg["model"]["default"] = model
        cfg["providers"]["nous-key"]["default_model"] = model
        cfg_path.write_text(yaml.safe_dump(cfg))
        logger.info("[main] model override: %s", model)

    os.environ["HERMES_HOME"] = str(home)

    # The nous-key provider block in config.yaml reads this at request time;
    # fail at boot rather than on the first LLM turn.
    if not os.environ.get("NOUS_API_KEY"):
        raise RuntimeError("NOUS_API_KEY is not set")

    # The caller is a synthetic Veris actor with no way to complete Hermes's
    # DM-pairing flow, so the gateway's allowlist chain must be short-circuited
    # or every call would be dropped as unauthorized. The sandbox is isolated;
    # there are no third-party senders to gate.
    os.environ["GATEWAY_ALLOW_ALL_USERS"] = "true"

    # Any non-empty home channel suppresses the per-call "No home channel is
    # set" gateway notice; there is no cron/cross-platform delivery here.
    os.environ["VERIS_VOICE_HOME_CHANNEL"] = "call"

    logger.info("[main] HERMES_HOME=%s", home)

    # Imported after HERMES_HOME is set — hermes resolves its home at import
    # time in several modules.
    from gateway.run import start_gateway

    ok = asyncio.run(start_gateway())
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

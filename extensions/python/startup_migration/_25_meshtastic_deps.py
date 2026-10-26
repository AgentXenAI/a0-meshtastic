"""Re-install Meshtastic Python dependencies on every container start.

The Agent Zero backend runs from /opt/venv-a0, which lives inside the container
image rather than on the persistent /a0/usr volume. Any package installed into it
at runtime is therefore lost whenever the container is recreated (`docker compose
up -d`, an update, a rebuild).

That loss is easy to miss: `bridge.py` imports `meshtastic` and `pubsub` lazily
inside its listener thread, so a missing package does not fail startup - it shows
up as a crash-looping `meshtastic-a0-bridge` thread and silently dead inbound
mesh DMs. The plugin's own `hooks.install()` only runs on plugin install, never on
container start, which is the gap this migration closes.

The fast path (everything importable) is a single subprocess probe. The install
path runs at most once per recreate.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys

from helpers.extension import Extension

logger = logging.getLogger("meshtastic_deps")

# import name -> PyPI requirement
DEPENDENCIES: dict[str, str] = {
    "meshtastic": "meshtastic>=2.3",
    "pubsub": "pypubsub>=4.0",
}

_TIMEOUT_SECONDS = 300

_PROBE = (
    "import importlib.util, json, sys\n"
    "names = json.loads(sys.argv[1])\n"
    "print(json.dumps([n for n in names if importlib.util.find_spec(n) is None]))\n"
)


def framework_python() -> str:
    """Interpreter that actually runs the Agent Zero backend."""
    for candidate in ("/opt/venv-a0/bin/python", sys.executable, "python3"):
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return "python3"


def missing_requirements(python: str) -> list[str]:
    """Requirement strings that are not importable in `python`."""
    try:
        probe = subprocess.run(
            [python, "-c", _PROBE, json.dumps(list(DEPENDENCIES))],
            capture_output=True,
            text=True,
            timeout=60,
        )
        names = json.loads(probe.stdout.strip().splitlines()[-1])
    except Exception:  # noqa: BLE001 - treat an unreadable probe as "install everything"
        return list(DEPENDENCIES.values())
    return [DEPENDENCIES[n] for n in names if n in DEPENDENCIES]


def _install(python: str, requirement: str) -> None:
    uv = shutil.which("uv")
    if uv:
        cmd = [uv, "pip", "install", requirement, "--python", python]
    else:
        cmd = [python, "-m", "pip", "install", requirement]
    subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=_TIMEOUT_SECONDS)


def ensure_dependencies() -> dict[str, object]:
    python = framework_python()
    missing = missing_requirements(python)
    result: dict[str, object] = {"python": python, "installed": [], "errors": []}
    if not missing:
        return result
    errors: list[str] = []
    for requirement in missing:
        try:
            _install(python, requirement)
            result["installed"] = [*result["installed"], requirement]  # type: ignore[list-item]
        except Exception as exc:  # noqa: BLE001 - startup must never fail on this
            errors.append(f"{requirement}: {exc}")
    result["errors"] = errors
    return result


class MeshtasticDependencyMigration(Extension):
    def execute(self, **kwargs):
        try:
            result = ensure_dependencies()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Meshtastic dependency check failed: %s", exc)
            return
        if result["errors"]:
            logger.warning("Meshtastic dependencies failed to install: %s", result["errors"])
        elif result["installed"]:
            logger.info(
                "Meshtastic dependencies reinstalled after container recreate: %s",
                result["installed"],
            )


def run() -> dict[str, object]:
    """Manual repair entry point: python _25_meshtastic_deps.py"""
    return ensure_dependencies()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(run())

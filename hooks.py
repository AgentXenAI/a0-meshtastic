"""Lifecycle hooks for the Meshtastic A0 plugin (install/update/uninstall)."""
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger("meshtastic_hooks")


def _plugin_dir() -> Path:
    return Path(__file__).parent.resolve()


def _find_python() -> str:
    for c in ["/opt/venv-a0/bin/python3", sys.executable, "python3"]:
        if os.path.isfile(c) and os.access(c, os.X_OK):
            return c
    return "python3"


def install(**kwargs):
    plugin_dir = _plugin_dir()
    logger.info("Meshtastic A0 post-install hook...")
    toggle = plugin_dir / ".toggle-1"
    if not toggle.exists():
        toggle.touch()
    (plugin_dir / "data").mkdir(exist_ok=True)
    config_file = plugin_dir / "config.json"
    if not config_file.exists():
        fd = os.open(str(config_file), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump({}, f)
    init_script = plugin_dir / "initialize.py"
    if init_script.is_file():
        try:
            subprocess.run([_find_python(), str(init_script)], check=True,
                           capture_output=True, text=True, timeout=180)
        except Exception as exc:
            logger.warning("Meshtastic dependency install failed: %s", exc)
    logger.info("Meshtastic A0 post-install hook complete")


def update(**kwargs):
    logger.info("Meshtastic A0 update hook (no-op)")


def uninstall(**kwargs):
    logger.info("Meshtastic A0 uninstall hook (no-op)")

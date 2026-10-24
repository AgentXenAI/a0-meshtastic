"""One-time setup for the Meshtastic A0 plugin: install Python dependencies.

Called by the Init button in the Plugin List UI. Defines main() -> 0 on success.
"""
import shutil
import subprocess
import sys
from pathlib import Path


def _find_python():
    venv_python = Path("/opt/venv-a0/bin/python")
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def _install(pip_name, python):
    uv = shutil.which("uv")
    if uv:
        subprocess.check_call([uv, "pip", "install", pip_name, "--python", python])
    else:
        subprocess.check_call([python, "-m", "pip", "install", pip_name])


def main():
    python = _find_python()
    deps = {"meshtastic": "meshtastic>=2.3", "pubsub": "pypubsub>=4.0"}
    failed = []
    for import_name, pip_name in deps.items():
        try:
            __import__(import_name)
        except ImportError:
            try:
                _install(pip_name, python)
            except Exception as exc:
                failed.append(f"{pip_name}: {exc}")
    if failed:
        print("Some dependencies failed: " + "; ".join(failed))
        return 1
    print("Meshtastic A0 dependencies ready.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Authenticated bridge controls and secret-free status for the Meshtastic plugin.

`status` is what the plugin's WebUI panel renders, so two rules apply:

  * it must never return a secret - the Firefly block holds the web-UI password
    in plaintext, and this response goes to a browser;
  * `configured` must agree with `sync_bridge`, or the panel tells the operator
    the bridge is unconfigured while it is happily running.
"""
import logging
from helpers.api import ApiHandler, Request, Response

# Keys that must never leave the backend, whatever the config holds.
_SECRET_KEYS = {"password", "token", "api_key", "apikey", "secret"}


def _redact(obj):
    if isinstance(obj, dict):
        return {k: ("***" if str(k).lower() in _SECRET_KEYS else _redact(v))
                for k, v in obj.items()}
    if isinstance(obj, list):
        return [_redact(v) for v in obj]
    return obj


def _configured_fallback(conn):
    """Mirror of bridge.is_configured for when that module is not reloaded."""
    conn = conn or {}
    if str(conn.get("host", "")).strip():
        return True
    if str(conn.get("transport", "tcp")).lower() == "firefly":
        return bool(str((conn.get("firefly", {}) or {}).get("base_url", "")).strip())
    return False


class MeshtasticBridgeApi(ApiHandler):
    @classmethod
    def get_methods(cls) -> list[str]:
        return ["POST"]

    @classmethod
    def requires_csrf(cls) -> bool:
        return True

    async def process(self, input: dict, request: Request) -> dict | Response:
        import asyncio
        from usr.plugins.meshtastic_a0.helpers.config import get_meshtastic_config
        from usr.plugins.meshtastic_a0.helpers import bridge
        try:
            action = input.get("action", "status")
            config = get_meshtastic_config()
            if action == "status":
                conn = config.get("connection", {})
                # bridge.py may be an older already-imported module (plugin
                # modules are not hot-reloaded), so do not hard-depend on the
                # helper being present. Same rule as the WebUI: work before and
                # after a restart.
                fn = getattr(bridge, "is_configured", None)
                configured = fn(conn) if fn else _configured_fallback(conn)
                return {"ok": True,
                        "configured": configured,
                        "connection": _redact(conn),
                        "bridge": bridge.get_status(),
                        "recent": bridge.get_recent(10)}
            if action not in ("start", "stop", "restart"):
                return {"ok": False, "error": "Unknown bridge action."}
            if action in ("stop", "restart"):
                await asyncio.to_thread(bridge.stop_bridge)
            if action in ("start", "restart"):
                await asyncio.to_thread(bridge.start_bridge, config)
            return {"ok": True, "bridge": bridge.get_status()}
        except ValueError as exc:
            return {"ok": False, "error": str(exc)}
        except Exception:
            logging.getLogger("meshtastic_bridge").exception("Meshtastic bridge control failed")
            return {"ok": False, "error": "Bridge control failed. Check the Agent Zero log."}

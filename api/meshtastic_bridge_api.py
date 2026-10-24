"""Authenticated bridge controls and secret-free status for the Meshtastic plugin."""
import logging
from helpers.api import ApiHandler, Request, Response


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
                return {"ok": True, "configured": bool(config["connection"].get("host")),
                        "connection": config["connection"], "bridge": bridge.get_status(),
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

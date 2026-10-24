"""Auto-start the Meshtastic chat bridge when configured, without a WebUI chat."""
import logging
from helpers.extension import Extension


class MeshtasticBridge(Extension):
    async def execute(self, **kwargs):
        from usr.plugins.meshtastic_a0.helpers.config import get_meshtastic_config
        from usr.plugins.meshtastic_a0.helpers import bridge
        try:
            await bridge.sync_bridge(get_meshtastic_config())
        except Exception:
            logging.getLogger("meshtastic_bridge").exception("Meshtastic bridge synchronization failed")

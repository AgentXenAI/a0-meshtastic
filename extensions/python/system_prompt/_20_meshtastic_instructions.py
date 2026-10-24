"""Inject the Meshtastic bridge persona into bridge chat contexts."""
import json
from datetime import datetime, timezone
from helpers.extension import Extension


class MeshtasticInstructions(Extension):
    async def execute(self, system_prompt: list[str], **kwargs):
        if not self.agent:
            return
        node = self.agent.context.get_data("meshtastic_a0_node")
        if not node:
            return
        from usr.plugins.meshtastic_a0.helpers.config import get_meshtastic_config
        try:
            config = get_meshtastic_config(self.agent)
        except Exception:
            return
        bridge_cfg = config.get("bridge", {})
        profile_prompt = self.agent.read_prompt("agent.system.main.specifics.md")
        instructions = str(bridge_cfg.get("instructions", "") or "").strip()
        if instructions:
            profile_prompt += "\n\n## Meshtastic bridge instructions\n" + instructions
        extras = {
            "mesh_node": node,
            "connection": config.get("connection", {}).get("name", ""),
            "current_datetime": datetime.now(timezone.utc).isoformat(),
        }
        system_prompt.append(self.agent.read_prompt(
            "meshtastic.bridge.md",
            profile_prompt=profile_prompt,
            instructions=instructions,
            runtime_context=json.dumps(extras, ensure_ascii=False),
        ))

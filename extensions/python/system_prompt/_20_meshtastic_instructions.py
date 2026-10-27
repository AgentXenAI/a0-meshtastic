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
        trusted = bool(self.agent.context.get_data("meshtastic_a0_trusted"))
        profile_prompt = self.agent.read_prompt("agent.system.main.specifics.md")
        instructions = str(bridge_cfg.get("instructions", "") or "").strip()
        # The SILENT token and the public-channel reply gate both describe
        # broadcast airspace only. Presenting a declinable token in a trusted
        # operator DM is how the node ends up answering "Do you read me?" with
        # the literal word SILENT: a real answer is owed there, and the token
        # invites the model to skip it. Trusted DMs get neither clause.
        if trusted:
            instructions = ""
            silence_clause = (
                "- This is a trusted direct message from the operator. A reply is "
                "always expected: answer substantively, and never respond with the "
                "single word SILENT."
            )
        else:
            silence_clause = (
                "- Silence is an available answer. If you judge that a reply is not "
                "warranted, reply with exactly the single word SILENT and nothing "
                "else - the bridge will transmit nothing. Do not explain that you "
                "are declining, and do not write anything like \"no reply sent\" or "
                "\"staying silent\": that text would itself be broadcast, which "
                "defeats the point. Declining means the word SILENT, nothing else."
            )
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
            silence_clause=silence_clause,
            runtime_context=json.dumps(extras, ensure_ascii=False),
        ))

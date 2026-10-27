"""Lock down privileged tools in UNTRUSTED inbound Meshtastic bridge contexts.

Public broadcast traffic is attacker-controlled, so a prompt-injection payload
arriving over the radio must not be able to drive shell, filesystem, browser,
sub-agents, schedulers, or other messaging platforms. In an UNTRUSTED bridge
context (tagged by the bridge with `meshtastic_a0_node` and NOT
`meshtastic_a0_trusted`) only the tools in `bridge.broadcast_allowed_tools`
are permitted; the bridge delivers the `response` text back over the mesh.

Default allowlist is `response` + `search_engine` so the public persona can
answer weather / local-event questions with live info, while everything that
could touch the host stays blocked. Search results are treated as untrusted.

Trusted DMs (a DM from an allowlisted operator node) are NOT locked down and
run the operator's chosen profile with full tools.

Additive and reversible: delete this file to restore default behavior.
"""
from helpers.extension import Extension
from helpers.errors import RepairableException

# Safe fallback if plugin config cannot be read.
DEFAULT_ALLOWED = {"response", "search_engine", "meshtastic_read", "meshview"}


class BridgeToolLockdown(Extension):
    def _allowed_tools(self):
        try:
            from usr.plugins.meshtastic_a0.helpers.config import get_meshtastic_config
            cfg = get_meshtastic_config(self.agent)
            val = (cfg or {}).get("bridge", {}).get("broadcast_allowed_tools")
        except Exception:
            val = None
        if not val:
            allowed = set(DEFAULT_ALLOWED)
        elif isinstance(val, str):
            allowed = {s.strip() for s in val.split(",") if s.strip()}
        else:
            allowed = {str(s).strip() for s in val if str(s).strip()}
        allowed.add("response")  # the bridge always needs to be able to reply
        return allowed

    async def execute(self, tool_name: str = "", **kwargs):
        if not self.agent:
            return
        ctx = self.agent.context
        node = ctx.get_data("meshtastic_a0_node")
        if not node:
            return  # not a mesh bridge context
        if ctx.get_data("meshtastic_a0_trusted"):
            return  # trusted operator DM: full tools allowed
        if tool_name in self._allowed_tools():
            return
        # Deliberately lists NO allowed tools. An inventory of which doors are
        # soft is a key ring handed to the adversary: this string lands in the
        # same context the injection payload is written into. Refuse, say nothing
        # about what is permitted, and let the model discover it by trying.
        raise RepairableException(
            f"The tool `{tool_name}` is not available in this context."
        )

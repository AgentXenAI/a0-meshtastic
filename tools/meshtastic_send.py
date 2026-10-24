import asyncio
from helpers.tool import Tool, Response
from usr.plugins.meshtastic_a0.helpers.config import get_meshtastic_config
from usr.plugins.meshtastic_a0.helpers import radio
from usr.plugins.meshtastic_a0.helpers.radio import clamp_bytes, MAX_MESSAGE_BYTES


class MeshtasticSend(Tool):
    """Send a DM or broadcast over the configured Meshtastic node/proxy.

    Meshtastic enforces a hard 200-BYTE payload limit. The mesh is shared,
    low-bandwidth airspace, so text is clamped to 200 bytes before sending.
    """

    async def execute(self, **kwargs) -> Response:
        config = get_meshtastic_config(self.agent)
        conn = config["connection"]
        send_cfg = config["send"]
        host = str(self.args.get("host") or conn.get("host", "")).strip()
        port = self.args.get("port") or conn.get("port", 4403)
        action = str(self.args.get("action", "dm")).strip().lower()
        raw = str(self.args.get("text", "") or "").strip()

        if not host:
            return Response(message="Error: no Meshtastic host configured. Set connection.host in Config.", break_loop=False)
        if not raw:
            return Response(message="Error: text is required.", break_loop=False)

        text = clamp_bytes(raw)
        note = "" if text == raw else f" (truncated to {MAX_MESSAGE_BYTES} bytes)"

        try:
            if action == "dm":
                dest = str(self.args.get("dest", "") or "").strip()
                if not dest:
                    return Response(message="Error: dest is required for a DM (node id or name).", break_loop=False)
                dest_id = await asyncio.to_thread(radio.resolve_node, host, port, dest)
                channel = self.args.get("channel")
                await asyncio.to_thread(radio.send_text, host, port, dest_id, text,
                                        int(channel) if channel is not None else None)
                return Response(message=f"Sent DM to {dest_id} via {host}:{port}{note}.", break_loop=False)
            elif action == "broadcast":
                if not send_cfg.get("allow_broadcast", False):
                    return Response(message="Error: broadcasts are disabled. Enable send.allow_broadcast in Config (spam risk on a live mesh).", break_loop=False)
                channel = self.args.get("channel", send_cfg.get("broadcast_channel", 0))
                await asyncio.to_thread(radio.send_broadcast, host, port, text, int(channel))
                return Response(message=f"Broadcast sent on channel {channel} via {host}:{port}{note}.", break_loop=False)
            else:
                return Response(message=f"Unknown action '{action}'. Use 'dm' or 'broadcast'.", break_loop=False)
        except ValueError as exc:
            return Response(message=f"Error: {exc}", break_loop=False)
        except Exception as exc:
            return Response(message=f"Error sending over Meshtastic: {type(exc).__name__}: {exc}", break_loop=False)

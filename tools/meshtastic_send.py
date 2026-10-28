import asyncio
from copy import deepcopy
from helpers.tool import Tool, Response
from usr.plugins.meshtastic_a0.helpers.config import get_meshtastic_config
from usr.plugins.meshtastic_a0.helpers import radio, egress


class MeshtasticSend(Tool):
    """Send a DM or broadcast over the configured Meshtastic node/proxy.

    Every byte goes through the egress filter (helpers/egress.py): 200-byte
    clamp, silence/meta-refusal rejection, dedupe, templated-phrasing and
    tapback/test-reflex rejection, and the global rate limit. There is no
    bypass - this tool and the bridge reply path share one chokepoint.
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

        eff = deepcopy(config)
        eff["connection"]["host"] = host
        eff["connection"]["port"] = port

        transport = str(conn.get("transport", "tcp") or "tcp").strip().lower()

        try:
            if action == "dm":
                dest = str(self.args.get("dest", "") or "").strip()
                if not dest:
                    return Response(message="Error: dest is required for a DM (node id or name).", break_loop=False)
                if transport == "firefly":
                    from usr.plugins.meshtastic_a0.helpers import firefly as ff
                    dest_id = dest if dest.startswith("!") else "!" + format(int(dest), "08x") if dest.isdigit() else ""
                    if not dest_id:
                        client = await asyncio.to_thread(ff.get_client, eff["connection"])
                        rows = await asyncio.to_thread(client.nodes, 400)
                        matches = [r for r in rows if dest.casefold() in (r["name"] or "").casefold()
                                   or dest.casefold() == (r["short"] or "").casefold()]
                        if len(matches) == 1:
                            dest_id = matches[0]["id"]
                        elif not matches:
                            return Response(message=f"Error: no Firefly node matches '{dest}'. Use a node id from meshtastic_read.", break_loop=False)
                        else:
                            return Response(message=f"Error: '{dest}' is ambiguous; matches: "
                                                    + ", ".join(m["id"] for m in matches), break_loop=False)
                    via = eff["connection"].get("firefly", {}).get("base_url", "")
                    ok, reason, _mid = await asyncio.to_thread(
                        egress.send, eff, dest_id, raw,
                        channel=int(self.args.get("channel") or 0), kind="dm")
                    if ok:
                        return Response(message=f"Sent DM to {dest_id} via Firefly {via}.", break_loop=False)
                    return Response(message=f"Not sent (egress: {reason}).", break_loop=False)
                dest_id = await asyncio.to_thread(radio.resolve_node, host, port, dest)
                channel = self.args.get("channel")
                ok, reason, _mid = await asyncio.to_thread(
                    egress.send, eff, dest_id, raw,
                    channel=int(channel) if channel is not None else 0, kind="dm")
                if ok:
                    return Response(message=f"Sent DM to {dest_id} via {host}:{port}.", break_loop=False)
                return Response(message=f"Not sent (egress: {reason}).", break_loop=False)
            elif action == "broadcast":
                if not send_cfg.get("allow_broadcast", False):
                    return Response(message="Error: broadcasts are disabled. Enable send.allow_broadcast in Config (spam risk on a live mesh).", break_loop=False)
                channel = self.args.get("channel", send_cfg.get("broadcast_channel", 0))
                ok, reason, _mid = await asyncio.to_thread(
                    egress.send, eff, "", raw, channel=int(channel), kind="broadcast")
                if ok:
                    via = eff["connection"].get("firefly", {}).get("base_url", "") if transport == "firefly" else f"{host}:{port}"
                    return Response(message=f"Broadcast sent on channel {channel} via {via}.", break_loop=False)
                return Response(message=f"Not sent (egress: {reason}).", break_loop=False)
            else:
                return Response(message=f"Unknown action '{action}'. Use 'dm' or 'broadcast'.", break_loop=False)
        except ValueError as exc:
            return Response(message=f"Error: {exc}", break_loop=False)
        except Exception as exc:
            return Response(message=f"Error sending over Meshtastic: {type(exc).__name__}: {exc}", break_loop=False)

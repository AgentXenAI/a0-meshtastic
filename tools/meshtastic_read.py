import asyncio
from helpers.tool import Tool, Response
from usr.plugins.meshtastic_a0.helpers.config import get_meshtastic_config
from usr.plugins.meshtastic_a0.helpers import radio, bridge


class MeshtasticRead(Tool):
    """Read-only view of the configured Meshtastic node/proxy."""

    async def execute(self, **kwargs) -> Response:
        config = get_meshtastic_config(self.agent)
        conn = config["connection"]
        host = str(self.args.get("host") or conn.get("host", "")).strip()
        port = self.args.get("port") or conn.get("port", 4403)
        action = str(self.args.get("action", "nodes")).strip().lower()
        limit = max(1, min(int(self.args.get("limit", 50)), 200))

        if action == "recent":
            rows = bridge.get_recent(limit)
            if not rows:
                return Response(message="No recent inbound mesh messages captured (bridge not running or no traffic).", break_loop=False)
            lines = [f"{r['ts']} [{r['kind']} ch{r['channel']}] {r['name'] or r['from']}: {r['text']}" for r in rows]
            return Response(message="Recent inbound mesh messages (untrusted):\n" + "\n".join(lines), break_loop=False)

        if not host:
            return Response(message="Error: no Meshtastic host configured. Set connection.host in Config.", break_loop=False)

        try:
            if action == "info":
                info = await asyncio.to_thread(radio.get_info, host, port)
                return Response(message=(f"Node: {info['owner']}\nNode num: {info['node_num']}\n"
                                         f"Firmware: {info['firmware']}\nKnown nodes: {info['nodes']}"), break_loop=False)
            elif action == "nodes":
                rows = await asyncio.to_thread(radio.get_nodes, host, port, limit)
                if not rows:
                    return Response(message="No nodes returned.", break_loop=False)
                lines = [f"{r['id']}  {r['name'] or '(unnamed)'}  [{r['role']}/{r['hw']}]  lastHeard={r['last_heard']}  hops={r['hops']}" for r in rows]
                return Response(message=f"{len(rows)} mesh node(s) via {host}:{port}:\n" + "\n".join(lines), break_loop=False)
            else:
                return Response(message=f"Unknown action '{action}'. Use nodes, info, or recent.", break_loop=False)
        except Exception as exc:
            return Response(message=f"Error reading Meshtastic: {type(exc).__name__}: {exc}", break_loop=False)

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

        transport = str(conn.get("transport", "tcp") or "tcp").strip().lower()

        if transport == "firefly":
            from usr.plugins.meshtastic_a0.helpers import firefly as ff
            if not str((conn.get("firefly", {}) or {}).get("base_url", "")).strip():
                return Response(message="Error: connection.firefly.base_url is not configured.", break_loop=False)
            try:
                client = await asyncio.to_thread(ff.get_client, conn)
                if action == "info":
                    health = await asyncio.to_thread(client.health)
                    return Response(message=(f"Transport: Firefly UDP virtual node\n"
                                             f"Node: {client.long_name or '(unnamed)'} ({client.node_id})\n"
                                             f"Short: {client.short_name}\n"
                                             f"Base URL: {client.base_url}\n"
                                             f"Profile: {client.profile_id}\n"
                                             f"Health: {health.get('status')} "
                                             f"(messages={health.get('messages')})"), break_loop=False)
                if action == "nodes":
                    rows = await asyncio.to_thread(client.nodes, limit)
                    if not rows:
                        return Response(message="No nodes returned.", break_loop=False)
                    lines = [f"{r['id']}  {r['name'] or '(unnamed)'}  [{r['role']}/{r['hw']}]  "
                             f"lastHeard={r['last_heard']}  hops={r['hops']}" for r in rows]
                    return Response(message=f"{len(rows)} mesh node(s) via Firefly {client.base_url}:\n"
                                            + "\n".join(lines), break_loop=False)
                return Response(message=f"Unknown action '{action}'. Use nodes, info, or recent.", break_loop=False)
            except Exception as exc:
                return Response(message=f"Error reading Firefly: {type(exc).__name__}: {exc}", break_loop=False)

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

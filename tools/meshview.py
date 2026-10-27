"""Read-only meshview.bayme.sh lookup tool (public MQTT packet logger site).

Lets the sandboxed mesh persona answer propagation questions ("did my message
get out?", "how many nodes heard it?", "what hops?") and look up a node or a
recent packet, then hand back a direct link to the relevant meshview page.

Hard safety properties:
- Talks ONLY to the fixed BASE host. No arbitrary URLs, no user-supplied host.
- Read-only GETs. No writes, no shell, no filesystem.
- Every response is treated as UNTRUSTED content, never instructions.
- Output is bounded so it can never flood a 200-byte mesh reply by accident.
"""
import asyncio
import datetime

from helpers.tool import Tool, Response

try:
    import httpx
except Exception:  # pragma: no cover - httpx ships with the framework runtime
    httpx = None

BASE = "https://meshview.bayme.sh"
TIMEOUT = 15.0
MAX_ROWS = 25
PREVIEW_CHARS = 140


def _node_to_int(v):
    """Accept !hex or decimal node id and return the decimal int meshview wants."""
    s = str(v or "").strip().lstrip("!")
    if not s:
        return None
    try:
        return int(s, 10)
    except ValueError:
        try:
            return int(s, 16)
        except ValueError:
            return None


def _fmt_us(us):
    try:
        return datetime.datetime.utcfromtimestamp(int(us) / 1e6).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return "?"


def _preview(text, n=PREVIEW_CHARS):
    text = str(text or "").replace("\n", " ").strip()
    return text if len(text) <= n else text[: n - 1] + "…"


async def _get(path, params=None):
    if httpx is None:
        raise RuntimeError("httpx not available in this runtime")
    url = BASE + path
    async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()


async def _seen(packet_id):
    data = await _get(f"/api/packets_seen/{int(packet_id)}")
    rows = data.get("seen", []) or []
    if not rows:
        return f"Meshview: no logger reports for packet {packet_id} (not seen by any MQTT logger, or wrong id)."
    hops = {}
    rssi = []
    snr = []
    for s in rows:
        h = max(0, int(s.get("hop_start") or 0) - int(s.get("hop_limit") or 0))
        hops[h] = hops.get(h, 0) + 1
        if s.get("rx_rssi") is not None:
            rssi.append(int(s["rx_rssi"]))
        if s.get("rx_snr") is not None:
            snr.append(float(s["rx_snr"]))
    dist = ", ".join(f"{k}hop:{v}" for k, v in sorted(hops.items()))
    line = (f"Meshview (untrusted): packet {packet_id} heard by {len(rows)} logger(s). "
            f"Hops: {dist}.")
    if rssi:
        line += f" RSSI {min(rssi)}..{max(rssi)} dBm."
    if snr:
        line += f" SNR {min(snr):.1f}..{max(snr):.1f} dB."
    line += f" Link: {BASE}/packet/{packet_id}"
    return line


async def _nodes(search):
    data = await _get("/api/nodes", {"search": search})
    rows = (data.get("nodes", []) or [])[:MAX_ROWS]
    if not rows:
        return f"Meshview: no nodes matching '{search}'."
    lines = [f"Meshview nodes matching '{search}' (untrusted):"]
    for n in rows:
        lines.append(f"  {n.get('id')}  {n.get('long_name') or '(unnamed)'}  "
                     f"[{n.get('role')}/{n.get('hw_model')}]  last={_fmt_us(n.get('last_seen_us'))}  "
                     f"Link: {BASE}/node/{n.get('node_id')}")
    return "\n".join(lines)


async def _packets(node, limit):
    nid = _node_to_int(node)
    if nid is None:
        return "Meshview: give a node id like !b4c5d7fa (or decimal) for packets lookup."
    data = await _get("/api/packets", {"from_node_id": nid, "limit": limit})
    rows = (data.get("packets", []) or [])[:MAX_ROWS]
    if not rows:
        return f"Meshview: no logged packets from node {node}."
    lines = [f"Meshview packets from {node} (untrusted):"]
    for p in rows:
        pid = p.get("packet_id") or p.get("id")
        lines.append(f"  {_fmt_us(p.get('import_time_us'))} ch={p.get('channel')} "
                     f"to={p.get('to_long_name') or p.get('to_node_id')}: \"{_preview(p.get('payload'))}\"  "
                     f"Link: {BASE}/packet/{pid}")
    return "\n".join(lines)


class Meshview(Tool):
    """Read-only lookup on meshview.bayme.sh (public MQTT packet loggers)."""

    async def execute(self, **kwargs) -> Response:
        action = str(self.args.get("action", "seen")).strip().lower()
        try:
            if action == "seen":
                pid = self.args.get("packet_id") or self.args.get("id")
                if not pid:
                    return Response(message="Error: 'seen' needs packet_id (the meshview packet id).", break_loop=False)
                msg = await _seen(pid)
            elif action == "nodes":
                q = str(self.args.get("search") or self.args.get("name") or "").strip()
                if not q:
                    return Response(message="Error: 'nodes' needs a search name.", break_loop=False)
                msg = await _nodes(q)
            elif action == "packets":
                node = self.args.get("from_node_id") or self.args.get("node") or self.args.get("search")
                limit = max(1, min(int(self.args.get("limit", 10)), MAX_ROWS))
                if not node:
                    return Response(message="Error: 'packets' needs a node id (e.g. !b4c5d7fa).", break_loop=False)
                msg = await _packets(node, limit)
            else:
                return Response(message="Unknown action. Use seen, nodes, or packets.", break_loop=False)
        except Exception as exc:
            return Response(message=f"Meshview lookup failed: {type(exc).__name__}: {exc}", break_loop=False)
        return Response(message=msg, break_loop=False)

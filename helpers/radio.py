"""Meshtastic TCP radio helpers: connect, send, read.

All functions open a short-lived TCP connection to the node/proxy, perform one
operation, and close. Safe to call while the bridge holds its own connection
(Meshmonitor proxies allow multiple clients).

Meshtastic enforces a hard 200-BYTE payload limit per text message. The mesh is
shared, low-bandwidth airspace, so every outbound message is clamped to 200 bytes
(UTF-8, never splitting a multibyte character) at this single chokepoint.
"""
import collections
import logging
import threading

logger = logging.getLogger("meshtastic_radio")

# Meshtastic maximum text payload, in BYTES (not characters).
MAX_MESSAGE_BYTES = 200


def clamp_bytes(text, limit=MAX_MESSAGE_BYTES):
    """Truncate text to at most `limit` UTF-8 bytes without splitting a character."""
    text = str(text or "")
    if not text:
        return ""
    encoded = text.encode("utf-8")
    if len(encoded) <= limit:
        return text
    # Trim to the byte budget, then drop any partial trailing multibyte char.
    return encoded[:limit].decode("utf-8", errors="ignore")


class NoHeartbeatTCPInterface:
    """Factory returning a TCPInterface subclass with the heartbeat disabled.

    The Meshtastic client sends a periodic heartbeat; a Meshmonitor proxy drops
    the connection when it sees the idle heartbeat, which kills long-lived
    listeners. Disabling the heartbeat keeps the connection alive. Defined lazily
    so importing this module never requires the meshtastic package.
    """

    _cls = None
    _lock = threading.Lock()

    @classmethod
    def get(cls):
        with cls._lock:
            if cls._cls is None:
                from meshtastic.tcp_interface import TCPInterface

                class _NoHeartbeat(TCPInterface):
                    def _startHeartbeat(self):  # noqa: N802 - match library name
                        self.heartbeatTimer = None

                cls._cls = _NoHeartbeat
            return cls._cls


def _connect(host, port, no_nodes=False):
    iface_cls = NoHeartbeatTCPInterface.get()
    return iface_cls(host, portNumber=int(port), connectNow=True, noNodes=no_nodes)


def send_text(host, port, dest, text, channel=None):
    """Send a DM to a node id (e.g. '!12345678'). Text is clamped to 200 bytes.

    Returns the on-wire packet id (int) of the sent message, or None if the
    client did not report one. The bridge records that id so it can recognise a
    later reply to this message.
    """
    text = clamp_bytes(text)
    iface = _connect(host, port, no_nodes=True)
    try:
        kwargs = {"destinationId": dest}
        if channel is not None:
            kwargs["channelIndex"] = int(channel)
        pkt = iface.sendText(text, **kwargs)
        return getattr(pkt, "id", None)
    finally:
        try:
            iface.close()
        except Exception:
            pass


def send_broadcast(host, port, text, channel=0):
    """Broadcast to ^all on a channel. Text is clamped to 200 bytes.

    Returns the on-wire packet id (int) of the sent message, or None.
    """
    text = clamp_bytes(text)
    iface = _connect(host, port, no_nodes=True)
    try:
        pkt = iface.sendText(text, channelIndex=int(channel))
        return getattr(pkt, "id", None)
    finally:
        try:
            iface.close()
        except Exception:
            pass


def get_info(host, port):
    iface = _connect(host, port)
    try:
        info = getattr(iface, "myInfo", None)
        meta = getattr(iface, "metadata", None)
        owner = getattr(iface, "owner", "")
        node_num = getattr(info, "my_node_num", None) if info else None
        fw = getattr(meta, "firmware_version", "") if meta else ""
        return {
            "owner": owner,
            "node_num": node_num,
            "firmware": fw,
            "nodes": len(getattr(iface, "nodes", {}) or {}),
        }
    finally:
        try:
            iface.close()
        except Exception:
            pass


def get_nodes(host, port, limit=100):
    iface = _connect(host, port)
    try:
        rows = []
        for node_id, node in (getattr(iface, "nodes", {}) or {}).items():
            user = node.get("user", {}) if isinstance(node, dict) else {}
            row = {
                "id": node_id,
                "name": user.get("longName", ""),
                "short": user.get("shortName", ""),
                "role": user.get("role", ""),
                "hw": user.get("hwModel", ""),
                "last_heard": node.get("lastHeard"),
                "hops": node.get("hops"),
            }
            rows.append(row)
            if len(rows) >= limit:
                break
        return rows
    finally:
        try:
            iface.close()
        except Exception:
            pass


def resolve_node(iface_or_host, port=None, dest=""):
    """Resolve a node id or name to a node id string using a live connection."""
    dest = str(dest or "").strip()
    if not dest:
        return ""
    if dest.startswith("!") or dest.isdigit():
        return dest if dest.startswith("!") else "!" + format(int(dest), "08x")
    # Treat as a name lookup.
    host = iface_or_host
    rows = get_nodes(host, port, limit=200)
    matches = [r for r in rows if dest.casefold() in (r["name"] or "").casefold()
               or dest.casefold() == (r["short"] or "").casefold()]
    if len(matches) == 1:
        return matches[0]["id"]
    if not matches:
        raise ValueError(f"No mesh node matches '{dest}'. Use a node id from meshtastic_read.")
    raise ValueError(f"'{dest}' is ambiguous; matches: " + ", ".join(m["id"] for m in matches))

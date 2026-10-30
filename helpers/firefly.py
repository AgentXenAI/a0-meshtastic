"""Firefly transport: talk to a Firefly UDP virtual node over its HTTP API.

Firefly (github.com/pdxlocations/firefly) runs a Meshtastic *virtual node* on
UDP multicast (239.0.0.69:4403) instead of a serial radio, backed by meshdb for
node discovery. A typical deployment is a host-side container with
network_mode: host and the web UI on :5011.

Why this exists: the serialHal modem path was unreliable, and a bridge-network
container cannot do multicast at all - so the radio has to be a host-side
process. Firefly gives us that plus a web UI an operator can manage, and a
LoRa gateway node rebroadcasts our packets onto the air.

API facts this module depends on (verified against the running container):
  * Auth is session-cookie only. POST /login with form fields username/password
    and header X-Requested-With: fetch returns JSON. There is no API token.
  * Every useful endpoint is @login_required; only /api/health is open.
  * A profile must be bound to the session with POST /api/current-profile
    {"profile_id": ...} before /api/messages or /api/send-message will work.
  * /api/messages has no cursor: rows carry a UUID `id`, a monotonic `packet_id`
    and a `direction` of "sent" or "received". So the poller dedupes on
    packet_id and treats the first poll as a baseline.
  * /api/send-message returns a success flag but NO packet id. Our own sent
    packets show up in /api/messages with direction "sent" and a real packet_id,
    so the reply ledger is fed from there instead.

Nothing here imports meshtastic or mudp; it is plain HTTP with `requests`.
"""
from __future__ import annotations

import logging
import threading
import time

import requests

logger = logging.getLogger("meshtastic_firefly")

# Bounded set of packet ids already handled, so a restart or an overlapping poll
# cannot replay old traffic into the filters.
_SEEN_MAX = 2000


class FireflyError(RuntimeError):
    pass


class _SelfInfo:
    """Mimics meshtastic's `iface.myInfo` closely enough for the bridge."""

    def __init__(self, node_num):
        self.my_node_num = node_num


class FireflyAdapter:
    """Stand-in for a meshtastic interface object.

    bridge._handle_packet only ever touches `myInfo.my_node_num` and `nodes`, so
    a tiny shim keeps the dumb/smart filter code transport-agnostic.
    """

    def __init__(self, node_id="", node_num=None):
        self.owner = ""
        self.node_id = node_id
        self.myInfo = _SelfInfo(node_num) if node_num else None
        self.nodes = {}

    def remember_node(self, node_id, name=""):
        if not node_id:
            return
        entry = self.nodes.setdefault(node_id, {"user": {}})
        if name:
            entry["user"]["longName"] = name


class FireflyClient:
    """Session-cookie HTTP client for one Firefly instance."""

    def __init__(self, base_url, username, password, profile_id="", timeout=20):
        self.base_url = str(base_url or "").rstrip("/")
        self.username = str(username or "")
        self.password = str(password or "")
        self.profile_id = str(profile_id or "")
        self.timeout = timeout
        self._session = requests.Session()
        self._logged_in = False
        self._profile_bound = False
        self.node_id = ""
        self.node_num = None
        self.long_name = ""
        self.short_name = ""

    # ---- session plumbing -------------------------------------------------
    def _looks_unauthenticated(self, resp):
        if resp.status_code in (401, 403):
            return True
        # Flask redirects to /login and requests follows it, so a page that came
        # back 200 but is HTML means the session died.
        if "text/html" in (resp.headers.get("content-type") or ""):
            return True
        return False

    def login(self):
        if not self.base_url:
            raise FireflyError("firefly base_url is not configured")
        if not self.username or not self.password:
            raise FireflyError("firefly username/password are not configured")
        resp = self._session.post(
            self.base_url + "/login",
            data={"username": self.username, "password": self.password},
            headers={"X-Requested-With": "fetch"},
            timeout=self.timeout,
        )
        if resp.status_code not in (200, 302):
            raise FireflyError(f"firefly login failed: HTTP {resp.status_code} {resp.text[:160]}")
        self._logged_in = True
        self._profile_bound = False
        return True

    def select_profile(self, profile_id=None):
        pid = str(profile_id or self.profile_id or "").strip()
        if not pid:
            # No explicit profile: take the only one we own, if there is exactly one.
            rows = self._request("GET", "/api/profiles")
            items = rows.values() if isinstance(rows, dict) else (rows or [])
            ids = [p.get("id") for p in items if isinstance(p, dict) and p.get("id")]
            if len(ids) == 1:
                pid = ids[0]
            else:
                raise FireflyError(
                    "firefly profile_id is not configured and could not be inferred "
                    f"(found {len(ids)} profiles). Set connection.firefly.profile_id.")
        resp = self._session.post(
            self.base_url + "/api/current-profile",
            json={"profile_id": pid, "channel_index": 0},
            timeout=self.timeout,
        )
        if resp.status_code != 200:
            raise FireflyError(f"firefly profile select failed: HTTP {resp.status_code} {resp.text[:160]}")
        self._profile_bound = True
        self.profile_id = pid
        try:
            data = resp.json()
        except ValueError:
            data = {}
        prof = (data or {}).get("profile") or data or {}
        self.node_id = str(prof.get("node_id") or self.node_id or "")
        self.long_name = str(prof.get("long_name") or self.long_name or "")
        self.short_name = str(prof.get("short_name") or self.short_name or "")
        try:
            self.node_num = int(self.node_id.lstrip("!"), 16) if self.node_id else None
        except ValueError:
            self.node_num = None
        return pid

    def _request(self, method, path, **kw):
        for attempt in (1, 2):
            if not self._logged_in:
                self.login()
            if not self._profile_bound and path not in ("/api/profiles", "/api/health"):
                self.select_profile()
            resp = self._session.request(method, self.base_url + path, timeout=self.timeout, **kw)
            if self._looks_unauthenticated(resp):
                self._logged_in = False
                self._profile_bound = False
                if attempt == 1:
                    continue
                raise FireflyError(f"firefly auth rejected for {path}")
            if resp.status_code == 404:
                return None
            if resp.status_code >= 400:
                raise FireflyError(f"firefly {path} failed: HTTP {resp.status_code} {resp.text[:160]}")
            try:
                return resp.json()
            except ValueError:
                raise FireflyError(f"firefly {path} returned non-JSON")
        return None

    # ---- reads -------------------------------------------------------------
    def health(self):
        try:
            return self._session.get(self.base_url + "/api/health", timeout=self.timeout).json()
        except Exception as exc:
            raise FireflyError(f"firefly unreachable at {self.base_url}: {type(exc).__name__}")

    def messages(self):
        data = self._request("GET", "/api/messages") or {}
        return list(data.get("channel_messages") or []), list(data.get("dm_messages") or [])

    def nodes(self, limit=200):
        data = self._request("GET", "/api/nodes") or {}
        rows = data.get("nodes") or []
        out = []
        for row in rows[: int(limit)]:
            num = row.get("node_num") or row.get("num")
            node_id = row.get("node_id") or row.get("id") or ""
            if not node_id and num:
                node_id = "!" + format(int(num), "08x")
            out.append({
                "id": node_id,
                "name": row.get("long_name") or row.get("longName") or "",
                "short": row.get("short_name") or row.get("shortName") or "",
                "role": row.get("role") or "",
                "hw": row.get("hardware") or row.get("hw_model") or row.get("hwModel") or "",
                "last_heard": row.get("last_heard") or row.get("lastHeard"),
                "hops": row.get("hops") or row.get("hop_limit"),
            })
        return out

    # ---- write -------------------------------------------------------------
    def send(self, text, dest_num=None, channel_index=0, reply_packet_id=None):
        """Send one message. Returns True on success; Firefly reports no packet id."""
        payload = {"message": str(text)}
        if dest_num:
            payload["message_type"] = "dm"
            payload["target_node_num"] = int(dest_num)
        else:
            payload["message_type"] = "channel"
            payload["channel_index"] = int(channel_index or 0)
        if reply_packet_id:
            payload["reply_packet_id"] = int(reply_packet_id)
        for attempt in (1, 2):
            if not self._logged_in:
                self.login()
            if not self._profile_bound:
                self.select_profile()
            resp = self._session.post(self.base_url + "/api/send-message", json=payload,
                                      timeout=self.timeout)
            if self._looks_unauthenticated(resp):
                self._logged_in = False
                self._profile_bound = False
                if attempt == 1:
                    continue
                raise FireflyError("firefly auth rejected on send")
            if resp.status_code >= 400:
                raise FireflyError(f"firefly send failed: HTTP {resp.status_code} {resp.text[:200]}")
            return True
        return False


# ---- shared clients -------------------------------------------------------
# The poll loop and every send share one session per (base_url, username) so a
# bridge turn does not re-login for each message.
_clients = {}
_clients_lock = threading.RLock()


def get_client(conn):
    """Return a logged-in FireflyClient for a connection config block."""
    ff = (conn or {}).get("firefly") or {}
    base = str(ff.get("base_url") or "").strip()
    if not base:
        raise FireflyError("connection.firefly.base_url is not configured")
    key = (base, str(ff.get("username") or ""), str(ff.get("profile_id") or ""))
    with _clients_lock:
        client = _clients.get(key)
        if client is None:
            client = FireflyClient(base, ff.get("username", ""), ff.get("password", ""),
                                   ff.get("profile_id", ""))
            client.login()
            client.select_profile()
            _clients[key] = client
        return client


def reset_client(conn=None):
    """Drop cached clients (used after an auth failure or config change)."""
    with _clients_lock:
        if conn is None:
            _clients.clear()
            return
        ff = (conn or {}).get("firefly") or {}
        key = (str(ff.get("base_url") or ""), str(ff.get("username") or ""),
               str(ff.get("profile_id") or ""))
        _clients.pop(key, None)


def node_id_to_num(node_id):
    text = str(node_id or "").strip().lstrip("!")
    if not text:
        return None
    try:
        return int(text, 16)
    except ValueError:
        try:
            return int(text)
        except ValueError:
            return None


def row_to_packet(row):
    """Normalise a Firefly message row into the meshtastic packet shape.

    Keeping one packet shape means the dumb filter, smart filter and reply-gate
    in bridge.py are identical for TCP radios and for Firefly.
    """
    sender = str(row.get("sender") or "")
    if not sender.startswith("!") and row.get("sender_num"):
        sender = "!" + format(int(row["sender_num"]), "08x")
    target = row.get("target")
    if not target:
        target = "^all" if (row.get("message_type") or "channel") == "channel" else sender
    return {
        "from": int(row.get("sender_num") or 0) or None,
        "fromId": sender,
        "toId": target,
        "channel": int(row.get("channel") or 0),
        "decoded": {
            "portnum": "TEXT_MESSAGE_APP",
            "text": str(row.get("content") or ""),
            "replyId": row.get("reply_packet_id"),
        },
        "_firefly": {
            "packet_id": row.get("packet_id"),
            "direction": row.get("direction"),
            "sender_display": row.get("sender_display"),
            "timestamp": row.get("timestamp"),
        },
    }

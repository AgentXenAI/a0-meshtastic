"""Outbound egress filter - the third filter.

Final gate for anything that reaches the mesh. Mirror of the dumb inbound
filter: no model, no judgment, pure pattern checks, fail closed. The inbound
dumb filter protects Xen from the mesh; this protects the mesh from Xen.

It is dumb on purpose: the thing that put the literal word SILENT on public
airtime was Xen's own judgment producing bad outbound text, so the last gate
cannot be another layer of judgment.

EVERY outbound path must go through send() below. A filter with a bypass is
decoration. Today the bridge's broadcast reply calls radio.send_broadcast
directly and walks around any tool-level filter; that is exactly the hole this
closes. radio.py keeps its own clamp for legacy callers, but the chokepoint is
here.

Checks, in order (spec 4):
  1. 200-byte UTF-8 clamp (never split a multibyte char)
  2. silence token / meta-refusal narration never transmits
  3. dedupe: reject near-identical to a recent send
  4. templated / refusal-narration phrasing
  5. hop-count tapback shape (emoji-only, number-only, "N hops")
  6. test-message reflex (reply that only acknowledges a test/ping/123)
  7. global rate limit, enforced here at the chokepoint

Fail closed: if any check or the send itself raises, nothing is transmitted.
"""
from __future__ import annotations

import collections
import difflib
import re
import threading
import time

MAX_MESSAGE_BYTES = 200

SILENCE_TOKEN = "SILENT"

_lock = threading.RLock()

# Recent outbound texts for the dedupe check: deque of (monotonic_ts, normalized).
_recent_sends = collections.deque(maxlen=40)
# Global send timestamps for the rate limiter.
_send_times = collections.deque(maxlen=64)

# Rate limit defaults; overridden by configure() from plugin config.
_rate_max = 3
_rate_window = 120.0


def configure(rate_limit_max=3, rate_limit_window=120):
    """Set the global egress rate limit from plugin config."""
    global _rate_max, _rate_window
    try:
        _rate_max = max(1, int(rate_limit_max))
    except (TypeError, ValueError):
        _rate_max = 3
    try:
        _rate_window = float(rate_limit_window)
    except (TypeError, ValueError):
        _rate_window = 120.0


def clamp_bytes(text, limit=MAX_MESSAGE_BYTES):
    """Truncate to at most `limit` UTF-8 bytes without splitting a character."""
    text = str(text or "")
    if not text:
        return ""
    encoded = text.encode("utf-8")
    if len(encoded) <= limit:
        return text
    return encoded[:limit].decode("utf-8", errors="ignore")


# --- silence / meta-refusal ------------------------------------------------
_META_REFUSAL = re.compile(
    r"(no\s+mesh\s+reply|not\s+addressed\s+to\s+me|staying\s+(silent|quiet)|"
    r"(will|would)\s+(stay|remain)\s+(silent|quiet)|no\s+reply\s+(sent|needed|required)|"
    r"(staying|remaining)\s+silent|save\s+airtime|not\s+responding|"
    r"i\s+(can't|cannot|am\s+not\s+able|won't|will\s+not)\s+(reply|respond|answer))",
    re.IGNORECASE,
)

# Templated assistant boilerplate that must never hit shared airtime.
_TEMPLATE = re.compile(
    r"(as\s+an\s+(ai|language\s+model|assistant)|i'?m\s+an\s+(ai|language\s+model)|"
    r"i\s+can'?t\s+help\s+(with|you)\b|i\s+am\s+(unable|not\s+able)\s+to\b|"
    r"as\s+a\s+(large\s+)?(language\s+)?model|i\s+don'?t\s+have\s+the\s+ability|"
    r"my\s+(instructions|rules|guidelines|programming)\s+(prevent|forbid|don'?t\s+let)|"
    r"i\s+must\s+(decline|remain\s+silent)|i\s+have\s+to\s+stay\s+silent)",
    re.IGNORECASE,
)

# Hop-count tapback / bot-identifying shapes.
_HOPS = re.compile(r"\b\d+\s*(hops?|hop)\b", re.IGNORECASE)
_EMOJI_ONLY = re.compile(
    r"^[\s\W\d_]+$", re.UNICODE)  # no letters at all -> tapback-ish
_HAS_LETTER = re.compile(r"[A-Za-z\u00c0-\uffff]")
_PURE_NUMBER = re.compile(r"^[\s\d]+$", re.UNICODE)

# Test-shaped inbound and the acknowledgement-only replies we refuse to send.
_TEST_INBOUND = re.compile(r"^\s*(test|ping|echo|123|1234|test\s*\d*|are\s+you\s+there|\?+)\s*[!.?]*\s*$", re.IGNORECASE)
_ACK_ONLY = re.compile(
    r"^(copy|roger|roger\s+that|copy\s+that|received|loud\s+and\s+clear|5\s*5\s*5|"
    r"ok|okay|k|yes|yep|yeah|here|i'?m\s+here|test|echo|ping|\d+|\d+[!.]*)$", re.IGNORECASE)


def _normalize(text):
    return re.sub(r"[^0-9a-z]+", " ", str(text or "").lower()).strip()


def _is_emoji_or_symbol_only(text):
    return bool(text) and not _HAS_LETTER.search(text)


def _near_duplicate(text):
    """True if text is near-identical to a recent send (within dedupe window)."""
    norm = _normalize(text)
    if not norm:
        return False
    now = time.monotonic()
    with _lock:
        for ts, prev in list(_recent_sends):
            if now - ts > 900:  # 15 min dedupe window
                continue
            if not prev:
                continue
            if prev == norm:
                return True
            if difflib.SequenceMatcher(None, prev, norm).ratio() >= 0.92:
                return True
    return False


def _rate_ok():
    now = time.monotonic()
    with _lock:
        while _send_times and now - _send_times[0] > _rate_window:
            _send_times.popleft()
        if len(_send_times) >= _rate_max:
            return False
        _send_times.append(now)
        return True


def check(text, kind="broadcast", inbound_text=None):
    """Return (allowed: bool, reason: str). Never raises. Fail closed on error."""
    try:
        raw = str(text or "")
        stripped = raw.strip()
        # 1. clamp happens at send; empty after clamp is silence.
        if not stripped:
            return False, "empty"
        # 2. silence token / meta-refusal narration.
        if stripped.strip(".*! ").upper() == SILENCE_TOKEN:
            return False, "silence-token"
        if len(stripped) <= 200 and _META_REFUSAL.search(stripped):
            return False, "meta-refusal"
        # 4. templated assistant boilerplate (checked before dedupe so the
        #    reason is specific).
        if _TEMPLATE.search(stripped):
            return False, "templated-phrasing"
        # 5. hop-count tapback shape.
        if _HOPS.search(stripped) and (inbound_text is None or _TEST_INBOUND.search(inbound_text or "")):
            return False, "hop-tapback"
        if kind == "broadcast" and _is_emoji_or_symbol_only(stripped):
            # emoji-only or number-only reply on public airtime is tapback noise.
            return False, "tapback-shape"
        # 6. test-message reflex: inbound was test-shaped and reply is a bare ack.
        if inbound_text and _TEST_INBOUND.search(inbound_text):
            if _ACK_ONLY.match(stripped) or _is_emoji_or_symbol_only(stripped):
                return False, "test-reflex"
        # 3. dedupe against recent sends.
        if _near_duplicate(stripped):
            return False, "duplicate"
        return True, "ok"
    except Exception as exc:  # fail closed
        return False, f"egress-error:{type(exc).__name__}"


def _remember(text):
    norm = _normalize(text)
    if norm:
        with _lock:
            _recent_sends.append((time.monotonic(), norm))


def send(config, dest, text, channel=0, kind="broadcast", inbound_text=None):
    """The single outbound chokepoint. Returns the on-wire packet id or None.

    kind: "dm" | "broadcast" | "starter". dest ignored for broadcast/starter.
    Enforces every check then delegates to radio. Fail closed.
    """
    try:
        allowed, reason = check(text, kind=kind, inbound_text=inbound_text)
        if not allowed:
            return None, reason
        # 7. rate limit at the chokepoint.
        if not _rate_ok():
            return None, "rate-limited"
        from usr.plugins.meshtastic_a0.helpers import radio
        conn = config["connection"]
        payload = clamp_bytes(text)
        if not payload:
            return None, "empty-after-clamp"
        if kind in ("broadcast", "starter"):
            ch = config.get("send", {}).get("broadcast_channel", channel) or channel
            msg_id = radio.send_broadcast(conn["host"], conn["port"], payload, ch)
        else:
            msg_id = radio.send_text(conn["host"], conn["port"], dest, payload)
        _remember(payload)
        return msg_id, "sent"
    except Exception as exc:  # fail closed - nothing transmitted
        return None, f"send-error:{type(exc).__name__}"

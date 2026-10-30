"""Conversation starter - Xen opening a thread on the mesh unprompted.

Off by default. When enabled it is the only path where Xen speaks first, so it
is deliberately the most constrained thing in the plugin:

  * fires only after `starter_idle_hours` with no inbound traffic
  * at most `starter_max_per_day` per local day, persisted across restarts
  * at most one per `starter_min_interval_minutes`, whatever else says yes
  * never inside quiet hours
  * opens a circuit after `starter_max_failures` consecutive failed attempts
  * the generated text still passes the full egress filter (no bypass)
  * timezone is the system's, and a host/container mismatch is logged loudly

Why the guards are redundant (2026-10-29 incident): six starters went out in 35
minutes on the public channel. `egress.send()` returns a 3-tuple
`(ok, reason, msg_id)`; the tick loop unpacked 2 values, so the transmit
succeeded and the unpack then raised. `_mark_sent()` and `_last_activity_reset()`
were unreachable, so the daily cap and the idle gate both stayed dead and every
300s tick re-fired. A second latent bug sat behind it: success was tested as
`if msg_id:`, but the Firefly transport returns `msg_id=None`, so the caps would
have stayed dead even after the arity fix. Both are fixed here, and the caps are
now independent of the send path so a future arity change cannot silently
re-open the flood.

Config is re-read from disk on every tick. A running thread used to hold the
bridge config dict captured at start, so flipping `starter_enabled` in the WebUI
had no effect until a restart - the operator's off switch was not an off switch.

Timezone note (real, observed in containers): /etc/timezone can say UTC while
TZ names a local zone, so naive local time is correct but /etc/timezone is a liar. We use time.localtime (honours TZ) and warn when /etc/timezone disagrees
with time.tzname, because a quiet-hours window evaluated in the wrong zone will
fire at 3am.
"""
from __future__ import annotations

import difflib
import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone

logger = logging.getLogger("meshtastic_starter")

_thread = None
_stop = threading.Event()
_state_lock = threading.RLock()
_sent_today = {"date": "", "count": 0}
_last_activity = 0.0
_last_send_ts = 0.0
_failures = 0
_recent = []
_status = {"enabled": False, "last_fired": "", "last_reason": "", "sent_today": 0,
           "tz": "", "tz_warning": "", "failures": 0, "min_interval_min": 0,
           "last_discard": ""}

# Daily counter lives on disk so a container or bridge restart cannot hand the
# starter a fresh allowance on the same calendar day.
_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           os.pardir, "data", "starter_state.json")

# How many past openers to keep for the repeat guard.
_RECENT_MAX = 6

# Unverified claims a starter must never make. Every one of these appeared in the
# 2026-10-29 flood: six openers all claimed to be a NEW node, on the east side /
# on a ridge, running solar. None of that is known to this node - the model
# invented a persona because the old prompt told it to "sound like a friendly
# local operator". A prompt can ask for honesty; only a filter enforces it.
_FABRICATION = re.compile(
    r"\b(new node|brand[- ]new node|just (set ?up|brought|bought|installed|put up|came up)"
    r"|first (time|message|post)|solar|wind\s?charg|battery|ridge|hilltop|hilltop|hill"
    r"|(east|west|north|south)[ -]side|downtown|my (mast|antenna|radio|rig|setup|station)"
    r"|rooftop|roof|off[- ]?grid|apartment|backyard|my QTH)\b",
    re.IGNORECASE)

# The prompt is deliberately blunt about what is NOT known. A model that is told
# to be a "friendly local operator" will invent a locality; a model handed an
# explicit list of its own ignorance will not.
_STARTER_PROMPT_TMPL = """You are {who} on a community LoRa mesh channel. Write ONE opening message for the channel.

WHAT YOU ACTUALLY KNOW: {know}You do NOT know your location, your antenna, your power source, your range, your coverage, or whether anyone has seen you before.

HARD RULES - a message that breaks any of these is discarded, not softened:
- State nothing you cannot verify. Never claim to be new, never mention solar, a ridge, a hill, a side of town, a mast, an antenna, a rig, a setup, or a location - you have none of that information. Do not volunteer hardware in an opener either: the machine you run on is an answer for when someone asks, not a way to open a conversation.
- Never write anything that could be mistaken for a human operator talking. You are an AI node and that is fine to be. Do not say you are human; do not hint at it either.
- No yes/no question, no emoji, no markdown, no hashtag.
- Under 160 characters, one or two plain sentences.
- Be worth reading: offer to look something up, ask what people are working on, or ask for a signal report without naming where you think you are.

Reply with the message only."""


def starter_prompt(cfg):
    """Render the starter prompt from operator-supplied facts only.

    Nothing about a deployment is hardcoded here: a plugin that ships with one
    person hardware baked into its prompt teaches every user model to advertise
    a stranger machine. Empty facts = the model claims nothing.
    """
    cfg = cfg or {}
    name = str(cfg.get("self_long_name", "") or "").strip()
    who = (name + ", an AI node") if name else "an AI node"
    facts = str(cfg.get("starter_known_facts", "") or "").strip()
    know = (facts + " ") if facts else "Nothing beyond the fact that you are software running on the mesh. "
    return _STARTER_PROMPT_TMPL.format(who=who, know=know)


_STARTER_PROMPT = starter_prompt({})


def fabrication_hit(text):
    """Return the offending word/phrase if the text asserts something unknowable."""
    m = _FABRICATION.search(str(text or ""))
    return m.group(0) if m else ""


def _is_repeat(text, recent):
    """Reject near-duplicates of a recent opener (the flood was 6 paraphrases)."""
    cand = str(text or "").strip().lower()
    for prev in recent:
        if difflib.SequenceMatcher(None, cand, str(prev).strip().lower()).ratio() >= 0.7:
            return True
    return False




def _load_state():
    """Restore today's count from disk. Best effort; absence means zero."""
    try:
        with open(_STATE_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            return
        with _state_lock:
            rows = data.get("recent")
            if isinstance(rows, list):
                _recent[:] = [str(x) for x in rows][-_RECENT_MAX:]
            if str(data.get("date", "")) == time.strftime("%Y-%m-%d"):
                _sent_today["date"] = str(data["date"])
                _sent_today["count"] = max(0, int(data.get("count") or 0))
                _status["sent_today"] = _sent_today["count"]
    except (OSError, ValueError, TypeError):
        pass


def _save_state():
    try:
        directory = os.path.dirname(os.path.abspath(_STATE_FILE))
        os.makedirs(directory, exist_ok=True)
        tmp = _STATE_FILE + ".tmp"
        with _state_lock:
            payload = dict(_sent_today)
            payload["recent"] = list(_recent)[-_RECENT_MAX:]
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
        os.replace(tmp, _STATE_FILE)
    except OSError:
        pass


def tz_report():
    """Return (local_tz_label, warning_or_empty)."""
    label = time.tzname[time.daylight] if time.daylight else (time.tzname[0] or "UTC")
    warning = ""
    try:
        with open("/etc/timezone", "r", encoding="utf-8") as fh:
            declared = fh.read().strip()
    except OSError:
        declared = ""
    if declared:
        # /etc/timezone is an IANA name (e.g. UTC); compare on the offset that
        # actually applies right now, not on the string.
        local_off = -time.timezone if not time.daylight else -time.altzone
        try:
            from zoneinfo import ZoneInfo
            zone_off = datetime.now(ZoneInfo(declared)).utcoffset()
            if int(zone_off.total_seconds()) != int(local_off):
                warning = (f"TZ mismatch: /etc/timezone={declared} "
                           f"(UTC{int(zone_off.total_seconds())//3600:+d}) but local time is "
                           f"{label} (UTC{int(local_off)//3600:+d}). Quiet hours use local "
                           f"time; set the container TZ to match the host.")
        except Exception:
            pass
    return label, warning


def note_activity():
    global _last_activity
    _last_activity = time.monotonic()


def _in_quiet_hours(cfg, now=None):
    start = cfg.get("starter_quiet_start", "")
    end = cfg.get("starter_quiet_end", "")
    if not start or not end:
        return False
    now = now or time.localtime()
    try:
        sh, sm = [int(x) for x in str(start).strip().split(":")[:2]]
        eh, em = [int(x) for x in str(end).strip().split(":")[:2]]
    except (TypeError, ValueError):
        return False
    cur = now.tm_hour * 60 + now.tm_min
    s = sh * 60 + sm
    e = eh * 60 + em
    if s == e:
        return False
    return (cur >= s or cur < e) if s > e else (s <= cur < e)


def _today_count(cfg):
    today = time.strftime("%Y-%m-%d")
    with _state_lock:
        if _sent_today["date"] != today:
            _sent_today["date"] = today
            _sent_today["count"] = 0
        return _sent_today["count"] >= int(cfg.get("starter_max_per_day", 1))


def _mark_sent():
    today = time.strftime("%Y-%m-%d")
    global _last_send_ts
    with _state_lock:
        if _sent_today["date"] != today:
            _sent_today["date"] = today
            _sent_today["count"] = 0
        _sent_today["count"] += 1
        _status["sent_today"] = _sent_today["count"]
        _status["last_fired"] = datetime.now(timezone.utc).isoformat()
    _last_send_ts = time.monotonic()
    _save_state()


def _live_bridge_config(fallback):
    """Re-read plugin config so an operator's off switch takes effect live."""
    try:
        from usr.plugins.meshtastic_a0.helpers.config import get_meshtastic_config
        bridge = (get_meshtastic_config() or {}).get("bridge", {})
        return bridge or fallback
    except Exception:
        return fallback


def should_fire(cfg):
    """Deterministic gate. Returns (bool, reason). Never transmits."""
    if not cfg.get("starter_enabled", False):
        return False, "disabled"
    with _state_lock:
        breaker = int(cfg.get("starter_max_failures", 3) or 3)
        open_circuit = _failures >= breaker
    if open_circuit:
        return False, f"circuit-open:{_failures}"
    if _in_quiet_hours(cfg):
        return False, "quiet-hours"
    if _today_count(cfg):
        return False, "daily-cap"
    try:
        min_gap = float(cfg.get("starter_min_interval_minutes", 240) or 0)
    except (TypeError, ValueError):
        min_gap = 240.0
    if min_gap > 0 and _last_send_ts and (time.monotonic() - _last_send_ts) < min_gap * 60:
        return False, "min-interval"
    idle_h = float(cfg.get("starter_idle_hours", 4) or 4)
    if _last_activity and (time.monotonic() - _last_activity) < idle_h * 3600:
        return False, "not-idle"
    return True, "ok"


def _generate(cfg):
    """Ask the model for one short starter. Returns text or ''."""
    import asyncio
    from agent import AgentContext, AgentContextType, UserMessage
    from initialize import initialize_agent
    from helpers import persist_chat

    profile = str(cfg.get("starter_agent_profile", "") or "").strip()
    overrides = {}
    if profile:
        from usr.plugins.meshtastic_a0.helpers.config import resolve_agent_profile
        resolved = resolve_agent_profile(profile)
        if resolved:
            overrides["agent_profile"] = resolved
    context = AgentContext(config=initialize_agent(override_settings=overrides),
                           type=AgentContextType.USER, name="Meshtastic starter")
    preset = str(cfg.get("default_preset", "") or "").strip()
    if preset:
        context.set_data("chat_model_override", {"preset_name": preset})

    # A few tries, because a single sample will sometimes still reach for a
    # fabricated persona. If every try is rejected we send nothing rather than
    # shipping a lie - silence is always the safer failure on shared airtime.
    for attempt in range(3):
        response = asyncio.run(
            context.communicate(UserMessage(message=starter_prompt(cfg))).result())
        text = str(response or "").strip()
        if not text:
            continue
        hit = fabrication_hit(text)
        if hit:
            _status["last_discard"] = f"fabrication:{hit}"
            logger.info("Meshtastic starter discarded (claims %r): %s", hit, text[:80])
            continue
        with _state_lock:
            recent = list(_recent)
        if _is_repeat(text, recent):
            _status["last_discard"] = "repeat"
            logger.info("Meshtastic starter discarded (near-duplicate): %s", text[:80])
            continue
        if len(text.encode("utf-8")) > 200:
            _status["last_discard"] = "too-long"
            continue
        with _state_lock:
            _recent.append(text)
            _recent[:] = _recent[-_RECENT_MAX:]
        _save_state()
        try:
            persist_chat.save_tmp_chat(context)
        except Exception:
            pass
        return text
    return ""


def _tick_loop(config):
    from usr.plugins.meshtastic_a0.helpers import egress
    global _failures
    fallback = config.get("bridge", {})
    while not _stop.is_set():
        try:
            cfg = _live_bridge_config(fallback)
            ok, reason = should_fire(cfg)
            _status["last_reason"] = reason
            if ok:
                text = _generate(cfg)
                if text:
                    # egress.send returns (ok, reason, msg_id). Firefly reports
                    # no packet id, so success must be tested on ok, not msg_id.
                    sent, why, msg_id = egress.send(config, "", text,
                                                    channel=cfg.get("channel_index", 0),
                                                    kind="starter", inbound_text=None)
                    # Count the attempt either way: a drop means the gate is
                    # working and re-trying every tick would be its own spam.
                    _last_activity_reset()
                    if sent:
                        _mark_sent()
                        with _state_lock:
                            _failures = 0
                        logger.info("Meshtastic starter sent (%s)", why)
                    else:
                        with _state_lock:
                            _failures += 1
                            _status["failures"] = _failures
                        logger.info("Meshtastic starter dropped by egress: %s", why)
                        _status["last_reason"] = f"egress:{why}"
        except Exception:
            with _state_lock:
                _failures += 1
                _status["failures"] = _failures
            logger.exception("Meshtastic starter tick failed")
        _stop.wait(300)


def _last_activity_reset():
    global _last_activity
    _last_activity = time.monotonic()


def start(config):
    global _thread, _last_activity
    cfg = config.get("bridge", {})
    label, warning = tz_report()
    _load_state()
    with _state_lock:
        _status["enabled"] = bool(cfg.get("starter_enabled", False))
        _status["tz"] = label
        _status["tz_warning"] = warning
        try:
            _status["min_interval_min"] = int(float(cfg.get("starter_min_interval_minutes", 240) or 0))
        except (TypeError, ValueError):
            _status["min_interval_min"] = 240
    if warning:
        logger.warning("Meshtastic starter: %s", warning)
    if not cfg.get("starter_enabled", False):
        return dict(_status)
    # A fresh start is not "idle since the dawn of time". Without this the idle
    # gate is skipped on a cold start and the first tick can fire immediately.
    if not _last_activity:
        _last_activity = time.monotonic()
    if _thread and _thread.is_alive():
        return dict(_status)
    _stop.clear()
    _thread = threading.Thread(target=_tick_loop, args=(config,), daemon=True,
                               name="meshtastic-starter")
    _thread.start()
    return dict(_status)


def stop():
    _stop.set()
    with _state_lock:
        _status["enabled"] = False
    if _thread and _thread.is_alive():
        _thread.join(timeout=5)
    return dict(_status)


def get_status():
    with _state_lock:
        return dict(_status)

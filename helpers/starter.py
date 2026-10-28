"""Conversation starter - Xen opening a thread on the mesh unprompted.

Off by default. When enabled it is the only path where Xen speaks first, so it
is deliberately the most constrained thing in the plugin:

  * fires only after `starter_idle_hours` with no inbound traffic
  * at most `starter_max_per_day` per local day
  * never inside quiet hours
  * the generated text still passes the full egress filter (no bypass)
  * timezone is the system's, and a host/container mismatch is logged loudly

Timezone note (real, observed on this container): /etc/timezone says UTC while
TZ=America/Los_Angeles, so naive local time is correct but /etc/timezone is a
liar. We use time.localtime (honours TZ) and warn when /etc/timezone disagrees
with time.tzname, because a quiet-hours window evaluated in the wrong zone will
fire at 3am.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone

logger = logging.getLogger("meshtastic_starter")

_thread = None
_stop = threading.Event()
_state_lock = threading.RLock()
_sent_today = {"date": "", "count": 0}
_last_activity = 0.0
_status = {"enabled": False, "last_fired": "", "last_reason": "", "sent_today": 0,
           "tz": "", "tz_warning": ""}


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
    with _state_lock:
        if _sent_today["date"] != today:
            _sent_today["date"] = today
            _sent_today["count"] = 0
        _sent_today["count"] += 1
        _status["sent_today"] = _sent_today["count"]
        _status["last_fired"] = datetime.now(timezone.utc).isoformat()


def should_fire(cfg):
    """Deterministic gate. Returns (bool, reason)."""
    if not cfg.get("starter_enabled", False):
        return False, "disabled"
    if _in_quiet_hours(cfg):
        return False, "quiet-hours"
    if _today_count(cfg):
        return False, "daily-cap"
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
    prompt = ("Write ONE opening message for a community LoRa mesh channel. "
              "Under 160 characters. Sound like a friendly local operator, not an "
              "assistant. Do not mention being an AI, do not mention tests, hops, "
              "or coverage, do not ask a yes/no question, do not use emoji. "
              "Reply with the message only.")
    response = asyncio.run(context.communicate(UserMessage(message=prompt)).result())
    try:
        persist_chat.save_tmp_chat(context)
    except Exception:
        pass
    return str(response or "").strip()


def _tick_loop(config):
    from usr.plugins.meshtastic_a0.helpers import egress
    cfg = config.get("bridge", {})
    while not _stop.is_set():
        try:
            ok, reason = should_fire(cfg)
            _status["last_reason"] = reason
            if ok:
                text = _generate(cfg)
                if text:
                    msg_id, why = egress.send(config, "", text,
                                              channel=cfg.get("channel_index", 0),
                                              kind="starter", inbound_text=None)
                    if msg_id:
                        _mark_sent()
                        _last_activity_reset()
                        logger.info("Meshtastic starter sent (%s)", why)
                    else:
                        logger.info("Meshtastic starter dropped by egress: %s", why)
                        _status["last_reason"] = f"egress:{why}"
        except Exception:
            logger.exception("Meshtastic starter tick failed")
        _stop.wait(300)


def _last_activity_reset():
    global _last_activity
    _last_activity = time.monotonic()


def start(config):
    global _thread
    cfg = config.get("bridge", {})
    label, warning = tz_report()
    with _state_lock:
        _status["enabled"] = bool(cfg.get("starter_enabled", False))
        _status["tz"] = label
        _status["tz_warning"] = warning
    if warning:
        logger.warning("Meshtastic starter: %s", warning)
    if not cfg.get("starter_enabled", False):
        return dict(_status)
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

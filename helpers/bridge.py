"""Persistent Meshtastic chat bridge - three-filter rewrite.

Pipeline order is fixed by the architecture (spec 4/6):

  inbound packet
    -> DUMB FILTER   self-drop, hard banlist, evidence log, addressing gate
    -> SMART FILTER  triage (T1), escalation (T2/T3/T4), soft banlist reply-gate
    -> agent turn    (only if the smart filter did not refuse)
    -> EGRESS FILTER single chokepoint for every outbound byte (egress.send)

The dumb filter runs before any model and cannot be argued with; the smart
filter is Xen's judgment; the egress filter is the last mechanical veto and can
only ever say no. Veto precedence is egress > hard ban > Xen's discretion, which
is the reverse of execution order on purpose.

Security: only allowlisted nodes drive a trusted DM turn; everything else runs
sandboxed. Inbound text is untrusted. All outbound goes through egress.send.
"""
import asyncio
import collections
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone

logger = logging.getLogger("meshtastic_bridge")

_state_lock = threading.RLock()
_bridge = {"running": False, "status": "stopped", "last_error": "", "started_at": None,
           "host": "", "port": None, "node": ""}
_thread = None
_loop = None
_iface = None
_recent = collections.deque(maxlen=100)
_contexts = {}
_node_locks = {}

_INVISIBLE = re.compile(r"[\u200b-\u200f\u2060-\u2064\ufeff\u00ad]+")

_SENT_LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sent-log.jsonl")
_ledger = None
_SELF_LONG_NAME = "xenmesh"


def _sent_log_path():
    return os.path.abspath(_SENT_LOG)


def _gate():
    from usr.plugins.meshtastic_a0.helpers import gatekeeper
    return gatekeeper


def _banlists():
    from usr.plugins.meshtastic_a0.helpers import banlists
    return banlists


def _egress():
    from usr.plugins.meshtastic_a0.helpers import egress
    return egress


def _ledger_instance():
    global _ledger
    if _ledger is None:
        _ledger = _gate().Ledger(_sent_log_path())
    return _ledger


def _remember_sent(msg_id, channel):
    try:
        _ledger_instance().add(msg_id, channel)
    except Exception:
        logger.exception("Meshtastic sent-log write failed")


def _mention_tokens(bridge):
    return _gate().mention_tokens(bridge.get("self_node_id", ""), _SELF_LONG_NAME)


def _sanitize(text, limit=500):
    text = _INVISIBLE.sub("", str(text or ""))
    text = re.sub(r"<[^>]{0,200}>", " ", text)
    return text.strip()[:limit]


def get_status():
    with _state_lock:
        status = dict(_bridge)
    try:
        from usr.plugins.meshtastic_a0.helpers import starter
        status["starter"] = starter.get_status()
    except Exception:
        pass
    return status


def get_recent(limit=20):
    with _state_lock:
        return list(_recent)[-int(limit):]


def _record(node_id, name, text, kind, channel):
    with _state_lock:
        _recent.append({"ts": datetime.now(timezone.utc).isoformat(), "from": node_id,
                        "name": name, "text": text, "kind": kind, "channel": channel})


def _allowed(node_id, allowed):
    if not allowed:
        return True
    return str(node_id) in {str(a) for a in allowed}


def _get_context(node_id, name, config, trusted=False):
    from agent import AgentContext, AgentContextType
    from initialize import initialize_agent
    from helpers import persist_chat

    bridge = config["bridge"]
    key = f"{node_id}:{'dm' if trusted else 'bc'}"
    context = AgentContext.get(_contexts.get(key, ""))
    if context is None:
        overrides = {}
        profile = str(bridge.get("dm_agent_profile" if trusted else "broadcast_agent_profile", "") or "").strip()
        if not profile:
            profile = str(bridge.get("default_agent_profile", "") or "").strip()
        if profile:
            from usr.plugins.meshtastic_a0.helpers.config import resolve_agent_profile
            overrides["agent_profile"] = resolve_agent_profile(profile)
        context = AgentContext(config=initialize_agent(override_settings=overrides),
                               type=AgentContextType.USER,
                               name=f"Meshtastic {name or node_id}")
        preset = str(bridge.get("default_preset", "") or "").strip()
        if preset and context.get_data("chat_model_override") is None:
            context.set_data("chat_model_override", {"preset_name": preset})
        persist_chat.save_tmp_chat(context)
        _contexts[key] = context.id
    context.set_data("meshtastic_a0_node", node_id)
    context.set_data("meshtastic_a0_trusted", bool(trusted))
    return context


async def _run_agent(context, sender_name, text, config):
    from agent import UserMessage
    from helpers import persist_chat
    safe = _sanitize(text, 200)
    visible = f"[Meshtastic {sender_name}] {safe}"
    task = context.communicate(UserMessage(message=visible))
    response = await task.result()
    persist_chat.save_tmp_chat(context)
    return response


def _deliver(config, dest, reply, channel, kind, inbound_text):
    """Single outbound path: hand the model's reply to the egress chokepoint.

    egress.send enforces clamp, silence token, meta-refusal, dedupe, templated
    phrasing, tapback/test-reflex shapes and the global rate limit, then calls
    radio. Returns (sent: bool, reason).
    """
    try:
        msg_id, reason = _egress().send(config, dest, reply, channel=channel,
                                        kind=kind, inbound_text=inbound_text)
    except Exception as exc:  # fail closed
        logger.exception("Meshtastic egress send failed")
        return False, f"egress-exception:{type(exc).__name__}"
    if msg_id:
        _remember_sent(msg_id, channel)
        return True, reason
    logger.info("Meshtastic outbound dropped (%s): %s", kind, reason)
    return False, reason


def _process_message(config, frm, name, text, trusted, reply_mode, reply_channel):
    lock = _node_locks.setdefault(frm, threading.Lock())
    if not lock.acquire(blocking=False):
        return
    try:
        context = _get_context(frm, name, config, trusted)
        fut = asyncio.run_coroutine_threadsafe(_run_agent(context, name or frm, text, config), _loop)
        reply = fut.result(timeout=300)
        kind = "broadcast" if reply_mode == "broadcast" else "dm"
        dest = "" if reply_mode == "broadcast" else frm
        _deliver(config, dest, reply or "", reply_channel, kind, text)
    except Exception as exc:
        logger.exception("Meshtastic bridge processing failed")
        with _state_lock:
            _bridge["last_error"] = f"{type(exc).__name__}: {exc}"
    finally:
        lock.release()


def _handle_packet(packet, interface, config):
    """Called from the radio reader thread; never block here."""
    try:
        decoded = packet.get("decoded") or {}
        if decoded.get("portnum") != "TEXT_MESSAGE_APP":
            return
        text = decoded.get("text", "")
        if not text:
            return
        frm = packet.get("fromId") or ("!" + format(int(packet.get("from", 0)), "08x"))
        to = packet.get("toId") or ("!" + format(int(packet.get("to", 0)), "08x"))
        channel = packet.get("channel", 0)
        bridge = config["bridge"]
        my_id = getattr(getattr(interface, "myInfo", None), "my_node_num", None)
        my_id_str = "!" + format(int(my_id), "08x") if my_id else ""
        is_dm = bool(my_id_str) and to == my_id_str
        is_broadcast = to in ("^all", "!ffffffff")
        name = ""
        try:
            node = (getattr(interface, "nodes", {}) or {}).get(frm, {})
            name = (node.get("user", {}) or {}).get("longName", "")
        except Exception:
            pass

        # ---- DUMB FILTER (pre-model, cannot be argued with) ----
        # self-drop first: our own echoes must never drive a turn.
        try:
            if _gate().is_self(frm, my_id_str):
                return
        except Exception:
            pass

        # hard banlist: text never enters inference, never pings Jesse.
        try:
            if _banlists().hard_is_blocked(frm):
                logger.info("Meshtastic hard-banned node dropped (%s)", frm)
                return
        except Exception:
            logger.exception("Meshtastic hard banlist check failed")

        # evidence log (inside the gate): everything not self / hard-banned.
        _record(frm, name, _sanitize(text, 200), "dm" if is_dm else "broadcast", channel)
        try:
            from usr.plugins.meshtastic_a0.helpers import starter
            starter.note_activity()
        except Exception:
            pass
        try:
            _banlists().log_inbound(frm, name, text, "dm" if is_dm else "broadcast", channel)
        except Exception:
            logger.exception("Meshtastic inbound log failed")

        # addressing gate for public airtime; allowlist for DMs.
        if is_dm:
            if not _allowed(frm, bridge.get("allowed_nodes", [])):
                logger.info("Meshtastic ignored DM from unlisted node %s", frm)
                return
            trusted = True
            reply_mode, reply_channel = "dm", channel
        elif is_broadcast and bridge.get("respond_to_broadcasts", False):
            rid = (decoded or {}).get("replyId") or (decoded or {}).get("reply_id")
            allowed, reason = _gate().evaluate(text, rid, channel,
                                               _mention_tokens(bridge),
                                               _ledger_instance(), my_id_str, frm)
            if not allowed:
                logger.info("Meshtastic broadcast ignored (%s): %s", frm, reason)
                return
            trusted = False
            reply_mode, reply_channel = "broadcast", channel
        else:
            return

        # ---- SMART FILTER (Xen's judgment + escalation) ----
        # T1 triage writes a soft offence; escalation handles T2/T3/T4 and the
        # Discord ping. Both are bridge code, never the persona.
        t1_hit = None
        try:
            t1_hit = _banlists().triage_live(frm, name, text)
        except Exception:
            logger.exception("Meshtastic triage failed")
        try:
            from usr.plugins.meshtastic_a0.helpers import escalation
            refuse, info = escalation.handle(config, frm, name, text, t1_hit)
            if refuse:
                logger.info("Meshtastic smart-filter refused (%s): %s", frm, info)
                return
        except Exception:
            logger.exception("Meshtastic escalation failed")

        # soft banlist reply-gate: a soft-banned node still reached the model
        # path above for triage, but gets no reply.
        try:
            if _banlists().soft_is_banned(frm):
                logger.info("Meshtastic soft-banned node, no reply (%s)", frm)
                return
        except Exception:
            logger.exception("Meshtastic soft banlist check failed")

        threading.Thread(target=_process_message,
                         args=(config, frm, name, text, trusted, reply_mode, reply_channel),
                         daemon=True, name=f"meshtastic-turn-{frm}").start()
    except Exception:
        logger.exception("Meshtastic bridge packet handler error")


def _listen_loop(host, port, config):
    global _iface, _loop
    from usr.plugins.meshtastic_a0.helpers.radio import NoHeartbeatTCPInterface
    from pubsub import pub
    from usr.plugins.meshtastic_a0.helpers import egress

    # configure the egress rate limiter from plugin config at the chokepoint.
    b = config.get("bridge", {})
    egress.configure(b.get("rate_limit_max", 3), b.get("rate_limit_window", 120))

    loop = asyncio.new_event_loop()
    _loop = loop
    asyncio.set_event_loop(loop)
    handler = lambda packet, interface: _handle_packet(packet, interface, config)
    subscribed = False
    try:
        iface_cls = NoHeartbeatTCPInterface.get()
        _iface = iface_cls(host, portNumber=int(port), connectNow=True)
        pub.subscribe(handler, "meshtastic.receive")
        subscribed = True
        with _state_lock:
            _bridge.update({"running": True, "status": "connected", "last_error": "",
                            "started_at": datetime.now(timezone.utc).isoformat(),
                            "host": host, "port": port,
                            "node": getattr(_iface, "owner", "") or ""})
        logger.info("Meshtastic bridge connected to %s:%s", host, port)
        try:
            from usr.plugins.meshtastic_a0.helpers import starter
            starter.start(config)
        except Exception:
            logger.exception("Meshtastic starter start failed")
        loop.run_forever()
    except Exception as exc:
        logger.exception("Meshtastic bridge listener failed")
        with _state_lock:
            _bridge.update({"running": False, "status": "error", "last_error": f"{type(exc).__name__}: {exc}"})
    finally:
        try:
            from usr.plugins.meshtastic_a0.helpers import starter
            starter.stop()
        except Exception:
            pass
        if subscribed:
            try:
                pub.unsubscribe(handler, "meshtastic.receive")
            except Exception:
                pass
        try:
            if _iface is not None:
                _iface.close()
        except Exception:
            pass
        with _state_lock:
            _bridge["running"] = False
            if _bridge["status"] != "error":
                _bridge["status"] = "stopped"
        try:
            loop.close()
        except Exception:
            pass


def start_bridge(config):
    global _thread
    conn = config["connection"]
    if not str(conn.get("host", "")).strip():
        raise ValueError("Set a Meshtastic host in Config before starting the bridge.")
    with _state_lock:
        if _bridge["running"]:
            return get_status()
    _thread = threading.Thread(target=_listen_loop, args=(conn["host"], conn["port"], config),
                               daemon=True, name="meshtastic-a0-bridge")
    _thread.start()
    for _ in range(40):
        time.sleep(0.25)
        if _bridge["running"] or _bridge["status"] == "error":
            break
    return get_status()


def stop_bridge():
    with _state_lock:
        _bridge["running"] = False
    loop = _loop
    if loop is not None and loop.is_running():
        try:
            loop.call_soon_threadsafe(loop.stop)
        except Exception:
            pass
    if _thread and _thread.is_alive():
        _thread.join(timeout=10)
    return get_status()


async def sync_bridge(config):
    bridge = config.get("bridge", {})
    if bridge.get("auto_start") and config["connection"].get("host") and not _bridge["running"]:
        try:
            await asyncio.to_thread(start_bridge, config)
        except Exception:
            logger.exception("Meshtastic bridge auto-start failed")

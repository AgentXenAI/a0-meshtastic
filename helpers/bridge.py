"""Persistent Meshtastic chat bridge.

A background listener holds a TCP connection to the configured node/proxy, catches
inbound text messages, routes allowed DMs into an Agent Zero context, and sends the
reply back over the mesh. Modeled on the Discord bridge but scoped to the radio.

Security: only node IDs in bridge.allowed_nodes can drive the agent. Inbound text is
treated as untrusted content. Rate limits and length caps are enforced.
"""
import asyncio
import collections
import json
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
_rate = {}          # node_id -> deque[monotonic]
_contexts = {}      # node_id -> context_id
_node_locks = {}

_INVISIBLE = re.compile(r"[\u200b-\u200f\u2060-\u2064\ufeff\u00ad]+")

# Outbound messages this node has sent, remembered so an inbound packet can be
# recognised as a reply to one of them. The ledger lives in gatekeeper so the
# gate and the listener share exactly one store; it survives a restart.
_SENT_LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sent-log.jsonl")
_ledger = None

# Our own long name, used to build the literal @mention tokens.
_SELF_LONG_NAME = "xenmesh"


def _sent_log_path():
    return os.path.abspath(_SENT_LOG)


def _gate():
    """Lazy accessor for the dumb gate module (avoids an import cycle at load)."""
    from usr.plugins.meshtastic_a0.helpers import gatekeeper
    return gatekeeper


def _ledger_instance():
    global _ledger
    if _ledger is None:
        _ledger = _gate().Ledger(_sent_log_path())
    return _ledger


def _remember_sent(msg_id, channel):
    """Record an outbound message id so a later reply to it can be detected."""
    try:
        _ledger_instance().add(msg_id, channel)
    except Exception:
        logger.exception("Meshtastic sent-log write failed")


def _mention_tokens(bridge):
    """Literal @tokens that count as addressing this node on public airtime."""
    return _gate().mention_tokens(bridge.get("self_node_id", ""), _SELF_LONG_NAME)


def _sanitize(text, limit=500):
    text = _INVISIBLE.sub("", str(text or ""))
    text = re.sub(r"<[^>]{0,200}>", " ", text)  # strip angle-bracket tags
    return text.strip()[:limit]


def get_status():
    with _state_lock:
        return dict(_bridge)


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


def _rate_ok(node_id, max_n, window):
    now = time.monotonic()
    dq = _rate.setdefault(str(node_id), collections.deque())
    while dq and now - dq[0] > window:
        dq.popleft()
    if len(dq) >= max_n:
        return False
    dq.append(now)
    return True


def _get_context(node_id, name, config, trusted=False):
    from agent import AgentContext, AgentContextType
    from initialize import initialize_agent
    from helpers import persist_chat

    bridge = config["bridge"]
    # Cache contexts per trust class so a node's trusted-DM persona and the
    # public broadcast persona never bleed into each other.
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

    safe = _sanitize(text, 200)  # locked to the Meshtastic protocol cap
    visible = f"[Meshtastic {sender_name}] {safe}"
    task = context.communicate(UserMessage(message=visible))
    response = await task.result()
    persist_chat.save_tmp_chat(context)
    return response


# If the sandboxed persona has nothing to say it should answer with exactly this
# token, and the bridge transmits nothing. Gives the public persona a real
# option other than "say something", without relying on it being polite.
SILENCE_TOKEN = "SILENT"

# Belt and braces: a model that decides not to reply sometimes narrates that
# decision instead of staying quiet, and the narration then goes out over shared
# airtime ("No mesh reply sent: ... staying silent to save airtime."). That is
# still a transmission. Anything reading as a refusal-to-reply is dropped.
_META_REFUSAL = re.compile(
    r"(no\s+mesh\s+reply|not\s+addressed\s+to\s+me|staying\s+(silent|quiet)|"
    r"(will|would)\s+(stay|remain)\s+(silent|quiet)|no\s+reply\s+(sent|needed|required)|"
    r"(staying|remaining)\s+silent|save\s+airtime|not\s+responding)",
    re.IGNORECASE,
)


def _is_silence(text):
    stripped = str(text or "").strip()
    if not stripped:
        return True
    if stripped.strip(".*! ").upper() == SILENCE_TOKEN:
        return True
    # Only treat a meta-refusal as silence when the message is essentially just
    # that narration, so a real answer that mentions airtime still goes through.
    if len(stripped) <= 200 and _META_REFUSAL.search(stripped):
        return True
    return False


def _send_reply(config, dest, text, channel=0):
    from usr.plugins.meshtastic_a0.helpers import radio
    from usr.plugins.meshtastic_a0.helpers.radio import clamp_bytes
    conn = config["connection"]
    # Mesh is shared, low-bandwidth airspace: hard 200-byte cap on every reply.
    text = clamp_bytes(_sanitize(text, 400), radio.MAX_MESSAGE_BYTES)
    if not text:
        return
    msg_id = radio.send_text(conn["host"], conn["port"], dest, text)
    _remember_sent(msg_id, channel)


def _send_broadcast_reply(config, channel, text):
    from usr.plugins.meshtastic_a0.helpers import radio
    from usr.plugins.meshtastic_a0.helpers.radio import clamp_bytes
    conn = config["connection"]
    # Public reply to the channel (^all), same hard 200-byte cap.
    text = clamp_bytes(_sanitize(text, 400), radio.MAX_MESSAGE_BYTES)
    if not text:
        return
    msg_id = radio.send_broadcast(conn["host"], conn["port"], text, channel)
    _remember_sent(msg_id, channel)


def _process_message(config, frm, name, text, trusted=False, reply_mode="dm", reply_channel=0):
    """Run one agent turn and deliver the reply. Runs in a worker thread.

    reply_mode == "broadcast" sends the reply back to the public channel
    (used for any broadcast, even from an allowlisted node); otherwise it DMs
    the sender.
    """
    lock = _node_locks.setdefault(frm, threading.Lock())
    if not lock.acquire(blocking=False):
        return  # a turn for this node is already running
    try:
        context = _get_context(frm, name, config, trusted)
        fut = asyncio.run_coroutine_threadsafe(_run_agent(context, name or frm, text, config), _loop)
        reply = fut.result(timeout=300)
        # The persona is allowed to decline, in either direction; if it does,
        # nothing is transmitted. This guard used to be broadcast-only, so a
        # trusted DM whose turn answered SILENT transmitted the literal word
        # "SILENT" over the air - worse than silence and worse than a reply.
        # One contract, both modes: SILENT means send nothing.
        if _is_silence(reply):
            logger.info("Meshtastic %s turn declined as silent (%s)", reply_mode, frm)
            return
        if reply_mode == "broadcast":
            _send_broadcast_reply(config, reply_channel, reply or "")
        else:
            _send_reply(config, frm, reply or "", reply_channel)
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
        _record(frm, name, _sanitize(text, 200), "dm" if is_dm else "broadcast", channel)

        # Evidence store + live triage happen before the block check, so a
        # message from a node we refuse to answer is still recorded and scored.
        try:
            from usr.plugins.meshtastic_a0.helpers import blocklist
            blocklist.log_inbound(frm, name, text, "dm" if is_dm else "broadcast", channel)
            blocklist.flag_from_message(frm, name, text)
        except Exception:
            logger.exception("Meshtastic inbound triage failed")

        # HARD BLOCK: a blocked node gets no model turn and no reply, in either
        # direction. Enforced here, in code, before any model runs - it does not
        # depend on the model deciding to be polite about it.
        try:
            from usr.plugins.meshtastic_a0.helpers import blocklist
            if blocklist.is_blocked(frm):
                logger.info("Meshtastic blocked node ignored (%s)", frm)
                return
        except Exception:
            logger.exception("Meshtastic blocklist check failed")

        # Our own packets echo back at us (rebroadcasts, local enqueues). Never
        # let one of those drive a turn: that is a feedback loop waiting to fire.
        try:
            if _gate().is_self(frm, my_id_str):
                return
        except Exception:
            pass

        # Trust classes: a DM from an allowlisted node is the operator talking
        # to the agent (full profile + tools). Anything else that reaches the
        # agent is public broadcast traffic and runs sandboxed.
        if is_dm:
            if not _allowed(frm, bridge.get("allowed_nodes", [])):
                logger.info("Meshtastic ignored DM from unlisted node %s", frm)
                return
            trusted = True
            reply_mode, reply_channel = "dm", channel
        elif is_broadcast and bridge.get("respond_to_broadcasts", False):
            # Public airspace even when the sender is an allowlisted node: always
            # the sandboxed broadcast profile, and the reply goes back to the
            # channel (^all), not as a DM to the sender.
            #
            # HARD GATE: one dumb listener decides, in one place, and returns a
            # reason we can log. On shared airtime we transmit only when the
            # message names this node (@xenmesh / @!b4c5d7fa) or replies to a
            # packet this node actually sent. Everything else is recorded and
            # dropped. Silence is the default on public channels.
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
        if not _rate_ok(frm, bridge.get("rate_limit_max", 6), bridge.get("rate_limit_window", 120)):
            logger.info("Meshtastic rate-limited node %s", frm)
            return
        threading.Thread(target=_process_message, args=(config, frm, name, text, trusted, reply_mode, reply_channel),
                         daemon=True, name=f"meshtastic-turn-{frm}").start()
    except Exception:
        logger.exception("Meshtastic bridge packet handler error")


def _listen_loop(host, port, config):
    global _iface, _loop
    from usr.plugins.meshtastic_a0.helpers.radio import NoHeartbeatTCPInterface
    from pubsub import pub

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
        loop.run_forever()  # run_coroutine_threadsafe targets this loop
    except Exception as exc:
        logger.exception("Meshtastic bridge listener failed")
        with _state_lock:
            _bridge.update({"running": False, "status": "error", "last_error": f"{type(exc).__name__}: {exc}"})
    finally:
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
    """Auto-start on boot if configured and not already running."""
    bridge = config.get("bridge", {})
    if bridge.get("auto_start") and config["connection"].get("host") and not _bridge["running"]:
        try:
            await asyncio.to_thread(start_bridge, config)
        except Exception:
            logger.exception("Meshtastic bridge auto-start failed")

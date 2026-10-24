"""Persistent Meshtastic chat bridge.

A background listener holds a TCP connection to the configured node/proxy, catches
inbound text messages, routes allowed DMs into an Agent Zero context, and sends the
reply back over the mesh. Modeled on the Discord bridge but scoped to the radio.

Security: only node IDs in bridge.allowed_nodes can drive the agent. Inbound text is
treated as untrusted content. Rate limits and length caps are enforced.
"""
import asyncio
import collections
import logging
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


def _get_context(node_id, name, config):
    from agent import AgentContext, AgentContextType
    from initialize import initialize_agent
    from helpers import persist_chat

    bridge = config["bridge"]
    context = AgentContext.get(_contexts.get(node_id, ""))
    if context is None:
        overrides = {}
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
        _contexts[node_id] = context.id
    context.set_data("meshtastic_a0_node", node_id)
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


def _send_reply(config, dest, text):
    from usr.plugins.meshtastic_a0.helpers import radio
    from usr.plugins.meshtastic_a0.helpers.radio import clamp_bytes
    conn = config["connection"]
    # Mesh is shared, low-bandwidth airspace: hard 200-byte cap on every reply.
    text = clamp_bytes(_sanitize(text, 400), radio.MAX_MESSAGE_BYTES)
    if not text:
        return
    radio.send_text(conn["host"], conn["port"], dest, text)


def _process_message(config, frm, name, text):
    """Run one agent turn and deliver the reply. Runs in a worker thread."""
    lock = _node_locks.setdefault(frm, threading.Lock())
    if not lock.acquire(blocking=False):
        return  # a turn for this node is already running
    try:
        context = _get_context(frm, name, config)
        fut = asyncio.run_coroutine_threadsafe(_run_agent(context, name or frm, text, config), _loop)
        reply = fut.result(timeout=300)
        _send_reply(config, frm, reply or "")
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

        if not is_dm and not (is_broadcast and bridge.get("respond_to_broadcasts", False)):
            return
        if not _allowed(frm, bridge.get("allowed_nodes", [])):
            logger.info("Meshtastic ignored message from unlisted node %s", frm)
            return
        if not _rate_ok(frm, bridge.get("rate_limit_max", 6), bridge.get("rate_limit_window", 120)):
            logger.info("Meshtastic rate-limited node %s", frm)
            return
        threading.Thread(target=_process_message, args=(config, frm, name, text),
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

"""Config access for the Meshtastic A0 plugin."""
import os
from copy import deepcopy


def get_meshtastic_config(agent=None):
    """Load plugin settings through the framework with env overrides."""
    try:
        from helpers import plugins
        config = deepcopy(plugins.get_plugin_config("meshtastic_a0", agent=agent) or {})
    except Exception:
        config = {}

    conn = config.setdefault("connection", {})
    if os.environ.get("MESHTASTIC_HOST"):
        conn["host"] = os.environ["MESHTASTIC_HOST"]
    if os.environ.get("MESHTASTIC_PORT"):
        try:
            conn["port"] = int(os.environ["MESHTASTIC_PORT"])
        except ValueError:
            pass

    conn.setdefault("name", "Meshtastic")
    conn.setdefault("host", "")
    conn.setdefault("port", 4403)
    try:
        conn["port"] = int(conn["port"])
    except (TypeError, ValueError):
        conn["port"] = 4403
    # Radio transport. "tcp" = Meshtastic TCP API (meshtasticd / Meshmonitor).
    # "firefly" = Firefly UDP virtual node reached over its HTTP API.
    conn.setdefault("transport", "tcp")
    conn["transport"] = str(conn["transport"] or "tcp").strip().lower()
    if conn["transport"] not in ("tcp", "firefly"):
        conn["transport"] = "tcp"
    # Firefly connection settings (only used when transport == "firefly").
    ff = conn.setdefault("firefly", {})
    ff.setdefault("base_url", "")
    ff.setdefault("username", "")
    ff.setdefault("password", "")
    ff.setdefault("profile_id", "")
    ff.setdefault("poll_seconds", 3)
    try:
        ff["poll_seconds"] = max(1, int(ff["poll_seconds"]))
    except (TypeError, ValueError):
        ff["poll_seconds"] = 3
    if os.environ.get("FIREFLY_BASE_URL"):
        ff["base_url"] = os.environ["FIREFLY_BASE_URL"]
    if os.environ.get("FIREFLY_USERNAME"):
        ff["username"] = os.environ["FIREFLY_USERNAME"]
    if os.environ.get("FIREFLY_PASSWORD"):
        ff["password"] = os.environ["FIREFLY_PASSWORD"]
    if os.environ.get("FIREFLY_PROFILE_ID"):
        ff["profile_id"] = os.environ["FIREFLY_PROFILE_ID"]

    bridge = config.setdefault("bridge", {})
    bridge.setdefault("auto_start", False)
    bridge.setdefault("allowed_nodes", [])
    bridge.setdefault("respond_to_broadcasts", False)
    bridge.setdefault("channel_index", 0)
    # This node's own id, e.g. "!b4c5d7fa". Used to build the @mention token that
    # gates public replies. Left empty, only "@xenmesh" addresses the node.
    bridge.setdefault("self_node_id", "")
    bridge.setdefault("instructions", "")
    bridge.setdefault("default_agent_profile", "")
    # Per-trust-class profiles: trusted DMs vs public broadcasts.
    bridge.setdefault("dm_agent_profile", "")
    bridge.setdefault("broadcast_agent_profile", "")
    # Tools the sandboxed public-broadcast persona may use (besides response).
    bridge.setdefault("broadcast_allowed_tools", ["search_engine", "meshtastic_read", "meshview"])
    bridge.setdefault("default_preset", "")
    # Locked to the Meshtastic protocol cap (200 bytes). Not user-configurable;
    # any stored value is overridden so the UI cannot change it.
    bridge["max_message_length"] = 200
    bridge.setdefault("rate_limit_max", 3)
    bridge.setdefault("rate_limit_window", 120)
    # Escalation: Discord channel id that receives T2/T3 pings. Empty = no ping.
    bridge.setdefault("escalation_discord_channel", "")
    # Conversation starter (Xen opening a thread). Off by default.
    bridge.setdefault("starter_enabled", False)
    bridge.setdefault("starter_idle_hours", 4)
    bridge.setdefault("starter_max_per_day", 1)
    bridge.setdefault("starter_quiet_start", "23:00")
    bridge.setdefault("starter_quiet_end", "07:00")
    bridge.setdefault("starter_agent_profile", "")

    send = config.setdefault("send", {})
    send.setdefault("allow_broadcast", False)
    send.setdefault("broadcast_channel", 0)

    config["connection"] = conn
    config["bridge"] = bridge
    config["send"] = send
    return config


def resolve_agent_profile(value, project_name=None):
    profile = str(value or "").strip()
    if not profile:
        return ""
    try:
        from helpers.subagents import get_available_agents_dict
        profiles = get_available_agents_dict(project_name)
        if profile in profiles:
            return profile
        matches = [n for n, item in profiles.items() if item.title.casefold() == profile.casefold()]
        if len(matches) == 1:
            return matches[0]
    except Exception:
        return profile
    return profile

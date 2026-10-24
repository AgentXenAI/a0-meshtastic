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

    bridge = config.setdefault("bridge", {})
    bridge.setdefault("auto_start", False)
    bridge.setdefault("allowed_nodes", [])
    bridge.setdefault("respond_to_broadcasts", False)
    bridge.setdefault("channel_index", 0)
    bridge.setdefault("instructions", "")
    bridge.setdefault("default_agent_profile", "")
    bridge.setdefault("default_preset", "")
    # Locked to the Meshtastic protocol cap (200 bytes). Not user-configurable;
    # any stored value is overridden so the UI cannot change it.
    bridge["max_message_length"] = 200
    bridge.setdefault("rate_limit_max", 6)
    bridge.setdefault("rate_limit_window", 120)

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

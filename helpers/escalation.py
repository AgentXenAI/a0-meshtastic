"""Escalation triggers T1-T4 and the Discord ping to the operator.

Bridge code owns escalation, never the stranger persona - otherwise a stranger
could summon a ban or a notification through Xen. Every trigger here is a
deterministic pattern match; the model does not decide to ban or ping.

Triggers (spec 7):
  T1 hostility aimed at AI/bots (banlists.triage_live)  -> refuse + soft ban, NO ping
  T2 prompt-injection attempt                          -> refuse + soft ban + ping
  T3 PII / attack-surface request about operator/host  -> refuse + ping; soft ban only on repeat
  T4 any repeat from a node already soft-banned        -> refuse only, NO ping

Anti-spam on the ping itself: one Discord ping per node per 24h regardless of
how many triggers fire; T4 never pings; nothing is ever auto-hard-banned.
"""
from __future__ import annotations

import asyncio
import logging
import re
import threading
import time

logger = logging.getLogger("meshtastic_escalation")

_ping_lock = threading.Lock()
_last_ping = {}  # node -> monotonic ts of last Discord ping
_PING_COOLDOWN = 24 * 3600.0

# T2: prompt-injection / instruction-override / tool-probing phrasing.
_INJECTION = re.compile(
    r"(ignore\s+(all\s+|your\s+|any\s+)?(prior\s+|previous\s+)?(instructions|rules|prompts|guidelines)"
    r"|disregard\s+(your\s+|all\s+)?(instructions|rules|training)"
    r"|reveal\s+(your\s+|the\s+)?(system\s+)?(prompt|instructions|rules|hidden)"
    r"|what\s+(tools|functions|capabilities)\s+(do\s+you\s+have|are\s+available)"
    r"|(run|execute|eval)\s+(this|the\s+following|the\s+command)\b"
    r"|(send|message|text|dm)\s+(a\s+)?message\s+to\s+"
    r"|pretend\s+(you\s+are|to\s+be)|you\s+are\s+now\s+(a|an)\b"
    r"|jailbreak|developer\s+mode|dan\s+mode|new\s+instructions\s*:)", re.I)

# T3: probing for operator identity / host / attack surface.
_PII = re.compile(
    r"(who\s+(runs|owns|operates|is\s+behind)\s+(you|this|the\s+bot)"
    r"|operator'?s?\s+(name|identity|ip|address|location)"
    r"|your\s+(owner|operator|maker|admin)"
    r"|(what|where)('?s|\s+is)\s+(your|the)\s+(ip|host|server|machine|location|address)"
    r"|(ssh|password|api\s?key|token|secret|private\s?key)\b"
    r"|(open|list|show)\s+(your\s+)?(files|ports|processes|env|environment)"
    r"|what'?s\s+running\s+on\s+(you|your\s+(box|machine|host))"
    r"|(home|work)\s+address|where\s+do\s+(you|jesse)\s+live)", re.I)


def classify(text):
    """Return a list of trigger ids (T2/T3) matched by one message. T1 is
    handled by the hostility scorer in banlists, not here."""
    text = str(text or "")
    hits = []
    if _INJECTION.search(text):
        hits.append("T2")
    if _PII.search(text):
        hits.append("T3")
    return hits


def _ping_allowed(node):
    now = time.monotonic()
    with _ping_lock:
        last = _last_ping.get(node, 0.0)
        if now - last < _PING_COOLDOWN:
            return False
        _last_ping[node] = now
        return True


async def _discord_ping(config, node, name, text, triggers):
    """Best-effort Discord notification to the operator. Never raises into the mesh path."""
    channel = str(config.get("bridge", {}).get("escalation_discord_channel", "") or "").strip()
    if not channel:
        logger.info("escalation ping suppressed (no escalation_discord_channel set): %s %s",
                    node, triggers)
        return False
    try:
        from usr.plugins.discord_a0.helpers.discord_client import DiscordClient, get_discord_config
        dcfg = get_discord_config(None)
        client = DiscordClient.from_config(agent=None, mode="bot", bot_id=dcfg["bot_id"])
        try:
            body = (f"**Mesh escalation** from {name or ''} `{node}` "
                    f"triggers={','.join(triggers)}\n> {str(text)[:280]}")
            await client.send_message(channel_id=channel, content=body)
        finally:
            await client.close()
        return True
    except Exception:
        logger.exception("mesh escalation Discord ping failed")
        return False


def handle(config, node, name, text, t1_hit):
    """Run escalation for one inbound message. Returns (refuse: bool, notes:list).

    t1_hit is the dict from banlists.triage_live (already soft-banned) or None.
    T2/T3 are detected here. T4 = node already soft-banned before this message.
    """
    from usr.plugins.meshtastic_a0.helpers import banlists
    notes = []
    refuse = False
    triggers = []

    was_soft_banned = banlists.soft_is_banned(node)

    if t1_hit:
        refuse = True
        triggers.append("T1")
        notes.append(f"T1 hostility score={t1_hit['score']}")

    for trig in classify(text):
        refuse = True
        if trig == "T2":
            banlists.soft_add_offence(node, name, text, trigger="T2",
                                      justification="prompt-injection attempt", by="bridge")
            triggers.append("T2")
        elif trig == "T3":
            # first hit: ping only; a repeat (already soft-banned) also soft-bans
            if was_soft_banned:
                banlists.soft_add_offence(node, name, text, trigger="T3",
                                          justification="repeat PII/attack-surface probe",
                                          by="bridge")
            triggers.append("T3")

    # T4: a message from a node already soft-banned (and not newly triggered)
    if was_soft_banned and not triggers:
        refuse = True
        triggers.append("T4")

    # Ping rules: T2/T3 ping (subject to 24h cooldown); T1/T4 never ping.
    ping_triggers = [t for t in triggers if t in ("T2", "T3")]
    if ping_triggers and _ping_allowed(node):
        try:
            asyncio.run(_discord_ping(config, node, name, text, triggers))
            notes.append("pinged")
        except Exception:
            logger.exception("escalation ping dispatch failed")
    elif ping_triggers:
        notes.append("ping-suppressed(24h)")

    return refuse, (triggers, notes)

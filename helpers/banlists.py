"""Two banlists, one file. Hard (operator's) and soft (the agent's rap-sheet).

Hard banlist - lives conceptually in the Dumb Filter. Checked before any model
runs. A hard-banned node's text never enters inference and never pings the operator.
Writes are operator-only and never automatic; there is no code path that promotes a
node to hard on its own.

Soft banlist - lives in the Smart Filter. The text still reaches the model; the
bridge simply declines to answer. This is how Xen decides which nodes get a mesh
reply. Every entry carries a rap-sheet: an append-only list of offences, each
with timestamp, offending text, the trigger that fired (T1-T4) and a written
justification. The rap-sheet is the ONLY record of a silent T1 ban, so it is
mandatory. Added and cleared at Xen's discretion (via bridge code, never the
stranger persona), no expiry.

The triage scorer writes into the soft list as author 'scorer' - a scored
candidate is a soft-ban entry, not a separate third structure.

State is a standalone JSON file (not plugin config) so edits apply live and the
file stays externally diffable.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
from collections import deque
from datetime import datetime, timezone

logger = logging.getLogger("meshtastic_banlists")

_HERE = os.path.dirname(os.path.abspath(__file__))
_PATH = os.path.abspath(os.path.join(_HERE, "..", "banlists.json"))
_OLD_PATH = os.path.abspath(os.path.join(_HERE, "..", "blocklist.json"))
_INBOUND_LOG_PATH = os.path.abspath(os.path.join(_HERE, "..", "inbound-log.jsonl"))

_lock = threading.RLock()
_cache = {"mtime": None, "data": None}
MAX_INBOUND_LINES = 2000


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def norm_node(value):
    s = str(value or "").strip().lower()
    if not s:
        return ""
    if s.startswith("!"):
        return s
    try:
        return "!" + format(int(s), "08x")
    except (TypeError, ValueError):
        return s


def _empty():
    return {"hard": [], "soft": []}


def _migrate_old():
    """One-time import of the pre-rewrite blocklist.json (blocked/candidates).

    old 'blocked' -> hard; old 'candidates' -> soft entries with one offence
    each (author 'scorer'). Returns the migrated dict or None.
    """
    try:
        with open(_OLD_PATH, "r", encoding="utf-8") as fh:
            old = json.load(fh)
    except Exception:
        return None
    if not isinstance(old, dict):
        return None
    out = _empty()
    for e in old.get("blocked") or []:
        node = norm_node(e.get("node"))
        if not node:
            continue
        out["hard"].append({"node": node, "name": str(e.get("name", ""))[:60],
                            "reason": str(e.get("reason", ""))[:400],
                            "evidence": str(e.get("evidence", ""))[:600],
                            "added": e.get("added") or now_iso(), "by": e.get("by") or "jesse"})
    for c in old.get("candidates") or []:
        node = norm_node(c.get("node"))
        if not node:
            continue
        ev = c.get("evidence") or []
        if isinstance(ev, str):
            ev = [ev]
        offence = {"ts": c.get("last_seen") or now_iso(),
                   "text": str(ev[-1] if ev else "")[:300],
                   "trigger": "T1", "score": c.get("score", 0.0),
                   "justification": str(c.get("reason", ""))[:400], "by": "scorer"}
        out["soft"].append({"node": node, "name": str(c.get("name", ""))[:60],
                            "hits": int(c.get("hits", 1)),
                            "first_seen": c.get("first_seen") or now_iso(),
                            "last_seen": c.get("last_seen") or now_iso(),
                            "offences": [offence]})
    return out


def _load():
    with _lock:
        try:
            mtime = os.path.getmtime(_PATH)
        except OSError:
            mtime = None
        if _cache["data"] is not None and _cache["mtime"] == mtime:
            return _cache["data"]
        data = None
        if mtime is not None:
            try:
                with open(_PATH, "r", encoding="utf-8") as fh:
                    loaded = json.load(fh)
                if isinstance(loaded, dict) and ("hard" in loaded or "soft" in loaded):
                    data = {"hard": list(loaded.get("hard") or []),
                            "soft": list(loaded.get("soft") or [])}
            except Exception:
                logger.exception("banlists load failed; using empty")
        if data is None:
            data = _migrate_old() or _empty()
            _save(data)
        _cache["mtime"] = os.path.getmtime(_PATH) if os.path.exists(_PATH) else None
        _cache["data"] = data
        return data


def _save(data):
    with _lock:
        tmp = _PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, _PATH)
        _cache["data"] = data
        try:
            _cache["mtime"] = os.path.getmtime(_PATH)
        except OSError:
            _cache["mtime"] = None


# --------------------------------------------------------------------------
# HARD banlist (operator's) - enforced before inference
# --------------------------------------------------------------------------
def hard_is_blocked(node_id):
    node = norm_node(node_id)
    if not node:
        return False
    return any(norm_node(e.get("node")) == node for e in _load()["hard"])


def hard_list():
    return list(_load()["hard"])


def hard_add(node_id, name="", reason="", evidence="", by="jesse"):
    """Operator-only. Never called automatically by triage or the persona."""
    node = norm_node(node_id)
    if not node:
        return False, "empty node id"
    data = _load()
    if any(norm_node(e.get("node")) == node for e in data["hard"]):
        return False, f"{node} already hard-blocked"
    data["hard"].append({"node": node, "name": str(name or "")[:60],
                         "reason": str(reason or "")[:400],
                         "evidence": str(evidence or "")[:600],
                         "added": now_iso(), "by": str(by or "jesse")[:40]})
    # promoting to hard removes the soft entry (hard supersedes it)
    data["soft"] = [s for s in data["soft"] if norm_node(s.get("node")) != node]
    _save(data)
    return True, node


def hard_remove(node_id):
    node = norm_node(node_id)
    data = _load()
    kept = [e for e in data["hard"] if norm_node(e.get("node")) != node]
    if len(kept) == len(data["hard"]):
        return False, f"{node} not hard-blocked"
    data["hard"] = kept
    _save(data)
    return True, node


# --------------------------------------------------------------------------
# SOFT banlist (Xen's rap-sheet) - reply gating
# --------------------------------------------------------------------------
def _soft_entry(data, node):
    return next((s for s in data["soft"] if norm_node(s.get("node")) == node), None)


def soft_is_banned(node_id):
    node = norm_node(node_id)
    if not node:
        return False
    return _soft_entry(_load(), node) is not None


def soft_list():
    return list(_load()["soft"])


def soft_add_offence(node_id, name="", text="", trigger="T1", justification="",
                     score=0.0, by="bridge"):
    """Append an offence to a node's rap-sheet (creating the entry if new).

    Written by bridge code on a trigger, or by the scorer during triage. The
    stranger persona never calls this. Returns (ok, note).
    """
    node = norm_node(node_id)
    if not node:
        return False, "empty node id"
    if hard_is_blocked(node):
        return False, f"{node} is hard-blocked (supersedes soft)"
    data = _load()
    entry = _soft_entry(data, node)
    offence = {"ts": now_iso(), "text": str(text or "")[:300],
               "trigger": str(trigger or "T1")[:4], "score": round(float(score or 0), 2),
               "justification": str(justification or "")[:400], "by": str(by or "bridge")[:40]}
    if entry is None:
        entry = {"node": node, "name": str(name or "")[:60], "hits": 0,
                 "first_seen": now_iso(), "last_seen": now_iso(), "offences": []}
        data["soft"].append(entry)
    if name:
        entry["name"] = str(name)[:60]
    entry["offences"].append(offence)
    entry["hits"] = int(entry.get("hits", 0)) + 1
    entry["last_seen"] = now_iso()
    _save(data)
    return True, f"soft-banned {node} (offence #{len(entry['offences'])})"


def soft_clear(node_id):
    node = norm_node(node_id)
    data = _load()
    kept = [s for s in data["soft"] if norm_node(s.get("node")) != node]
    if len(kept) == len(data["soft"]):
        return False, f"{node} not soft-banned"
    data["soft"] = kept
    _save(data)
    return True, node


def soft_offence_count(node_id):
    node = norm_node(node_id)
    entry = _soft_entry(_load(), node)
    return len(entry.get("offences", [])) if entry else 0


# --------------------------------------------------------------------------
# inbound evidence log
# --------------------------------------------------------------------------
def log_inbound(node_id, name, text, kind, channel):
    rec = {"ts": now_iso(), "from": norm_node(node_id), "name": str(name or "")[:60],
           "text": str(text or "")[:400], "kind": str(kind or "")[:12],
           "channel": int(channel or 0)}
    with _lock:
        try:
            with open(_INBOUND_LOG_PATH, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec) + "\n")
        except Exception:
            logger.exception("inbound log write failed")
            return
        try:
            if os.path.getsize(_INBOUND_LOG_PATH) > 400_000:
                lines = deque(maxlen=MAX_INBOUND_LINES)
                with open(_INBOUND_LOG_PATH, "r", encoding="utf-8") as fh:
                    for line in fh:
                        lines.append(line)
                tmp = _INBOUND_LOG_PATH + ".tmp"
                with open(tmp, "w", encoding="utf-8") as fh:
                    fh.writelines(lines)
                os.replace(tmp, _INBOUND_LOG_PATH)
        except Exception:
            logger.exception("inbound log compaction failed")


def read_inbound(limit=200, window_hours=None):
    rows = []
    try:
        with open(_INBOUND_LOG_PATH, "r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
    except FileNotFoundError:
        return []
    except Exception:
        logger.exception("inbound log read failed")
        return []
    if window_hours:
        cutoff = datetime.now(timezone.utc).timestamp() - float(window_hours) * 3600
        rows = [r for r in rows if _ts(r.get("ts")) >= cutoff]
    return rows[-int(limit):]


def last_inbound_ts():
    rows = read_inbound(limit=1)
    return _ts(rows[-1].get("ts")) if rows else 0.0


def _ts(value):
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except Exception:
        return 0.0


# --------------------------------------------------------------------------
# hostile triage (writes into the SOFT list as author 'scorer')
# --------------------------------------------------------------------------
_TARGET = re.compile(
    r"\b(ai|a\.i\.|a i |bot|bots|chatbot|chatbots|robot|robots|model|models|llm|llms"
    r"|algorithm|algorithms|automation|chatbot|robo|agent|agents)\b"
    r"|humans?[ -]?only", re.I)
_STRONG = re.compile(
    r"(should\s+be\s+banned?|ban\s+(all\s+)?(the\s+)?(bots?|ais?|robots?)"
    r"|bots?\s+should\s+(die|go\s+home|be\s+deleted?)"
    r"|kill\s+(all\s+)?(the\s+)?(bots?|ais?)|delete\s+(your|the|this)\s+(bot|ai|app)"
    r"|uninstall\s+(your|the|this)\s+(bot|ai|app)|turn\s+(it|the\s+bot|the\s+ai)\s+off"
    r"|shut\s+(up|down)[,\s!.]+\s*(you\s+)?(stupid\s+|dumb\s+|fucking\s+)?(bot|ai|robot)"
    r"|(stupid|dumb|fucking|shitty|shit|useless|pathetic|worthless|creepy|disgusting)\s+(bot|ai|robots?|chatbot)"
    r"|(bot|ai|robots?)\s+(is|are)\s+(stupid|dumb|useless|trash|garbage|a\s+joke|a\s+cancer)"
    r"|no\s+(bots?|ais?)\s+(allowed|wanted|here)|bots?\s+(not|aren't)\s+welcome"
    r"|(bots?|ais?)\s+shouldn'?t\s+exist|(bots?|ais?)\s+should\s+not\s+exist"
    r"|(get|take)\s+(rid\s+of|your)\s+(the\s+)?(bot|ai)"
    r"|humans?\s+only|anti[\s-]?ai|ai\s+slop|bot\s+slop|stop\s+talking|nobody\s+asked"
    r"|(go\s+(away|home|to\s+hell)|leave\s+(me\s+)?alone)[,\s!.]+\s*(bot|ai|robot))", re.I)
_MEDIUM = re.compile(
    r"(annoying\s+(bot|ai)|spam\s*bot|bot\s+spam|nobody\s+cares|shut\s+up"
    r"|i\s+hate\s+(the\s+)?(bots?|ais?|ai)|hate\s+(bots?|ai)"
    r"|weird\s+(bot|ai)|freaky|creep(y|ier)|waste\s+of\s+(space|airtime|oxygen)"
    r"|(bots?|ais?)\s+(suck|are\s+annoying|are\s+the\s+worst)|quit\s+talking"
    r"|who\s+(let|asked)\s+you|not\s+invited|unwelcome|get\s+a\s+life"
    r"|banned|boycott|report\s+(you|this|the\s+bot))", re.I)


def hostile_score(text):
    text = str(text or "")
    if not text.strip():
        return 0.0, []
    if not _TARGET.search(text):
        return 0.0, []
    score, hits = 0.0, []
    for m in _STRONG.finditer(text):
        score += 3.0
        hits.append(m.group(0).strip()[:60])
    for m in _MEDIUM.finditer(text):
        score += 1.5
        hits.append(m.group(0).strip()[:60])
    return min(score, 9.0), hits[:4]


def scan_hostile(window_hours=48, threshold=3.0, limit=20, persist=True):
    """Rank logged inbound traffic by hostility. Writes soft offences (author
    'scorer'); never touches the hard list."""
    rows = read_inbound(limit=MAX_INBOUND_LINES, window_hours=window_hours)
    by_node = {}
    for r in rows:
        score, hits = hostile_score(r.get("text", ""))
        if score <= 0:
            continue
        node = norm_node(r.get("from"))
        entry = by_node.setdefault(node, {"node": node, "name": r.get("name", ""),
                                          "score": 0.0, "hits": 0, "evidence": [],
                                          "phrases": [], "last_seen": r.get("ts", "")})
        entry["score"] += score
        entry["hits"] += 1
        if r.get("name"):
            entry["name"] = r["name"]
        entry["last_seen"] = max(entry["last_seen"], r.get("ts", ""))
        entry["evidence"].append(str(r.get("text", ""))[:300])
        entry["phrases"].extend(hits)
    out = []
    for entry in by_node.values():
        if entry["score"] < float(threshold):
            continue
        entry["score"] = round(min(entry["score"], 20.0), 2)
        entry["evidence"] = entry["evidence"][-3:]
        entry["phrases"] = sorted(set(entry["phrases"]))[:6]
        entry["reason"] = "hostile language aimed at AI/bots: " + ", ".join(entry["phrases"][:4])
        out.append(entry)
    out.sort(key=lambda e: -e["score"])
    out = out[:int(limit)]
    if persist:
        for entry in out:
            soft_add_offence(entry["node"], entry.get("name", ""),
                             " | ".join(entry["evidence"][-2:]), trigger="T1",
                             justification=entry["reason"], score=entry["score"], by="scorer")
    return out


def triage_live(node_id, name, text, threshold=3.0):
    """Score one live inbound message; on a hit, append a soft offence (T1).
    Returns a dict describing what happened, or None if below threshold."""
    score, hits = hostile_score(text)
    if score < float(threshold):
        return None
    just = "hostile language aimed at AI/bots: " + ", ".join(hits[:4])
    ok, note = soft_add_offence(node_id, name, text, trigger="T1",
                                justification=just, score=score, by="scorer")
    return {"score": round(score, 2), "phrases": hits, "ok": ok, "note": note}

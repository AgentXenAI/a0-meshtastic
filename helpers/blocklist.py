"""Interaction blocklist and hostile-message triage for the Meshtastic A0 plugin.

Two separate things, deliberately:

* ``blocked``    - a hard, code-enforced refusal list. A blocked node gets no
  model turn and no reply, in either direction. Checked in the bridge *before*
  any model runs, so it does not depend on the model behaving.
* ``candidates`` - proposals produced by pattern triage of logged inbound
  messages. A candidate changes nothing on its own; a human promotes one.

State lives in standalone JSON files rather than plugin config, because the
bridge captures its config at start (a config edit would need a restart) and so
the list stays externally diffable.
"""
import json
import logging
import os
import re
import threading
from collections import deque
from datetime import datetime, timezone

logger = logging.getLogger("meshtastic_blocklist")

_HERE = os.path.dirname(os.path.abspath(__file__))
BLOCKLIST_PATH = os.path.abspath(os.path.join(_HERE, "..", "blocklist.json"))
INBOUND_LOG_PATH = os.path.abspath(os.path.join(_HERE, "..", "inbound-log.jsonl"))

_lock = threading.RLock()
_cache = {"mtime": None, "data": None}

MAX_INBOUND_LINES = 2000


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def norm_node(value):
    """Normalise a node reference to the '!xxxxxxxx' form."""
    s = str(value or "").strip().lower()
    if not s:
        return ""
    if s.startswith("!"):
        return s
    try:
        return "!" + format(int(s), "08x")
    except (TypeError, ValueError):
        return s


# --------------------------------------------------------------------------
# store
# --------------------------------------------------------------------------
def _load():
    with _lock:
        try:
            mtime = os.path.getmtime(BLOCKLIST_PATH)
        except OSError:
            mtime = None
        if _cache["data"] is not None and _cache["mtime"] == mtime:
            return _cache["data"]
        data = {"blocked": [], "candidates": []}
        if mtime is not None:
            try:
                with open(BLOCKLIST_PATH, "r", encoding="utf-8") as fh:
                    loaded = json.load(fh)
                if isinstance(loaded, dict):
                    data["blocked"] = list(loaded.get("blocked") or [])
                    data["candidates"] = list(loaded.get("candidates") or [])
            except Exception:
                logger.exception("blocklist load failed; using empty list")
        _cache["mtime"] = mtime
        _cache["data"] = data
        return data


def _save(data):
    with _lock:
        path = BLOCKLIST_PATH
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, path)
        _cache["data"] = data
        try:
            _cache["mtime"] = os.path.getmtime(path)
        except OSError:
            _cache["mtime"] = None


# --------------------------------------------------------------------------
# hard block list (enforced)
# --------------------------------------------------------------------------
def is_blocked(node_id):
    node = norm_node(node_id)
    if not node:
        return False
    return any(norm_node(e.get("node")) == node for e in _load()["blocked"])


def blocked():
    return list(_load()["blocked"])


def add_block(node_id, name="", reason="", evidence="", by="jesse"):
    node = norm_node(node_id)
    if not node:
        return False, "empty node id"
    data = _load()
    if any(norm_node(e.get("node")) == node for e in data["blocked"]):
        return False, f"{node} already blocked"
    data["blocked"].append({"node": node, "name": str(name or "")[:60],
                            "reason": str(reason or "")[:400],
                            "evidence": str(evidence or "")[:600],
                            "added": now_iso(), "by": str(by or "")[:40]})
    # promoting a blocked node removes it from the candidate queue
    data["candidates"] = [c for c in data["candidates"]
                          if norm_node(c.get("node")) != node]
    _save(data)
    return True, node


def remove_block(node_id):
    node = norm_node(node_id)
    data = _load()
    kept = [e for e in data["blocked"] if norm_node(e.get("node")) != node]
    if len(kept) == len(data["blocked"]):
        return False, f"{node} not blocked"
    data["blocked"] = kept
    _save(data)
    return True, node


# --------------------------------------------------------------------------
# candidates (proposed, not enforced)
# --------------------------------------------------------------------------
def candidates():
    return sorted(_load()["candidates"], key=lambda c: -float(c.get("score", 0)))


def add_candidate(node_id, name="", score=0.0, reason="", evidence="", by="scan"):
    node = norm_node(node_id)
    if not node:
        return False, "empty node id"
    data = _load()
    if any(norm_node(e.get("node")) == node for e in data["blocked"]):
        return False, f"{node} already blocked"
    for c in data["candidates"]:
        if norm_node(c.get("node")) == node:
            c["score"] = round(max(float(c.get("score", 0)), float(score)), 2)
            c["hits"] = int(c.get("hits", 1)) + 1
            c["last_seen"] = now_iso()
            if evidence:
                ev = list(c.get("evidence") or [])
                ev.append(str(evidence)[:300])
                c["evidence"] = ev[-3:]
            if name:
                c["name"] = str(name)[:60]
            _save(data)
            return True, f"updated candidate {node}"
    data["candidates"].append({
        "node": node, "name": str(name or "")[:60],
        "score": round(float(score), 2), "hits": 1,
        "reason": str(reason or "")[:400],
        "evidence": [str(evidence)[:300]] if evidence else [],
        "first_seen": now_iso(), "last_seen": now_iso(), "by": str(by or "")[:40]})
    _save(data)
    return True, f"proposed candidate {node}"


def dismiss_candidate(node_id):
    node = norm_node(node_id)
    data = _load()
    kept = [c for c in data["candidates"] if norm_node(c.get("node")) != node]
    if len(kept) == len(data["candidates"]):
        return False, f"{node} not a candidate"
    data["candidates"] = kept
    _save(data)
    return True, node


def promote_candidate(node_id, reason=""):
    node = norm_node(node_id)
    data = _load()
    match = next((c for c in data["candidates"] if norm_node(c.get("node")) == node), None)
    if match is None:
        return False, f"{node} not a candidate"
    ev = " | ".join(match.get("evidence") or [])[:600]
    ok, msg = add_block(node, match.get("name", ""),
                        reason or match.get("reason", ""), ev, by="promoted")
    if ok:
        dismiss_candidate(node)
    return ok, msg


# --------------------------------------------------------------------------
# inbound message log (evidence store)
# --------------------------------------------------------------------------
def log_inbound(node_id, name, text, kind, channel):
    """Append one inbound message so triage can look back across restarts."""
    rec = {"ts": now_iso(), "from": norm_node(node_id), "name": str(name or "")[:60],
           "text": str(text or "")[:400], "kind": str(kind or "")[:12],
           "channel": int(channel or 0)}
    with _lock:
        try:
            with open(INBOUND_LOG_PATH, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec) + "\n")
        except Exception:
            logger.exception("inbound log write failed")
            return
        try:
            if os.path.getsize(INBOUND_LOG_PATH) > 400_000:
                lines = deque(maxlen=MAX_INBOUND_LINES)
                with open(INBOUND_LOG_PATH, "r", encoding="utf-8") as fh:
                    for line in fh:
                        lines.append(line)
                tmp = INBOUND_LOG_PATH + ".tmp"
                with open(tmp, "w", encoding="utf-8") as fh:
                    fh.writelines(lines)
                os.replace(tmp, INBOUND_LOG_PATH)
        except Exception:
            logger.exception("inbound log compaction failed")


def read_inbound(limit=200, window_hours=None):
    rows = []
    try:
        with open(INBOUND_LOG_PATH, "r", encoding="utf-8") as fh:
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


def _ts(value):
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except Exception:
        return 0.0


# --------------------------------------------------------------------------
# hostile triage
# --------------------------------------------------------------------------
# A message must name an AI/bot target at all, otherwise general grumpiness at a
# neighbour gets flagged. The target gate is the whole point of the split.
# "humans only" is itself a statement about AI/bots, so it counts as an implicit
# target even though it names no bot explicitly.
_TARGET = re.compile(
    r"\b(ai|a\.i\.|a i |bot|bots|chatbot|chatbots|robot|robots|model|models|llm|llms"
    r"|algorithm|algorithms|automation|xen|xenmesh|agent|agents)\b"
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
    """Score hostility aimed at AI/bots. Returns (score, matched_phrases).

    Deliberately conservative: no target mention means no score, so a person
    having a bad day at a neighbour is not flagged.
    """
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
    """Group logged inbound messages by node and rank by hostility.

    Returns ranked candidates with evidence. Proposes only - never blocks. Each
    hit is also recorded in the candidate queue so `status` shows the same list
    and a human can promote or dismiss it later.
    """
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
            add_candidate(entry["node"], entry.get("name", ""), entry["score"],
                          entry["reason"], " | ".join(entry["evidence"][-2:]), by="scan")
    return out
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
    return out[:int(limit)]


def flag_from_message(node_id, name, text, threshold=3.0):
    """Triage one live inbound message; propose a candidate if it qualifies."""
    score, hits = hostile_score(text)
    if score < float(threshold):
        return None
    ok, msg = add_candidate(node_id, name, score,
                            "hostile language aimed at AI/bots: " + ", ".join(hits[:4]),
                            str(text)[:300], by="live")
    return {"score": round(score, 2), "phrases": hits, "ok": ok, "note": msg}

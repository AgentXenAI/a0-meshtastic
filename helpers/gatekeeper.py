"""Dumb inbound gate for the Meshtastic bridge.

This module has ONE job: decide whether a received mesh message is addressed to
this node and therefore deserves a model turn. It contains no agent, no model,
no network I/O and no imports from the bridge, so its whole decision surface is
readable in one screen and testable without a radio.

Design rules, in order of priority:

1. Default is silence. A message is passed ONLY if it matches an explicit
   mention token (@mynode, @!aabbccdd, ...) or is a reply to a packet this
   node actually sent. Everything else is dropped.
2. The mention test is a regex over literal tokens. No fuzzy matching and no
   substring matching. Bare (un-@'d) tokens are allowed only if they are listed
   in `mention_bare_tokens` / derived by `bare_tokens()`, and even then they must
   match as a WHOLE WORD - so "ab12" addresses us but "ab123" does not.
3. The reply test uses a ledger of our own on-wire packet ids. The ledger is fed
   by OBSERVING our own packets come back over the air (rebroadcasts), not only
   by the send call reporting an id - a send that returns no id would otherwise
   silently punch a hole in reply tracking.
4. Fail closed. Any exception in this module means "do not respond".

Run standalone to audit what the gate would have decided:

    /opt/venv-a0/bin/python gatekeeper.py --audit 20
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import re

# Bounded so a long-lived bridge cannot grow memory without limit.
LEDGER_MAX = 500


class Ledger:
    """Bounded set of (packet_id, channel) this node transmitted.

    Persisted to jsonl so a bridge restart does not forget what we said; a reply
    arriving five minutes after a restart must still be recognised.
    """

    def __init__(self, path):
        self.path = os.path.abspath(path)
        self._ids = collections.deque(maxlen=LEDGER_MAX)
        self._loaded = False

    def load(self):
        if self._loaded:
            return
        self._loaded = True
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                for line in fh:
                    try:
                        row = json.loads(line)
                        self._ids.append((int(row["id"]), int(row.get("channel", 0))))
                    except Exception:
                        continue
        except FileNotFoundError:
            pass

    def add(self, msg_id, channel=0):
        if not msg_id:
            return False
        self.load()
        pair = (int(msg_id), int(channel or 0))
        if pair in self._ids:
            return False
        self._ids.append(pair)
        try:
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps({"id": pair[0], "channel": pair[1]}) + "\n")
            self._compact()
        except OSError:
            pass
        return True

    def _compact(self):
        """Rewrite the file from the bounded deque so it cannot grow forever."""
        try:
            if sum(1 for _ in open(self.path, "r", encoding="utf-8")) <= LEDGER_MAX * 2:
                return
            with open(self.path, "w", encoding="utf-8") as fh:
                for pair in self._ids:
                    fh.write(json.dumps({"id": pair[0], "channel": pair[1]}) + "\n")
        except OSError:
            pass

    def has(self, msg_id, channel=None):
        if not msg_id:
            return False
        self.load()
        try:
            mid = int(msg_id)
        except (TypeError, ValueError):
            return False
        if channel is None:
            return any(pid == mid for pid, _ in self._ids)
        if (mid, int(channel or 0)) in self._ids:
            return True
        # Some clients, and locally-observed copies of our own packets, carry the
        # raw encrypted channel index (e.g. 31) instead of the logical one, so an
        # exact pair miss is not proof we did not send it. Accept an id-only
        # match rather than silently losing reply tracking.
        return any(pid == mid for pid, _ in self._ids)

    def snapshot(self):
        self.load()
        return list(self._ids)


def mention_tokens(self_node_id, long_name):
    """Literal @tokens that count as addressing this node.

    Both the long name and the node id are accepted, because mesh users type
    whichever they can see in their client.
    """
    tokens = []
    name = str(long_name or "").strip()
    node = str(self_node_id or "").strip()
    if name:
        tokens.append("@" + name)
    if node:
        tokens.append("@" + node)
        # Some clients render the node id without the leading bang.
        tokens.append("@" + node.lstrip("!"))
    # Deduplicate, preserve order, drop empties.
    seen, out = set(), []
    for tok in tokens:
        if tok.strip("@") and tok not in seen:
            seen.add(tok)
            out.append(tok)
    return out


def bare_tokens(self_node_id, long_name, extra=None):
    """Whole-word tokens that address this node WITHOUT an `@`.

    Mesh clients show a node's short name and the last 4 hex of its node id, and
    people type those bare - "hello ab12" is plainly addressed at us even though
    it has no `@` and no `!`. Without this the gate silently drops the only form
    of address most users know.

    `extra` (config `mention_bare_tokens`) is appended verbatim. The long name is
    deliberately NOT included by default: a bare dictionary word lets ordinary
    prose ("mynode is a bot") summon a reply on shared airtime. Add it to the
    config list only if that trade-off is wanted.
    """
    out = []
    node = str(self_node_id or "").strip().lstrip("!")
    if len(node) >= 4 and re.fullmatch(r"[0-9a-fA-F]+", node):
        out.append(node[-4:])
    for tok in (extra or []):
        tok = str(tok or "").strip()
        if tok:
            out.append(tok)
    seen, res = set(), []
    for tok in out:
        low = tok.lower()
        if low not in seen:
            seen.add(low)
            res.append(tok)
    return res


def build_bare_regex(tokens):
    """Compile a whole-word, case-insensitive regex for bare mention tokens.

    The boundaries exclude alphanumerics and underscore so a token cannot match
    inside a longer word, while still allowing punctuation and whitespace
    adjacency ("ab12,", "hi (ab12)").
    """
    if not tokens:
        return None
    parts = [re.escape(tok) for tok in tokens]
    return re.compile(r"(?<![0-9a-z_])(" + "|".join(parts) + r")(?![0-9a-z_])",
                      re.IGNORECASE)


def build_regex(tokens):
    """Compile one case-insensitive regex from literal mention tokens.

    re.escape each token so a node id or name containing regex metacharacters
    cannot widen the match. A token must be followed by a non-word boundary
    (whitespace, punctuation or end of string) so '@xen' does not match
    '@mynode' - a shorter name must not impersonate a longer one.
    """
    if not tokens:
        return None
    parts = [re.escape(tok) for tok in tokens]
    return re.compile(r"(" + "|".join(parts) + r")(?![0-9a-z_])", re.IGNORECASE)


def is_self(frm, self_node_id):
    """True if this packet came from our own node."""
    return bool(self_node_id) and str(frm) == str(self_node_id)


def evaluate(text, reply_id, channel, tokens, ledger, self_node_id=None, frm=None,
             bare=None):
    """Return (allowed: bool, reason: str). Never raises.

    reply_id is the packet's decoded replyId - the on-wire id of the message the
    sender was replying to, when their client set one.
    """
    try:
        # Never respond to our own transmissions, however they arrive.
        if is_self(frm, self_node_id):
            return False, "self"
        rx = build_regex(tokens)
        if rx is None:
            # No mention tokens configured at all: we cannot tell who we are, so
            # we must not speak on shared airtime.
            return False, "no-mention-tokens-configured"
        match = rx.search(str(text or ""))
        if match:
            return True, f"mention:{match.group(0)}"
        brx = build_bare_regex(bare)
        if brx is not None:
            bmatch = brx.search(str(text or ""))
            if bmatch:
                return True, f"bare-mention:{bmatch.group(0)}"
        if reply_id and ledger.has(reply_id, channel):
            return True, f"reply-to-mine:{reply_id}"
        return False, "not-addressed"
    except Exception as exc:  # fail closed
        return False, f"gate-error:{type(exc).__name__}"


def _audit(limit):
    """Replay the inbound log through the gate and print each decision."""
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log = os.path.join(base, "inbound-log.jsonl")
    cfg_path = os.path.join(base, "config.json")
    cfg = {}
    try:
        cfg = json.load(open(cfg_path, "r", encoding="utf-8"))
    except Exception:
        pass
    bridge_cfg = cfg.get("bridge", {})
    self_node = str(bridge_cfg.get("self_node_id", "") or "")
    long_name = str(bridge_cfg.get("self_long_name", "") or "").strip()
    tokens = mention_tokens(self_node, long_name)
    bare = bare_tokens(self_node, long_name,
                       bridge_cfg.get("mention_bare_tokens") or [])
    ledger = Ledger(os.path.join(base, "sent-log.jsonl"))
    print(f"self={self_node}  tokens={tokens}  ledger={len(ledger.snapshot())} ids")
    print("-" * 78)
    try:
        rows = [json.loads(l) for l in open(log, "r", encoding="utf-8") if l.strip()]
    except FileNotFoundError:
        print("no inbound log yet")
        return
    for row in rows[-int(limit):]:
        allowed, reason = evaluate(row.get("text", ""), None, row.get("channel", 0),
                                   tokens, ledger, self_node, row.get("from"),
                                   bare=bare)
        flag = "PASS " if allowed else "drop "
        print(f"{flag} [{row.get('kind','?'):9}] {str(row.get('name',''))[:22]:22} "
              f"{reason:22} {str(row.get('text',''))[:44]!r}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Meshtastic inbound gate (audit mode)")
    ap.add_argument("--audit", type=int, metavar="N", help="replay last N inbound messages")
    args = ap.parse_args()
    _audit(args.audit or 20)

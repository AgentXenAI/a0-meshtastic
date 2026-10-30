import asyncio
from helpers.tool import Tool, Response
from usr.plugins.meshtastic_a0.helpers import banlists as bl


class MeshBlocklist(Tool):
    """Inspect and manage the Meshtastic banlists.

    Two lists, one file (banlists.json):
      hard  - the operator's. Enforced before any model runs; a hard node's text never
              reaches inference and never pings. READ-ONLY here on purpose: there
              is no agent-reachable write path to the hard list. The operator edits
              banlists.json directly.
      soft  - the agent's rap-sheet. Reply-gating: the text still reaches the model,
              the bridge just declines to answer. Each entry carries an
              append-only offence history (timestamp, text, trigger, reason).
              Xen adds/clears soft entries at its discretion; the bridge writes
              them automatically on a triage/escalation trigger.
    """

    async def execute(self, **kwargs) -> Response:
        action = str(self.args.get("action", "status")).strip().lower()
        node = str(self.args.get("node", "") or "").strip()
        name = str(self.args.get("name", "") or "")
        reason = str(self.args.get("reason", "") or "")

        try:
            if action == "status":
                hard = bl.hard_list()
                soft = bl.soft_list()
                lines = [f"hard ({len(hard)}) - JESSE ONLY, enforced pre-model, read-only here:"]
                lines += [f"  {e['node']}  {e.get('name','')}  {e.get('reason','')[:80]}"
                          for e in hard] or ["  (none)"]
                lines.append(f"soft ({len(soft)}) - Xen rap-sheet, reply-gating:")
                for e in soft:
                    n = len(e.get("offences", []))
                    last = (e.get("offences") or [{}])[-1]
                    lines.append(f"  {e['node']}  {e.get('name','')}  offences={n}  "
                                 f"last={last.get('trigger','')} {last.get('justification','')[:70]}")
                if not soft:
                    lines.append("  (none)")
                return Response(message="\n".join(lines), break_loop=False)

            if action == "rap":
                if not node:
                    return Response(message="Error: 'node' required for rap.", break_loop=False)
                soft = {bl.norm_node(e['node']): e for e in bl.soft_list()}
                entry = soft.get(bl.norm_node(node))
                if not entry:
                    return Response(message=f"{node} has no rap-sheet (not soft-banned).",
                                    break_loop=False)
                lines = [f"rap-sheet {entry['node']} {entry.get('name','')} "
                         f"(hits={entry.get('hits',0)}, first={entry.get('first_seen','')}):"]
                for o in entry.get("offences", []):
                    lines.append(f"  [{o.get('ts','')}] {o.get('trigger','')} "
                                 f"score={o.get('score','')} by={o.get('by','')} :: {o.get('justification','')}")
                    if o.get("text"):
                        lines.append(f"      said: {o['text'][:200]}")
                return Response(message="\n".join(lines), break_loop=False)

            if action == "scan":
                window = float(self.args.get("window_hours", 48))
                threshold = float(self.args.get("threshold", 3.0))
                rows = await asyncio.to_thread(bl.scan_hostile, window, threshold, 20)
                if not rows:
                    return Response(message=f"No nodes scored >= {threshold} in the last "
                                            f"{window:g}h of logged inbound traffic.",
                                    break_loop=False)
                lines = [f"{len(rows)} node(s) scored >= {threshold}; each now has a soft "
                         f"offence appended (rap-sheet). Hard list untouched:"]
                for r in rows:
                    lines.append(f"  {r['node']}  {r['name']}  score={r['score']} hits={r['hits']}")
                    for ev in r.get("evidence", [])[-2:]:
                        lines.append(f"      said: {ev[:200]}")
                return Response(message="\n".join(lines), break_loop=False)

            if action == "recent":
                rows = bl.read_inbound(int(self.args.get("limit", 30)))
                if not rows:
                    return Response(message="No logged inbound messages yet.", break_loop=False)
                lines = [f"{len(rows)} logged inbound message(s):"]
                for r in rows:
                    lines.append(f"  {r.get('ts','')} [{r.get('kind','')}] "
                                 f"{r.get('name','') or r.get('from','')}: {r.get('text','')[:160]}")
                return Response(message="\n".join(lines), break_loop=False)

            if action == "score":
                text = str(self.args.get("text", "") or "")
                score, hits = bl.hostile_score(text)
                return Response(message=f"score={score} phrases={hits}", break_loop=False)

            if action in ("softban", "softclear") and not node:
                return Response(message="Error: 'node' is required for this action.",
                                break_loop=False)
            if action == "softban":
                ok, msg = bl.soft_add_offence(node, name, self.args.get("text", ""),
                                              trigger=str(self.args.get("trigger", "T1")),
                                              justification=reason or "manual soft ban",
                                              by="xen")
                return Response(message=(f"SOFT-BANNED {msg} - replies gated, rap-sheet updated."
                                         if ok else f"Not soft-banned: {msg}"), break_loop=False)
            if action == "softclear":
                ok, msg = bl.soft_clear(node)
                return Response(message=f"Cleared soft ban {msg}" if ok else f"Nothing cleared: {msg}",
                                break_loop=False)

            if action in ("block", "unblock", "promote"):
                return Response(
                    message=("The HARD banlist has no agent-reachable write path (by design). "
                             "The operator edits banlists.json directly. Use softban/softclear for "
                             "the agent's own reply-gating."), break_loop=False)

            return Response(message="Unknown action. Use status, rap, scan, recent, score, "
                                    "softban, softclear.", break_loop=False)
        except Exception as exc:
            return Response(message=f"Error: {type(exc).__name__}: {exc}", break_loop=False)

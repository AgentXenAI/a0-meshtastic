import asyncio
from helpers.tool import Tool, Response
from usr.plugins.meshtastic_a0.helpers import bridge
from usr.plugins.meshtastic_a0.helpers import blocklist as bl


class MeshBlocklist(Tool):
    """Inspect and manage the Meshtastic interaction blocklist.

    `blocked` entries are enforced in code: a blocked node gets no model turn and
    no reply, in either direction. `candidates` are proposals from hostile-message
    triage and enforce nothing until promoted.
    """

    async def execute(self, **kwargs) -> Response:
        action = str(self.args.get("action", "status")).strip().lower()
        node = str(self.args.get("node", "") or "").strip()
        name = str(self.args.get("name", "") or "")
        reason = str(self.args.get("reason", "") or "")
        evidence = str(self.args.get("evidence", "") or "")

        try:
            if action == "status":
                blocked = bl.blocked()
                cands = bl.candidates()
                lines = [f"blocked ({len(blocked)}) - ENFORCED:"]
                lines += [f"  {e['node']}  {e.get('name','')}  {e.get('reason','')[:90]}"
                          for e in blocked] or ["  (none)"]
                lines.append(f"candidates ({len(cands)}) - NOT enforced:")
                lines += [f"  {c['node']}  {c.get('name','')}  score={c.get('score')} "
                          f"hits={c.get('hits')}  {c.get('reason','')[:90]}"
                          for c in cands] or ["  (none)"]
                return Response(message="\n".join(lines), break_loop=False)

            if action == "scan":
                window = float(self.args.get("window_hours", 48))
                threshold = float(self.args.get("threshold", 3.0))
                rows = await asyncio.to_thread(bl.scan_hostile, window, threshold, 20)
                if not rows:
                    return Response(message=f"No nodes scored >= {threshold} in the last "
                                            f"{window:g}h of logged inbound traffic.",
                                    break_loop=False)
                lines = [f"{len(rows)} candidate(s) from logged inbound traffic "
                         f"(proposals only, nothing blocked):"]
                for r in rows:
                    lines.append(f"  {r['node']}  {r['name']}  score={r['score']} "
                                 f"hits={r['hits']}")
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

            if not node:
                return Response(message="Error: 'node' is required for this action.",
                                break_loop=False)

            if action == "block":
                ok, msg = bl.add_block(node, name, reason, evidence, by="jesse")
                return Response(message=(f"BLOCKED {msg} - enforced immediately, no restart "
                                         f"needed." if ok else f"Not blocked: {msg}"),
                                break_loop=False)
            if action == "unblock":
                ok, msg = bl.remove_block(node)
                return Response(message=f"Unblocked {msg}" if ok else f"Nothing removed: {msg}",
                                break_loop=False)
            if action == "promote":
                ok, msg = bl.promote_candidate(node, reason)
                return Response(message=f"Promoted to blocked: {msg}" if ok else f"Failed: {msg}",
                                break_loop=False)
            if action == "dismiss":
                ok, msg = bl.dismiss_candidate(node)
                return Response(message=f"Dismissed candidate {msg}" if ok else f"Failed: {msg}",
                                break_loop=False)
            if action == "score":
                text = str(self.args.get("text", "") or "")
                score, hits = bl.hostile_score(text)
                return Response(message=f"score={score} phrases={hits}", break_loop=False)

            return Response(message="Unknown action. Use status, scan, recent, block, "
                                    "unblock, promote, dismiss, or score.", break_loop=False)
        except Exception as exc:
            return Response(message=f"Error: {type(exc).__name__}: {exc}", break_loop=False)

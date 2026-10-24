# Meshtastic A0

Chat bridge and messaging between Agent Zero and a Meshtastic LoRa mesh node,
reached over the Meshtastic TCP API (port 4403). Works with a node running
`meshtasticd` directly or through a **Meshmonitor** proxy (which lets several
clients share one node at once).

This mirrors the Discord bridge: inbound mesh messages can drive Agent Zero and
the reply goes back out over the radio, and an agent tool sends DMs/broadcasts
on demand.

## What it gives you
- **Bridge (inbound):** a persistent listener catches text messages on the mesh.
  A DM from an allowed node is routed into an Agent Zero context and the reply is
  sent back to that node over the mesh. Great for reaching the agent when you are
  off-grid (e.g. from a handheld or a base node).
- **`meshtastic_send` tool (outbound):** the agent sends a DM to a node or, if
  explicitly enabled, a broadcast to `^all`.
- **`meshtastic_read` tool:** read node list, my info, and recently captured
  inbound messages.
- **Web UI controls:** start/stop/status for the bridge in plugin Config.

## Setup
1. Install the plugin (dependencies install automatically into the Agent Zero
   runtime via the install hook).
2. In **Config**, set the connection:
   - `name`: label (e.g. `My Node`)
   - `host`: the node or Meshmonitor proxy hostname/IP (e.g. `node.local`)
   - `port`: `4403`
3. Set `bridge.allowed_nodes` to the node IDs you trust (e.g. your base node).
   Empty means any sender is allowed (still rate-limited) — set an allowlist on a
   live mesh.
4. Turn on `bridge.auto_start` (or press **Start** in Config) to begin listening.

## Live-mesh safety
Meshtastic is a shared radio. This plugin is conservative by default:
- It only auto-replies to **DMs** (broadcasts are ignored unless you opt in).
- **Broadcasts are disabled** for the send tool unless `send.allow_broadcast` is on.
- Per-sender rate limiting and a message length cap are enforced.

## Notes
- The listener disables the Meshtastic client heartbeat (via a subclass, not a
  global patch) so idle connections through a Meshmonitor proxy are not dropped.
- The bridge runs inside the Agent Zero framework process and talks to the radio
  over TCP; the container must be able to reach `host:port`.


## Message size limit

Meshtastic enforces a hard **200-byte** payload limit per text message. The mesh is shared, low-bandwidth airspace. Every outbound message (tool sends and bridge replies) is clamped to 200 bytes (UTF-8, never splitting a multibyte character) at the single chokepoint in `helpers/radio.py` (`clamp_bytes`). Keep replies short — one or two plain-text sentences.

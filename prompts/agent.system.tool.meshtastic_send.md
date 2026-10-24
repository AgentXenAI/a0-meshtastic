### meshtastic_send
Send a message over the Meshtastic LoRa mesh using the configured node/proxy.

action: dm (default) or broadcast
- dm: send a direct message to one node. Requires `dest` (node id like `!12345678` or a name from meshtastic_read).
- broadcast: send to `^all` on a channel. Only works if `send.allow_broadcast` is enabled in Config (spam risk on a live mesh).

args:
- dest: target node id or name (dm only)
- text: message text (required)
- channel: channel index (optional, default from config)
- host / port: optional override of the configured connection

Meshtastic enforces a hard 200-BYTE payload limit; text is clamped to 200 bytes before sending. Be careful on a live mesh: prefer DMs, keep messages short, and do not spam — bandwidth is shared airspace.

{"tool_name":"meshtastic_send","tool_args":{"action":"dm","dest":"!12345678","text":"Hello from Agent Zero."}}

# R7 hub: presence, checks, and notification

## Scope and durability boundary

The hub is an NCP-resident, always-on (by the operating assumption) relay for
live node presence and small operator-facing events. It is not a source of
record and deliberately has no persistence layer.

The durable split is fixed:

- **Supabase stage 2** retains file relay and the recipient's offline buffer.
- **Hub** owns live presence, node-local heartbeat check results, and optional
  operator notification while nodes are connected.

Consequently a hub outage must not block stage 1 or stage 2. A
`panewired` daemon reconnects in the background with exponential backoff from
one second to a maximum of 60 seconds. Hub events are bounded in memory and
may be dropped during an outage; that is intentional because they are not a
file-delivery path.

R6 has no command dispatch, shell execution, or remote task-launch endpoint.
Any command channel requires an allowlist policy design and a separately
reviewed future round.

## Server and credentials

Run the server only on loopback. The CLI rejects `0.0.0.0`, IPv6 wildcard, and
every host other than `127.0.0.1`.

```sh
panewire hub \
  --listen 127.0.0.1:9377 \
  --hub-auth /etc/panewire/hub-auth.env \
  --hub-tg-env /etc/panewire/hub-tg.env \
  --hub-grace 2m
```

`--hub-auth` is an explicit regular file with mode `0600` (not a symlink). Its
only non-comment entries are static node tokens:

```sh
HUB_TOKEN_operator=replace-with-operator-token
HUB_TOKEN_mac-a=replace-with-mac-a-token
HUB_TOKEN_mac-b=replace-with-mac-b-token
```

`operator` is reserved: `HUB_TOKEN_operator` authenticates `/v1/nodes`,
`/v1/events`, and `panewire hub-status`. Each other key is a node machine ID
for `/v1/agent`; its token cannot authenticate operator endpoints. Tokens are
issued by an operator editing this file. There is no enrollment or token
minting endpoint.

Endpoints are:

| Endpoint | Authentication | Purpose |
| --- | --- | --- |
| `GET /healthz` | none | local health response |
| `GET /v1/nodes` | `Authorization: Bearer` operator token | sorted presence list |
| `WS /v1/agent` | `X-Panewire-Machine-ID` plus that node's bearer token | node connection |
| `WS /v1/events` | operator bearer token | all-node event subscription |

The node list includes `machine_id`, `connected_since`, `last_ping_ms` (the
non-negative elapsed milliseconds since the last node keepalive), and
`remote_meta` (the protocol version and peer address), plus `state`:
`connected`, `stale`, or `disconnected`. A WebSocket close marks a node
`disconnected` immediately. The hub marks a live connection `stale` after 30
seconds with no application keepalive and restores `connected` after a new
`ping` or `pong`.

The bearer token is never copied to logs, error responses, event payloads, or
status output.

`--hub-tg-env` is optional. When set, it is an explicit non-symlink regular
mode-`0600` file with `TG_BOT_TOKEN` and `TG_CHAT_ID`. The hub sends one
incident after a node has stayed `disconnected` or `stale` for `--hub-grace`
(two minutes by default), then one recovery after two connected observations.
Each failed local check likewise needs two consecutive heartbeat observations;
its recovery also needs two healthy observations. Telegram text contains only
the machine ID, reason, and check name.

## Protocol

The WebSocket envelope is closed JSON. A node first sends:

```json
{"type":"hello","machine_id":"mac-a","version":"panewired-r7"}
```

It can then send `{"type":"ping"}` and events:

```json
{"type":"event","kind":"heartbeat","payload":{"status":"alive","checks":{"disk":"ok","service":"fail"}}}
{"type":"event","kind":"note","payload":{"message":"operator-visible note"}}
```

The hub replies to node `ping` with `pong`, sends its own `ping` keepalive,
and accepts the node's `pong` response. Unknown fields, malformed envelopes,
and unknown event kinds are ignored while an internal counter advances; they
do not disconnect an authenticated node. Authentication failures are rejected
before the WebSocket handshake.

`panewired` sends a heartbeat event after connecting and alongside its
periodic ping. The heartbeat accepts only `status:"alive"` and a map of local
check names to `ok` or `fail`; argv and command output never leave the node.

## Node and operator clients

Node credentials also require an explicit mode-`0600` regular file:

```sh
HUB_MACHINE_ID=mac-a
HUB_TOKEN=replace-with-mac-a-token
```

Hub connectivity is off by default. Add these options to the existing node
daemon invocation:

```sh
panewire daemon \
  --hub-url wss://hub.robinco.dev \
  --hub-token-env /Users/you/.config/panewire/hub-node.env \
  --hub-cf-env /Users/you/.config/panewire/hub-cf-access.env \
  --checks-config /Users/you/.config/panewire/checks.json
```

In production `--hub-url` must be a `wss://` base URL. The daemon appends
`/v1/agent`, maintains the outbound connection, and emits a constant warning
on retry without exposing a URL response or token. It does not read a default
credential path.

When the Cloudflare Access application uses Service Auth, `--hub-cf-env` is
the explicit optional mode-`0600` regular file (not a symlink) containing:

```sh
CF_ACCESS_CLIENT_ID=replace-with-access-service-client-id
CF_ACCESS_CLIENT_SECRET=replace-with-access-service-client-secret
```

The daemon sends those values only as `CF-Access-Client-Id` and
`CF-Access-Client-Secret` on each WebSocket upgrade request. They are never
printed or logged. Omit the flag when the hub does not require Cloudflare
Access Service Auth.

`--checks-config` is optional and local to that node. Its simple JSON schema
contains only a `checks` array of `{name, argv, timeout}` values. The file is
explicit, regular, and not a symlink. A check result is `ok` or `fail`; command
output is discarded.

For an operator, create another explicit mode-`0600` file:

```sh
HUB_MACHINE_ID=operator
HUB_TOKEN=replace-with-operator-token
```

Then request the human-readable status table:

```sh
panewire hub-status \
  --hub-url https://hub.robinco.dev \
  --hub-token-env /safe/operator/hub-operator.env \
  --hub-cf-env /safe/operator/hub-cf-access.env
```

`hub-status` uses the same optional Access env format and sends its two values
only as headers on the HTTPS request.

## NCP deployment checklist

1. On the NCP host, install the reviewed `panewire` binary and create
   `/etc/panewire/hub-auth.env` as mode `0600` owned by the dedicated
   `panewire` service account (keep the containing directory root-controlled).
   Do not put its values in a systemd unit or command line.
2. Create `/etc/panewire/hub-tg.env` as a mode-`0600` file only when Telegram
   notification is desired. Install a `panewire-hub.service` unit whose
   `ExecStart` is equivalent to `panewire hub --listen 127.0.0.1:9377
   --hub-auth /etc/panewire/hub-auth.env --hub-tg-env
   /etc/panewire/hub-tg.env --hub-grace 2m`.
   Run it as a dedicated unprivileged service account with `Restart=always`.
   Verify locally with `curl http://127.0.0.1:9377/healthz`.
3. Configure `cloudflared` on that same NCP host to map the hostname
   `hub.robinco.dev` to `http://127.0.0.1:9377`. The hub itself remains
   loopback-only; do not open an NCP firewall listener for port 9377.
4. Create a Cloudflare Access application for `hub.robinco.dev` before
   distributing node flags. Restrict it to the approved machines/service
   identities. The hub's static bearer authentication remains required behind
   Access, so Access is a network gate rather than a replacement for node
   identity.
5. On each Mac, write its own node env file and, for Service Auth, its separate
   Access env file with mode `0600`; add
   `--hub-url wss://hub.robinco.dev --hub-token-env … --hub-cf-env …
   --checks-config …` to the existing daemon launch configuration, and restart
   the daemon. Verify with the operator
   `hub-status` command and an Access-authenticated `/v1/events` client.

The deployment assumes the NCP host is the intentionally always-on hub for
this R7 scope. It is still not a durable data authority: recovery is simply
restart the service/tunnel, while Supabase retains offline files.

# Fleet state

Every node publishes the state it owns. The hub keeps the latest value per key
and serves it at `GET /v1/state`, so panewire reads the current state instead of
measuring it on every spawn.

Before this, panewire measured on demand: it re-read quota and host load for each
spawn. A host that relied on another host's measurement saw that reading go
stale whenever the measuring host was idle longer than the freshness window,
and its spawns were refused with `target account quota unavailable: stale`.

## Messages (v1)

A state message is one JSON object. The hub owns the envelope fields `v`, `seq`,
`host`, `kind`, `key` and `received_at`. The publisher owns
`measured_at` and the kind's body.

```json
{
  "v": 1,
  "seq": 42,
  "host": "node-a",
  "kind": "quota",
  "key": "claude:0123456789abcdef",
  "measured_at": "2026-11-01T02:40:00Z",
  "received_at": "2026-11-01T02:40:01Z",
  "quota": {
    "account_key": "claude:0123456789abcdef",
    "pool": "claude",
    "source": "usage-api",
    "buckets": [{"window": "5h", "used_pct": 41, "resets_at": "2026-11-01T05:00:00Z", "scope": "account"}]
  }
}
```

Each `(host, kind, key)` triple names one latest value.

| kind | key | publisher | body |
|---|---|---|---|
| `quota` | account key (`<pool>:<16-hex>`) | the node that runs the measurement | `quota`: `account_key`, `pool`, `source` (pacelever's measurement source), `buckets[]` (`window`, `used_pct`, `resets_at`, `scope`, `model`) |
| `load` | empty | the hub, computed from the node's own heartbeats with `/etc/panewire/placement.json` | `load`: `load_ratio`, `score`, `accepting`, `usable`, `connected`, `slot_full`, `active_jobs`, `reason` |
| `publisher` | empty | the hub, from the node's connection | `publisher`: `connected`, `last_seen` |

- **Host binding.** `host` is always the machine id of the authenticated
  connection the value arrived on. A node may name only itself. A message that
  names another host is refused and counted as an unknown message.
- **Load.** The node already publishes its raw load and memory in its
  heartbeat. The hub adds the placement policy (score, task
  slots, acceptance) through the same computation as `GET /v1/placement`, but
  without the wake-on-spill side effect. `measured_at` is the time of that
  computation. `accepting` is placement's effective acceptance: connected,
  accepting after operator overrides, and no memory pressure. `usable` is
  placement's own verdict for the host.
- **Spawn is retired.** A node no longer publishes `spawn`. The hub accepts one
  from an old node and drops it. Job state comes from `GET /v1/jobs`.
- **No secrets.** Messages carry account keys (fingerprints that the shared
  quota cache already uses), never credentials, provider error text, brief text
  or pane contents.

## Freshness rule

A consumer uses a hub value only when both of these hold:

1. **The publisher is alive.** The latest `publisher` message for the host has
   `connected: true`, and `at - last_seen <= publisher_ttl`. `at` is the
   snapshot time. The hub emits a
   `publisher` beat at most every 20 s while heartbeats arrive (nodes beat every
   10 s), and immediately on connect and disconnect. A dead publisher is
   announced once, on the transition, and is not repeated. The default
   `publisher_ttl` is 90 s.
2. **The value is fresh.** For `quota`, `now - measured_at` must be between
   −1 minute and the consumer's `fresh_ttl_s`, the same rule as the shared
   quota cache. For `load`, `now - measured_at <= publisher_ttl`.

This rule separates two cases. "Nothing changed" is an alive publisher with an
older `measured_at` that is still inside the TTL; the value stays usable.
"Publisher dead" is a missing or stale `publisher` beat; the value is unusable
however recent its `measured_at`. Nodes republish an unchanged quota reading at
most every 5 minutes.

When the rule fails, panewire falls back to the direct measurement it used before
this feature. It records which source it used (see panewire `docs/config.md`,
"Fleet state").

## Endpoints

| Route | Caller | Answer |
|---|---|---|
| `GET /v1/state` | operator, or a service with `state.read` | `{"v":1,"seq":N,"at":…,"states":[…]}`: every latest value, ordered by `seq` |
| node websocket event `state` | node | publish one value for the node's own host |

Node tokens cannot read the route (401). A service without `state.read` gets
a 403.

### Bounds

- Latest values: 64 `quota` keys per host. The oldest is dropped first when a
  host is full.
- Each node has a publish budget: a burst of 320 values, refilled at 2 per
  second. A value over the budget is dropped and counted as an unknown message.
- One node `state` event is at most 4 KiB.

## Node publisher

`panewire daemon` publishes only with `--state-publish`. With that flag it
publishes:

- `quota` for each pool in `--state-quota-pools` (comma-separated, empty by
  default, which disables quota publishing). Every `--state-quota-interval`
  (default 60 s, minimum 10 s) it runs `pacelever refresh <pool>`. pacelever reads the shared
  cache first and probes only when the reading is stale, so a 60 s interval
  keeps the account's shared reading fresh at the cost of one cache read per
  pool per minute. The node publishes a reading only when pacelever reports it
  as not stale, with an account fingerprint of kind `account`. It republishes
  when the reading changes, and otherwise at least every 5 minutes. A failing
  refresh logs one warning per failure streak and publishes nothing. The
  reading then ages past its TTL, and consumers measure directly.

Every new hub connection re-announces the node's current readings, so a
restarted hub regains them immediately.

List only pools whose accounts this host measures: pacelever's
`measurable_accounts`, plus the pools it derives from the local login.

## Rollout

Order: hub, then nodes, then panewire. Each step is safe on its own.

1. **Hub.** Deploy the hub build. `GET /v1/state` starts answering with `load`
   and `publisher` values right away. Older nodes keep working because they
   never send `state` events. If panewire reads with a service token, add
   `state.read` to that service's `HUB_SCOPES_<client>` line; the operator
   token needs nothing.
2. **Nodes.** Deploy the node build. Add `--state-publish` to every node's
   service arguments. On each host that measures an account, also add
   `--state-quota-pools <pools>`. Check that
   `GET /v1/state` lists that host's `quota` values with a recent
   `measured_at`. A new node that connects to an old hub only adds to the hub's
   unknown-message count, because the old hub ignores `state` events.
3. **panewire.** Deploy panewire. Placement and the quota gate read the hub state
   and record `state.source` (`hub` or `direct`) in the placement record. With
   `[hub] state = "off"`, panewire skips the hub state and measures directly as
   before.

## Rollback

Reverse order. Each step is independent.

1. **panewire.** Set `[hub] state = "off"`, or redeploy the previous panewire.
   panewire then measures directly, as before.
2. **Nodes.** Remove `--state-publish` (and `--state-quota-pools`), or
   redeploy the previous node build. With no publisher, panewire falls back to direct measurement and
   records the reason.
3. **Hub.** Redeploy the previous hub build. `/v1/state` returns 404, and
   panewire records `hub_unavailable` and measures directly.

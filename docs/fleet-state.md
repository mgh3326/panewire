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
| `quota` | account key (`<pool>:<16-hex>`) | the node that runs the measurement | `quota`: `account_key`, `pool`, `source` (측정 소스), `buckets[]` (`window`, `used_pct`, `resets_at`, `scope`, `model`), `free_bucket` (`state`, `resets_at`) for a pool with a free tier (devin's SWE-2 bucket; a free-only reading may carry no bucket) |
| `load` | empty | the hub, computed from the node's own heartbeats with `/etc/panewire/placement.json` | `load`: `load_ratio`, `score`, `accepting`, `usable`, `connected`, `slot_full`, `active_jobs`, `reason` |
| `publisher` | empty | the hub, from the node's connection | `publisher`: `connected`, `last_seen` |
| `readiness` | empty | the hub, from the node's heartbeat and its own launch counts | `readiness`: `readiness_unknown`, `readiness` (the node's report), `backoff_until` (harness kind to time). See [placement.md](placement.md#readiness) |

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
- **Readiness.** A node that sends readiness has its last report here;
  one that never did has `readiness_unknown: true` and no report. A consumer
  applies the same freshness rule as for `load` and, when it fails, reads the
  host as `readiness_unknown`: no readiness check drops it.
- **Spawn is retired.** A node no longer publishes `spawn`. The hub accepts one
  from an old node and drops it. Job state comes from `GET /v1/jobs`.
- **No secrets.** Messages carry account keys (fingerprints that the shared
  quota cache already uses), never credentials, provider error text, brief text
  or pane contents.

## Freshness rule

*A hub spawn decision does not reuse a snapshot across decisions (hk 4448): the freshness window below is how old a value may be, and a decision that waited at the placement gate reads the state again after it. A host the hub reports slot-full stays excluded, never turned into an unreported host. See docs/placement.md, "Spawn bridge".*

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
  (default 60 s, minimum 10 s) it refreshes the pool. The node measures
  `claude`, `codex`, `commandcode`, `devin` and `agy` in its own process
  ([docs/quota.md](quota.md#native-측정-풀)). Other pools are unmeasurable:
  they are neither refreshed nor published. The node reads the shared cache
  first and probes only when the reading is stale, so a 60 s interval keeps
  the account's shared reading fresh at the cost of one cache read per pool
  per minute. The node publishes a reading only when it is current and carries an
  account fingerprint of kind `account`. It republishes
  when the reading changes, and otherwise at least every 5 minutes. A failing
  refresh logs one warning per failure streak and publishes nothing. The
  reading then ages past its TTL, and consumers measure directly.

A failing refresh of kind `auth`, `auth_failed`, `credentials` or `token_expired`
marks the pool's login missing. The one exception is agy on a host with no
measurable account (`[quota].measurable_accounts` lacks its `[quota.accounts]`
fingerprint): that failure reads unknown, because the host is a configuration
gap, not a login problem (hk 3783). The login probe leaves that host alone too.

Every new hub connection re-announces the node's current readings, so a
restarted hub regains them immediately.

### Login probe

The agy account is shared by several hosts, and a shared reading is per
account. While it is fresh the loop above returns it without measuring, so a
host can go on indefinitely without using its own credential and never learn
that the credential stopped working (hk 3857). Two node-side signals close
that gap. Both feed the `login` field of the node's readiness report
([placement.md](placement.md#readiness)); the hub's one-time "log in" notice
and the `login_missing` placement drop are unchanged.

- **Probe.** With `[quota] login_probe_interval = "<duration>"` set (a Go
  duration such as `"30m"`; default unset, which disables the probe), the node
  measures each pool in `--state-quota-pools` once per interval with this
  host's own credential, even when the shared reading is fresh. The probe runs
  right after the loop's own measurement of that pool in the same goroutine,
  so the two never overlap. It takes no lease, publishes no reading and
  announces no presence. A pool is probed only when this host lists its
  account in `[quota].measurable_accounts` (for agy, the `[quota.accounts]`
  fingerprint); a host with no measurable account is never probed and never
  marked by the probe (hk 3783). A failure of kind `auth`, `auth_failed`,
  `credentials` or `token_expired` sets `login_missing`, as a failed loop
  measurement does. A pass clears it. Any other failure (network, rate limit,
  timeout) changes nothing. The node logs one line per state change and never
  the credential. The key is read at each round, so a change needs no restart.
  Enable it per host only after checking that a probe does not rotate a token
  other hosts share.
- **Login screen on rc=76.** A hub spawn that ends rc=76
  (`BRIEF_SUBMISSION_UNCONFIRMED`) leaves the executing node a screen capture,
  `jobs/<job>/spawn-failure-*.txt` (`captureFailure`). When the newest capture
  is the harness's login screen and pane cleanup was verified, the node marks
  that (host, harness) `login_missing` at once and sends rc=88 with the
  `AUTH_UNAVAILABLE` refusal and sign-in hint. An unchanged hub classifies it
  as `auth_unavailable`; the desk gets its one-time `log in` notice. The rc=76
  refusal comes after the node's hk job claim, so the operation ends on that
  host (hk answers another host's claim of the job id with 409) and the host is
  dropped from the next placement. Without verified cleanup, or without a complete
  matching capture, the node keeps rc=76 and the original refusal. The saved
  capture and job record are unchanged. The screens are the
  `harnessLoginScreens` table in `relay_harness_adapter.go`; every listed line
  must appear. Only agy has a row: it comes from the 2026-11-10 mac-dev
  captures ("Open the URL below in your browser:" and "After authenticating,
  copy the code displayed in the browser and paste it below:"). claude and
  codex have none until someone records their login screen. When the daemon
  publishes state and polls this measurable pool through
  `--state-quota-pools`, the mark stays until a passing login probe, a
  successful launch of that kind, or a successful quota measurement made with
  this host's credential clears it. A fresh shared reading does not clear it;
  a hub `quota.refresh` checks this host's credential when the mark is set.
  With probes off, if another host keeps the shared reading fresh, the mark can
  remain until this node restarts (O1).
  When the daemon has no periodic measurement path for the pool (it is not
  measurable here, is not polled, or state publishing is off), the mark expires
  after 30 minutes (the first-run dialog TTL). The marks are in node memory; a
  node restart forgets them.

`--state-quota-pools`에는 이 호스트가 측정할 수 있는 풀만 지정합니다. 지원 풀은
`claude`, `codex`, `commandcode`, `devin`, `agy`입니다. `measurable_accounts`와
로컬 로그인 정보가 측정 계정의 범위를 결정합니다.

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

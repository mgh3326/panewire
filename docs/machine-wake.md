# Machine wake and node idle shutdown

Two small pieces share the Wake-on-LAN machinery: an operator-invoked wake
primitive for one machine, and an opt-in node-local poweroff for machines
whose idle policy is to be off.

## `panewire node wake <machine>`

The hub holds a routing table, not the packet: which authenticated node may be
asked to emit a magic packet for each target. `panewire hub` loads it from
`--wake-config /etc/panewire/wake.json` (mode-0600 regular file, hot-reloaded
like the burst policy — invalid edits keep the last good table):

```json
{
  "targets": [
    { "machine": "desktop", "via": "rpi" }
  ]
}
```

The operator posts through a fixed allowlisted endpoint:

```sh
panewire node wake --hub-url https://hub.example --hub-token-env /etc/panewire/hub-operator.env desktop
# optional: --timeout 120s (max 10m)
```

Only the `operator` credential may call it; node credentials are rejected.
The hub emits one closed three-field event — `{"type":"wake","machine":...,
"emitted_at":...}` — to the configured waker's agent stream, then waits for
the target's own authenticated heartbeat to mark it connected. Answers:

- `200 {"machine","via","state":"connected","waited_ms"}` — target heartbeat
  returned inside the timeout; `already_connected` is set when no wake was
  needed at all.
- `404 {"reason":"unknown_target"}` — machine is not in the wake config.
- `503 {"reason":"waker_unavailable"}` — the configured waker is not
  connected.
- `504 {"reason":"wake_timeout"}` — packet(s) dispatched but the target did
  not return in time.

UDP loss is ordinary, so the hub redispatches the wake event every 15s while
waiting; each dispatch carries a fresh `emitted_at`.

### Waker node config

The waker node's own allowlist decides which machines a `wake` event may
actually send a packet for, and carries everything the packet needs —
the wire event carries no MAC or destination. On the waker (e.g. the rpi
node):

```sh
panewire daemon ... --wake-targets /etc/panewire/wake-targets.json
```

```json
{
  "targets": [
    { "machine": "desktop", "mac": "02:1a:2b:3c:4d:5e", "broadcast": "255.255.255.255:9" }
  ]
}
```

`broadcast` is any UDPv4 host:port (directed broadcast or a last-known unicast
address are both legitimate); port 9 is conventional. The node dedupes
(machine, emitted_at) and warns + ignores events for machines outside the
file. The packet itself is the standard 6×0xff + 16×MAC payload already used
by failover and burst wake.

## Node idle shutdown (`--idle-shutdown`)

Off by default (`--idle-shutdown 0`). When a duration is given, the node's
daemon powers the machine off once it has been continuously idle for that
long — every observation inside the window must be clear. It only runs on a
hub-connected node (`--hub-url` + `--hub-token-env`), since the decision must
reach operator-desk.

```sh
panewire daemon ... --idle-shutdown 30m \
  --idle-shutdown-command 'sudo -n /usr/sbin/poweroff'
```

Shutdown requires **all** of these to hold for the whole window:

- no herdr agent pane is `working` or `blocked`;
- no herdr pane is `idle` while bound to an active job lease;
- no active job leases in the node's `jobs/*/events` inbox;
- no GitHub Actions runner — no process whose argv[0] basename is
  `Runner.Worker`;
- no interactive login sessions (`who` is empty);
- no keep-awake marker at `~/.config/panewire/keep-awake` (override with
  `--idle-shutdown-keep-awake`).

Any probe failure counts as busy — a node that cannot see its own state must
not power itself off. Once the decision is made the manager logs it, writes
exactly one durable `lane.event` (`{"kind":"idle_shutdown","machine":...,
"idle_seconds":...}`) to `--idle-shutdown-lane` (default `operator-desk`)
through the normal events-lane relay, waits 2s for the event to leave, then
runs the configured command as a structured argv — never a shell line. A
failed command restarts the idle window; it never retries in a tight loop. A
committed decision fires at most once per daemon run.

Other knobs: `--idle-shutdown-poll` (probe interval, default 1m). Tests never
execute the command; the process list, login probe, marker, clock, and exec
are all injectable seams.

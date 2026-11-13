# Fleet Alloy log shipper

This directory records fleet Alloy configurations. It sends logs to Grafana
Cloud Loki and node_exporter-compatible host metrics to a hosted Prometheus
remote-write endpoint (Grafana Cloud) over basic auth. Do not place a service
URL, username, token, instance id, hostname, or a real launchd plist here.

## Credentials

Linux reads a repository-external `loki.env` containing `LOKI_URL`, `LOKI_USER`,
and `LOKI_TOKEN`, and requires `PROM_REMOTE_WRITE_URL`,
`PROM_REMOTE_WRITE_USER`, and `PROM_REMOTE_WRITE_TOKEN` in its environment —
the endpoint URL, the hosted metrics instance id, and a token with metrics
write scope. The installer writes those values plus `MACHINE_ID` into the
platform's Alloy systemd EnvironmentFile. macOS receives the same variables
through the user launchd plist; start from `mac.plist.example` and keep the
resulting plist outside this repository.

Two optional variables tune collection per host. The Linux installer copies
them into the EnvironmentFile when they are set in its environment; on macOS
they belong in the private plist's `EnvironmentVariables`.

| Variable | Effect | Default |
| --- | --- | --- |
| `ALLOY_ENABLE_COLLECTORS` | JSON list of extra unix-exporter collectors, e.g. `["thermal"]` or `["textfile"]`. Thermal needs hardware CPU power status; hosts without it log an error per scrape, so it is opt-in. | unset → `[]` (no extra collectors) |
| `FLEET_TEXTFILE_DIR` | Directory of `*.prom` files scraped when `"textfile"` is in `ALLOY_ENABLE_COLLECTORS` (app gauges such as session and quota counters). Required when enabling textfile. | unset |
| `ALLOY_DISABLE_COLLECTORS` | JSON list of default unix-exporter collectors to turn off. macOS enables `thermal` by default; set `["thermal"]` on Macs without hardware CPU power status (Apple silicon), otherwise every scrape logs an error. | unset → `[]` |

## Install

Stage `config.linux.alloy`, `install-linux.sh`, and a mode-0600 local `loki.env`
on the target. Run the Linux installer with its machine identity:

```sh
sudo ALLOY_LOKI_ENV=/secure/path/loki.env ./install-linux.sh MACHINE_ID
```

The optional second positional argument is also a credentials path. If neither
is given, the installer uses `/tmp/alloy/loki.env` for compatibility with the
existing hosts. It consumes (removes) the credentials file after creating the
root-owned systemd EnvironmentFile.

| Host | Package / privilege | Invocation |
| --- | --- | --- |
| NCP | RPM, root | `ALLOY_LOKI_ENV=/secure/loki.env ./install-linux.sh ncp-id` |
| OCI | RPM, sudo, no Docker | `sudo ALLOY_LOKI_ENV=/secure/loki.env ./install-linux.sh oci-id` |
| RPi | APT, sudo | `sudo ALLOY_LOKI_ENV=/secure/loki.env ./install-linux.sh rpi-id` |
| macOS personal | Homebrew, user launchd | Copy the plist example outside the repo, replace placeholders, then `launchctl bootstrap gui/$(id -u) /path/to/dev.alloy.fleet.plist`. |

On macOS, install Alloy with `brew install alloy`; the unix exporter inside
Alloy replaces a separate node_exporter. Hosts that support thermal telemetry
set `ALLOY_ENABLE_COLLECTORS=["thermal"]` in the plist and should confirm the
exporter exposes `node_thermal_cpu_speed_limit_ratio`,
`node_thermal_zone_temp`, and `node_load5`. Set the plist's absolute
configuration path and distinct
`MACHINE_ID` (for example `mac-b`, never an existing enrolled id by copy/paste), then
keep that plist under the user's launchd management. Copy `run-alloy.sh` beside
the private plist and use it as shown: it fails closed if identity or any
`PROM_REMOTE_WRITE_*` variable is missing.

## Process metrics

Per-process CPU and memory answer "which program is slowing this host" when
host-level node metrics (CPU %, disk, network) look unremarkable. Every host,
Linux and macOS, runs the same script, `proc-metrics.sh`, which writes
`proc-metrics.prom` into the textfile directory every 30 seconds. The unix
exporter serves it, so the metrics need nothing beyond `"textfile"` in
`ALLOY_ENABLE_COLLECTORS` and `FLEET_TEXTFILE_DIR` (see the table above). They
are grouped by process name in the `groupname` label:

| Metric | Meaning |
| --- | --- |
| `namedprocess_namegroup_cpu_seconds_total` | CPU time of the group |
| `namedprocess_namegroup_memory_bytes{memtype="resident"}` | Resident memory of the group |
| `namedprocess_namegroup_num_procs` | Processes in the group |

Typical queries:

```promql
topk(5, sum by (machine_id, groupname) (rate(namedprocess_namegroup_cpu_seconds_total[5m])))
topk(5, namedprocess_namegroup_memory_bytes{machine_id="HOST"})
```

`rate()` of the CPU counter is cores in use. Memory the groups do not account
for (kernel "wired" memory on macOS) stays in the node metrics.

### Why a script and not Alloy's process exporter

`prometheus.exporter.process` has no way to cap the number of groups and
Alloy's relabeling is stateless, so it cannot rank them either. Measured on the
fleet, hosts run 148 to 239 distinct process names; at 4 series per group that
is about 3,200 series for the Linux hosts alone, against a 10k limit for the
whole account. The script caps every host instead, and macOS (which has no
`/proc` for the exporter) needs it anyway. The exporter is not configured on
any host; a test enforces that.

### How it samples

One `ps -o pid=,ppid=,time=,rss=,comm=` per run. Processes are summed by
executable name (path stripped, 32 characters of `[A-Za-z0-9._ ()+@:-]`).
Linux kernel threads (pid 2 and its children) are skipped.

CPU is exact cumulative CPU time, not `ps %cpu` (a lifetime average on Linux and
a smoothed value on macOS). The previous run's per-process totals live in
`.proc-metrics.state`; the growth since then is the CPU used in the interval,
and the per-group sums accumulate into the counter. A process first seen since
the last run contributes all its CPU time; a reused pid starts over. CPU used
between a process's last sample and its exit is not seen, so very short-lived
processes (single `go build` steps, `sleep`) are undercounted; anything that
lives across a 30 second sample is counted. A gap over two minutes, such as a
sleeping laptop, only re-baselines. The textfile is replaced by temp file +
rename, and a failed run removes it rather than leaving stale gauges.

### Enable on a host

1. In the Alloy environment (EnvironmentFile on Linux, plist on macOS) set
   `ALLOY_ENABLE_COLLECTORS` to a list containing `"textfile"`, e.g.
   `["textfile"]`, and `FLEET_TEXTFILE_DIR` to an existing directory the job
   user can write and Alloy can read.
2. Schedule the script with the same `FLEET_TEXTFILE_DIR`:
   * macOS: copy `proc-metrics.plist.example` outside the repository, fill in
     the paths, and `launchctl bootstrap gui/$(id -u) /path/to/dev.panewire.proc-metrics.plist`.
   * Linux: install `proc-metrics.service.example` and
     `proc-metrics.timer.example` as `proc-metrics.service` / `.timer` in
     `/etc/systemd/system`, fill in the paths, and
     `systemctl enable --now proc-metrics.timer`. Needs procps `ps` (any
     distribution's default). The service user must see every process; with
     `/proc` mounted `hidepid=1|2` it sees only its own and the gauges
     undercount without any error.

### Series budget

The free Grafana Cloud tier allows 10k active series for the whole account.
Each run keeps the top 15 groups by CPU used in the interval (only groups that
used CPU) and the top 15 by resident memory as their own series, and sums
everything else into `groupname="other"` (a process literally named `other`, or
`other_` and so on, gets one more underscore). That is at most 30 named groups + `other` = 31 groups x 3
series = **93 series per host**, independent of how many programs run. Seven
hosts: 7 x ~90 = **~630 series, about 6% of the tier**. A group that leaves the
top set and returns restarts its counter from zero, which `rate()` handles as a
reset. Check the cap with:

```promql
count by (machine_id) (namedprocess_namegroup_num_procs) > 31
```

## Fleet events

panewire, panewire and mergewake record failures and decisions as **fleet-event
v1** lines. Each line is one JSON object with these string fields. All are always
present except `request_id`; a panewire `placement` line adds `chosen` and
`candidates`, and a successful `node_spawn` line adds `profile` and
`duration_ms`:

| Field | Meaning |
| --- | --- |
| `ts` | RFC 3339 UTC time; Alloy uses it as the entry timestamp |
| `host` | Short OS hostname of the writer. Alloy ships it as the `os_host` label; the `host` label is the fleet machine id (see below) |
| `tool` | `panewire`, `panewire` or `mergewake` (label) |
| `kind` | What happened: `spawn`, `plan`, `placement`, `placement_shadow`, `quota_observation`, `relay`, `spawn_held`, `job_result` (panewire); `node_spawn` (panewire); `reviewer`, `review` (mergewake) (label) |
| `job` | Job id, or `<repo>#<pr>` for reviews |
| `request_id` | Hub spawn request id, on panewire `node_spawn`, `placement` and hub `spawn` `stuck` lines only; omitted when empty. It joins the line to the hub's spawn operation and the `request` tag of its Sentry event |
| `chosen` | Panewire `placement` lines only: the host placement chose |
| `candidates` | Panewire `placement` lines only: an array with one object per candidate host: `host`, `score`, the terms `cpu`, `mem`, `io`, `load`, `jobs` (each 0..1), `free` (their product), `power`, `bridge` (claims inside the spawn bridge) and `outcome`. A host the hub reported nothing for has only `host` and `outcome`. At most 16 hosts. See panewire `docs/placement.md` |
| `selection` | Automatic panewire `placement` lines only, including refusals: hosts in admission rank order with `host`, `rank`, `outcome`, and `tried` rungs. Each rung has `profile`, `effort`, `pool`, and `outcome`. Outcomes are `admitted`, `quota_exhausted`, `quota_unknown`, `no_rung`, or `not_tried`. At most 16 hosts and 32 rungs per host. Account identities and gate output are omitted. See panewire `docs/placement.md` |
| `shadow` | Panewire `placement_shadow` lines only: `role`, `work_class`, `live`, `arms` (name to `outcome`, `first`, `order`, `shadow_error`), `inputs` per host, `load_measure`, and `waste_first` (hk 3651: `outcome`, the `live` and `waste_first` account choices, `left_unspent`, `unknown`; account keys, no hostnames beyond fleet ids). At most 16 hosts. See panewire `docs/placement.md` ("Shadow comparison") |
| `profile` | Panewire `node_spawn` lines of a successful spawn only: the launch profile |
| `duration_ms` | Panewire `node_spawn` lines of a successful spawn only: milliseconds the node took, as a number |
| `outcome` | `refused`, `failed`, `skipped`, `missing`, `placed`, `stale`, `unusable`, `ok`, `stuck` (a hub spawn request still pending 30 minutes past its deadline), `delivered`, `dropped:<reason>`, `unconfirmed:<reason>`, `attached`, `conflict` (label). The relay, held-reservation and job-result outcomes are described in panewire `docs/observability.md` |
| `reason` | Short code: exit status class, spawn.json rule, ranking metric, skip reason. On `placement` it is the decision rule (`policy_order`, `challenger`, `below_free_floor`, `all_loaded`) when the chosen host is the hub's pick, else the ranking metric. On `node_spawn` it can be a spawn-refusal code from panewire `internal/fleet/refusal.go`, such as `foreground_missing`, or `spawned` with outcome `ok` |
| `detail` | At most 1 KiB of `key=value` facts and a stderr tail |

Every tool writes to `<state home>/<tool>/events/fleet-events.jsonl`, where the
state home is `$XDG_STATE_HOME` or `~/.local/state`. mergewake writes under its
`state_dir`, which defaults to the same place. Each writer:

- redacts every field: tokens, `NAME=secret` assignments, PEM keys, URL
  credentials and email addresses;
- normalises `kind`, `outcome` and `reason` to `[a-z0-9_.:-]`;
- writes mode 0640 in a 0750 directory;
- rotates the file to `.1` at 4 MiB. Alloy never reads `.1`.

Writing an event never changes a command's result. Per-tool tables are in
panewire `docs/config.md` ("Fleet events"), mergewake `docs/config.md`
("Fleet events and crash reports"), and panewire `hub_spawn_node.go`.

Both Alloy configs carry the same processing pipeline between `// BEGIN fleet
events` and `// END fleet events`, with platform-specific file paths:

- On Linux, `local.file_match` names the `panewire` and `mergewake` event files
  separately. A wildcard across tool directories can fail as a whole if Alloy
  cannot traverse one sibling directory. A literal path that is absent yields no
  target and leaves the others intact. The explicit list is the whole Linux
  scope: a tool missing from it ships nothing from a Linux host. The macOS
  config still lists legacy `runtug`; a Linux host that still runs `runtug`
  (or any new fleet tool) needs its `__path__` added to this list on that host;
  until then its events are not shipped. `TestLinuxAlloyFleetEventsUseExplicitToolPaths`
  pins this list. On macOS it reads
  `$FLEET_EVENTS_ROOT/*/events/fleet-events.jsonl`; the root defaults to
  `$HOME/.local/state`, which is right for the user agent. On Linux, set
  `FLEET_EVENTS_ROOT` to the fleet user's state home, because the Alloy system
  user's `$HOME` is not that user's.
- `loki.process "fleet_events"` turns `host`, `tool`, `kind` and `outcome` into
  labels and sets the timestamp from `ts`. It drops the `filename` label and
  rate-limits each host to 0.5 lines/s with a burst of 200.
- The `host` label is the host's `MACHINE_ID` (`mac-dev`, `oci`, `ncp`, ...),
  the fleet machine id that hk, Sentry, placement and panewire use, so fleet
  events join those sources on `host`. The line's own `host` field, the OS
  hostname, becomes the `os_host` label. If `MACHINE_ID` is unset or empty,
  `host` stays the OS hostname, as before, and no event is dropped. Each host
  has one `os_host`, so the extra label adds no streams.
- `job`, `request_id`, `reason` and `detail` stay in the line. Query them with
  `| json` or `| regexp`.

Alloy resumes from its positions file, and there is no `tail_from_end`. A
line written while Alloy is down or restarting still ships, and so does the
start of the fresh file after a rotation.

`TestAlloyFleetEventsPipelineShipsLabelledLines` runs this block in the real
`alloy` binary, with `loki.echo` in place of the cloud writer. It checks:

- the labels and the timestamp;
- that a line written before the first start ships;
- that a line written while Alloy was stopped ships after a restart, exactly
  once;
- that `.1` is never shipped;
- that `host` is the OS hostname without `MACHINE_ID` and the machine id with
  it, while `os_host` keeps the OS hostname.

### Free-tier volume estimate

| Quantity | Typical | Worst case |
| --- | --- | --- |
| Events | about 2,000/day fleet-wide (see below) | 0.5 lines/s/host (Alloy limit) = 43,200/day/host |
| Bytes per line | 300–600 | about 1.4 KB (1 KiB detail, JSON and labels) |
| Loki ingest | about 1.2 MB/day, **under 40 MB/month** | about 60 MB/day/host, **about 1.8 GB/month/host**; 8 hosts about 14.5 GB/month |
| Share of the Grafana Cloud free logs tier (50 GB/month) | under 0.1 % | about 29 % with 8 hosts all saturated |
| Active streams | about 20 label combinations × hosts (about 160 at 8 hosts) | the same: labels are closed sets |
| Local disk | under 1 MB | 8 MiB per tool per host (live file plus `.1`) |
| Sentry crash reports | 0 | at most 3/day/host (budget kept across restarts) = 90/month/host; 8 hosts = 720/month, under the 5,000/month free tier |

The typical figure is based on measurements taken on 2026-11-01. panewire,
runtug and mergewake opened 116 PRs in 14 days, about 8 PRs/day, and this host (Historical measurement: runtug was replaced by panewire.)
started 39 jobs in 7 days. That gives fewer than 100 spawns/day per host and a
few hundred review events/day. Multiplying by 10 for fleet growth gives the
2,000/day figure. The free-tier numbers are the published Grafana Cloud Free
(50 GB logs, 14-day retention) and Sentry Developer (5k errors) quotas. Re-check
them before relying on the headroom.

### Rollout (desk)

The tools start writing events as soon as their new binaries are deployed. No
configuration is needed for that. Shipping, crash reports and Grafana need the
following steps, in this order:

1. **macOS hosts:** install the updated `config.mac.alloy` at the plist's
   config path and restart the agent:
   `launchctl kickstart -k gui/$(id -u)/<label>`. The default root
   `$HOME/.local/state` applies.
2. **Linux hosts that run a fleet tool:**
   1. Create the event directories as the fleet user. These snippets are
      POSIX `sh`, so they run under dash too:

      ```sh
      for t in panewire mergewake; do
        mkdir -p -m 0750 "$HOME/.local/state/$t/events"
      done
      ```
   2. Let the alloy user traverse to them and read them. Run this as root,
      with `U` set to the fleet user's home:

      On the RPi, install the `acl` package first so `setfacl` is available:
      `sudo apt-get install acl`.

      ```sh
      setfacl -m u:alloy:x "$U" "$U/.local" "$U/.local/state"
      for t in panewire mergewake; do
        setfacl -m u:alloy:x "$U/.local/state/$t"
        setfacl -m u:alloy:rx "$U/.local/state/$t/events"
        setfacl -d -m u:alloy:r "$U/.local/state/$t/events"
      done
      ```

      The default ACL covers files created later, including the file created
      after rotation. The writers set the event file to 0640 and its
      directory to 0750 whatever the service umask is (panewire as of #140),
      so the ACL mask allows reads.
   3. Rerun the installer with
      `FLEET_EVENTS_ROOT=$U/.local/state` in its environment. It writes the
      variable into the EnvironmentFile. Alternatively, add the line to that
      file and restart alloy.
3. **Check the shipping.** Run `panewire plan` with no flags; it refuses with a
   usage error and writes a `plan`/`refused` event. Then open Grafana Explore
   with `{source="fleet_events"}` and confirm the line arrives with the `host`
   (the machine id), `os_host`, `tool`, `kind` and `outcome` labels.
4. **Turn on Sentry:**
   - Add `SENTRY_DSN_PANEWIRE` to the environment of the `panewire daemon` and
     `panewire hub` services, in the launchd plist `EnvironmentVariables` or
     the systemd EnvironmentFile.
   - Add `SENTRY_DSN_MERGEWAKE` to the mergewake daemon's environment.
   - Restart the services.

   Without these variables, crash reporting is off.
5. **Apply the dashboard and alerts** from `deploy/grafana/` (see its README):
   1. Run `./apply.sh --dry-run` and review the output.
   2. Run it again with `GRAFANA_CLOUD_URL` and `GRAFANA_CLOUD_SA_TOKEN`
      set. The script looks up the Loki data source uid itself.
   3. Give the stack's default notification policy a contact point. The rules
      carry `team=fleet` for routing.

### Deploying the machine-id `host` label (desk)

Before this change the `host` label on fleet events was the OS hostname
(`robinui-macbookpro`, `mbp-server`, ...). After deploy it is the machine id.
**Dashboards, alerts and saved queries that match `host=` against an OS
hostname must be updated after deploy:** match the machine id instead, or
switch them to `os_host=`. The repository's dashboard and alerts already use
`host` only for grouping. For up to the longest rule window (2 h) after a
host's restart, its counts split across the old and the new `host` value.

`MACHINE_ID` is already set on every enrolled host (the Linux installer and
`run-alloy.sh` refuse to run without it), so only the configuration changes.
Deploy it to every host that runs Alloy:

1. **macOS hosts:** install the updated `config.mac.alloy` at the plist's
   config path and restart the agent:
   `launchctl kickstart -k gui/$(id -u)/<label>`.
2. **Linux hosts** (NCP, OCI and RPi in the Install table): stage
   `config.linux.alloy`, `install-linux.sh` and a mode-0600 `loki.env` as in
   Install, then rerun the installer with the host's existing machine id, for
   example `sudo ALLOY_LOKI_ENV=/secure/loki.env ./install-linux.sh oci-id`.
   The installer rewrites the EnvironmentFile from its environment, so pass
   the `PROM_REMOTE_WRITE_*` variables and any `FLEET_EVENTS_ROOT`,
   `FLEET_TEXTFILE_DIR` and `ALLOY_*_COLLECTORS` values that the host already
   has. A variable you leave out is removed from the file.
3. **Check:** in Grafana Explore, `{source="fleet_events"}` shows `host` equal
   to the host's `machine_id` label and `os_host` equal to its OS hostname.

## Label contract

| Label | Meaning | Populated by |
| --- | --- | --- |
| `machine_id` | Stable host identity | Required `MACHINE_ID` environment variable on every host |
| `env` | Deployment environment | config external label (`prod`) |
| `source` | Input type | `journal`, `docker`, `fleet_events`, or each macOS file source |
| `unit` | systemd unit | journal relabeling |
| `app` | syslog identifier | journal relabeling |
| `level` | journal priority keyword | journal relabeling |
| `container` | Docker container name | Docker relabeling |
| `file` | Basename of a macOS tailed file | file relabeling |
| `host` | Fleet machine id of the host that shipped a fleet event, the same value as `machine_id`; the OS hostname when `MACHINE_ID` is unset | `loki.process "fleet_events"` from `MACHINE_ID` |
| `os_host` | OS hostname from the fleet event's `host` field | `loki.process "fleet_events"` from the JSON line |
| `tool`, `kind`, `outcome` | Fleet event fields (see Fleet events) | `loki.process "fleet_events"` from the JSON line |

Labels not applicable to a source are absent. Queries should therefore scope by
`source` before expecting `unit`, `container`, or `file`.

## Operational pitfalls

* River `rule {}` blocks must not put multiple assignments on one comma-separated
  line. Use one assignment per line as in these configurations.
* RPM Alloy units read `/etc/sysconfig/alloy`; it must contain both
  `CONFIG_FILE=/etc/alloy/config.alloy` and the credentials/identity/Prometheus
  variables. The installer fails before writing if `MACHINE_ID` or any
  `PROM_REMOTE_WRITE_*` variable is absent.
  The installer writes this. Debian-family systems use `/etc/default/alloy`.
* On a host without `/var/run/docker.sock`, remove the configuration from
  `// BEGIN docker containers` through `// END docker containers` (inclusive).
  The Linux installer performs exactly this removal before starting Alloy; do not
  delete any journal or file source blocks.

## Validation

Run `sh -n install-linux.sh run-alloy.sh proc-metrics.sh ../grafana/apply.sh`, `gitleaks git --no-banner`, and `go test ./...`.
When the `alloy` binary is available, the Go tests do three things: run
`alloy fmt` and `alloy validate` on both configurations, check the shared
label/pipeline contract on both fleet events blocks, and run the Linux block end to end.
Without the binary, they report an explicit skip. The Grafana files are
covered by `grafana_provisioning_test.go`.

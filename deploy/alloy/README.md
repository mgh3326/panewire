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

runtug, panewire and mergewake record failures and decisions as **fleet-event
v1** lines. Each line is one JSON object with exactly these string fields:

| Field | Meaning |
| --- | --- |
| `ts` | RFC 3339 UTC time; Alloy uses it as the entry timestamp |
| `host` | Short hostname of the writer (label) |
| `tool` | `runtug`, `panewire` or `mergewake` (label) |
| `kind` | What happened: `spawn`, `plan`, `placement`, `quota_observation` (runtug); `node_spawn` (panewire); `reviewer`, `review` (mergewake) (label) |
| `job` | Job id, or `<repo>#<pr>` for reviews |
| `outcome` | `refused`, `failed`, `skipped`, `missing`, `placed`, `stale`, `unusable` (label) |
| `reason` | Short code: exit status class, spawn.json rule, ranking metric, skip reason |
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
runtug `docs/config.md` ("Fleet events"), mergewake `docs/config.md`
("Fleet events and crash reports"), and panewire `hub_spawn_node.go`.

Both Alloy configs carry the same pipeline between `// BEGIN fleet events` and
`// END fleet events`:

- `local.file_match` reads `$FLEET_EVENTS_ROOT/*/events/fleet-events.jsonl`.
  The root defaults to `$HOME/.local/state`, which is right for the macOS user
  agent. On Linux, set `FLEET_EVENTS_ROOT` to the fleet user's state home,
  because the alloy system user's `$HOME` is not that user's.
- `loki.process "fleet_events"` turns `host`, `tool`, `kind` and `outcome` into
  labels and sets the timestamp from `ts`. It drops the `filename` label and
  rate-limits each host to 0.5 lines/s with a burst of 200.
- `job`, `reason` and `detail` stay in the line. Query them with `| json` or
  `| regexp`.

Alloy resumes from its positions file, and there is no `tail_from_end`. A
line written while Alloy is down or restarting still ships, and so does the
start of the fresh file after a rotation.

`TestAlloyFleetEventsPipelineShipsLabelledLines` runs this block in the real
`alloy` binary, with `loki.echo` in place of the cloud writer. It checks:

- the labels and the timestamp;
- that a line written before the first start ships;
- that a line written while Alloy was stopped ships after a restart, exactly
  once;
- that `.1` is never shipped.

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
runtug and mergewake opened 116 PRs in 14 days, about 8 PRs/day, and this host
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
      for t in runtug panewire mergewake; do
        mkdir -p -m 0750 "$HOME/.local/state/$t/events"
      done
      ```
   2. Let the alloy user traverse to them and read them. Run this as root,
      with `U` set to the fleet user's home:

      ```sh
      setfacl -m u:alloy:x "$U" "$U/.local" "$U/.local/state"
      for t in runtug panewire mergewake; do
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
3. **Check the shipping.** Run `runtug plan` with no flags; it refuses with a
   usage error and writes a `plan`/`refused` event. Then open Grafana Explore
   with `{source="fleet_events"}` and confirm the line arrives with the `host`,
   `tool`, `kind` and `outcome` labels.
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
| `host`, `tool`, `kind`, `outcome` | Fleet event fields (see Fleet events) | `loki.process "fleet_events"` from the JSON line |

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
`alloy fmt` and `alloy validate` on both configurations, check that the
fleet events block is identical in both, and run that block end to end.
Without the binary, they report an explicit skip. The Grafana files are
covered by `grafana_provisioning_test.go`.

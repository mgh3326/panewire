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

## Label contract

| Label | Meaning | Populated by |
| --- | --- | --- |
| `machine_id` | Stable host identity | Required `MACHINE_ID` environment variable on every host |
| `env` | Deployment environment | config external label (`prod`) |
| `source` | Input type | `journal`, `docker`, or each macOS file source |
| `unit` | systemd unit | journal relabeling |
| `app` | syslog identifier | journal relabeling |
| `level` | journal priority keyword | journal relabeling |
| `container` | Docker container name | Docker relabeling |
| `file` | Basename of a macOS tailed file | file relabeling |

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

Run `sh -n install-linux.sh run-alloy.sh proc-metrics.sh`, `gitleaks git --no-banner`, and `go test ./...`.
The Go tests run `alloy fmt` and `alloy validate` on both configurations when the `alloy` binary
is available; otherwise it reports an explicit skip.

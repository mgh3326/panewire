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
host-level node metrics (CPU %, disk, network) look unremarkable. Every host
sends the same three families, grouped by process name in the `groupname`
label:

| Metric | Meaning |
| --- | --- |
| `namedprocess_namegroup_cpu_seconds_total` | CPU time of the group (Linux adds a `mode` label: `user`, `system`) |
| `namedprocess_namegroup_memory_bytes{memtype="resident"}` | Resident memory of the group |
| `namedprocess_namegroup_num_procs` | Processes in the group |

Typical queries (they work on both platforms):

```promql
topk(5, sum by (machine_id, groupname) (rate(namedprocess_namegroup_cpu_seconds_total[5m])))
topk(5, namedprocess_namegroup_memory_bytes{machine_id="HOST"})
```

`rate()` of the CPU counter is cores in use. Memory the groups do not account
for (kernel "wired" memory on macOS) stays in the node metrics.

### Linux

`config.linux.alloy` runs `prometheus.exporter.process` with one group per
process name (`{{.Comm}}`, kernel threads excluded by `cmdline = [".+"]`) and
forwards it through an **allowlist** `prometheus.relabel`: only the three
families above survive, and only the `resident` memory type. Every other
process metric (virtual/swapped memory, threads, file descriptors, page
faults, I/O, context switches, states, ...) is dropped before remote write, as
is the scrape bookkeeping except `up`. No host action is needed beyond
redeploying the config.

### macOS

The process exporter reads `/proc`, which darwin lacks, so
`proc-metrics.sh` builds the same families from one `ps -axo pcpu=,rss=,comm=`
sample and writes `proc-metrics.prom` into the textfile directory every 30
seconds (temp file + rename, so the collector never reads a partial file).
`ps` only gives a CPU rate, so the CPU counter is that rate integrated over
the time since the previous run (kept in `.proc-metrics.state`); gaps over two
minutes, such as a sleeping laptop, are not integrated. Treat it as an
estimate, good for ranking programs, not for billing CPU seconds.

To enable on a Mac:

1. In the Alloy plist, set `ALLOY_ENABLE_COLLECTORS` to a list containing
   `"textfile"` (e.g. `["textfile"]`) and `FLEET_TEXTFILE_DIR` to an existing
   directory.
2. Copy `proc-metrics.plist.example` outside the repository, point it at
   `proc-metrics.sh` and the same `FLEET_TEXTFILE_DIR`, and
   `launchctl bootstrap gui/$(id -u) /path/to/dev.panewire.proc-metrics.plist`.

Do not enable the script on a Linux host: its families would collide with the
exporter's.

### Series budget

The free Grafana Cloud tier allows 10k active series for the whole account.

* **macOS** caps itself per run. Groups are summed by name, the top 15 by CPU
  (only groups actually using CPU) and the top 15 by resident memory keep their
  own series, and everything else is summed into `groupname="other"`. That is at
  most 30 named groups + `other` = 31 groups x 3 series = **93 series per host**
  (a process literally named `other` is renamed `other_`). Group names are
  limited to 32 characters of `[A-Za-z0-9._ ()+@:-]`.
* **Linux** is not capped to a top-N. Alloy relabeling is stateless, so it
  cannot rank groups, and `prometheus.exporter.process` has no limit option.
  The alternative the design allowed, a `ps`-based textfile script on Linux, was
  rejected: procps reports `%cpu` averaged over the process lifetime, which
  ranks a long-running process that just went busy as idle, whereas the
  exporter reads exact kernel counters. Linux costs 4 series per group (CPU
  `user` and `system`, resident memory, process count). The fleet's Linux hosts
  are headless, so expect tens of groups, not hundreds: 30 groups = 120 series,
  50 = 200.

Fleet estimate for 7 hosts at the 30-group target: 7 x ~90 = **~630 series**
(about 6% of the free tier; ~870 even if every host were Linux with 30 groups). Watch
the group count per host and revisit the Linux decision if it grows:

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
The Go test invokes `alloy fmt` for both configurations when the `alloy` binary
is available; otherwise it reports an explicit skip.

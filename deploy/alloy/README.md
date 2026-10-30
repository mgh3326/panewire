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

Run `sh -n install-linux.sh run-alloy.sh`, `gitleaks git --no-banner`, and `go test ./...`.
The Go test invokes `alloy fmt` for both configurations when the `alloy` binary
is available; otherwise it reports an explicit skip.

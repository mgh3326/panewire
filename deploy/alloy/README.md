# Fleet Alloy log shipper

This directory records the Alloy configurations already running on the NCP, OCI,
RPi, and personal macOS hosts as of 2026-09-02. It sends logs outward to Loki
only. Do not place a Loki URL, username, token, or a real launchd plist here.

## Credentials

Linux reads a repository-external `loki.env` containing `LOKI_URL`, `LOKI_USER`,
and `LOKI_TOKEN`. The installer writes those values into the platform's Alloy
systemd EnvironmentFile. macOS receives the same three variables through the
user launchd plist; start from `mac.plist.example` and keep the resulting plist
outside this repository.

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

On macOS, install Alloy with `brew install alloy`, set the plist's absolute
configuration path, and keep that plist under the user's launchd management.

## Label contract

| Label | Meaning | Populated by |
| --- | --- | --- |
| `machine_id` | Stable host identity | Linux `MACHINE_ID`; macOS's live static value |
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
  `CONFIG_FILE=/etc/alloy/config.alloy` and the credentials/identity variables.
  The installer writes this. Debian-family systems use `/etc/default/alloy`.
* On a host without `/var/run/docker.sock`, remove the configuration from
  `// BEGIN docker containers` through `// END docker containers` (inclusive).
  The Linux installer performs exactly this removal before starting Alloy; do not
  delete any journal or file source blocks.

## Validation

Run `sh -n install-linux.sh`, `gitleaks git --no-banner`, and `go test ./...`.
The Go test invokes `alloy fmt` for both configurations when the `alloy` binary
is available; otherwise it reports an explicit skip.

#!/usr/bin/env bash
# Install Grafana Alloy (official repo) and wire the already-running Linux fleet config.
# Captured 2026-09-02; preserve the live runtime behavior. Run as root.
# Credentials are repository-external: pass ALLOY_LOKI_ENV=/secure/loki.env (or optional $2).
# The legacy default /tmp/alloy/loki.env remains for live-host compatibility.
set -euo pipefail
MACHINE_ID="$1"
LOKI_ENV="${ALLOY_LOKI_ENV:-${2:-/tmp/alloy/loki.env}}"
[ -n "$MACHINE_ID" ]
[ -r "$LOKI_ENV" ]
if command -v apt-get >/dev/null; then
  mkdir -p /etc/apt/keyrings; wget -q -O - https://apt.grafana.com/gpg.key | gpg --dearmor > /etc/apt/keyrings/grafana.gpg 2>/dev/null || true
  echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" > /etc/apt/sources.list.d/grafana.list
  apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq alloy >/dev/null
else
  cat > /etc/yum.repos.d/grafana.repo <<'REPO'
[grafana]
name=grafana
baseurl=https://rpm.grafana.com
repo_gpgcheck=1
enabled=1
gpgcheck=1
gpgkey=https://rpm.grafana.com/gpg.key
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
REPO
  dnf install -y -q alloy >/dev/null
fi
install -m 644 /tmp/alloy/config.linux.alloy /etc/alloy/config.alloy
# Hosts without Docker omit only the explicitly bounded Docker components.
[ -S /var/run/docker.sock ] || sed -i '/^\/\/ BEGIN docker containers$/,/^\/\/ END docker containers$/d' /etc/alloy/config.alloy
# credentials + identity in the unit's EnvironmentFile (root-owned, alloy-readable via systemd)
ENVF=/etc/default/alloy; [ -d /etc/sysconfig ] && ENVF=/etc/sysconfig/alloy
{ echo "CUSTOM_ARGS="; echo "CONFIG_FILE=/etc/alloy/config.alloy"; echo "MACHINE_ID=$MACHINE_ID"; cat "$LOKI_ENV"; } > "$ENVF"
chmod 600 "$ENVF"
usermod -aG systemd-journal alloy 2>/dev/null || true
getent group docker >/dev/null && usermod -aG docker alloy 2>/dev/null || true
systemctl daemon-reload; systemctl enable --now alloy >/dev/null 2>&1 || true; systemctl restart alloy
sleep 4; systemctl is-active alloy; journalctl -u alloy -n 3 --no-pager -o cat | grep -iE "error|level=warn" | head -3 || true
rm -f "$LOKI_ENV"

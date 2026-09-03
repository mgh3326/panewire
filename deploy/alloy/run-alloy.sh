#!/usr/bin/env sh
# Keep the identity contract fail-closed on launchd as well as systemd.
set -eu
[ -n "${MACHINE_ID:-}" ] || { echo "MACHINE_ID is required" >&2; exit 2; }
[ -n "${PROM_REMOTE_WRITE_URL:-}" ] || { echo "PROM_REMOTE_WRITE_URL is required" >&2; exit 2; }
[ "$#" -eq 2 ] || { echo "usage: run-alloy.sh ALLOY CONFIG" >&2; exit 2; }
exec "$1" run "$2"

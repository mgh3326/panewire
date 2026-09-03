#!/bin/sh
# Example only. Orch supplies real endpoint, operator env, and <target>.
set -eu

case "${1:-}" in
pre)
  if lease_json="$(panewire burst request --target '<target>' --hold 30m --reason pg-backup-secondary \
    --hub-url "$PANEWIRE_HUB_URL" --hub-token-env "$PANEWIRE_OPERATOR_ENV")"; then
    PG_BACKUP_REMOTE_SECONDARY='<target>'
    export PG_BACKUP_REMOTE_SECONDARY
    printf '%s\n' "$lease_json" > "${PANEWIRE_BURST_LEASE_FILE:-/tmp/panewire-pg-backup-lease.json}"
  else
    unset PG_BACKUP_REMOTE_SECONDARY
  fi
  ;;
post)
  lease_file="${PANEWIRE_BURST_LEASE_FILE:-/tmp/panewire-pg-backup-lease.json}"
  if [ -r "$lease_file" ]; then
    lease_id="$(sed -n 's/.*"id":"\([^"]*\)".*/\1/p' "$lease_file")"
    [ -z "$lease_id" ] || panewire burst release --lease-id "$lease_id" --hub-url "$PANEWIRE_HUB_URL" --hub-token-env "$PANEWIRE_OPERATOR_ENV" || true
  fi
  ;;
*) echo "usage: $0 pre|post" >&2; exit 2 ;;
esac

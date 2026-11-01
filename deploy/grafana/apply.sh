#!/usr/bin/env sh
# Apply the fleet observability dashboard and alert rules to Grafana Cloud.
# The desk runs this by hand after merge; nothing provisions it automatically.
#
#   GRAFANA_URL=https://<stack>.grafana.net GRAFANA_TOKEN=<service-account token> \
#   LOKI_DATASOURCE_UID=<uid> ./apply.sh
#   ./apply.sh --dry-run        # prints every request; needs no credentials
#
# GRAFANA_TOKEN is a Grafana service-account token with dashboard, folder and
# alert-rule write scope (Editor). LOKI_DATASOURCE_UID is the uid of the
# stack's Loki data source (Connections > Data sources > the Loki source; the
# uid is the last path segment of its URL). Every request is idempotent: the
# folder create tolerates "exists", the dashboard and rule group overwrite by uid.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
dry=0
case "${1:-}" in
  --dry-run) dry=1 ;;
  "") ;;
  *) echo "usage: apply.sh [--dry-run]" >&2; exit 2 ;;
esac

folder_uid=fleet-observability
group=fleet-observability
loki_uid=${LOKI_DATASOURCE_UID:-}
if [ "$dry" -eq 1 ]; then
  url=${GRAFANA_URL:-https://grafana.invalid}
  [ -n "$loki_uid" ] || loki_uid='<LOKI_DATASOURCE_UID>'
else
  [ -n "${GRAFANA_URL:-}" ] || { echo "GRAFANA_URL is required" >&2; exit 2; }
  [ -n "${GRAFANA_TOKEN:-}" ] || { echo "GRAFANA_TOKEN is required" >&2; exit 2; }
  [ -n "$loki_uid" ] || { echo "LOKI_DATASOURCE_UID is required" >&2; exit 2; }
  url=$GRAFANA_URL
fi
url=${url%/}
case "$loki_uid" in
  *[!A-Za-z0-9_\<\>-]*) echo "LOKI_DATASOURCE_UID has unexpected characters" >&2; exit 2 ;;
esac

folder_body='{"uid":"'"$folder_uid"'","title":"Fleet observability"}'
dashboard_body=$(printf '{"folderUid":"%s","overwrite":true,"message":"deploy/grafana/apply.sh","dashboard":' "$folder_uid"; cat "$here/fleet-dashboard.json"; printf '}')
rules_body=$(sed "s/\${LOKI_DATASOURCE_UID}/$loki_uid/g" "$here/fleet-alert-rules.json")

# The token goes to curl through a mode-0600 header file, never argv (ps).
if [ "$dry" -eq 0 ]; then
  headers=$(umask 077 && mktemp)
  trap 'rm -f "$headers"' EXIT INT TERM
  printf 'Authorization: Bearer %s\nContent-Type: application/json\nX-Disable-Provenance: true\n' "$GRAFANA_TOKEN" > "$headers"
fi

# send METHOD PATH BODY ["STATUS ..." tolerated as already present]
send() {
  if [ "$dry" -eq 1 ]; then
    printf '### %s %s%s\n%s\n' "$1" "$url" "$2" "$3"
    return 0
  fi
  status=$(printf '%s' "$3" | curl -sS -o /dev/stderr -w '%{http_code}' -X "$1" \
    -H "@$headers" --data-binary @- "$url$2")
  case "$status" in
    2??) echo "$1 $2 -> $status" ;;
    *) case " ${4:-} " in
         *" $status "*) echo "$1 $2 -> $status (already present)" ;;
         *) echo "$1 $2 -> $status" >&2; exit 1 ;;
       esac ;;
  esac
}

send POST /api/folders "$folder_body" "409 412"
send POST /api/dashboards/db "$dashboard_body"
send PUT "/api/v1/provisioning/folder/$folder_uid/rule-groups/$group" "$rules_body"

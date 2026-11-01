#!/usr/bin/env sh
# Apply the fleet observability dashboard and alert rules to Grafana Cloud.
# The desk runs this by hand after merge; nothing provisions it automatically.
#
#   GRAFANA_CLOUD_URL=https://<stack>.grafana.net GRAFANA_CLOUD_SA_TOKEN=<token> ./apply.sh
#   ./apply.sh --dry-run        # prints every request; reads no credentials
#
# GRAFANA_CLOUD_SA_TOKEN is a Grafana service-account token; writing the
# folder, dashboard and alert rules needs the Editor role. The Loki data
# source uid is looked up with GET /api/datasources (type loki). Every write
# is idempotent: the folder create tolerates "exists", the dashboard and the
# rule group overwrite by uid. Needs curl and python3.
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
if [ "$dry" -eq 1 ]; then
  url=https://grafana.invalid
else
  [ -n "${GRAFANA_CLOUD_URL:-}" ] || { echo "GRAFANA_CLOUD_URL is required" >&2; exit 2; }
  [ -n "${GRAFANA_CLOUD_SA_TOKEN:-}" ] || { echo "GRAFANA_CLOUD_SA_TOKEN is required" >&2; exit 2; }
  command -v curl >/dev/null || { echo "curl is required" >&2; exit 2; }
  command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 2; }
  url=${GRAFANA_CLOUD_URL%/}
  # The token goes to curl through a mode-0600 header file, never argv (ps).
  headers=$(umask 077 && mktemp)
  trap 'rm -f "$headers"' EXIT INT TERM
  printf 'Authorization: Bearer %s\nContent-Type: application/json\nX-Disable-Provenance: true\n' "$GRAFANA_CLOUD_SA_TOKEN" > "$headers"
fi

# loki_uid prints the uid of the stack's Loki data source: the only one, or
# Grafana Cloud's own grafanacloud-*-logs when there are several.
loki_uid() {
  if [ "$dry" -eq 1 ]; then
    printf '### GET %s/api/datasources\n' "$url" >&2
    echo '<loki-uid from GET /api/datasources>'
    return 0
  fi
  curl -sS -f -H "@$headers" "$url/api/datasources" | python3 -c '
import json, sys
loki = [d for d in json.load(sys.stdin) if d.get("type") == "loki"]
if len(loki) > 1:
    loki = [d for d in loki if d.get("name", "").startswith("grafanacloud-") and d.get("name", "").endswith("-logs")] or loki
if len(loki) != 1:
    sys.exit("expected one Loki data source, found: %s" % ", ".join(d.get("name", "?") for d in loki) or "none")
print(loki[0]["uid"])
'
}

uid=$(loki_uid)
case "$uid" in
  ""|*[!A-Za-z0-9_\<\>\ /.-]*) echo "unexpected Loki data source uid" >&2; exit 1 ;;
esac

folder_body='{"uid":"'"$folder_uid"'","title":"Fleet observability"}'
dashboard_body=$(printf '{"folderUid":"%s","overwrite":true,"message":"deploy/grafana/apply.sh","dashboard":' "$folder_uid"; cat "$here/fleet-dashboard.json"; printf '}')
rules_body=$(sed "s|\${LOKI_DATASOURCE_UID}|$uid|g" "$here/fleet-alert-rules.json")

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
         *" $status "*) echo "$1 $2 -> $status (already present)"; return 0 ;;
       esac
       case "$status" in
         401|403) echo "$1 $2 -> $status: the service account lacks permission (needs Editor)" >&2 ;;
         *) echo "$1 $2 -> $status" >&2 ;;
       esac
       exit 1 ;;
  esac
}

send POST /api/folders "$folder_body" "409 412"
send POST /api/dashboards/db "$dashboard_body"
send PUT "/api/v1/provisioning/folder/$folder_uid/rule-groups/$group" "$rules_body"

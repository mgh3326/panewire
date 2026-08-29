#!/bin/sh
# Local-only AC1/AC2 verifier.  It never contacts Supabase and removes its one
# permitted Docker container on exit.  Its container name intentionally has the
# required pw-r2-pg-<random> form.
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
container_name=""
local_root=""
local_socket=""
mode=""

cleanup() {
  if [ "$mode" = docker ]; then
    docker rm -f "$container_name" >/dev/null 2>&1 || true
  elif [ "$mode" = local ] && [ -n "$local_root" ]; then
    pg_ctl -D "$local_root/data" -m immediate stop >/dev/null 2>&1 || true
    rm -rf "$local_root"
  fi
}
trap cleanup EXIT INT TERM

if command -v docker >/dev/null 2>&1; then
  mode=docker
  container_name="pw-r2-pg-$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"
  docker run -d --rm --name "$container_name" \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -e POSTGRES_DB=panewire_r2 \
    postgres:16-alpine >/dev/null
  ready=0
  attempt=0
  while [ "$attempt" -lt 30 ]; do
    if docker exec "$container_name" pg_isready -U postgres -d panewire_r2 >/dev/null 2>&1; then
      ready=1
      break
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
  [ "$ready" -eq 1 ] || { echo "local PostgreSQL did not become ready" >&2; exit 1; }
  run_psql() { docker exec -i "$container_name" psql -U postgres -d panewire_r2 -v ON_ERROR_STOP=1 "$@"; }
else
  command -v initdb >/dev/null 2>&1 && command -v pg_ctl >/dev/null 2>&1 && command -v psql >/dev/null 2>&1 || {
    echo "Docker and local PostgreSQL tools are both unavailable" >&2
    exit 1
  }
  mode=local
  local_root=$(mktemp -d "${TMPDIR:-/tmp}/pw-r2-pg.XXXXXX")
  local_socket="$local_root/socket"
  mkdir "$local_socket"
  initdb -D "$local_root/data" -A trust -U postgres --no-locale >/dev/null
  pg_ctl -D "$local_root/data" -o "-k $local_socket -p 55432" -w start >/dev/null
  run_psql() { psql -h "$local_socket" -p 55432 -U postgres -d postgres -v ON_ERROR_STOP=1 "$@"; }
fi

run_psql < "$repo_root/provision/local-auth-stub.sql"
run_psql < "$repo_root/provision/schema.sql"
run_psql < "$repo_root/provision/schema.sql"
run_psql < "$repo_root/provision/local-contract-check.sql"

run_psql <<'SQL'
SELECT n.nspname, c.relname
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'panewire' AND c.relkind = 'r'
 ORDER BY c.relname;
SELECT schemaname, tablename, policyname, cmd
  FROM pg_policies WHERE schemaname = 'panewire'
 ORDER BY tablename, policyname;
SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid)
  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
 WHERE n.nspname = 'panewire' AND p.proname LIKE 'panewire_%'
 ORDER BY p.proname;
SQL

run_psql < "$repo_root/provision/teardown.sql"
run_psql < "$repo_root/provision/schema.sql"

run_psql <<'SQL'
SELECT n.nspname, c.relname
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'panewire' AND c.relkind = 'r'
 ORDER BY c.relname;
SELECT schemaname, tablename, policyname, cmd
  FROM pg_policies WHERE schemaname = 'panewire'
 ORDER BY tablename, policyname;
SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid)
  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
 WHERE n.nspname = 'panewire' AND p.proname LIKE 'panewire_%'
 ORDER BY p.proname;
SQL

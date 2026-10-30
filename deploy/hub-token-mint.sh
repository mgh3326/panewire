#!/usr/bin/env bash
# Mint one hub client credential. Run by the operator on the hub VM.
#
#   deploy/hub-token-mint.sh <client-id> <client-env-out> [<scope>[,<scope>...]]
#
# Generates a 256-bit random token, writes the client's HUB_MACHINE_ID/HUB_TOKEN
# env file at <client-env-out> (created mode 0600, never overwritten), and prints
# the hub auth-file lines to stdout for the operator to add to the hub's
# --hub-auth file. With scopes the client is a service; without, a node.
# The hub hot-reloads its auth file, so adding the lines enables the client and
# deleting them revokes it. Contract: docs/hub-scopes.md.
set -euo pipefail

usage() {
  echo "usage: $0 <client-id> <client-env-out> [<scope>[,<scope>...]]" >&2
  exit 2
}

[ "$#" -ge 2 ] && [ "$#" -le 3 ] || usage
client="$1"
out="$2"
scopes="${3:-}"

# Same alphabet as the hub's machine ids.
if ! printf '%s' "$client" | grep -Eq '^[a-z0-9][a-z0-9._-]{0,62}$'; then
  echo "client id must match ^[a-z0-9][a-z0-9._-]{0,62}\$" >&2
  exit 2
fi
if [ "$client" = "operator" ]; then
  echo "the operator credential is not minted by this script" >&2
  exit 2
fi
# The hub validates each scope on load; this only keeps the line one token.
if [ -n "$scopes" ] && ! printf '%s' "$scopes" | grep -Eq '^[A-Za-z0-9._:@*,-]+$'; then
  echo "scopes must be a comma-separated list without spaces" >&2
  exit 2
fi
if [ -e "$out" ] || [ -L "$out" ]; then
  echo "refusing to overwrite $out" >&2
  exit 1
fi

token="$(od -An -tx1 -N32 /dev/urandom | tr -d ' \n')"
if [ "${#token}" -ne 64 ]; then
  echo "could not read 32 random bytes" >&2
  exit 1
fi

umask 077
set -o noclobber
printf 'HUB_MACHINE_ID=%s\nHUB_TOKEN=%s\n' "$client" "$token" > "$out"
chmod 0600 "$out"

printf 'HUB_TOKEN_%s=%s\n' "$client" "$token"
if [ -n "$scopes" ]; then
  printf 'HUB_SCOPES_%s=%s\n' "$client" "$scopes"
fi

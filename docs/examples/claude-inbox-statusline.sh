#!/bin/sh
# Display only. Never reads or acknowledges inbox content.
# Set PANEWIRE_LANE, PANEWIRE_HUB_URL, PANEWIRE_HUB_TOKEN_ENV and optionally
# PANEWIRE_HUB_CF_ENV in Claude Code's environment. Use the node's own token.
cat >/dev/null
if [ -z "${PANEWIRE_LANE:-}" ]; then
  printf '%s\n' 'inbox: lane unset'
  exit 0
fi
if ! count=$(panewire inbox count --lane "$PANEWIRE_LANE" 2>/dev/null); then
  count='inbox: unavailable'
fi
printf '%s\n' "$count"

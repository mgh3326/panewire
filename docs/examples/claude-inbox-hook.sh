#!/bin/sh
# Synchronous SessionStart hook. systemMessage is display-only; plain stdout
# on SessionStart would become model context, so always return a JSON object.
cat >/dev/null
if [ -z "${PANEWIRE_LANE:-}" ]; then
  printf '%s\n' '{"systemMessage":"inbox: lane unset"}'
elif count=$(panewire inbox count --lane "$PANEWIRE_LANE" --json 2>/dev/null); then
  # The only content printed is a numeric unread count, never event text.
  printf '%s' "$count" | python3 -c 'import json,sys; n=sum(json.load(sys.stdin).values()); print(json.dumps({"systemMessage": "inbox: %d unread" % n}))'
else
  printf '%s\n' '{"systemMessage":"inbox: unavailable"}'
fi
exit 0

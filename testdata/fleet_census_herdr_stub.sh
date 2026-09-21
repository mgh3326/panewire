#!/usr/bin/env bash
# Fixture herdr for the fleet-census reap-equivalence test. Answers exactly
# the two reads wrk reap performs — `agent get <pane>` and `tab list` — from
# the JSON document named by STUB_HERDR_FIXTURE:
#
#   {"agents": {"w1:p1": {"status": "idle", "tab_id": "w1:t1"},
#               "w1:p2": {"error": "agent_not_found"}},
#    "tab_list_fails": false,
#    "tabs": [{"tab_id": "w1:t1", "pane_count": 1}]}
#
# Error envelopes go to stderr with exit 1, matching real herdr's contract
# (wrk merges the streams when probing). pane_count values pass through
# unmodified so the fixture can express "?", true, missing, and duplicate
# tab rows.
set -uo pipefail
python3 - "$STUB_HERDR_FIXTURE" "$@" <<'PY'
import json, sys

fixture = json.load(open(sys.argv[1], encoding="utf-8"))
args = sys.argv[2:]

if args[:2] == ["agent", "get"] and len(args) == 3:
    pane = args[2]
    entry = fixture.get("agents", {}).get(pane)
    if entry is None or "error" in entry:
        code = entry.get("error", "agent_not_found") if isinstance(entry, dict) else "agent_not_found"
        print(json.dumps({"error": {"code": code, "message": "stub: no such agent"}}), file=sys.stderr)
        raise SystemExit(1)
    print(json.dumps({"result": {"agent": {"agent_status": entry.get("status", ""), "tab_id": entry.get("tab_id", "")}}}))
elif args[:2] == ["tab", "list"]:
    if fixture.get("tab_list_fails"):
        raise SystemExit(1)
    print(json.dumps({"result": {"tabs": fixture.get("tabs", [])}}))
else:
    raise SystemExit(2)
PY

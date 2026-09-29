# M1 two-daemon layout — `mac-work` + `mac-work-default`

M1 (the operator's mobile Mac, tailnet name `macbookpro`) runs **two**
panewire node daemons against two different herdr sessions. One daemon can
serve exactly one herdr socket, therefore exactly one herdr session, and the
hub session snapshot it reports (`session_snapshot.go` `HubSession`) carries
only that session's pane ids — it has no session-name field. The second
daemon exists so the operator's default herdr session is reachable by lanes
too.

The facts below were recorded by director-1 and the operator desk on
2026-09-29; M1 is not reachable from every host, so the desk checklist at
the end is the source of truth for "is it still like this".

## Layout

| field | fleet daemon | default-session daemon |
|---|---|---|
| machine id | `mac-work` | `mac-work-default` |
| launchd label | `dev.panewire.panewired` | `dev.panewire.panewired-default` |
| herdr session | fleet session | default session (operator's company-work) |
| `--db` | `~/Library/Application Support/panewire/panewire.sqlite3` | own db, e.g. `~/Library/Application Support/panewire/panewire-default.sqlite3` |
| `--hub-jobs-root` | shared inbox root (`~/work/herdr-inbox`) | **same** shared inbox root (`~/work/herdr-inbox`) — `wrk emit` rejects a mismatched inbox root |
| `--hub-accepting` | set → `hub-status` `ACCEPTING=true` | omitted → `ACCEPTING=false` |
| lanes (2026-09-29) | fleet lanes | `work-kairos` → pane `w3:p1G` (no parent), `work-builder-2` → pane `wR:pA` (parent `work-kairos`) |

Both daemons reported version `pw-bb64078` at 2026-09-29 04:56Z.

The reference plist in the repo,
[deploy/dev.panewire.panewired.plist](../../deploy/dev.panewire.panewired.plist),
describes the fleet daemon only. The default-session daemon is a second
plist derived from it with a different `Label`, a different `--db`, the
**default session's** `--herdr-socket`, and no
`--hub-accepting` (a non-accepting node still heartbeats, still reports its
session snapshot, and still receives lane events — it is just never offered
jobs). Its `--hub-jobs-root` is the **same** `~/work/herdr-inbox` as the
fleet daemon's: both daemons read the one shared jobs root, and `wrk emit`
rejects a mismatched inbox root rather than writing a second tree. Only the
`--db` and the launchd logs are per-daemon.

## Registering a lane

The `--machine` value chooses which daemon's session snapshot the lane is
audited against — it must be the id of the daemon whose herdr session
actually owns the pane:

```sh
# pane lives in the default session → machine id mac-work-default
panewire lanes add work-kairos --machine mac-work-default --pane w3:p1G \
  --hub-url <hub HTTPS URL> --hub-token-env <operator token env path> [--hub-cf-env <cf env path>]

# child lane of work-kairos
panewire lanes add work-builder-2 --machine mac-work-default --pane wR:pA --parent work-kairos \
  --hub-url <hub HTTPS URL> --hub-token-env <operator token env path> [--hub-cf-env <cf env path>]

# pane lives in the fleet session → machine id mac-work
panewire lanes add <lane> --machine mac-work --pane <w*:p*> \
  --hub-url <hub HTTPS URL> --hub-token-env <operator token env path> [--hub-cf-env <cf env path>]
```

`lanes add` is an upsert (`PUT /v1/lanes/<lane>`), so the same command
re-registers a lane after its pane id changed. The token env paths are
mode-0600 files; pass the paths only, never print or paste their contents.

Registering a lane with the wrong one of the two ids produces a distinctive
failure: the pane is alive on M1 but the lane reads `dead` everywhere. See
"session mismatch" below.

## Verifying delivery

1. Emit one `lane.event` from any node's inbox and confirm it is injected
   into the lane's pane:

   ```sh
   panewire emit --kind lane.event --lane work-kairos \
     --event-id <unique-producer-id> --text "audit check" \
     --inbox-root <local inbox root>
   ```

   The last recorded check of this path delivered the event to `w3:p1G` on
   2026-09-29 13:57 KST.

2. Audit the lanes from any host:

   ```sh
   panewire lanes-audit --hub-url <hub HTTPS URL> --hub-token-env <operator token env path> \
     --sibling mac-work=mac-work-default
   ```

   Expect `verdict=alive` for `work-kairos` and `work-builder-2` (both read
   `alive` at 2026-09-29 05:04Z).

### Session mismatch

Because the two daemons report sibling machine ids for one physical host,
a lane whose `--machine` names the wrong daemon is `dead` on its own
machine while the pane is demonstrably alive on the sibling. With
`--sibling mac-work=mac-work-default` the audit marks such a lane
`verdict=dead reason=session_mismatch sibling=<other id>` instead of a
bare `dead`. The verdict stays `dead` on purpose — the lane genuinely does
not route to a pane in the machine id it names — and the fix is to
re-register the lane against the sibling id (or move the pane), not to
treat it as healthy. See [r30-lanes-audit](../r30-lanes-audit.md).

The sibling match itself is on **pane id only**: `auditLaneSiblingHit`
compares `session.PaneID` to the lane's pane and never checks the sibling
session's label or agent. An unrelated pane in the sibling daemon's
snapshot can share the same `w*:p*` id — herdr reassigns ids freely — so
before re-registering against the sibling id the desk must confirm the
sibling pane is the intended session, not a same-id stranger. Confirm the
sibling pane's label/agent in that daemon's own snapshot with
`panewire sessions find <label> --machine mac-work-default`, or compare the
audit row's pane against `hub-status` output for the sibling machine id.

## What breaks when the M1 orchestrator (herdr) restarts

- **Pane ids are reassigned.** herdr does not preserve `w*:p*` ids across a
  restart, so every lane still pointing at the old pane id becomes `dead`
  on its next audit: the daemon's snapshot is fresh and complete, and the
  pane is simply not in it.
- **Routed traffic is stored, not delivered.** `lane.event`s, escalations
  and idle-wakes addressed to those lanes are recorded by the hub as
  undelivered; replay rows retire or exhaust their attempts while the lane
  stays dead.
- **The daemons themselves come back.** Both launchd jobs have
  `KeepAlive`, so `mac-work` and `mac-work-default` reconnect to the hub on
  their own — but after a reboot herdr resumes claude panes without their
  original flags/environment (known fleet fact), so resurrected panes are
  not equivalent to the ones that died.
- **Every lane must be re-registered** with the new pane id once the new
  pane exists, for each daemon's lanes:

  ```sh
  panewire lanes add work-kairos --machine mac-work-default --pane <new pane> \
    --hub-url <hub HTTPS URL> --hub-token-env <operator token env path>
  ```

  Until then the lanes are honestly `dead`; `session_mismatch` will appear
  only if the pane was recreated in the *other* session.

## Desk verification checklist

Run on M1 itself (requires shell access there):

```sh
launchctl list | grep panewire
# expect BOTH: dev.panewire.panewired  and  dev.panewire.panewired-default

ps -eo pid,command | grep '[p]anewire daemon'
# expect two processes with DIFFERENT --herdr-socket and --db values, the
# SAME --hub-jobs-root (~/work/herdr-inbox — shared, not per-daemon), and
# --hub-accepting only on the mac-work one.
# Redact --hub-token-env/--hub-cf-env paths when pasting output anywhere.
```

From any host with the operator token env:

```sh
panewire hub-status --hub-url <hub HTTPS URL> --hub-token-env <operator token env path> [--hub-cf-env <cf env path>]
# expect rows mac-work (ACCEPTING=true) and mac-work-default (ACCEPTING=false),
# both STATE=connected, same version string

panewire lanes-audit --hub-url <hub HTTPS URL> --hub-token-env <operator token env path> \
  --sibling mac-work=mac-work-default
# expect work-kairos and work-builder-2 verdict=alive; a dead lane with
# reason=session_mismatch means it is registered on the wrong of the two ids
```

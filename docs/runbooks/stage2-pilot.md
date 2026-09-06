# Stage 2 cross-machine pilot

This is the approved macOS-personal → NCP file-delivery pilot. The existing
launchd plist stays unchanged: only these foreground commands use stage 2.
Do not pass `--stage2-wrk-gate` or `--request-wrk`; the pilot is spawn-free.
Keep all client env files mode 0600 and never print their contents.

## 1. Enroll macOS and NCP identities

On the operator Mac, use a mode-0600 admin env outside the repository.

```sh
export ADMIN_ENV="$HOME/.config/panewire/pilot-admin.env"
export MAC_CLIENT_ENV="$HOME/.config/panewire/pilot-mac-personal.env"
export NCP_CLIENT_ENV="$HOME/.config/panewire/pilot-ncp.env"
chmod 600 "$ADMIN_ENV"
panewire enroll-machine --admin-env "$ADMIN_ENV" --machine-id mac-personal --out "$MAC_CLIENT_ENV" --confirm
panewire enroll-machine --admin-env "$ADMIN_ENV" --machine-id ncp-pilot --out "$NCP_CLIENT_ENV" --confirm
stat -f '%Lp %N' "$MAC_CLIENT_ENV" "$NCP_CLIENT_ENV"
```

Expected: each enrollment prints `CONFIRMED: enrolled machine_id=...` without
credentials, and each final mode is exactly `600`.

## 2. Build and stage the NCP binary

```sh
export NCP_HOST='operator-approved-host'
scripts/build-linux.sh /tmp/panewire-linux-amd64
file /tmp/panewire-linux-amd64
ssh "$NCP_HOST" 'mkdir -p "$HOME/panewire-pilot/bin"'
scp /tmp/panewire-linux-amd64 "$NCP_HOST:panewire-pilot/bin/panewire"
scp "$NCP_CLIENT_ENV" "$NCP_HOST:panewire-pilot/ncp-pilot.env"
ssh "$NCP_HOST" 'chmod 755 "$HOME/panewire-pilot/bin/panewire" && chmod 600 "$HOME/panewire-pilot/ncp-pilot.env" && stat -c "%a %n" "$HOME/panewire-pilot/bin/panewire" "$HOME/panewire-pilot/ncp-pilot.env"'
```

Expected: `file` begins `ELF 64-bit`; NCP reports `755` for the binary and
`600` for the client env. Stop if the credential file has any other mode.

## 3. Start the NCP receiver in the foreground

Use a dedicated NCP shell; do not create a systemd unit. Herdr is optional:
when absent, stage 1 logs a fail-closed schema-guard warning while stage 2
continues its independent polling loop.

```sh
export NCP_BIN="$HOME/panewire-pilot/bin/panewire"
export NCP_CLIENT_ENV_REMOTE="$HOME/panewire-pilot/ncp-pilot.env"
export NCP_INBOX="$HOME/panewire-pilot/inbox"
export NCP_STAGE1_DB="$HOME/panewire-pilot/panewire-stage1.sqlite3"
export NCP_STAGE2_DB="$HOME/panewire-pilot/panewire-stage2.sqlite3"
"$NCP_BIN" daemon \
  --db "$NCP_STAGE1_DB" \
  --stage2-db "$NCP_STAGE2_DB" \
  --stage2-client-env "$NCP_CLIENT_ENV_REMOTE" \
  --stage2-inbox-root "$NCP_INBOX" \
  --stage2-poll 30s
```

Expected: the process remains foregrounded. A `herdr schema guard failed`
warning is expected on a herdr-free NCP. A private sibling staging directory
`.$(basename "$NCP_INBOX")-stage2-staging` is created outside the inbox watcher
root.

## 4. Start the macOS daemon in the foreground

```sh
export MAC_INBOX="$HOME/panewire-pilot/inbox"
export MAC_STAGE1_DB="$HOME/panewire-pilot/panewire-stage1.sqlite3"
export MAC_STAGE2_DB="$HOME/panewire-pilot/panewire-stage2.sqlite3"
panewire daemon \
  --db "$MAC_STAGE1_DB" \
  --stage2-db "$MAC_STAGE2_DB" \
  --stage2-client-env "$MAC_CLIENT_ENV" \
  --stage2-inbox-root "$MAC_INBOX" \
  --stage2-poll 30s
```

Expected: the daemon stays foregrounded and immediately performs one
publish/claim poll, then repeats at 30 seconds. No `wrk` process is started.

## 5. Submit, materialize, and confirm reverse completion

On the Mac, submit an allow-listed file. `submit` only durably records the
outbox row; the daemon publishes it.

```sh
export JOB_ID="pilot-$(date +%Y%m%d-%H%M%S)"
export BRIEF="$HOME/panewire-pilot/$JOB_ID-brief.md"
printf '%s\n' '# Stage 2 pilot' 'cross-machine delivery check' > "$BRIEF"
MESSAGE_ID=$(panewire submit \
  --db "$MAC_STAGE2_DB" --file "$BRIEF" \
  --from-machine mac-personal --to ncp-pilot \
  --path "jobs/$JOB_ID/brief.md" \
  --classification personal_non_company)
printf 'submitted message_id=%s\n' "$MESSAGE_ID"
panewire outbox list --db "$MAC_STAGE2_DB"
```

Expected: submit prints one ID. The Mac outbox row transitions `SUBMITTED` to
`PUBLISHED`; NCP materializes `$NCP_INBOX/jobs/$JOB_ID/brief.md`.

On NCP, verify materialization and explicitly send the reverse completion:

```sh
test -f "$NCP_INBOX/jobs/$JOB_ID/brief.md"
sed -n '1,20p' "$NCP_INBOX/jobs/$JOB_ID/brief.md"
export COMPLETE_BODY="$HOME/panewire-pilot/$JOB_ID-completion.json"
printf '%s\n' '{"outcome":"received"}' > "$COMPLETE_BODY"
"$NCP_BIN" submit \
  --db "$NCP_STAGE2_DB" --file "$COMPLETE_BODY" \
  --from-machine ncp-pilot --to mac-personal \
  --path "completions/$MESSAGE_ID.json" \
  --kind workflow.completion \
  --correlation-id "$MESSAGE_ID" --causation-id received \
  --classification personal_non_company
"$NCP_BIN" outbox list --db "$NCP_STAGE2_DB"
panewire outbox list --db "$MAC_STAGE2_DB"
```

Expected: the NCP completion transitions to `PUBLISHED`. Once the Mac receives
it, the original Mac row becomes `COMPLETED` and the completion file appears at
`$MAC_INBOX/completions/$MESSAGE_ID.json`. Stage-1 fsnotify may observe the
materialized file; that is expected and does not create a second stage-2 job.

If a message accidentally uses `--request-wrk` while this daemon has no
`--stage2-wrk-gate`, it is terminally rejected as `gate_not_installed`, not
`GATE_DENIED`. Do not exercise that path in the pilot.

## 6. Stop and list residuals

Press `Ctrl-C` in both foreground terminals. Before operator-approved cleanup,
list residual evidence rather than deleting it:

```sh
find "$MAC_INBOX" -maxdepth 4 -type f -print
find "$NCP_INBOX" -maxdepth 4 -type f -print
ls -ld "$(dirname "$MAC_INBOX")/.${MAC_INBOX##*/}-stage2-staging" \
       "$(dirname "$NCP_INBOX")/.${NCP_INBOX##*/}-stage2-staging"
ls -l "$MAC_STAGE1_DB" "$MAC_STAGE2_DB" "$NCP_STAGE1_DB" "$NCP_STAGE2_DB"
```

Expected residuals: materialized pilot files, metadata-only SQLite databases
(and WAL/SHM files while open), and owned private staging directories. The Mac
source file remains canonical. Revoke both enrolled identities only after the
operator approves teardown.

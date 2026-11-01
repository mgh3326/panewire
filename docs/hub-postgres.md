# Hub inbox on Postgres

The hub's only database is its lane inbox (`lane_inbox_store.go`, described in
[quiet delivery](quiet-delivery.md#durability-and-replay)). It has two
backends behind one method set:

| `PANEWIRE_HUB_DATABASE_URL` | Backend |
|---|---|
| unset (default) | SQLite file at `--inbox-store`, unchanged behaviour |
| a `postgres://` URL | Postgres; `--inbox-store` is ignored |

Node daemons are unaffected: their store (`store.go`) stays a local SQLite file.

## Contract

- **The URL is a credential.** The hub reads it only from the environment. It
  never logs it. It reports a connect or migration failure as the failed step
  and, for a server error, the SQLSTATE. A report never names the user, host,
  database or password.
- **Migrations.** The schema lives in `hub_postgres_migrations/*.sql`, embedded
  in the binary. On every start the hub applies the files that are not yet
  recorded in `panewire_schema_migrations`, in name order. It applies them in
  one transaction under `pg_advisory_xact_lock`. Hubs that start at once apply
  each file once, and a rerun changes nothing. A schema change is a new file,
  never an edit to a file that was already applied.
- **Durability.** Every connection forces `synchronous_commit=on`. A producer
  gets its ACK only after its inbox row is durably committed, as with
  SQLite's FULL mode.
- **Bounded calls.** Every store call runs with a 10 s deadline; the startup
  scans get 2 min. Hub shutdown cancels calls in flight, so a stalled
  database cannot hold the hub open. `migrate-sqlite --apply` waits at most
  10 s for a lock (`lock_timeout`).
- **Event keys.** The hub joins key parts with NUL, which Postgres `text`
  cannot hold, so the Postgres store keeps `event_key` hex-encoded. SQLite
  keeps the raw key.
- **Must-ack.** Must-ack works as it does on SQLite: `ready`, `decision`,
  `blocked` and `findings` items stay unread until `panewire inbox ack`, and
  digests never include them.
- **One hub per database.** The inbox serializes waiter wake-ups and batch
  claims inside the hub process.
- **Kill switch.** `--delivery-policy off` still opens no database, whichever
  backend is set.

## Rollout (desk)

1. **Create a dedicated database and role.** The role owns the database and
   needs no other privileges. In `psql` as an administrator:

   ```sql
   CREATE ROLE panewire_hub LOGIN PASSWORD '<generated>';
   CREATE DATABASE panewire_hub OWNER panewire_hub;
   ```

2. **Set the URL in the hub's systemd `EnvironmentFile`.** The file must be
   root-owned, mode 0600, and outside any repository:

   ```sh
   PANEWIRE_HUB_DATABASE_URL=postgres://panewire_hub:<password>@<db-host>:<port>/panewire_hub?sslmode=verify-full
   ```

   For a database on another machine, use `sslmode=verify-full`. Without a
   `sslmode`, pgx uses `prefer`, which can fall back to plaintext. Use
   `sslmode=disable` only for a database on the hub's own host, reached
   over loopback or a Unix socket.

   Do not add the URL to the unit's `ExecStart`, to shell history or to a
   ticket.
3. **Stop the hub and copy the inbox.**

   ```sh
   systemctl stop <hub unit>
   # With the same environment as the hub:
   panewire hub migrate-sqlite --from <inbox-store path>           # dry run: counts and target state
   panewire hub migrate-sqlite --from <inbox-store path> --apply   # copy
   ```

   - The dry run prints the source row counts and the target state: `schema
     not created yet`, `empty`, `already holds this source`, or the counts of
     a target that differs.
   - `--apply` creates the schema, copies every row and checks the copy row
     for row. It does all three in one transaction, so a failure leaves the
     target unchanged. The copy keeps item ids, batch order, read and ack
     state, and must-ack rows.
   - The source is read from a private copy of the database file and its
     `-wal`. Rows that a killed hub left uncheckpointed in the `-wal` are
     included. The SQLite file itself is never opened for writing.
   - **Rerun.** If the target already holds exactly this source, `--apply`
     reports `unchanged` and exits 0. If the target holds other rows, for
     example after the hub has run on Postgres, `--apply` refuses.
4. **Start the hub** with the new environment: `systemctl start <hub unit>`.
   Then check the inbox: `panewire inbox list --unread` shows the same unread
   items as before the move.

Exit codes for `migrate-sqlite`:

| Code | Meaning |
|---|---|
| 0 | done, or unchanged |
| 2 | usage error |
| 5 | refused: unreadable source, not an inbox store, `--apply` without the URL, a target with other rows, or a copy mismatch |
| 70 | database failure |

## Rollback

1. Stop the hub.
2. Remove `PANEWIRE_HUB_DATABASE_URL` from the `EnvironmentFile`.
3. Start the hub.

The hub opens the SQLite file at `--inbox-store` again. Migration never
changed that file, so the inbox is back where it was before the move. Items
accepted while the hub ran on Postgres stay in Postgres. They are not copied
back. Keep the SQLite file until the hub has run on Postgres long enough that
you no longer need a rollback.

## Backup

Back up the Postgres database with your usual tooling, for example `pg_dump`
or the platform's own backups. The SQLite backup advice in
[quiet delivery](quiet-delivery.md#durability-and-replay) applies only to the
SQLite backend.

## Tests

- `lane_inbox_store_contract_test.go` runs every store case, including
  must-ack, against both backends.
- `inbox_must_ack_test.go` runs the hub-level must-ack tests on both
  backends.
- `hub_migrate_sqlite_test.go` covers the copy, a source with a pending
  `-wal`, and reruns.

The Postgres half needs `PANEWIRE_TEST_HUB_DATABASE_URL`. CI's `postgres` job
sets it from a service container, and fails if any test in that job skips.
Each test uses its own schema. Locally the variable is unset, so those cases
skip. Do not start a database server on a host to run them.

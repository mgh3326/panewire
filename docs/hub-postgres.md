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
  database cannot hold the hub open.
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

## Setup (new database)

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
## Live state

The live hub runs on Postgres. Host check on ncp, 2026-11-13 about 12:00 KST:

- The hub unit loads `EnvironmentFile=/etc/panewire/hub-db.env`, which sets
  `PANEWIRE_HUB_DATABASE_URL`. The hub process holds established connections to
  the Postgres port.
- `newHubInbox` opens Postgres whenever the URL is set, so the
  `--inbox-store` SQLite file is inert on the live hub.
- That file was last written 2026-11-01 15:29. It holds only the lane inbox
  tables (`lane_inbox`, `lane_inbox_batches`, `lane_inbox_batch_quarantine`)
  and has no spawn tables (`spawn_operations`, `spawn_reservations`,
  `spawn_notifications`). Its rows are history: no compatibility rule keeps
  them (operator decision 208).

There is no copy command. `panewire hub migrate-sqlite` is removed, so the
SQLite file is not a cutover source. Do not unset the URL to roll back: the hub
would open the SQLite file as it stood on 2026-11-01 and miss every item written
since. Any move between backends needs a new migration decision first.

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

The Postgres half needs `PANEWIRE_TEST_HUB_DATABASE_URL`. CI's `postgres` job
sets it from a service container, and fails if any test in that job skips.
Each test uses its own schema. Locally the variable is unset, so those cases
skip. Do not start a database server on a host to run them.

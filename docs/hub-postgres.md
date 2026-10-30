# Hub inbox on Postgres

The hub's only database is its lane inbox (`lane_inbox_store.go`, described in
[quiet delivery](quiet-delivery.md#durability-and-replay)). It has two
backends behind one method set:

| `PANEWIRE_HUB_DATABASE_URL` | Backend |
|---|---|
| unset (default) | SQLite file at `--inbox-store` — unchanged behaviour |
| a `postgres://` URL | Postgres; `--inbox-store` is ignored |

Node daemons are unaffected: their store (`store.go`) stays a local SQLite file.

## Contract

- The URL is a credential. The hub reads it only from the environment, never
  logs it, and reports a connect or migration failure as the failed step plus,
  for a server error, its SQLSTATE — never user, host, database or password.
- The schema lives in `hub_postgres_migrations/*.sql`, embedded in the binary.
  On every start the hub applies the files it has not recorded in
  `panewire_schema_migrations`, in name order, in one transaction under an
  advisory lock. Re-running is a no-op; a new migration is a new file, never an
  edit to an applied one.
- Every connection forces `synchronous_commit=on`: a producer is acknowledged
  only after its inbox row is durably committed, as with SQLite's FULL mode.
- Run one hub per database. The inbox's waiter wake-ups and batch claims are
  serialized inside the hub process.
- `--delivery-policy off` still opens no database, whichever backend is set.

## Moving an existing hub

`panewire hub migrate-sqlite --from PATH` copies an inbox SQLite file into the
database named by `PANEWIRE_HUB_DATABASE_URL`.

1. Without `--apply` (the default) it only reads: it prints the source row
   counts and, when the URL is set, whether the target schema exists and is
   empty. The source is opened read-only.
2. Stop the hub, then run it again with `--apply`. It creates the schema,
   refuses a target that already holds inbox rows, copies every row keeping
   item ids and batch order, checks the copy row-for-row and commits — all in
   one transaction, so a failure leaves the target unchanged.
3. Start the hub with `PANEWIRE_HUB_DATABASE_URL` set. Keep the SQLite file
   until the hub has run on Postgres; rollback is unsetting the variable,
   which returns the hub to that file (items accepted on Postgres meanwhile
   stay there).

Exit codes: 0 done, 2 usage, 5 refused (unreadable source, missing URL for
`--apply`, non-empty target, copy mismatch), 70 database failure.

## Tests

`lane_inbox_store_contract_test.go` runs every store case against both
backends. The Postgres half needs `PANEWIRE_TEST_HUB_DATABASE_URL`, which CI's
`postgres` job sets from a service container; each test uses its own schema.
Locally the variable is unset and those cases skip — do not start a database
server on a host to run them.

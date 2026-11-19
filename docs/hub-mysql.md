# Hub store on MySQL

The hub's database is its lane inbox and spawn store (`lane_inbox_store.go`,
described in [quiet delivery](quiet-delivery.md#durability-and-replay)). It has
three backends behind one method set. This page is the MySQL backend of
hk 4898, phase 1b: the dialect, the schema and the CI suite. Nothing on this
page changes a live hub: the hub moves to MySQL in phase 3, on the operator's
go (see [Status](#status)).

## Selecting MySQL

`PANEWIRE_HUB_DATABASE_URL` chooses the backend by its scheme. There is no
second key.

| `PANEWIRE_HUB_DATABASE_URL` | Backend |
|---|---|
| unset (default) | SQLite file at `--inbox-store` |
| a `postgres://` URL | Postgres ([hub inbox on Postgres](hub-postgres.md)) |
| a `mysql://` URL | MySQL (this page); `--inbox-store` is ignored |

The MySQL URL is

```
mysql://<user>:<password>@<host>:<port>/<database>?tls-ca=<path to the CA file>
```

- **`tls-ca` pins the server's CA** and is required for every host but a
  loopback address. The connection is verified against that CA alone
  (`VERIFY_CA`). The HeatWave server certificate is signed by the instance's
  own CA and has no SAN, so Go's host name check cannot pass; the hub replaces
  it with a chain check against the pinned CA, at the current time, for server
  authentication. Verification is never skipped. The file is a PEM file of CA
  certificates, at most 64 KiB; a file that is missing, empty, not PEM or holds
  a certificate that is not a CA fails the start. This is the shape of the pin
  in mergewake (hk 4246).
- **No other query parameter is accepted.** A URL can neither turn TLS off,
  relax it, nor change the connection settings below.
- **A loopback host (`localhost`, `127.0.0.0/8`, `::1`) may omit `tls-ca`.**
  That is for a test database on the same host (CI's service container); a
  remote database always needs the CA.
- The password is percent-encoded in the URL. The URL is a credential and is
  handled as the Postgres one is: read from the environment only, never logged
  or echoed. A failure names the step and the MySQL error number, never the
  user, host or database.

The live instance (read 2026-11-19): MySQL HeatWave 26.10.0-cloud, shared with
handoffkeep; database `panewire_hub` (`utf8mb4`, `utf8mb4_bin`), account
`panewire_hub_app` with `REQUIRE SSL` and `MAX_USER_CONNECTIONS 24`; default
isolation REPEATABLE-READ; `innodb_flush_log_at_trx_commit=1`. The credentials
live in Infisical `/panewire-hub` (`PANEWIRE_HUB_MYSQL_*`, see
`reference/infisical-fleet`); the operator composes the URL from them at
cutover.

## Connection settings

The hub sets these itself:

| Setting | Why |
|---|---|
| `clientFoundRows=true` | `RowsAffected` counts the rows an `UPDATE` matched, not those it changed. Every compare-and-set in the hub reads the count as "the guarded row was there", so an update that matches and changes nothing must still report 1. |
| `parseTime`, `time_zone='+00:00'`, `loc=UTC` | the ledger's `DATETIME(3)` reads as UTC. |
| `collation=utf8mb4_bin` | the collation of the database; a comparison is byte for byte. |
| `transaction_isolation='READ-COMMITTED'` | the level for a statement outside a transaction. |
| every transaction: `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` | InnoDB's default is REPEATABLE READ with next-key locks; the hub's statements assume READ COMMITTED. `begin` states it on every transaction, so a server or account default cannot change it. |
| at most 16 open connections | the account is capped at 24; the pool stays under it, leaving room for the migration's pinned connection and for a restart that briefly overlaps. |
| the server's `innodb_flush_log_at_trx_commit` must be 1 | a producer's ACK follows the commit, so the log must be flushed at every commit. MySQL has no per-session form of this (Postgres' `synchronous_commit` is one), so the hub reads the server's setting at start and refuses to open on any other value. |

## The dialect

`mysqlDialect` (`hub_mysql.go`) implements the whole seam of
[The dialect seam](hub-postgres.md#the-dialect-seam):

| Seam operation | MySQL |
|---|---|
| `insertIgnore` | `INSERT ... ON DUPLICATE KEY UPDATE t.k=t.k`, a duplicate-key no-op on the first column. **Never `INSERT IGNORE`**, which turns every other error (a value too long for its column, a NULL in a NOT NULL column) into a warning and a stored default. |
| `insertOnce` | the plain `INSERT`; `duplicateKey` is error 1062. See below. |
| `upsert`, `upsertGreatest` | `INSERT ... VALUES(...) AS incoming ON DUPLICATE KEY UPDATE c=incoming.c`, with the row alias of MySQL 8.0.19; `GREATEST()` for the larger of two values. |
| `upsertWhere` | `IF(<guard>, incoming.c, t.c)` per column: `ON DUPLICATE KEY UPDATE` has no `WHERE`. |
| `castInt`, `concat`, `escapeClause` | `CAST(? AS SIGNED)`, `CONCAT(a,b,c)`, ` ESCAPE '\\'` |
| `updateOrdered` | `UPDATE t SET ... WHERE ... ORDER BY ... LIMIT ?`; MySQL rejects the subselect on the same table the other dialects use (error 1093). |
| `deleteWhereIn` | `DELETE FROM t WHERE k IN (SELECT * FROM (<bounded select>) AS picked)`; MySQL rejects `LIMIT` inside `IN` (1235) and a subselect that reads the table being deleted from (1093), and a derived table with a `LIMIT` is materialized first. |
| `begin` | `READ COMMITTED`, stated. |
| `lockMigration` | `GET_LOCK` on the one pinned connection the migration runs on, released with `RELEASE_LOCK`; the name holds the database name's MD5, because lock names are server-wide and the instance is shared. A hub that does not get the lock in 60 s fails its start. If the release fails, the connection is discarded, which frees the lock. |
| `lockLaneCause` | a row lock: `INSERT ... ON DUPLICATE KEY UPDATE` of the SHA-256 of the (lane, cause) pair into `supersede_lane_cause` holds the row exclusively until the transaction ends. `GET_LOCK` is session-scoped and would outlive the transaction. |
| `rowsMatched` | `RowsAffected`, with `clientFoundRows`. |
| `storedKey` | the raw bytes of the key, bound as bytes into a `VARBINARY` column: no hex encoding. |
| `batchOrder`, `laneInboxInsert`, `ownsSchema`, `beforeInboxPrune` | `seq` (an `AUTO_INCREMENT` column), the Postgres `lane_inbox` insert, false, nothing (InnoDB hands out an id above the largest it ever gave). |

### An insert whose count is read

`insertIgnore`'s duplicate no-op is no use to a caller that reads the row count
as "this call stored the row". With `clientFoundRows`, MySQL reports an
unchanged duplicate as 1, the same as a stored row. Five call sites read the
difference (a fresh lane event, a consult, the two spawn reservations, a
placement snapshot), so they call `insertOnce`: on SQLite and Postgres it is
`insertIgnore`, on MySQL the plain `INSERT`, and the duplicate-key error 1062 is
the answer "a row was already there". A failed statement leaves an InnoDB
transaction open, so the answer does not abort it.

### SET lists read the old row

MySQL assigns a `SET` list left to right, so a later expression sees the
columns an earlier one set; Postgres and SQLite read the old row for all of
them. `TestHubUpdatesReadNoEarlierAssignment` fails on a store `UPDATE` whose
later assignment reads an earlier-assigned column, except the one pair it lists
with its proof (`parkSpawn`). Write a new update so each column is read before
it is set.

## Migrations

The schema lives in `hub_mysql_migrations/NNNN_feature.sql`, embedded in the
binary. It is the whole schema after phase 0, 25 files; the Postgres files and
their partial indexes are not translated one for one:

- **One statement per file.** MySQL commits DDL as it runs, so a transaction
  cannot make a file atomic, and a file of two statements that stops between
  them leaves a half-applied file no ledger row describes. A file with a second
  statement fails `TestHubMySQLMigrationsAreOneGuardedStatement` and the start.
- **Every file checks before it acts.** The line `-- applied-if: SELECT COUNT(*)
  FROM information_schema...` is a count of the statement's effect; a count
  above zero skips the statement. MySQL has no `IF NOT EXISTS` for indexes and
  columns. A run that stopped after a statement and before its ledger row is
  therefore repeated without error. A statement that changes the schema must
  guard on `information_schema`.
- **The ledger** is `panewire_schema_migrations`, one row per applied file, in
  name order. Hubs starting together migrate once: the second waits on the lock
  and finds every file recorded. A schema change is a new file with the next
  number, never an edit to an applied one.
- **Types.** Text keys are `VARCHAR(n)` (utf8mb4: an index key is at most 3072
  bytes, so at most 768 characters), long text is `MEDIUMTEXT`, `event_key` is
  `VARBINARY(2048)` (raw bytes), ids are `AUTO_INCREMENT`, millisecond
  timestamps stay `BIGINT`, the ledger's time is `DATETIME(3)`. A `TEXT` column
  with a default carries it as an expression, `DEFAULT ('')`.
- **Indexes.** MySQL has no partial indexes. The hot sets are small tables
  (`spawn_queue`, the outboxes), and an index that was partial leads with the
  columns its predicate filtered on.
- `TestHubMySQLMigrationsCoverThePostgresSchema` compares every table and
  column with the Postgres migrations, so a column added on one side fails
  until the other has it. `supersede_lane_cause` is the one table MySQL adds.

## Tests

The store contract suite is the same tests on all three backends:
`laneInboxBackends` and `forEachHubInboxStorage` list `sqlite`, `postgres` and
`mysql`, so a test written for one runs on each. Each MySQL case gets its own
database (`utf8mb4_bin`) and drops it at the end.

- No database is needed for `hub_sql_dialect_test.go` (each operation's text
  and each call site's statement, per dialect), `hub_mysql_test.go` (the URL,
  the pinned CA, the migration files, schema coverage) and
  `hub_sql_update_order_test.go`.
- The MySQL half needs `PANEWIRE_TEST_HUB_MYSQL_URL`, a `mysql://` URL whose
  user may create and drop databases. CI's `mysql` job sets it from a
  `mysql:8.4` service container, runs the suite with `-race`, and fails if a
  MySQL test skips. Locally the variable is unset and those cases skip. **Do
  not start a database server on a host to run them.**
- The mutants the suite catches: `INSERT IGNORE` for the no-op
  (`TestSQLDialectGeneratedStatements`, `TestMySQLDuplicateKeyNoOpIsNotInsertIgnore`,
  and on a server `TestMySQLDuplicateKeyNoOpDoesNotSwallowOtherErrors`); a
  transaction without its isolation setting (`TestSQLDialectBeginsAtItsIsolationLevel`,
  and on a server `TestMySQLTransactionsStateReadCommitted`); a migration file
  with two statements (`TestHubMySQLMigrationsAreOneGuardedStatement`).
- A compare-and-set that matches and changes nothing reports one row:
  `TestLaneInboxStoreContractCompareAndSetCountsMatchedRows`, on every backend.
- A migration run twice changes nothing:
  `TestLaneInboxStoreMySQLMigrationsTwiceChangeNothing`; a run that stopped
  after a statement is repeated without error:
  `TestLaneInboxStoreMySQLRerunAfterAStoppedMigrationIsANoOp`.

## Status

Phase 1b delivers the dialect, the schema and the CI suite. The hub still runs
on Postgres. The copier and the rehearsal are phase 2 and the cutover is
phase 3 (hk 4898); Postgres support is removed after the cutover's rollback
window. Setting a `mysql://` URL on a live hub before then is the operator's
cutover step, not a configuration option.

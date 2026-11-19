package panewire

import (
	"context"
	"database/sql"
	"encoding/hex"
	"strconv"
	"strings"
)

// sqlDialect is the one seam for what differs between the databases the hub
// store runs on (docs/hub-postgres.md, "The dialect seam"). A store query that
// needs one of these forms asks the dialect for it and never spells it inline:
// TestHubStoreQueriesGoThroughTheDialectSeam fails on a statement that does.
//
// SQLite and Postgres implement it today; the MySQL dialect of hk 4898 is a
// third implementation, not a third copy of every query. Each method's comment
// names the form the other implementations may not share.
type sqlDialect interface {
	// rebind rewrites a statement written with ?-placeholders. No hub
	// statement carries a literal '?', so a positional rewrite is exact.
	rebind(query string) string

	// begin opens a transaction at the isolation level the hub's code assumes.
	// Every hub transaction goes through it, never through db.BeginTx, so the
	// level is never left to a server default.
	begin(ctx context.Context, db *sql.DB) (*sql.Tx, error)

	// insertIgnore is insert, made a no-op when a row with the same target key
	// exists. An empty target means any unique key.
	insertIgnore(insert, target string) string
	// upsert is insert, with assignments applied when a row with the same
	// target key exists. An assignment reads the proposed row through incoming.
	upsert(insert, target string, assignments ...string) string
	// upsertWhere is upsert limited to rows where guard holds on the stored
	// row, for the columns named. table qualifies the stored row.
	upsertWhere(insert, target, table, guard string, columns ...string) string
	// upsertGreatest is upsert that keeps the larger of the stored and the
	// proposed value of column.
	upsertGreatest(insert, target, table, column string) string
	// incoming names the value an upsert's insert proposed for column.
	incoming(column string) string
	// assignIncoming assigns each column its proposed value.
	assignIncoming(columns ...string) string

	// castInt casts an expression, a bound placeholder in a CASE branch, to
	// the integer type of the *_ms columns.
	castInt(expr string) string
	// concat joins SQL string expressions.
	concat(parts ...string) string
	// escapeClause follows a LIKE pattern whose backslash escapes a wildcard.
	escapeClause() string
	// updateOrdered is the UPDATE that sets set on the first limit rows of
	// table, by order, among those matching where; limit is a bound
	// placeholder. key is a unique column of table.
	updateOrdered(table, key, set, where, order string) string

	// deleteWhereIn is the DELETE of the rows of table whose key a bounded
	// SELECT (ORDER BY ... LIMIT ?) names. selected may read table itself or
	// another one.
	deleteWhereIn(table, key, selected string) string

	// lockMigration serializes schema migration across hubs sharing a
	// database, for the life of tx.
	lockMigration(ctx context.Context, tx *sql.Tx) error
	// lockLaneCause serializes the supersede of one (lane, cause) pair, for
	// the life of tx. Where the writer lock already does so it does nothing.
	lockLaneCause(ctx context.Context, tx *sql.Tx, lane, cause string) error

	// rowsMatched is the number of rows an UPDATE or DELETE matched. Every
	// compare-and-set in the hub reads it as "the guarded row was there", so a
	// dialect whose driver counts only changed rows must be opened so that it
	// counts matched ones.
	rowsMatched(result sql.Result) (int64, error)

	// storedKey is the event_key column's form of a hub key. Keys join their
	// parts with NUL and producer parts need not be UTF-8; where the column
	// cannot hold that, the key is hex-encoded.
	storedKey(key string) string

	// ownsSchema is true when the store builds its own schema at open (SQLite)
	// and false when numbered migration files do.
	ownsSchema() bool
	// batchOrder is the column batches are sent in: creation order.
	batchOrder() string
	// laneInboxInsert is the lane_inbox INSERT, without its conflict clause.
	// The columns after the id are the same for every dialect.
	laneInboxInsert() string
	// beforeInboxPrune runs in the retention transaction ahead of the delete.
	beforeInboxPrune(ctx context.Context, tx *sql.Tx) error
	// loggable returns err fit for a log line.
	loggable(step string, err error) error
}

// The dialect values. Compare a store's dialect with them, never with a name.
var (
	dialectSQLite   sqlDialect = sqliteDialect{}
	dialectPostgres sqlDialect = postgresDialect{}
)

// onConflict is the clause SQLite and Postgres share: the target key in
// parentheses, or nothing for any unique key.
func onConflict(target string) string {
	if target == "" {
		return " ON CONFLICT"
	}
	return " ON CONFLICT(" + target + ")"
}

// conflictDialect holds the forms SQLite and Postgres spell the same way.
type conflictDialect struct{}

func (conflictDialect) insertIgnore(insert, target string) string {
	return insert + onConflict(target) + " DO NOTHING"
}

func (conflictDialect) upsert(insert, target string, assignments ...string) string {
	return insert + onConflict(target) + " DO UPDATE SET " + strings.Join(assignments, ",")
}

func (conflictDialect) upsertWhere(insert, target, table, guard string, columns ...string) string {
	return insert + onConflict(target) + " DO UPDATE SET " + conflictDialect{}.assignIncoming(columns...) + " WHERE " + table + "." + guard
}

func (conflictDialect) upsertGreatest(insert, target, table, column string) string {
	stored, proposed := table+"."+column, conflictDialect{}.incoming(column)
	return conflictDialect{}.upsert(insert, target, column+"=CASE WHEN "+proposed+">"+stored+" THEN "+proposed+" ELSE "+stored+" END")
}

func (conflictDialect) incoming(column string) string { return "excluded." + column }

func (conflictDialect) assignIncoming(columns ...string) string {
	assignments := make([]string, len(columns))
	for i, column := range columns {
		assignments[i] = column + "=" + conflictDialect{}.incoming(column)
	}
	return strings.Join(assignments, ",")
}

func (conflictDialect) castInt(expr string) string { return "CAST(" + expr + " AS BIGINT)" }

func (conflictDialect) concat(parts ...string) string { return strings.Join(parts, "||") }

func (conflictDialect) escapeClause() string { return ` ESCAPE '\'` }

func (conflictDialect) updateOrdered(table, key, set, where, order string) string {
	return `UPDATE ` + table + ` SET ` + set + ` WHERE ` + key + ` IN (SELECT ` + key + ` FROM ` + table + ` WHERE ` + where + ` ORDER BY ` + order + ` LIMIT ?)`
}

func (conflictDialect) deleteWhereIn(table, key, selected string) string {
	return `DELETE FROM ` + table + ` WHERE ` + key + ` IN (` + selected + `)`
}

func (conflictDialect) rowsMatched(result sql.Result) (int64, error) { return result.RowsAffected() }

type sqliteDialect struct{ conflictDialect }

func (sqliteDialect) rebind(query string) string { return query }

// begin takes the driver's only isolation: SQLite serializes writers.
func (sqliteDialect) begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	return db.BeginTx(ctx, nil)
}

// lockMigration does nothing: the SQLite schema is built in Go at open, under
// the file's own write lock.
func (sqliteDialect) lockMigration(context.Context, *sql.Tx) error { return nil }

// lockLaneCause does nothing: the store's mutex and SQLite's single writer
// already serialize a supersede.
func (sqliteDialect) lockLaneCause(context.Context, *sql.Tx, string, string) error { return nil }

func (sqliteDialect) storedKey(key string) string { return key }

func (sqliteDialect) ownsSchema() bool { return true }

// batchOrder is SQLite's implicit rowid.
func (sqliteDialect) batchOrder() string { return "rowid" }

// laneInboxInsert takes the id from the floor row, so an id is never reused
// after a prune empties the table.
func (sqliteDialect) laneInboxInsert() string {
	return `INSERT INTO lane_inbox(id,event_key,lane,kind,class,payload,created_at,read_at,due_at,digest_at,must_ack,hold_until,sender,event_id,cause,delivery_stage,delivery_at) VALUES((SELECT max(COALESCE((SELECT max(id) FROM lane_inbox),0),floor)+1 FROM lane_inbox_id_floor WHERE singleton=1),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
}

// beforeInboxPrune raises the id floor to the largest id, so the pruned ids
// are not handed out again.
func (sqliteDialect) beforeInboxPrune(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `UPDATE lane_inbox_id_floor SET floor=max(floor,COALESCE((SELECT max(id) FROM lane_inbox),0)) WHERE singleton=1`)
	return err
}

// loggable: a SQLite error carries no credential.
func (sqliteDialect) loggable(_ string, err error) error { return err }

type postgresDialect struct{ conflictDialect }

func (postgresDialect) rebind(query string) string {
	var b strings.Builder
	n := 0
	for _, r := range query {
		if r == '?' {
			n++
			b.WriteString("$" + strconv.Itoa(n))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// begin states READ COMMITTED, the level the hub's statements assume, so a
// server or role default of another level cannot change what they see.
func (postgresDialect) begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	return db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

func (postgresDialect) lockMigration(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(hubPostgresMigrationLock))
	return err
}

func (d postgresDialect) lockLaneCause(ctx context.Context, tx *sql.Tx, lane, cause string) error {
	_, err := tx.ExecContext(ctx, d.rebind(`SELECT pg_advisory_xact_lock(hashtext(?))`), lane+"\x1f"+cause)
	return err
}

// storedKey hex-encodes: Postgres text holds neither NUL nor non-UTF-8.
func (postgresDialect) storedKey(key string) string { return hex.EncodeToString([]byte(key)) }

func (postgresDialect) ownsSchema() bool { return false }

// batchOrder is the identity column Postgres needs in place of a rowid.
func (postgresDialect) batchOrder() string { return "seq" }

func (postgresDialect) laneInboxInsert() string {
	return `INSERT INTO lane_inbox(event_key,lane,kind,class,payload,created_at,read_at,due_at,digest_at,must_ack,hold_until,sender,event_id,cause,delivery_stage,delivery_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
}

// beforeInboxPrune does nothing: an identity column never reuses an id.
func (postgresDialect) beforeInboxPrune(context.Context, *sql.Tx) error { return nil }

// loggable reduces the error: one after connect can still name the server's
// address (a dropped connection's net error).
func (postgresDialect) loggable(step string, err error) error { return hubDatabaseError(step, err) }

package panewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"embed"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/netip"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

// The hub store on MySQL (hk 4898 phase 1b; docs/hub-mysql.md). The dialect
// below is the third implementation of the seam in hub_sql_dialect.go. This
// file also holds what opening a MySQL store needs: the URL and its pinned
// CA, the connection pool, and the migration runner for hub_mysql_migrations.

// hubMySQLMaxConns is the pool ceiling. The hub's account on the shared
// instance is capped at 24 connections (MAX_USER_CONNECTIONS, hk 4898 a299),
// and a restart or the cutover briefly runs the old and the new hub process on
// that one account: two pools of 12 fit the cap exactly, so neither process
// meets error 1040. The migration's pinned connection comes out of the pool.
const hubMySQLMaxConns = 12

// hubMySQLMigrationLockWait is how long a hub waits for another hub's
// migration to finish before it gives up and fails its start.
const hubMySQLMigrationLockWait = 60 // seconds

// hubMySQLMigrations are applied in file-name order, each at most once. MySQL
// commits DDL implicitly, so a file is one statement and a transaction cannot
// make a run atomic; a file's applied-if guard is what lets a run that stopped
// between a statement and its ledger row be repeated.
//
//go:embed hub_mysql_migrations/*.sql
var hubMySQLMigrations embed.FS

type mysqlDialect struct{}

func (mysqlDialect) rebind(query string) string { return query }

// begin states READ COMMITTED on every transaction: InnoDB's default is
// REPEATABLE READ with next-key locks, and the hub's statements assume the
// level Postgres gives them. The driver issues SET TRANSACTION ISOLATION
// LEVEL ahead of START TRANSACTION.
func (mysqlDialect) begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	return db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

// insertTarget is the table and first column of an INSERT. The duplicate-key
// no-op assigns that column to itself.
var insertTarget = regexp.MustCompile(`(?is)^\s*INSERT\s+INTO\s+(\w+)\s*\(\s*(\w+)`)

// insertIgnore is a duplicate-key no-op, never INSERT IGNORE: that form turns
// every other error (a value too long, a NULL in a NOT NULL column) into a
// warning and a stored default. The assignment names the table so an
// INSERT ... SELECT whose source has a column of the same name is not
// ambiguous. target is unused: any unique key is a duplicate.
func (mysqlDialect) insertIgnore(insert, _ string) string {
	m := insertTarget.FindStringSubmatch(insert)
	if m == nil {
		return insert + " ON DUPLICATE KEY UPDATE"
	}
	return insert + " ON DUPLICATE KEY UPDATE " + m[1] + "." + m[2] + "=" + m[1] + "." + m[2]
}

// insertOnce is the plain insert: with clientFoundRows an unchanged duplicate
// reports one row, the same as a stored row, so insertIgnore's count cannot
// tell them apart. A failed statement leaves an InnoDB transaction open, so
// the duplicate-key error is an answer, not an abort.
func (mysqlDialect) insertOnce(insert, _ string) string { return insert }

// duplicateKey is ER_DUP_ENTRY.
func (mysqlDialect) duplicateKey(err error) bool {
	var myErr *mysql.MySQLError
	return errors.As(err, &myErr) && myErr.Number == 1062
}

// rowAlias names the proposed row (MySQL 8.0.19). The VALUES() function it
// replaces is deprecated.
const mysqlRowAlias = "incoming"

func (mysqlDialect) upsert(insert, _ string, assignments ...string) string {
	return insert + " AS " + mysqlRowAlias + " ON DUPLICATE KEY UPDATE " + strings.Join(assignments, ",")
}

// upsertWhere guards each column with IF(): ON DUPLICATE KEY UPDATE has no
// WHERE.
func (mysqlDialect) upsertWhere(insert, _, table, guard string, columns ...string) string {
	assignments := make([]string, len(columns))
	for i, column := range columns {
		assignments[i] = column + "=IF(" + table + "." + guard + "," + mysqlDialect{}.incoming(column) + "," + table + "." + column + ")"
	}
	return mysqlDialect{}.upsert(insert, "", assignments...)
}

func (mysqlDialect) upsertGreatest(insert, _, table, column string) string {
	return mysqlDialect{}.upsert(insert, "", column+"=GREATEST("+table+"."+column+","+mysqlDialect{}.incoming(column)+")")
}

func (mysqlDialect) incoming(column string) string { return mysqlRowAlias + "." + column }

func (mysqlDialect) assignIncoming(columns ...string) string {
	assignments := make([]string, len(columns))
	for i, column := range columns {
		assignments[i] = column + "=" + mysqlDialect{}.incoming(column)
	}
	return strings.Join(assignments, ",")
}

func (mysqlDialect) castInt(expr string) string { return "CAST(" + expr + " AS SIGNED)" }

func (mysqlDialect) concat(parts ...string) string { return "CONCAT(" + strings.Join(parts, ",") + ")" }

// escapeClause: a MySQL string literal reads \\ as one backslash.
func (mysqlDialect) escapeClause() string { return ` ESCAPE '\\'` }

// updateOrdered is the native form: MySQL updates the first rows of an ORDER
// BY ... LIMIT directly, and rejects the subselect-on-itself the other
// dialects use (error 1093).
func (mysqlDialect) updateOrdered(table, _, set, where, order string) string {
	return `UPDATE ` + table + ` SET ` + set + ` WHERE ` + where + ` ORDER BY ` + order + ` LIMIT ?`
}

// deleteWhereIn names the rows through a derived table: MySQL rejects LIMIT
// inside IN (error 1235) and a subselect that reads the table being deleted
// from (1093); a derived table with a LIMIT is materialized first, which
// avoids both.
func (mysqlDialect) deleteWhereIn(table, key, selected string) string {
	return `DELETE FROM ` + table + ` WHERE ` + key + ` IN (SELECT * FROM (` + selected + `) AS picked)`
}

// mysqlMigrationLockName is per database: GET_LOCK names are server-wide, and
// the instance is shared. MD5 keeps the name within the 64 characters MySQL
// allows.
const mysqlMigrationLockName = `CONCAT('panewire-migrate-',MD5(DATABASE()))`

// lockMigration takes a session lock on x's connection, which the caller has
// pinned (a *sql.Conn). The lock outlives a transaction, so release gives it
// back on the same connection.
func (mysqlDialect) lockMigration(ctx context.Context, x sqlExecer) (func() error, error) {
	var got sql.NullInt64
	if err := x.QueryRowContext(ctx, `SELECT GET_LOCK(`+mysqlMigrationLockName+`,?)`, hubMySQLMigrationLockWait).Scan(&got); err != nil {
		return nil, err
	}
	if !got.Valid || got.Int64 != 1 {
		return nil, errors.New("hub database migration lock is held by another hub")
	}
	return func() error {
		// The caller's context may be spent: release with its own.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), hubConnectTimeout)
		defer cancel()
		var released sql.NullInt64
		if err := x.QueryRowContext(ctx, `SELECT RELEASE_LOCK(`+mysqlMigrationLockName+`)`).Scan(&released); err != nil {
			return err
		}
		if !released.Valid || released.Int64 != 1 {
			return errors.New("hub database migration lock was not held")
		}
		return nil
	}, nil
}

// lockLaneCause is a row lock: GET_LOCK is session-scoped and would outlive
// the transaction. The statement inserts the (lane, cause) row, or finds it,
// and either way holds it exclusively until tx ends. A second supersede of the
// same pair waits on the row.
func (mysqlDialect) lockLaneCause(ctx context.Context, tx *sql.Tx, lane, cause string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO supersede_lane_cause(lock_key) VALUES(SHA2(?,256)) AS incoming ON DUPLICATE KEY UPDATE lock_key=supersede_lane_cause.lock_key`, lane+"\x1f"+cause)
	return err
}

// rowsMatched reads the matched count: the DSN sets clientFoundRows, so an
// UPDATE that matches a row and changes nothing still reports it.
func (mysqlDialect) rowsMatched(result sql.Result) (int64, error) { return result.RowsAffected() }

// mysqlEventKeyMax is the size of the event_key column.
const mysqlEventKeyMax = 2048

// mysqlHashedKeyPrefix starts the stored form of a key that does not fit the
// column. 0xFF never starts a hub key (a key begins with a kind name), and a
// key that does start with it is hashed too, so no raw key equals a hashed one.
const mysqlHashedKeyPrefix = "\xffsha256:"

// storedKey binds the raw bytes: event_key is a VARBINARY column. A key longer
// than the column (a producer part such as a report path has no bound of its
// own) is stored as its SHA-256 instead, so it is deduplicated exactly as a
// short key is and never truncated or refused. event_key is only ever matched,
// never read back, so the form is free.
func (mysqlDialect) storedKey(key string) any {
	if len(key) > mysqlEventKeyMax || strings.HasPrefix(key, mysqlHashedKeyPrefix) {
		sum := sha256.Sum256([]byte(key))
		return append([]byte(mysqlHashedKeyPrefix), sum[:]...)
	}
	return []byte(key)
}

func (mysqlDialect) ownsSchema() bool { return false }

// batchOrder is the AUTO_INCREMENT column MySQL needs in place of a rowid.
func (mysqlDialect) batchOrder() string { return "seq" }

func (mysqlDialect) laneInboxInsert() string { return dialectPostgres.laneInboxInsert() }

// beforeInboxPrune does nothing: InnoDB hands out an AUTO_INCREMENT id above
// the largest it ever gave, even after a prune empties the table.
func (mysqlDialect) beforeInboxPrune(context.Context, *sql.Tx) error { return nil }

func (mysqlDialect) loggable(step string, err error) error { return hubDatabaseError(step, err) }

// hubMySQLCAFileMax bounds the CA file the URL names.
const hubMySQLCAFileMax = 64 * 1024

// hubMySQLConfig reads a mysql:// URL of the form
//
//	mysql://user:password@host:port/database?tls-ca=/path/to/ca.pem
//
// into a connection config. tls-ca pins the CA the server's certificate must
// chain to (VERIFY_CA); it is required unless the host is a loopback address,
// where a test database needs no TLS. No other query parameter is accepted: a
// URL can neither relax the TLS check nor widen the connection cap. The error
// carries no part of the URL.
func hubMySQLConfig(raw string) (*mysql.Config, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "mysql" || u.Hostname() == "" || u.Fragment != "" || u.Opaque != "" {
		return nil, errHubDatabaseURL
	}
	name := strings.TrimPrefix(u.Path, "/")
	if name == "" || strings.Contains(name, "/") {
		return nil, errHubDatabaseURL
	}
	query := u.Query()
	for key := range query {
		if key != "tls-ca" {
			return nil, errHubDatabaseURL
		}
	}
	port := u.Port()
	if port == "" {
		port = "3306"
	}
	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(u.Hostname(), port)
	cfg.DBName = name
	if u.User != nil {
		cfg.User = u.User.Username()
		cfg.Passwd, _ = u.User.Password()
	}
	// RowsAffected counts matched rows, which every compare-and-set in the
	// hub reads as "the guarded row was there" (sqlDialect.rowsMatched).
	cfg.ClientFoundRows = true
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	cfg.Collation = "utf8mb4_bin"
	// Only the dial is bounded here: every store call carries its own
	// deadline (laneInboxOpTimeout, the start-up budget), and a socket
	// timeout would cut a migration's long statement or a lock wait short.
	cfg.Timeout = hubConnectTimeout
	cfg.Params = map[string]string{
		// Statements outside a transaction run at the level the
		// transactions state explicitly (mysqlDialect.begin).
		"transaction_isolation": "'READ-COMMITTED'",
		"time_zone":             "'+00:00'",
		// An explicit strict mode, the server default minus nothing: an
		// oversized or NULL value is an error, never a truncation, whatever the
		// shared instance's own mode is. NO_BACKSLASH_ESCAPES stays out: the
		// LIKE escape (mysqlDialect.escapeClause) reads a backslash as one.
		"sql_mode": "'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'",
	}
	if caFile := query.Get("tls-ca"); caFile != "" {
		if cfg.TLS, err = hubMySQLPinnedCA(caFile); err != nil {
			return nil, err
		}
	} else if !hubMySQLLoopback(u.Hostname()) {
		return nil, errHubDatabaseURL
	}
	return cfg, nil
}

// hubMySQLLoopback is true for the host names of this machine only. A name
// that resolves elsewhere is not loopback.
func hubMySQLLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	addr, err := netip.ParseAddr(host)
	return err == nil && addr.IsLoopback()
}

var errHubMySQLCA = errors.New("hub database CA file is unusable")

// hubMySQLPinnedCA reads the CA the instance's server certificate is signed
// by. The HeatWave certificate has no SAN (hk 4246), so Go's hostname check
// cannot pass; VerifyConnection replaces it with the chain check against this
// pool alone, so the certificate is still verified (VERIFY_CA).
func hubMySQLPinnedCA(path string) (*tls.Config, error) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > hubMySQLCAFileMax {
		return nil, errHubMySQLCA
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errHubMySQLCA
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, hubMySQLCAFileMax+1))
	if err != nil || len(contents) > hubMySQLCAFileMax {
		return nil, errHubMySQLCA
	}
	roots := x509.NewCertPool()
	count := 0
	for len(bytes.TrimSpace(contents)) > 0 {
		block, rest := pem.Decode(bytes.TrimSpace(contents))
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, errHubMySQLCA
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !cert.IsCA || !cert.BasicConstraintsValid {
			return nil, errHubMySQLCA
		}
		roots.AddCert(cert)
		count++
		contents = rest
	}
	if count == 0 {
		return nil, errHubMySQLCA
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		// Verification is not skipped: Go's own check names the host, which
		// the certificate does not carry, so VerifyConnection does the chain
		// check instead.
		InsecureSkipVerify: true, //nolint:gosec
		VerifyConnection:   hubMySQLVerifyChain(roots),
	}, nil
}

// hubMySQLVerifyChain verifies the server's certificate chain against roots,
// for server authentication, at the current time. It does not compare names.
func hubMySQLVerifyChain(roots *x509.CertPool) func(tls.ConnectionState) error {
	return func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return errors.New("hub database certificate verification failed")
		}
		intermediates := x509.NewCertPool()
		for _, cert := range state.PeerCertificates[1:] {
			intermediates.AddCert(cert)
		}
		if _, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{
			Roots: roots, Intermediates: intermediates,
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}); err != nil {
			return errors.New("hub database certificate verification failed")
		}
		return nil
	}
}

// openLaneInboxStoreMySQL opens the hub store on MySQL and applies the
// embedded migrations.
func openLaneInboxStoreMySQL(ctx context.Context, dsn string) (*laneInboxStore, error) {
	config, err := hubMySQLConfig(dsn)
	if err != nil {
		return nil, err
	}
	return openLaneInboxStoreMySQLConfig(ctx, config)
}

func openLaneInboxStoreMySQLConfig(ctx context.Context, config *mysql.Config) (*laneInboxStore, error) {
	db, err := openHubMySQL(ctx, config)
	if err != nil {
		return nil, err
	}
	ctx, cancel := hubMigrationContext(ctx)
	defer cancel()
	if err := migrateHubMySQL(ctx, db); err != nil {
		db.Close()
		return nil, hubDatabaseError("migrate", err)
	}
	return newLaneInboxStore(db, dialectMySQL), nil
}

// openHubMySQL connects without touching the schema.
func openHubMySQL(ctx context.Context, config *mysql.Config) (*sql.DB, error) {
	connector, err := mysql.NewConnector(config)
	if err != nil {
		return nil, errHubDatabaseURL
	}
	db := sql.OpenDB(hubConnector{connector})
	db.SetMaxOpenConns(hubMySQLMaxConns)
	db.SetMaxIdleConns(4)
	db.SetConnMaxIdleTime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(ctx, hubConnectTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, hubDatabaseError("connect", err)
	}
	// Producer ACKs follow the commit, so the server must flush its log at
	// every commit, as the SQLite store's FULL synchronous commits do and as
	// Postgres' synchronous_commit=on is set per session. MySQL has no
	// session setting for it: the hub reads the server's and refuses to open
	// on a relaxed one.
	var flush int
	if err := db.QueryRowContext(ctx, `SELECT @@GLOBAL.innodb_flush_log_at_trx_commit`).Scan(&flush); err != nil {
		db.Close()
		return nil, hubDatabaseError("durability check", err)
	}
	if flush != 1 {
		db.Close()
		return nil, errors.New("hub database does not flush its log at every commit (innodb_flush_log_at_trx_commit is not 1)")
	}
	return db, nil
}

// hubMySQLLedger records the files applied. DDL commits implicitly, so the
// ledger row is written after the statement; applied-if closes the gap.
const hubMySQLLedger = `CREATE TABLE IF NOT EXISTS panewire_schema_migrations (
 version VARCHAR(190) NOT NULL, applied_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 PRIMARY KEY (version)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`

// migrateHubMySQL applies the embedded migrations not yet recorded, on one
// pinned connection holding the migration lock.
func migrateHubMySQL(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	release, err := dialectMySQL.lockMigration(ctx, conn)
	if err != nil {
		return err
	}
	err = migrateHubMySQLConn(ctx, conn)
	if releaseErr := release(); releaseErr != nil {
		// A lock that may still be held must not return to the pool with its
		// session: closing the session frees it.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		if err == nil {
			err = releaseErr
		}
	}
	return err
}

func migrateHubMySQLConn(ctx context.Context, conn *sql.Conn) error {
	names, err := fs.Glob(hubMySQLMigrations, "hub_mysql_migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	if _, err := conn.ExecContext(ctx, hubMySQLLedger); err != nil {
		return err
	}
	for _, name := range names {
		version := strings.TrimSuffix(strings.TrimPrefix(name, "hub_mysql_migrations/"), ".sql")
		var applied int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM panewire_schema_migrations WHERE version=?`, version).Scan(&applied); err != nil {
			return err
		}
		if applied > 0 {
			continue
		}
		body, err := hubMySQLMigrations.ReadFile(name)
		if err != nil {
			return err
		}
		migration, err := parseHubMySQLMigration(string(body))
		if err != nil {
			return fmt.Errorf("migration %s: %w", version, err)
		}
		var done int
		if err := conn.QueryRowContext(ctx, migration.appliedIf).Scan(&done); err != nil {
			return err
		}
		if done == 0 {
			// No arguments: the driver sends the statement as text.
			if _, err := conn.ExecContext(ctx, migration.statement); err != nil {
				return err
			}
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO panewire_schema_migrations(version) VALUES(?)`, version); err != nil {
			return err
		}
	}
	return nil
}

// hubMySQLMigration is one migration file: a guard and one statement.
type hubMySQLMigration struct {
	// appliedIf is a SELECT of one count; a count above zero means the
	// statement's effect is already in the database.
	appliedIf string
	// statement is the one DDL or data statement.
	statement string
}

const appliedIfPrefix = "-- applied-if:"

// parseHubMySQLMigration splits a migration file into its guard and its one
// statement, and refuses a file that is neither: MySQL commits each DDL
// statement as it runs, so a file of two statements that stops between them
// leaves a half-applied file no ledger row describes.
func parseHubMySQLMigration(body string) (hubMySQLMigration, error) {
	var m hubMySQLMigration
	var code []string
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, appliedIfPrefix):
			if m.appliedIf != "" {
				return m, errors.New("more than one applied-if guard")
			}
			m.appliedIf = strings.TrimSpace(strings.TrimPrefix(line, appliedIfPrefix))
		case strings.HasPrefix(line, "--") || line == "":
		default:
			code = append(code, line)
		}
	}
	if !strings.HasPrefix(strings.ToUpper(m.appliedIf), "SELECT ") {
		return m, errors.New("no applied-if SELECT guard")
	}
	statement := strings.TrimSpace(strings.Join(code, "\n"))
	statement = strings.TrimSpace(strings.TrimSuffix(statement, ";"))
	if statement == "" {
		return m, errors.New("no statement")
	}
	if strings.Contains(statement, ";") || strings.Contains(m.appliedIf, ";") {
		return m, errors.New("more than one statement")
	}
	m.statement = statement
	switch word, _, _ := strings.Cut(strings.ToUpper(statement), " "); word {
	case "CREATE", "ALTER", "DROP":
		if !strings.Contains(strings.ToLower(m.appliedIf), "information_schema") {
			return m, errors.New("a schema statement's guard must read information_schema")
		}
	}
	return m, nil
}

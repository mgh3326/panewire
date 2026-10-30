package panewire

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// hubDatabaseURLEnv names the hub's Postgres DSN. When it is unset the hub
// keeps the SQLite inbox store; see docs/hub-postgres.md. The value carries a
// credential, so it is never logged or echoed in an error.
const hubDatabaseURLEnv = "PANEWIRE_HUB_DATABASE_URL"

// hubPostgresMigrations are applied in file-name order, each at most once,
// inside one transaction under an advisory lock, so concurrent hub starts and
// re-runs converge on the same schema.
//
//go:embed hub_postgres_migrations/*.sql
var hubPostgresMigrations embed.FS

// hubPostgresMigrationLock is the pg_advisory_xact_lock key that serializes
// schema migration across processes sharing one database.
const hubPostgresMigrationLock = 0x70616e6577697265 // "panewire"

const hubPostgresConnectTimeout = 10 * time.Second

// errHubDatabaseURL deliberately carries no detail: pgx parse errors can
// quote parts of the connection string.
var errHubDatabaseURL = errors.New("hub database URL is invalid")

// openLaneInboxStorePostgres opens the hub inbox on Postgres and applies the
// embedded migrations.
func openLaneInboxStorePostgres(ctx context.Context, dsn string) (*laneInboxStore, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, errHubDatabaseURL
	}
	return openLaneInboxStorePostgresConfig(ctx, config)
}

func openLaneInboxStorePostgresConfig(ctx context.Context, config *pgx.ConnConfig) (*laneInboxStore, error) {
	db, err := openHubPostgres(ctx, config)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, hubPostgresConnectTimeout)
	defer cancel()
	if err := migrateHubPostgres(ctx, db); err != nil {
		db.Close()
		return nil, hubDatabaseError("migrate", err)
	}
	return &laneInboxStore{db: db, dialect: laneInboxPostgres, notify: make(chan struct{})}, nil
}

// openHubPostgres connects without touching the schema.
func openHubPostgres(ctx context.Context, config *pgx.ConnConfig) (*sql.DB, error) {
	// Producer ACKs follow the commit, so a DSN must not be able to relax
	// durability below the SQLite store's FULL synchronous commits.
	config.RuntimeParams["synchronous_commit"] = "on"
	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(4)
	db.SetConnMaxIdleTime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(ctx, hubPostgresConnectTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, hubDatabaseError("connect", err)
	}
	return db, nil
}

// hubDatabaseError reduces a driver error to the step and, for a server
// error, its SQLSTATE. Connection errors from pgx name the user, host and
// database, which this repository does not print.
func hubDatabaseError(step string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return fmt.Errorf("hub database %s failed: SQLSTATE %s", step, pgErr.Code)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("hub database %s failed: timeout", step)
	}
	return fmt.Errorf("hub database %s failed", step)
}

func migrateHubPostgres(ctx context.Context, db *sql.DB) error {
	names, err := fs.Glob(hubPostgresMigrations, "hub_postgres_migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(hubPostgresMigrationLock)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS panewire_schema_migrations (
 version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	for _, name := range names {
		version := strings.TrimSuffix(strings.TrimPrefix(name, "hub_postgres_migrations/"), ".sql")
		var applied bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM panewire_schema_migrations WHERE version=$1)`, version).Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}
		body, err := hubPostgresMigrations.ReadFile(name)
		if err != nil {
			return err
		}
		// No arguments: pgx runs this over the simple protocol, which
		// accepts a multi-statement migration file.
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO panewire_schema_migrations(version) VALUES($1)`, version); err != nil {
			return err
		}
	}
	return tx.Commit()
}

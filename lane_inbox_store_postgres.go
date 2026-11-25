package panewire

import (
	"context"
	"database/sql"
	"embed"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// hubPostgresMigrations are applied in file-name order, each at most once,
// inside one transaction under an advisory lock, so concurrent hub starts and
// re-runs converge on the same schema.
//
//go:embed hub_postgres_migrations/*.sql
var hubPostgresMigrations embed.FS

// hubPostgresMigrationLock is the pg_advisory_xact_lock key that serializes
// schema migration across processes sharing one database.
const hubPostgresMigrationLock = 0x70616e6577697265 // "panewire"

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
	ctx, cancel := hubMigrationContext(ctx)
	defer cancel()
	marked, err := migrateHubPostgres(ctx, db)
	if err != nil {
		db.Close()
		return nil, hubDatabaseError("migrate", err)
	}
	store := newLaneInboxStore(db, dialectPostgres)
	store.succeededNoticesMarked = marked
	if err := store.seedDeliveryReconcilerState(ctx); err != nil {
		store.close()
		return nil, hubDatabaseError("seed", err)
	}
	return store, nil
}

// openHubPostgres connects without touching the schema.
func openHubPostgres(ctx context.Context, config *pgx.ConnConfig) (*sql.DB, error) {
	// Producer ACKs follow the commit, so a DSN must not be able to relax
	// durability below the SQLite store's FULL synchronous commits.
	config.RuntimeParams["synchronous_commit"] = "on"
	db := sql.OpenDB(hubConnector{stdlib.GetConnector(*config)})
	db.SetMaxOpenConns(4)
	db.SetConnMaxIdleTime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(ctx, hubConnectTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, hubDatabaseError("connect", err)
	}
	return db, nil
}

// migrateHubPostgres commits the migrations not yet recorded. marked is the
// rows the succeeded-notice backfill updated, when this run applied it.
func migrateHubPostgres(ctx context.Context, db *sql.DB) (*int64, error) {
	tx, err := dialectPostgres.begin(ctx, db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	marked, err := migrateHubPostgresTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return marked, nil
}

// migrateHubPostgresTx applies the embedded migrations not yet recorded, on
// the caller's transaction: the hub commits them alone.
func migrateHubPostgresTx(ctx context.Context, tx *sql.Tx) (marked *int64, err error) {
	names, err := fs.Glob(hubPostgresMigrations, "hub_postgres_migrations/*.sql")
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	if _, err := dialectPostgres.lockMigration(ctx, tx); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS panewire_schema_migrations (
 version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return nil, err
	}
	for _, name := range names {
		version := strings.TrimSuffix(strings.TrimPrefix(name, "hub_postgres_migrations/"), ".sql")
		var applied bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM panewire_schema_migrations WHERE version=$1)`, version).Scan(&applied); err != nil {
			return nil, err
		}
		if applied {
			continue
		}
		body, err := hubPostgresMigrations.ReadFile(name)
		if err != nil {
			return nil, err
		}
		// No arguments: pgx runs this over the simple protocol, which
		// accepts a multi-statement migration file.
		result, err := tx.ExecContext(ctx, string(body))
		if err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO panewire_schema_migrations(version) VALUES($1)`, version); err != nil {
			return nil, err
		}
		if strings.HasSuffix(version, succeededNoticeMigration) {
			rows, err := dialectPostgres.rowsMatched(result)
			if err != nil {
				return nil, err
			}
			marked = &rows
		}
	}
	return marked, nil
}

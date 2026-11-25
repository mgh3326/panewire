package panewire

// The store copier takes its rows as they are stored, so a source must already
// hold the one-time succeeded-notice backfill (hk 6283, hub_spawn_notice_recorded.go).
// A source that has not run it would copy unmarked succeeded notices, and a
// target migrated after that copy would never mark them. The copier refuses
// such a source before it touches the target.

import (
	"context"
	"errors"
	"strings"
)

// errStoreCopySourceNotBackfilled names the operator's step: the source hub's
// own start on this build runs the backfill (migrations on Postgres and MySQL,
// ensureSpawnTasks on SQLite).
var errStoreCopySourceNotBackfilled = errors.New("source has not run the succeeded-notice backfill (hk 6283): start the source hub once on this build, then retry")

// storeCopySourceBackfilled reports whether the source's own schema records the
// backfill: its migration ledger holds the backfill file (Postgres 0039, MySQL
// 0032), or on SQLite user_version reached sqliteSucceededNoticeVersion. The
// copier never migrates a Postgres or MySQL source, so only the source's own
// start can have run it.
func storeCopySourceBackfilled(ctx context.Context, src *storeCopyDB) (bool, error) {
	if src.sqlite {
		var version int
		if err := src.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
			return false, hubDatabaseError("inspect source backfill", err)
		}
		return version >= sqliteSucceededNoticeVersion, nil
	}
	var ledger bool
	var err error
	if src.dialect == dialectPostgres {
		err = src.db.QueryRowContext(ctx, `SELECT to_regclass('panewire_schema_migrations') IS NOT NULL`).Scan(&ledger)
	} else {
		err = src.db.QueryRowContext(ctx, `SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='panewire_schema_migrations'`).Scan(&ledger)
	}
	if err != nil {
		return false, hubDatabaseError("inspect source ledger", err)
	}
	if !ledger {
		return false, nil
	}
	rows, err := src.db.QueryContext(ctx, `SELECT version FROM panewire_schema_migrations`)
	if err != nil {
		return false, hubDatabaseError("read source ledger", err)
	}
	defer rows.Close()
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return false, hubDatabaseError("read source ledger", err)
		}
		if strings.HasSuffix(version, succeededNoticeMigration) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, hubDatabaseError("read source ledger", err)
	}
	return false, nil
}

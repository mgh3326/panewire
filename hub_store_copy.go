package panewire

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// storeCopyTables is the portable hub state, in primary-key order. The
// migration ledger is target-specific; supersede_lane_cause contains only
// MySQL's transient lock rows and has no Postgres counterpart.
type storeCopyTable struct{ name, key string }

var storeCopyTables = []storeCopyTable{
	{"lane_inbox", "id"}, {"lane_inbox_batches", "id"}, {"lane_inbox_batch_quarantine", "batch_id"},
	{"lane_inbox_surfaces", "delivery_key"}, {"spawn_operations", "request_id"},
	{"spawn_reservations", "job"}, {"spawn_notifications", "event_id"}, {"drain_wake_lanes", "lane"},
	{"spawn_placement_snapshots", "request_id,attempt"}, {"spawn_attempt_grants", "node_id"},
	{"quota_refresh_attempts", "machine,pool"}, {"consult_requests", "id"},
	{"spawn_attempt_start_by", "node_id"}, {"pool_actions", "id"}, {"account_credit_marks", "account_key"},
	{"supersede_fence", "singleton"}, {"supersede_landings", "request_id"},
	{"one_time_action_notices", "machine,kind,need"}, {"placement_record_outbox", "request_id,attempt"},
	{"spawn_notice_outbox", "event_id"}, {"spawn_queue", "request_id"},
	{"task_fail_notice_outbox", "request_id"}, {"owner_notice_decisions", "event_key"},
}

const storeCopyBatchSize = 256

type storeCopyDB struct {
	db      *sql.DB
	dialect sqlDialect
	sqlite  bool
}

func openStoreCopyDB(ctx context.Context, raw string, source bool) (*storeCopyDB, error) {
	if strings.HasPrefix(raw, "sqlite://") {
		if !source {
			return nil, errors.New("SQLite is supported only as a source")
		}
		u, err := url.Parse(raw)
		if err != nil || u.Host != "" || u.Path == "" || u.RawQuery != "" {
			return nil, errHubDatabaseURL
		}
		u.Scheme = "file"
		u.RawQuery = "mode=ro"
		db, err := sql.Open("sqlite", u.String())
		if err != nil {
			return nil, hubDatabaseError("open source", err)
		}
		db.SetMaxOpenConns(1)
		if err = db.PingContext(ctx); err != nil {
			db.Close()
			return nil, hubDatabaseError("open source", err)
		}
		return &storeCopyDB{db: db, dialect: dialectSQLite, sqlite: true}, nil
	}
	if strings.HasPrefix(strings.ToLower(raw), "mysql://") {
		cfg, err := hubMySQLConfig(raw)
		if err != nil {
			return nil, err
		}
		db, err := openHubMySQL(ctx, cfg)
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(1)
		return &storeCopyDB{db: db, dialect: dialectMySQL}, nil
	}
	cfg, err := pgx.ParseConfig(raw)
	if err != nil {
		return nil, errHubDatabaseURL
	}
	db, err := openHubPostgres(ctx, cfg)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return &storeCopyDB{db: db, dialect: dialectPostgres}, nil
}

func runHubStoreCopyCLI(args []string, out, stderr io.Writer) int {
	fs := flag.NewFlagSet("hub store-copy", flag.ContinueOnError)
	fs.SetOutput(stderr)
	from := fs.String("from", "", "source URL")
	to := fs.String("to", "", "target URL")
	tables := fs.String("tables", "", "comma-separated tables")
	verify := fs.Bool("verify-only", false, "compare without copying")
	replace := fs.Bool("replace", false, "replace target rows")
	if fs.Parse(args) != nil || *from == "" || *to == "" || fs.NArg() != 0 || (*verify && *replace) {
		fmt.Fprintln(stderr, "store-copy: invalid arguments")
		return ExitConditionInvalid
	}
	if err := copyHubStore(context.Background(), *from, *to, *tables, *verify, *replace, out); err != nil {
		fmt.Fprintln(stderr, "store-copy:", err)
		return ExitInternal
	}
	return 0
}

func copyHubStore(ctx context.Context, from, to, selection string, verify, replace bool, out io.Writer) error {
	selected := make(map[string]bool)
	if selection != "" {
		for _, s := range strings.Split(selection, ",") {
			selected[strings.TrimSpace(s)] = true
		}
	}
	var tables []storeCopyTable
	for _, table := range storeCopyTables {
		if selection == "" || selected[table.name] {
			tables = append(tables, table)
			delete(selected, table.name)
		}
	}
	if len(selected) != 0 || len(tables) == 0 {
		return errors.New("unknown or empty table selection")
	}
	src, err := openStoreCopyDB(ctx, from, true)
	if err != nil {
		return err
	}
	defer src.db.Close()
	dst, err := openStoreCopyDB(ctx, to, false)
	if err != nil {
		return err
	}
	defer dst.db.Close()
	if src.dialect == dialectPostgres && dst.dialect == dialectPostgres || src.dialect == dialectMySQL && dst.dialect == dialectMySQL {
		return errors.New("source and target must use different backends")
	}
	var fenceExists bool
	if dst.dialect == dialectPostgres {
		err = dst.db.QueryRowContext(ctx, `SELECT to_regclass('supersede_fence') IS NOT NULL`).Scan(&fenceExists)
	} else {
		err = dst.db.QueryRowContext(ctx, `SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='supersede_fence'`).Scan(&fenceExists)
	}
	if err != nil {
		return hubDatabaseError("inspect target schema", err)
	}
	if dst.dialect == dialectPostgres {
		err = migrateHubPostgres(ctx, dst.db)
	} else {
		err = migrateHubMySQL(ctx, dst.db)
	}
	if err != nil {
		return hubDatabaseError("migrate target", err)
	}
	source, err := src.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return hubDatabaseError("begin source snapshot", err)
	}
	defer source.Rollback()
	target, err := dst.db.BeginTx(ctx, nil)
	if err != nil {
		return hubDatabaseError("begin target", err)
	}
	defer target.Rollback()
	// Check every selected table before touching any of them.
	if !verify {
		for _, table := range tables {
			var count int64
			if err = target.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table.name).Scan(&count); err != nil {
				return hubDatabaseError("inspect "+table.name, err)
			}
			// Both schema migrations seed the initial fence. It is initialization, not
			// an existing hub's state, only when it still has its initial value.
			if table.name == "supersede_fence" && count == 1 && !fenceExists {
				var generation int64
				if err = target.QueryRowContext(ctx, "SELECT generation FROM supersede_fence WHERE singleton=1").Scan(&generation); err != nil {
					return hubDatabaseError("inspect fence", err)
				}
				if generation == 1 {
					count = 0
				}
			}
			if count != 0 && !replace {
				return fmt.Errorf("target table %s is non-empty (use --replace)", table.name)
			}
		}
	}
	for _, table := range tables {
		columns, err := storeCopyColumns(ctx, target, table.name)
		if err != nil {
			return hubDatabaseError("columns "+table.name, err)
		}
		if !verify {
			if _, err = target.ExecContext(ctx, "DELETE FROM "+table.name); err != nil {
				return hubDatabaseError("clear "+table.name, err)
			}
		}
		sourceResult, err := storeCopyScan(ctx, source, src, dst, target, table, columns, !verify)
		if err != nil {
			return fmt.Errorf("%s: %w", table.name, err)
		}
		targetResult, err := storeCopyScan(ctx, target, dst, dst, nil, table, columns, false)
		if err != nil {
			return fmt.Errorf("%s: %w", table.name, err)
		}
		fmt.Fprintf(out, "%s source=%d sha256=%x target=%d sha256=%x oversized_keys=%d\n", table.name, sourceResult.count, sourceResult.sum, targetResult.count, targetResult.sum, sourceResult.oversized)
		if sourceResult.count != targetResult.count || sourceResult.sum != targetResult.sum {
			return fmt.Errorf("%s checksum or count mismatch", table.name)
		}
	}
	if verify {
		return nil
	}
	highWater, err := storeCopyHighWater(ctx, source, src, tables)
	if err != nil {
		return err
	}
	if err = target.Commit(); err != nil {
		return hubDatabaseError("commit target", err)
	}
	return resetStoreCopySequences(ctx, dst, tables, highWater)
}

func storeCopyColumns(ctx context.Context, tx *sql.Tx, table string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, "SELECT * FROM "+table+" WHERE 1=0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return rows.Columns()
}

type storeCopyResult struct {
	count, oversized int64
	sum              [32]byte
}

func storeCopyScan(ctx context.Context, tx *sql.Tx, source, target *storeCopyDB, writer *sql.Tx, table storeCopyTable, cols []string, copyRows bool) (storeCopyResult, error) {
	var result storeCopyResult
	h := sha256.New()
	queryCols := append([]string(nil), cols...)
	if source.sqlite && table.name == "lane_inbox_batches" {
		for i, c := range queryCols {
			if c == "seq" {
				queryCols[i] = "rowid"
			}
		}
	}
	parts := strings.Split(table.key, ",")
	for i, p := range parts {
		numeric := p == "attempt" || p == "singleton" || (table.name == "lane_inbox" && p == "id")
		if !numeric && source.dialect == dialectPostgres {
			parts[i] = p + ` COLLATE "C"`
		} else if !numeric && source.dialect == dialectMySQL {
			parts[i] = "BINARY " + p
		}
	}
	order := strings.Join(parts, ",")
	query := "SELECT " + strings.Join(queryCols, ",") + " FROM " + table.name + " ORDER BY " + order + " LIMIT ? OFFSET ?"
	insert := "INSERT INTO " + table.name + " (" + strings.Join(cols, ",") + ") "
	if target.dialect == dialectPostgres && table.name == "lane_inbox_batches" {
		insert += "OVERRIDING SYSTEM VALUE "
	}
	insert += "VALUES (" + strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",") + ")"
	for offset := 0; ; offset += storeCopyBatchSize {
		rows, err := tx.QueryContext(ctx, source.dialect.rebind(query), storeCopyBatchSize, offset)
		if err != nil {
			return result, hubDatabaseError("read", err)
		}
		batch := make([][]any, 0, storeCopyBatchSize)
		for rows.Next() {
			vals := make([]any, len(cols))
			ptr := make([]any, len(cols))
			for i := range vals {
				ptr[i] = &vals[i]
			}
			if err := rows.Scan(ptr...); err != nil {
				rows.Close()
				return result, hubDatabaseError("scan", err)
			}
			for i, v := range vals {
				if b, ok := v.([]byte); ok {
					vals[i] = append([]byte(nil), b...)
				}
			}
			batch = append(batch, vals)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return result, hubDatabaseError("read", err)
		}
		for _, vals := range batch {
			for i, col := range cols {
				if table.name == "lane_inbox" && col == "event_key" {
					raw := storeCopyBytes(vals[i])
					if source.dialect == dialectPostgres {
						raw, err = hex.DecodeString(string(raw))
						if err != nil {
							return result, errors.New("invalid PostgreSQL event key")
						}
					}
					if len(raw) > mysqlEventKeyMax {
						raw = dialectMySQL.storedKey(string(raw)).([]byte)
					}
					if strings.HasPrefix(string(raw), mysqlHashedKeyPrefix) {
						result.oversized++
					}
					if target.dialect == dialectPostgres {
						vals[i] = hex.EncodeToString(raw)
					} else {
						vals[i] = raw
					}
				}
			}
			storeCopyHashRow(h, vals, table, cols, target)
			if copyRows {
				if _, err := writer.ExecContext(ctx, target.dialect.rebind(insert), vals...); err != nil {
					return result, hubDatabaseError("insert", err)
				}
			}
			result.count++
		}
		if len(batch) < storeCopyBatchSize {
			break
		}
	}
	copy(result.sum[:], h.Sum(nil))
	return result, nil
}
func storeCopyBytes(v any) []byte {
	switch v := v.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	default:
		return []byte(fmt.Sprint(v))
	}
}
func storeCopyHashRow(h hash.Hash, vals []any, table storeCopyTable, cols []string, dialect *storeCopyDB) {
	for i, v := range vals {
		if v == nil {
			h.Write([]byte{0})
			continue
		}
		h.Write([]byte{1})
		b := storeCopyBytes(v)
		if table.name == "lane_inbox" && cols[i] == "event_key" && dialect.dialect == dialectPostgres {
			b, _ = hex.DecodeString(string(b))
		}
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(b)))
		h.Write(size[:])
		h.Write(b)
	}
}
func storeCopyHighWater(ctx context.Context, tx *sql.Tx, source *storeCopyDB, tables []storeCopyTable) (map[string]int64, error) {
	result := make(map[string]int64)
	for _, t := range tables {
		if t.name != "lane_inbox" && t.name != "lane_inbox_batches" {
			continue
		}
		col := "id"
		if t.name == "lane_inbox_batches" {
			col = "seq"
		}
		var n sql.NullInt64
		var err error
		switch {
		case source.sqlite && t.name == "lane_inbox":
			err = tx.QueryRowContext(ctx, `SELECT floor FROM lane_inbox_id_floor WHERE singleton=1`).Scan(&n)
		case source.sqlite:
			continue
		case source.dialect == dialectPostgres:
			err = tx.QueryRowContext(ctx, `SELECT pg_sequence_last_value(pg_get_serial_sequence($1,$2)::regclass)`, t.name, col).Scan(&n)
		default:
			err = tx.QueryRowContext(ctx, `SELECT AUTO_INCREMENT-1 FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=?`, t.name).Scan(&n)
		}
		if err != nil {
			return nil, hubDatabaseError("read sequence "+t.name, err)
		}
		if n.Valid {
			result[t.name] = n.Int64
		}
	}
	return result, nil
}

func resetStoreCopySequences(ctx context.Context, dst *storeCopyDB, tables []storeCopyTable, highWater map[string]int64) error {
	for _, t := range tables {
		if t.name != "lane_inbox" && t.name != "lane_inbox_batches" {
			continue
		}
		col := "id"
		if t.name == "lane_inbox_batches" {
			col = "seq"
		}
		if dst.dialect == dialectPostgres {
			q := "SELECT setval(pg_get_serial_sequence('" + t.name + "','" + col + "'), GREATEST(COALESCE(MAX(" + col + "),0)," + strconv.FormatInt(highWater[t.name], 10) + ",1), GREATEST(COALESCE(MAX(" + col + "),0)," + strconv.FormatInt(highWater[t.name], 10) + ")>0) FROM " + t.name
			if _, err := dst.db.ExecContext(ctx, q); err != nil {
				return hubDatabaseError("reset "+t.name, err)
			}
		} else {
			var next int64
			if err := dst.db.QueryRowContext(ctx, "SELECT COALESCE(MAX("+col+"),0)+1 FROM "+t.name).Scan(&next); err != nil {
				return hubDatabaseError("reset "+t.name, err)
			}
			if next <= highWater[t.name] {
				next = highWater[t.name] + 1
			}
			if _, err := dst.db.ExecContext(ctx, "ALTER TABLE "+t.name+" AUTO_INCREMENT="+strconv.FormatInt(next, 10)); err != nil {
				return hubDatabaseError("reset "+t.name, err)
			}
		}
	}
	return nil
}

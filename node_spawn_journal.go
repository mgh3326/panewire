package panewire

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Entries are retained as deduplication tombstones. A restart with an executing
// entry never reruns it: there may already be a live pane without a result.
// Completion is fsynced before any response; replay also recovers a lost ACK.
type nodeSpawnJournal struct{ db *sql.DB }

func openNodeSpawnJournal(root string) (*nodeSpawnJournal, error) {
	if root == "" {
		return nil, errors.New("spawn journal directory required")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	path := filepath.Join(root, "spawn-inbox.sqlite3")
	if info, err := os.Lstat(path); err == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return nil, errors.New("unsafe spawn journal")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	err = f.Chmod(0600)
	closeErr := f.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, q := range []string{`PRAGMA busy_timeout=5000`, `PRAGMA journal_mode=WAL`, `PRAGMA synchronous=FULL`, `CREATE TABLE IF NOT EXISTS spawn_inbox (request_id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,status TEXT NOT NULL,result TEXT NOT NULL DEFAULT '')`} {
		if _, err = db.ExecContext(ctx, q); err != nil {
			db.Close()
			return nil, err
		}
	}
	// job_id and label name what an executing row launched, so a restart can
	// settle it from the pane (hub_spawn_settle.go). Rows written before the
	// columns existed keep an empty value and settle as lost.
	for _, column := range []string{"job_id", "label"} {
		var present int
		if err = db.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('spawn_inbox') WHERE name=?`, column).Scan(&present); err == nil && present == 0 {
			_, err = db.ExecContext(ctx, `ALTER TABLE spawn_inbox ADD COLUMN `+column+` TEXT NOT NULL DEFAULT ''`)
		}
		if err != nil {
			db.Close()
			return nil, err
		}
	}
	// strict marks a row whose job_id and label came from the strict
	// flag/value pairs. A row an older build wrote is 0: its identity may name
	// a forged --purpose value, so the takeover proof holds while any such row
	// is still executing (hk 4799).
	var strictPresent int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('spawn_inbox') WHERE name='strict'`).Scan(&strictPresent); err == nil && strictPresent == 0 {
		_, err = db.ExecContext(ctx, `ALTER TABLE spawn_inbox ADD COLUMN strict INTEGER NOT NULL DEFAULT 0`)
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	// sparse_hash and checkout_mode preserve the sparse checkout identity and
	// execution mode durably across capability filtering, restart and replay
	// (hk 5104 / hk 5211).
	for _, column := range []string{"sparse_hash", "checkout_mode"} {
		var present int
		if err = db.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('spawn_inbox') WHERE name=?`, column).Scan(&present); err == nil && present == 0 {
			_, err = db.ExecContext(ctx, `ALTER TABLE spawn_inbox ADD COLUMN `+column+` TEXT NOT NULL DEFAULT ''`)
		}
		if err != nil {
			db.Close()
			return nil, err
		}
	}
	return &nodeSpawnJournal{db}, nil
}
func sparseListsHash(checkout *hubSpawnCheckout) string {
	if checkout == nil || (len(checkout.Exclude) == 0 && len(checkout.Include) == 0) {
		return ""
	}
	raw, _ := json.Marshal(struct {
		Exclude []string `json:"exclude,omitempty"`
		Include []string `json:"include,omitempty"`
	}{Exclude: checkout.Exclude, Include: checkout.Include})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func spawnFingerprint(message hubOutboundMessage) string {
	// Only execution fields contribute; transport/reconnect metadata does not.
	// The sparse lists are not part of the identity: the hub adds or drops them
	// with the target node's checkout_sparse_v1 capability, so a replay across
	// that node's upgrade or rollback must still match its journaled request.
	// They change what is on disk, never which commit, directory or origin the
	// job runs in (hk 5098 review).
	checkout := message.Checkout
	if checkout != nil && (len(checkout.Exclude) != 0 || len(checkout.Include) != 0) {
		stripped := *checkout
		stripped.Exclude, stripped.Include = nil, nil
		checkout = &stripped
	}
	raw, _ := json.Marshal(struct {
		ID, CWD, Inline, Doc string
		Args                 []string
		Checkout             *hubSpawnCheckout
		Wait                 int
		Deadline             int64
	}{message.RequestID, message.CWDKey, message.BriefInline, message.BriefDoc, message.Args, checkout, message.WaitSeconds, message.DeadlineMS})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (j *nodeSpawnJournal) claim(ctx context.Context, message hubOutboundMessage) (bool, *hubSpawnResult, error) {
	// The journal's identity columns are read back by the takeover proof and
	// by restart settlement, so they take the strict flag/value pairs: a
	// --purpose value is free text sent before --job, and the lax scan would
	// record its value as this row's job (hk 4799).
	job, label := hubSpawnFlagArg(message.Args, "--job"), hubSpawnFlagArg(message.Args, "-l")
	sparseHash := sparseListsHash(message.Checkout)
	result, err := j.db.ExecContext(ctx, `INSERT INTO spawn_inbox(request_id,fingerprint,status,job_id,label,strict,sparse_hash,checkout_mode) VALUES(?,?,'executing',?,?,1,?,'') ON CONFLICT DO NOTHING`, message.RequestID, spawnFingerprint(message), job, label, sparseHash)
	if err != nil {
		return false, nil, err
	}
	n, _ := result.RowsAffected()
	if n == 1 {
		return true, nil, nil
	}
	_, cached, err := j.lookup(ctx, message)
	return false, cached, err
}

// refuseUnstarted records a request this node will never start as completed
// with result, unless a row for it already exists.
func (j *nodeSpawnJournal) refuseUnstarted(ctx context.Context, message hubOutboundMessage, result hubSpawnResult) (bool, error) {
	raw, err := json.Marshal(result)
	if err != nil {
		return false, err
	}
	sparseHash := sparseListsHash(message.Checkout)
	inserted, err := j.db.ExecContext(ctx, `INSERT INTO spawn_inbox(request_id,fingerprint,status,result,job_id,label,strict,sparse_hash,checkout_mode) VALUES(?,?,'completed',?,?,?,1,?,?) ON CONFLICT DO NOTHING`, message.RequestID, spawnFingerprint(message), string(raw), hubSpawnFlagArg(message.Args, "--job"), hubSpawnFlagArg(message.Args, "-l"), sparseHash, result.CheckoutMode)
	if err != nil {
		return false, err
	}
	n, err := inserted.RowsAffected()
	return n == 1, err
}

// lookup checks the same fingerprint as claim without creating an executing
// row. An absent row is still replayable after a node restart.
func (j *nodeSpawnJournal) lookup(ctx context.Context, message hubOutboundMessage) (bool, *hubSpawnResult, error) {
	var old, status, stored, storedSparseHash, storedCheckoutMode string
	err := j.db.QueryRowContext(ctx, `SELECT fingerprint,status,result,sparse_hash,checkout_mode FROM spawn_inbox WHERE request_id=?`, message.RequestID).Scan(&old, &status, &stored, &storedSparseHash, &storedCheckoutMode)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, err
	}
	if old != spawnFingerprint(message) {
		return false, nil, errors.New("spawn request payload conflicts with durable ownership")
	}
	incomingSparseHash := sparseListsHash(message.Checkout)
	if storedSparseHash != "" && incomingSparseHash != "" && storedSparseHash != incomingSparseHash {
		return false, nil, errors.New("spawn request payload conflicts with durable ownership")
	}
	if status != "completed" {
		return true, nil, nil
	}
	var cached hubSpawnResult
	if err = json.Unmarshal([]byte(stored), &cached); err != nil {
		return false, nil, err
	}
	if cached.CheckoutMode == "" && storedCheckoutMode != "" {
		cached.CheckoutMode = storedCheckoutMode
	}
	// A stripped replay of a sparse row does not report sparse: the requester
	// dropped the lists with node/hub capabilities (or did not ask for them),
	// so returning sparse would send an unrequested/unsupported mode.
	// Meanwhile, a replay with lists of a stripped first send keeps the
	// truthful checkout_mode of the row that actually executed (full).
	if incomingSparseHash == "" && cached.CheckoutMode == nodeCheckoutSparse {
		cached.CheckoutMode = ""
	}
	return true, &cached, nil
}
func (j *nodeSpawnJournal) complete(ctx context.Context, result hubSpawnResult) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = j.db.ExecContext(ctx, `UPDATE spawn_inbox SET status='completed',result=?,checkout_mode=? WHERE request_id=? AND status='executing'`, string(raw), result.CheckoutMode, result.RequestID)
	return err
}

// executingSpawn is a journal row no completion has been written for. Strict
// is false for a row an older build wrote, whose identity may be a forged
// --purpose value (hk 4799).
type executingSpawn struct {
	RequestID, Job, Label string
	Strict                bool
}

func (j *nodeSpawnJournal) executing(ctx context.Context) ([]executingSpawn, error) {
	rows, err := j.db.QueryContext(ctx, `SELECT request_id,job_id,label,strict FROM spawn_inbox WHERE status='executing' ORDER BY rowid`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []executingSpawn
	for rows.Next() {
		var row executingSpawn
		var strict int
		if err = rows.Scan(&row.RequestID, &row.Job, &row.Label, &strict); err != nil {
			return nil, err
		}
		row.Strict = strict != 0
		out = append(out, row)
	}
	return out, rows.Err()
}

// checkoutOwners maps each completed spawn's prepared *-checkout cwd to its
// job id, for the checkout sweep's attribution of a directory whose job
// record is gone. Only rows whose stored result names such a cwd are
// decoded; the LIKE prefilter keeps the scan off every other row.
func (j *nodeSpawnJournal) checkoutOwners(ctx context.Context) (map[string]string, error) {
	rows, err := j.db.QueryContext(ctx, `SELECT job_id,result FROM spawn_inbox WHERE status='completed' AND job_id != '' AND result LIKE '%-checkout%'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	owners := map[string]string{}
	for rows.Next() {
		var jobID, stored string
		if err := rows.Scan(&jobID, &stored); err != nil {
			return nil, err
		}
		var result struct {
			Receipt *struct {
				CWD string `json:"cwd"`
			} `json:"receipt"`
		}
		if json.Unmarshal([]byte(stored), &result) != nil || result.Receipt == nil {
			continue
		}
		cwd := result.Receipt.CWD
		if cwd == "" || !strings.HasSuffix(filepath.Base(filepath.Clean(cwd)), "-checkout") {
			continue
		}
		owners[nodeResolvedPath(cwd)] = jobID
	}
	return owners, rows.Err()
}

// resultOf returns the stored completion of a request, if it has one.
func (j *nodeSpawnJournal) resultOf(ctx context.Context, requestID string) (*hubSpawnResult, error) {
	var status, stored, storedCheckoutMode string
	if err := j.db.QueryRowContext(ctx, `SELECT status,result,checkout_mode FROM spawn_inbox WHERE request_id=?`, requestID).Scan(&status, &stored, &storedCheckoutMode); err != nil || status != "completed" {
		return nil, err
	}
	var result hubSpawnResult
	if err := json.Unmarshal([]byte(stored), &result); err != nil {
		return nil, err
	}
	if result.CheckoutMode == "" && storedCheckoutMode != "" {
		result.CheckoutMode = storedCheckoutMode
	}
	return &result, nil
}

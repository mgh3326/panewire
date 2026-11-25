package panewire

import (
	"cmp"
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

	"github.com/mgh3326/panewire/internal/fleet"
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
	// Job identity supports restart settle and takeover proof (hk 4799).
	// sparse_hash preserves checkout identity across replay (hk 5104).
	// The admission columns default old executing rows to legacy holds.
	for _, column := range []struct{ name, definition string }{
		{"job_id", "TEXT NOT NULL DEFAULT ''"},
		{"label", "TEXT NOT NULL DEFAULT ''"},
		{"strict", "INTEGER NOT NULL DEFAULT 0"},
		{"sparse_hash", "TEXT NOT NULL DEFAULT ''"},
		{"overlap_key", "TEXT NOT NULL DEFAULT ''"},
		{"root_real", "TEXT NOT NULL DEFAULT ''"},
		{"final_cwd", "TEXT NOT NULL DEFAULT ''"},
		{"has_checkout", "INTEGER NOT NULL DEFAULT 0"},
		{"created_ms", "INTEGER NOT NULL DEFAULT 0"},
		{"deadline_ms", "INTEGER NOT NULL DEFAULT 0"},
		{"start_by_ms", "INTEGER NOT NULL DEFAULT 0"},
	} {
		var present int
		if err = db.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('spawn_inbox') WHERE name=?`, column.name).Scan(&present); err == nil && present == 0 {
			_, err = db.ExecContext(ctx, `ALTER TABLE spawn_inbox ADD COLUMN `+column.name+" "+column.definition)
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

func (j *nodeSpawnJournal) claim(ctx context.Context, message hubOutboundMessage, hold spawnJournalHold) (bool, *hubSpawnResult, error) {
	// The journal's identity columns are read back by the takeover proof, by
	// restart settlement and by the checkout sweep, so they name what the
	// launcher will: a flag's value is never read as a flag (--purpose is free
	// text sent before --job, hk 4799), and the job is the label when --job is
	// absent.
	identity := hubSpawnIdentity(message.Args)
	sparseHash := sparseListsHash(message.Checkout)
	result, err := j.db.ExecContext(ctx, `INSERT INTO spawn_inbox(request_id,fingerprint,status,job_id,label,strict,sparse_hash,overlap_key,root_real,has_checkout,created_ms,deadline_ms,start_by_ms) VALUES(?,?,'executing',?,?,1,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING`, message.RequestID, spawnFingerprint(message), identity.Job, identity.Label, sparseHash, hold.Key, hold.RootReal, message.Checkout != nil, hold.CreatedMS, hold.DeadlineMS, message.StartByMS)
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
	identity, sparseHash := hubSpawnIdentity(message.Args), sparseListsHash(message.Checkout)
	inserted, err := j.db.ExecContext(ctx, `INSERT INTO spawn_inbox(request_id,fingerprint,status,result,job_id,label,strict,sparse_hash) VALUES(?,?,'completed',?,?,?,1,?) ON CONFLICT DO NOTHING`, message.RequestID, spawnFingerprint(message), string(raw), identity.Job, identity.Label, sparseHash)
	if err != nil {
		return false, err
	}
	n, err := inserted.RowsAffected()
	return n == 1, err
}

// lookup checks the same fingerprint as claim without creating an executing
// row. An absent row is still replayable after a node restart.
func (j *nodeSpawnJournal) lookup(ctx context.Context, message hubOutboundMessage) (bool, *hubSpawnResult, error) {
	var old, status, stored, storedSparseHash string
	err := j.db.QueryRowContext(ctx, `SELECT fingerprint,status,result,sparse_hash FROM spawn_inbox WHERE request_id=?`, message.RequestID).Scan(&old, &status, &stored, &storedSparseHash)
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
	cached.CheckoutMode = replayCheckoutMode(cached.CheckoutMode, incomingSparseHash)
	return true, &cached, nil
}

// replayCheckoutMode is the checkout_mode a replay may report for a journaled
// row (hk 5104, hk 5211, hk 5223). A replay that carries no sparse lists does
// not report sparse: the requester dropped the lists with a node or hub
// capability, or did not ask for them, so sparse would be an unrequested mode.
// A replay with lists reports the mode the row executed with. The lookup and
// the settle answer (answerKnownSpawn) both go through here, so a replay gets
// the same answer whichever path produced the row's result. The hub records a
// missing mode as full (hub_spawn.go resolveSpawn), so this answer is the
// requester's view of the spawn, not the on-disk checkout mode.
func replayCheckoutMode(mode, sparseHash string) string {
	if sparseHash == "" && mode == nodeCheckoutSparse {
		return ""
	}
	return mode
}

func (j *nodeSpawnJournal) complete(ctx context.Context, result hubSpawnResult) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = j.db.ExecContext(ctx, `UPDATE spawn_inbox SET status='completed',result=? WHERE request_id=? AND status='executing'`, string(raw), result.RequestID)
	return err
}

// spawnJournalHold is the durable admission metadata of an executing row.
type spawnJournalHold struct {
	Key, RootReal, FinalCWD          string
	HasCheckout                      bool
	CreatedMS, DeadlineMS, StartByMS int64
}

func (j *nodeSpawnJournal) recordSpawnRoot(ctx context.Context, requestID, root string) error {
	_, err := j.db.ExecContext(ctx, `UPDATE spawn_inbox SET root_real=? WHERE request_id=? AND status='executing'`, root, requestID)
	return err
}

func (j *nodeSpawnJournal) recordSpawnCWD(ctx context.Context, requestID, cwd string) error {
	_, err := j.db.ExecContext(ctx, `UPDATE spawn_inbox SET final_cwd=? WHERE request_id=? AND status='executing'`, cwd, requestID)
	return err
}

// executingSpawn is a journal row no completion has been written for. Strict
// is false for a row an older build wrote, whose identity may be a forged
// --purpose value (hk 4799).
type executingSpawn struct {
	RequestID, Job, Label string
	Strict                bool
	spawnJournalHold
}

func (j *nodeSpawnJournal) executing(ctx context.Context) ([]executingSpawn, error) {
	rows, err := j.db.QueryContext(ctx, `SELECT request_id,job_id,label,strict,overlap_key,root_real,final_cwd,has_checkout,created_ms,deadline_ms,start_by_ms FROM spawn_inbox WHERE status='executing' ORDER BY rowid`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []executingSpawn
	for rows.Next() {
		var row executingSpawn
		var strict int
		if err = rows.Scan(&row.RequestID, &row.Job, &row.Label, &strict, &row.Key, &row.RootReal, &row.FinalCWD, &row.HasCheckout, &row.CreatedMS, &row.DeadlineMS, &row.StartByMS); err != nil {
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

// checkoutJob folds every journal row of job for the checkout sweep's orphan
// judgement (hk 6291): whether a spawn of it is still executing, whether every
// finished one failed without a pane, and every label and pane the rows name.
// A result that cannot be decoded proves no failure.
func (j *nodeSpawnJournal) checkoutJob(ctx context.Context, job string) (fleet.CheckoutOrphan, error) {
	rows, err := j.db.QueryContext(ctx, `SELECT status,label,result FROM spawn_inbox WHERE job_id=? ORDER BY rowid`, job)
	if err != nil {
		return fleet.CheckoutOrphan{}, err
	}
	defer rows.Close()
	orphan := fleet.CheckoutOrphan{Job: job, NeverLaunched: true}
	for rows.Next() {
		var status, label, stored string
		if err := rows.Scan(&status, &label, &stored); err != nil {
			return fleet.CheckoutOrphan{}, err
		}
		orphan.Labels = append(orphan.Labels, label)
		if status != "completed" {
			orphan.InFlight, orphan.NeverLaunched = true, false
			continue
		}
		var result hubSpawnResult
		if json.Unmarshal([]byte(stored), &result) != nil {
			orphan.NeverLaunched = false
			continue
		}
		pane := result.Pane
		if result.Receipt != nil {
			orphan.Panes = append(orphan.Panes, result.Receipt.Pane)
			orphan.Labels = append(orphan.Labels, result.Receipt.Label)
			pane = cmp.Or(pane, result.Receipt.Pane)
		}
		orphan.Panes = append(orphan.Panes, result.Pane)
		if result.RC == 0 || pane != "" {
			orphan.NeverLaunched = false
		}
	}
	return orphan, rows.Err()
}

// resultOf returns the stored completion of a request, if it has one.
func (j *nodeSpawnJournal) resultOf(ctx context.Context, requestID string) (*hubSpawnResult, error) {
	var status, stored string
	if err := j.db.QueryRowContext(ctx, `SELECT status,result FROM spawn_inbox WHERE request_id=?`, requestID).Scan(&status, &stored); err != nil || status != "completed" {
		return nil, err
	}
	var result hubSpawnResult
	if err := json.Unmarshal([]byte(stored), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

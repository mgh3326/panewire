package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Acceptance hk 5104 / hk 5211:
// 1. same request replayed with lists after a stripped first send is accepted;
// 2. same request replayed with a different non-empty list conflicts;
// 3. a stripped replay of a sparse row does not report sparse.

func TestNodeSpawnJournalReplayWithDifferentSparseListsConflicts(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	j, err := openNodeSpawnJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	defer j.db.Close()

	checkout1 := &hubSpawnCheckout{
		Origin:  "https://github.com/example/project.git",
		Mode:    "detached",
		Sha:     strings.Repeat("a", 40),
		Fetch:   "refs/heads/main",
		Dir:     "project.test-job",
		Exclude: []string{"Assets"},
	}
	msg1 := hubOutboundMessage{
		RequestID:   "00000000-0000-4000-8000-000000000001",
		CWDKey:      "project",
		BriefInline: "task",
		Args:        []string{"--job", "test-job"},
		Checkout:    checkout1,
		WaitSeconds: 60,
	}

	claimed, cached, err := j.claim(ctx, msg1)
	if err != nil || !claimed || cached != nil {
		t.Fatalf("first claim failed: claimed=%v cached=%v err=%v", claimed, cached, err)
	}

	result := hubSpawnResult{
		RequestID:    msg1.RequestID,
		RC:           0,
		JobID:        "test-job",
		Pane:         "pane-1",
		CheckoutMode: nodeCheckoutSparse,
	}
	if err := j.complete(ctx, result); err != nil {
		t.Fatal(err)
	}

	// Replay with different non-empty sparse lists under the same request ID must conflict.
	checkout2 := &hubSpawnCheckout{
		Origin:  "https://github.com/example/project.git",
		Mode:    "detached",
		Sha:     strings.Repeat("a", 40),
		Fetch:   "refs/heads/main",
		Dir:     "project.test-job",
		Exclude: []string{"Docs"},
	}
	msg2 := msg1
	msg2.Checkout = checkout2

	claimed2, cached2, err2 := j.claim(ctx, msg2)
	if err2 == nil {
		t.Fatalf("different sparse lists replayed under same request ID must conflict: claimed=%v cached=%v", claimed2, cached2)
	}
	if !strings.Contains(err2.Error(), "conflicts with durable ownership") {
		t.Fatalf("unexpected conflict error: %v", err2)
	}

	known, cachedLookup, errLookup := j.lookup(ctx, msg2)
	if errLookup == nil {
		t.Fatalf("lookup with different sparse lists must conflict: known=%v cached=%v", known, cachedLookup)
	}
}

func TestNodeSpawnJournalStrippedReplayOfSparseRowDoesNotReportSparse(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	j, err := openNodeSpawnJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	defer j.db.Close()

	checkout := &hubSpawnCheckout{
		Origin:  "https://github.com/example/project.git",
		Mode:    "detached",
		Sha:     strings.Repeat("a", 40),
		Fetch:   "refs/heads/main",
		Dir:     "project.test-job",
		Exclude: []string{"Assets"},
	}
	msg := hubOutboundMessage{
		RequestID:   "00000000-0000-4000-8000-000000000002",
		CWDKey:      "project",
		BriefInline: "task",
		Args:        []string{"--job", "test-job-2"},
		Checkout:    checkout,
		WaitSeconds: 60,
	}

	claimed, cached, err := j.claim(ctx, msg)
	if err != nil || !claimed || cached != nil {
		t.Fatalf("first claim failed: claimed=%v cached=%v err=%v", claimed, cached, err)
	}

	result := hubSpawnResult{
		RequestID:    msg.RequestID,
		RC:           0,
		JobID:        "test-job-2",
		Pane:         "pane-2",
		CheckoutMode: nodeCheckoutSparse,
	}
	if err := j.complete(ctx, result); err != nil {
		t.Fatal(err)
	}

	// Stripped replay (e.g. node capability downgrade or hub stripped lists)
	strippedCheckout := *checkout
	strippedCheckout.Exclude, strippedCheckout.Include = nil, nil
	strippedMsg := msg
	strippedMsg.Checkout = &strippedCheckout

	claimed2, cached2, err2 := j.claim(ctx, strippedMsg)
	if err2 != nil {
		t.Fatalf("stripped replay should be accepted: %v", err2)
	}
	if claimed2 {
		t.Fatal("replay of completed row should not claim")
	}
	if cached2 == nil {
		t.Fatal("replay should return cached result")
	}
	if cached2.CheckoutMode == nodeCheckoutSparse {
		t.Fatalf("a stripped replay of a sparse row must not report sparse: checkout_mode=%q", cached2.CheckoutMode)
	}
}

func TestNodeSpawnJournalReplayWithListsAfterStrippedFirstSendAccepted(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	j, err := openNodeSpawnJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	defer j.db.Close()

	// First send is stripped (full checkout executed).
	strippedCheckout := &hubSpawnCheckout{
		Origin: "https://github.com/example/project.git",
		Mode:   "detached",
		Sha:    strings.Repeat("a", 40),
		Fetch:  "refs/heads/main",
		Dir:    "project.test-job",
	}
	strippedMsg := hubOutboundMessage{
		RequestID:   "00000000-0000-4000-8000-000000000003",
		CWDKey:      "project",
		BriefInline: "task",
		Args:        []string{"--job", "test-job-3"},
		Checkout:    strippedCheckout,
		WaitSeconds: 60,
	}

	claimed, cached, err := j.claim(ctx, strippedMsg)
	if err != nil || !claimed || cached != nil {
		t.Fatalf("first claim failed: claimed=%v cached=%v err=%v", claimed, cached, err)
	}

	result := hubSpawnResult{
		RequestID:    strippedMsg.RequestID,
		RC:           0,
		JobID:        "test-job-3",
		Pane:         "pane-3",
		CheckoutMode: nodeCheckoutFull,
	}
	if err := j.complete(ctx, result); err != nil {
		t.Fatal(err)
	}

	// Replay arrives WITH lists.
	withLists := *strippedCheckout
	withLists.Exclude = []string{"Assets"}
	replayMsg := strippedMsg
	replayMsg.Checkout = &withLists

	claimed2, cached2, err2 := j.claim(ctx, replayMsg)
	if err2 != nil {
		t.Fatalf("replay with lists after stripped first send must be accepted: %v", err2)
	}
	if claimed2 {
		t.Fatal("replay should not claim")
	}
	if cached2 == nil {
		t.Fatal("replay should return cached result")
	}
	if cached2.CheckoutMode != nodeCheckoutFull {
		t.Fatalf("truthful checkout_mode for executed row must be %q, got %q", nodeCheckoutFull, cached2.CheckoutMode)
	}
}

func TestNodeSpawnJournalMigrationAddsSparseIdentity(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(root, "spawn-inbox.sqlite3")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	// Create legacy table schema without sparse_hash and checkout_mode.
	ctx := context.Background()
	_, err = db.ExecContext(ctx, `CREATE TABLE spawn_inbox (request_id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,status TEXT NOT NULL,result TEXT NOT NULL DEFAULT '',job_id TEXT NOT NULL DEFAULT '',label TEXT NOT NULL DEFAULT '',strict INTEGER NOT NULL DEFAULT 0)`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO spawn_inbox(request_id,fingerprint,status,result,job_id,label,strict) VALUES('legacy-req','fp','completed','{"request_id":"legacy-req","rc":0,"job_id":"job-leg","checkout_mode":"full"}','job-leg','label-leg',1)`)
	db.Close()
	if err != nil {
		t.Fatal(err)
	}

	j, err := openNodeSpawnJournal(root)
	if err != nil {
		t.Fatalf("openNodeSpawnJournal on legacy database failed: %v", err)
	}
	defer j.db.Close()

	// sparse_hash is added; checkout_mode is not a column (hk 5223), and the
	// legacy row's journaled result still carries its mode.
	var present int
	if err := j.db.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('spawn_inbox') WHERE name='sparse_hash'`).Scan(&present); err != nil || present != 1 {
		t.Fatalf("column sparse_hash missing after migration: present=%d err=%v", present, err)
	}
	legacy, err := j.resultOf(ctx, "legacy-req")
	if err != nil || legacy == nil || legacy.CheckoutMode != nodeCheckoutFull {
		t.Fatalf("legacy row after migration: result=%+v err=%v", legacy, err)
	}
}

func TestNodeSpawnJournalAdmissionWaiterDetectsDifferentSparseLists(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	j, err := openNodeSpawnJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	defer j.db.Close()

	client := &HubClient{}
	checkout1 := &hubSpawnCheckout{
		Origin:  "https://github.com/example/project.git",
		Mode:    "detached",
		Sha:     strings.Repeat("a", 40),
		Fetch:   "refs/heads/main",
		Dir:     "project.test-job",
		Exclude: []string{"Assets"},
	}
	msg1 := hubOutboundMessage{
		RequestID:   "00000000-0000-4000-8000-000000000004",
		CWDKey:      "project",
		BriefInline: "task",
		Args:        []string{"--job", "test-job-4"},
		Checkout:    checkout1,
		WaitSeconds: 60,
	}

	known, cached, active, waiter, err := client.beginSpawnAdmission(ctx, j, msg1)
	if err != nil || known || cached != nil || active || waiter == nil {
		t.Fatalf("beginSpawnAdmission 1: known=%v cached=%v active=%v waiter=%v err=%v", known, cached, active, waiter, err)
	}

	checkout2 := &hubSpawnCheckout{
		Origin:  "https://github.com/example/project.git",
		Mode:    "detached",
		Sha:     strings.Repeat("a", 40),
		Fetch:   "refs/heads/main",
		Dir:     "project.test-job",
		Exclude: []string{"Docs"},
	}
	msg2 := msg1
	msg2.Checkout = checkout2

	_, _, _, _, err2 := client.beginSpawnAdmission(ctx, j, msg2)
	if err2 == nil {
		t.Fatal("concurrent admission with different sparse lists must conflict")
	}
	if !strings.Contains(err2.Error(), "conflicts with pending admission") {
		t.Fatalf("unexpected error: %v", err2)
	}
}

func TestSpawnSettleRestartPreservesSparseCheckoutMode(t *testing.T) {
	herdr := &nodeFakeHerdr{
		agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "label-a", "working", "/work/a"))},
		panes:  settlePanes("w1:p1", "label-a"),
	}
	f := newSettleFixture(t, herdr.run)

	// Record a job.spawned event carrying checkout_mode="sparse"
	dir := filepath.Join(f.jobs, "jobs", "job-a", "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	event, _ := json.Marshal(map[string]any{
		"kind": "job.spawned",
		"payload": map[string]any{
			"pane_id":       "w1:p1",
			"herdr_session": "worker",
			"host":          "machine-a",
			"profile":       "codex-terra",
			"model_id":      "m",
			"role":          "implementer",
			"cwd":           "/work/a",
			"pool":          "codex",
			"account_key":   "codex:0123456789abcdef",
			"checkout_mode": nodeCheckoutSparse,
		},
	})
	if err := os.WriteFile(filepath.Join(dir, "01.json"), event, 0600); err != nil {
		t.Fatal(err)
	}

	f.client.runSpawnSettle(t.Context())
	result := f.stored(t)
	if result == nil || result.RC != 0 {
		t.Fatalf("settle failed: %+v", result)
	}
	if result.CheckoutMode != nodeCheckoutSparse {
		t.Fatalf("settled result must preserve checkout_mode %q, got %q", nodeCheckoutSparse, result.CheckoutMode)
	}
}

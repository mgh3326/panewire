package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The loop ledger (hk 5116, track A1) records every hub mechanism, writes one
// daily row and notices a silent stop. These tests pin the acceptance
// contract: a store error in the spawn scan counts, logs once per streak and
// names the mechanism in the row and the notice; a stopped loop notices after
// three intervals; a healthy run has rows and no notice; the midnight
// boundary writes exactly one doc per day, even across a restart.

func loopLedgerTestHub(t *testing.T, now *time.Time, logs *bytes.Buffer) *HubServer {
	t.Helper()
	var writer *bytes.Buffer
	if logs != nil {
		writer = logs
	} else {
		writer = &bytes.Buffer{}
	}
	h, err := NewHubServer(HubServerConfig{
		Tokens:         map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"},
		InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3"),
		Now:            func() time.Time { return now.UTC() },
		Logger:         slog.New(slog.NewTextHandler(writer, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func loopLedgerCountLines(logs *bytes.Buffer, substr string) int {
	n := 0
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

// A store error in the spawn coordinator scan shows in the ledger, logs one
// line per streak, and names the mechanism in the daily row and the notice.
func TestLoopLedgerSpawnScanErrorCountsLogsRowAndNotice(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	var logs bytes.Buffer
	h := loopLedgerTestHub(t, &now, &logs)
	ctx := context.Background()

	h.inbox.store.abort()
	h.spawnCoordinatorScan(ctx, func(hubSpawnTask) bool { return true })
	h.spawnCoordinatorScan(ctx, func(hubSpawnTask) bool { return true })

	snap := h.loopSnapshot()
	if snap[loopSpawnCoordinator].Errors < 1 {
		t.Fatalf("spawn coordinator errors=%d, want >= 1", snap[loopSpawnCoordinator].Errors)
	}
	if snap[loopSpawnCoordinator].SilentErrors < 1 {
		t.Fatalf("spawn coordinator silent errors=%d, want >= 1", snap[loopSpawnCoordinator].SilentErrors)
	}
	if got := loopLedgerCountLines(&logs, "hub loop silent error"); got != 2 {
		t.Fatalf("silent log lines=%d, want 2 (one per failing path per streak), log:\n%s", got, logs.String())
	}

	doc := h.loopsRowDocument(now)
	rowByMechanism := make(map[string]loopsRowEntry, len(doc.Rows))
	for _, row := range doc.Rows {
		rowByMechanism[row.Mechanism] = row
	}
	row, ok := rowByMechanism[string(loopSpawnCoordinator)]
	if !ok {
		t.Fatalf("daily row has no spawn_coordinator row: %+v", doc.Rows)
	}
	if row.SilentErrors < 1 {
		t.Fatalf("row silent_errors=%d, want >= 1", row.SilentErrors)
	}

	found := false
	for _, hit := range h.loopNoticeHits(now) {
		if hit.Mechanism == loopSpawnCoordinator && strings.Contains(hit.Reason, "silent") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no silent notice hit for spawn_coordinator: %+v", h.loopNoticeHits(now))
	}
}

// A loop that stops ticking notices after three intervals on a fake clock.
func TestLoopLedgerStaleNoticeAfterThreeIntervals(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h := loopLedgerTestHub(t, &now, nil)

	for _, m := range loopMechanisms {
		h.loopTick(m)
	}
	if hits := h.loopsStaleHits(now); len(hits) != 0 {
		t.Fatalf("fresh ledger stale hits=%v, want none", hits)
	}
	now = now.Add(3*h.loopInterval(loopSpawnCoordinator) + time.Second)
	hits := h.loopsStaleHits(now)
	found := false
	for _, m := range hits {
		if m == loopSpawnCoordinator {
			found = true
		}
	}
	if !found {
		t.Fatalf("stale hits=%v, want spawn_coordinator after three intervals", hits)
	}
}

// A healthy run has rows for every mechanism and no notice.
func TestLoopLedgerHealthyRunRowsPresentNoNotice(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h := loopLedgerTestHub(t, &now, nil)

	for _, m := range loopMechanisms {
		h.loopTick(m)
		h.loopAddMade(m, 1)
		h.loopAddDone(m, 1)
	}
	doc := h.loopsRowDocument(now)
	if len(doc.Rows) != len(loopMechanisms) {
		t.Fatalf("rows=%d, want %d", len(doc.Rows), len(loopMechanisms))
	}
	for _, row := range doc.Rows {
		if row.Machine != "hub" {
			t.Fatalf("row machine=%q, want hub", row.Machine)
		}
		if row.Ticks != 1 || row.Made != 1 || row.Done != 1 {
			t.Fatalf("row %+v, want ticks=made=done=1", row)
		}
	}
	if hits := h.loopNoticeHits(now); len(hits) != 0 {
		t.Fatalf("healthy run notice hits=%v, want none", hits)
	}
}

// The midnight boundary writes exactly one doc per day, even across a hub
// restart: already written is never rewritten.
func TestLoopLedgerDailyRowOncePerDayAcrossRestart(t *testing.T) {
	now := time.Date(2026, 11, 18, 23, 59, 0, 0, time.UTC)
	published := map[string]int{}
	existing := map[string]bool{}
	h := loopLedgerTestHub(t, &now, nil)
	h.loopsDocPublish = func(_ context.Context, key, _ string) error {
		published[key]++
		existing[key] = true
		return nil
	}
	h.loopsDocExists = func(_ context.Context, key string) (bool, error) { return existing[key], nil }
	ctx := context.Background()

	for _, m := range loopMechanisms {
		h.loopTick(m)
	}
	h.maybeWriteLoopsRow(ctx)
	h.maybeWriteLoopsRow(ctx)
	if got := published[loopsRowKey("2026-11-18")]; got != 1 {
		t.Fatalf("same-day publishes=%d, want 1", got)
	}

	now = time.Date(2026, 11, 19, 0, 1, 0, 0, time.UTC)
	h.maybeWriteLoopsRow(ctx)
	if got := published[loopsRowKey("2026-11-19")]; got != 1 {
		t.Fatalf("next-day publishes=%d, want 1", got)
	}

	restarted := loopLedgerTestHub(t, &now, nil)
	restarted.loopsDocPublish = func(_ context.Context, key, _ string) error {
		published[key]++
		existing[key] = true
		return nil
	}
	restarted.loopsDocExists = func(_ context.Context, key string) (bool, error) { return existing[key], nil }
	restarted.maybeWriteLoopsRow(ctx)
	if got := published[loopsRowKey("2026-11-19")]; got != 1 {
		t.Fatalf("restarted hub rewrote 2026-11-19: publishes=%d, want 1", got)
	}
}

// The progress notice fires on made >= 20 with nothing (or nearly nothing)
// done, and stays quiet below the floor.
func TestLoopLedgerProgressNotice(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h := loopLedgerTestHub(t, &now, nil)

	h.loopTick(loopConsultCoordinator)
	h.loopAddMade(loopConsultCoordinator, 19)
	if hits := h.loopsProgressHits(); len(hits) != 0 {
		t.Fatalf("below-floor progress hits=%v, want none", hits)
	}
	h.loopAddMade(loopConsultCoordinator, 1)
	found := false
	for _, m := range h.loopsProgressHits() {
		if m == loopConsultCoordinator {
			found = true
		}
	}
	if !found {
		t.Fatalf("made=20 done=0 has no progress hit: %+v", h.loopsProgressHits())
	}

	h2 := loopLedgerTestHub(t, &now, nil)
	h2.loopTick(loopConsultCoordinator)
	h2.loopAddMade(loopConsultCoordinator, 100)
	h2.loopAddDone(loopConsultCoordinator, 5)
	found = false
	for _, m := range h2.loopsProgressHits() {
		if m == loopConsultCoordinator {
			found = true
		}
	}
	if !found {
		t.Fatalf("done/made=5%% has no progress hit")
	}

	var body string
	h.loopsDocPublish = func(_ context.Context, _, b string) error { body = b; return nil }
	h.loopsDocExists = func(_ context.Context, _ string) (bool, error) { return false, nil }
	h.maybeWriteLoopsRow(context.Background())
	var doc loopsRowDoc
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("row body is not JSON: %v", err)
	}
	if doc.V != loopsRowVersion || doc.Machine != "hub" {
		t.Fatalf("row header=%+v, want v=%d machine=hub", doc, loopsRowVersion)
	}
}

// Several reasons for one mechanism still send at most one notice per
// mechanism per day.
func TestLoopLedgerNoticeAtMostOnePerMechanismPerDay(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h := loopLedgerTestHub(t, &now, nil)
	ctx := context.Background()

	h.loopNoteSilentError(loopInbox, "hub_inbox.go:pendingBatches", errors.New("boom"))
	now = now.Add(time.Second)
	inboxHits := 0
	for _, hit := range h.loopNoticeHits(now) {
		if hit.Mechanism == loopInbox {
			inboxHits++
		}
	}
	if inboxHits < 2 {
		t.Fatalf("inbox hits=%d, want >= 2 (stale plus silent)", inboxHits)
	}
	h.maybeSendLoopsNotice(ctx)
	h.maybeSendLoopsNotice(ctx)
	st := h.loopLedger()
	sent := 0
	st.mu.Lock()
	for key := range st.noticeSent {
		if strings.HasPrefix(key, string(loopInbox)+"|") {
			sent++
		}
	}
	st.mu.Unlock()
	if sent != 1 {
		t.Fatalf("inbox notices sent=%d, want 1", sent)
	}
}

// Loops whose interval is configured off never tick, and must not be
// reported as stopped: a disabled interval is excluded from the stale check.
func TestLoopLedgerDisabledLoopsExcludedFromStale(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h := loopLedgerTestHub(t, &now, nil)
	h.poolTriggerInterval = 0
	if got := h.loopInterval(loopFleetStatus); got != 0 {
		t.Fatalf("fleet_status interval=%v, want 0 when disabled", got)
	}
	if got := h.loopInterval(loopPaneSweep); got != 0 {
		t.Fatalf("pane_sweep interval=%v, want 0 when disabled", got)
	}
	if got := h.loopInterval(loopPoolTriggers); got != 0 {
		t.Fatalf("pool_triggers interval=%v, want 0 when disabled", got)
	}
	now = now.Add(2 * time.Hour)
	for _, m := range h.loopsStaleHits(now) {
		if m == loopFleetStatus || m == loopPaneSweep || m == loopPoolTriggers {
			t.Fatalf("disabled loop %s flagged stale: %v", m, h.loopsStaleHits(now))
		}
	}
}

// An unreadable row is neither absent nor present: the write is skipped, the
// failure counts, and the next sweep retries the read instead of
// overwriting the day's counts with a restart-fresh ledger.
func TestLoopLedgerUnreadableRowSkipsWriteAndRetries(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	published := 0
	readErr := errors.New("hk timeout")
	h := loopLedgerTestHub(t, &now, nil)
	h.loopsDocPublish = func(_ context.Context, _, _ string) error { published++; return nil }
	h.loopsDocExists = func(_ context.Context, _ string) (bool, error) { return false, readErr }
	ctx := context.Background()

	for _, m := range loopMechanisms {
		h.loopTick(m)
	}
	h.maybeWriteLoopsRow(ctx)
	if published != 0 {
		t.Fatalf("unreadable row published=%d, want 0", published)
	}
	if got := h.loopSnapshot()[loopFleetStatus].Errors; got < 1 {
		t.Fatalf("fleet_status errors=%d, want >= 1 for the unreadable row", got)
	}
	readErr = nil
	h.maybeWriteLoopsRow(ctx)
	if published != 1 {
		t.Fatalf("retried publishes=%d, want 1", published)
	}
}

// A notice whose lane.event persist fails is not marked sent, so the next
// sweep retries it instead of losing it for the day.
func TestLoopLedgerNoticeRetryWhenPersistFails(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h := loopLedgerTestHub(t, &now, nil)
	ctx := context.Background()

	h.loopNoteSilentError(loopInbox, "hub_inbox.go:pendingBatches", errors.New("boom"))
	h.inbox.store.abort()
	h.maybeSendLoopsNotice(ctx)
	st := h.loopLedger()
	st.mu.Lock()
	_, marked := st.noticeSent[string(loopInbox)+"|"+now.Format("2006-01-02")]
	st.mu.Unlock()
	if marked {
		t.Fatalf("failed notice was marked sent; the next sweep would skip it")
	}
}

// An idle hub owes no drain lines, so the drain sweep counts no made: the
// made >= 20 progress notice must not fire on a healthy idle hub.
func TestLoopLedgerDrainIdleCountsNoMade(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h := loopLedgerTestHub(t, &now, nil)
	h.reportRelayPath = r20LanesFile(t, `{"lanes":{"x-design":{"machine":"machine-a","pane":"w1:p1"}}}`)
	ctx := context.Background()
	h.sweepDrainWakes(ctx, now)
	h.sweepDrainWakes(ctx, now)
	if got := h.loopSnapshot()[loopLaneStallDrainWake].Made; got != 0 {
		t.Fatalf("idle drain made=%d, want 0", got)
	}
}

// A backoff pause is an intentional gate, not a stopped loop: the inbox
// records last_tick even while nextAttempt is in the future.
func TestLoopLedgerInboxBackoffStillTicks(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h := loopLedgerTestHub(t, &now, nil)
	h.inbox.nextAttempt = now.Add(time.Minute)
	h.flushInbox()
	if last := h.loopSnapshot()[loopInbox].LastTick; last.IsZero() {
		t.Fatalf("inbox last_tick not recorded during backoff")
	}
}

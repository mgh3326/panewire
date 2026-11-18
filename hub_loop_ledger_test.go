package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The loop ledger (hk 5116, track A1) records every hub mechanism, writes one
// daily row and notices a silent stop. Day buckets scope every rule to the
// current UTC date; flow mechanisms count first-seen items per day, scan
// mechanisms count ticks and errors only.

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
// line per streak and counts once per streak, and names the mechanism in the
// daily row and the notice.
func TestLoopLedgerSpawnScanErrorCountsLogsRowAndNotice(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	var logs bytes.Buffer
	h := loopLedgerTestHub(t, &now, &logs)
	ctx := context.Background()

	h.inbox.store.abort()
	h.spawnCoordinatorScan(ctx, func(hubSpawnTask) bool { return true })
	h.spawnCoordinatorScan(ctx, func(hubSpawnTask) bool { return true })

	snap := h.loopSnapshot()
	if snap[loopSpawnCoordinator].Errors != 2 || snap[loopSpawnCoordinator].SilentErrors != 2 {
		t.Fatalf("spawn coordinator errors=%d silent=%d, want 2 and 2 (one per failing path per streak, not per sweep)",
			snap[loopSpawnCoordinator].Errors, snap[loopSpawnCoordinator].SilentErrors)
	}
	if got := loopLedgerCountLines(&logs, "hub loop silent error"); got != 2 {
		t.Fatalf("silent log lines=%d, want 2 (one per failing path per streak), log:\n%s", got, logs.String())
	}

	doc := h.loopsRowDocument(loopDateOf(now), now)
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
		if loopKindOf(m) == loopKindFlow {
			h.loopNoteMade(m, "item-1")
			h.loopNoteDone(m, "item-1")
		}
	}
	doc := h.loopsRowDocument(loopDateOf(now), now)
	if len(doc.Rows) != len(loopMechanisms) {
		t.Fatalf("rows=%d, want %d", len(doc.Rows), len(loopMechanisms))
	}
	for _, row := range doc.Rows {
		if row.Machine != "hub" {
			t.Fatalf("row machine=%q, want hub", row.Machine)
		}
		if row.Ticks != 1 {
			t.Fatalf("row %+v, want ticks=1", row)
		}
	}
	if hits := h.loopNoticeHits(now); len(hits) != 0 {
		t.Fatalf("healthy run notice hits=%v, want none", hits)
	}
}

// The midnight boundary writes each date's row once, from that date's
// bucket, at the first tick of the next date — even across a restart.
func TestLoopLedgerDailyRowOncePerDayAcrossRestart(t *testing.T) {
	now := time.Date(2026, 11, 18, 23, 59, 0, 0, time.UTC)
	published := map[string]int{}
	bodies := map[string]string{}
	existing := map[string]bool{}
	h := loopLedgerTestHub(t, &now, nil)
	h.loopsDocPublish = func(_ context.Context, key, body string) error {
		published[key]++
		existing[key] = true
		bodies[key] = body
		return nil
	}
	h.loopsDocExists = func(_ context.Context, key string) (bool, error) { return existing[key], nil }
	ctx := context.Background()

	for _, m := range loopMechanisms {
		h.loopTick(m)
	}
	h.maybeWriteLoopsRow(ctx)
	if len(published) != 0 {
		t.Fatalf("same-day publishes=%v, want none", published)
	}

	now = time.Date(2026, 11, 19, 0, 1, 0, 0, time.UTC)
	h.maybeWriteLoopsRow(ctx)
	if got := published[loopsRowKey("2026-11-18")]; got != 1 {
		t.Fatalf("previous-day publishes=%d, want 1", got)
	}
	h.maybeWriteLoopsRow(ctx)
	if got := published[loopsRowKey("2026-11-18")]; got != 1 {
		t.Fatalf("repeat publishes=%d, want 1", got)
	}

	restarted := loopLedgerTestHub(t, &now, nil)
	restarted.loopsDocPublish = func(_ context.Context, key, body string) error {
		published[key]++
		existing[key] = true
		bodies[key] = body
		return nil
	}
	restarted.loopsDocExists = func(_ context.Context, key string) (bool, error) { return existing[key], nil }
	for _, m := range loopMechanisms {
		restarted.loopTick(m)
	}
	now = time.Date(2026, 11, 20, 0, 1, 0, 0, time.UTC)
	restarted.maybeWriteLoopsRow(ctx)
	if got := published[loopsRowKey("2026-11-18")]; got != 1 {
		t.Fatalf("restarted hub rewrote 2026-11-18: publishes=%d, want 1", got)
	}
	if got := published[loopsRowKey("2026-11-19")]; got != 1 {
		t.Fatalf("restarted hub publishes for 2026-11-19=%d, want 1", got)
	}
}

// The progress notice fires on made >= 20 with nothing (or nearly nothing)
// done in the current date, and stays quiet below the floor.
func TestLoopLedgerProgressNotice(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h := loopLedgerTestHub(t, &now, nil)

	h.loopTick(loopConsultCoordinator)
	for i := range 19 {
		h.loopNoteMade(loopConsultCoordinator, "consult-"+string(rune('a'+i)))
	}
	if hits := h.loopsProgressHits(); len(hits) != 0 {
		t.Fatalf("below-floor progress hits=%v, want none", hits)
	}
	h.loopNoteMade(loopConsultCoordinator, "consult-t")
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
	for i := range 100 {
		h2.loopNoteMade(loopConsultCoordinator, "consult-"+string(rune('a'+i%26))+string(rune('0'+i/26)))
	}
	for i := range 5 {
		h2.loopNoteDone(loopConsultCoordinator, "consult-"+string(rune('a'+i))+string(rune('0')))
	}
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
	now = time.Date(2026, 11, 19, 0, 1, 0, 0, time.UTC)
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
// reported as stopped: a non-positive interval is excluded from the stale
// check.
func TestLoopLedgerDisabledLoopsExcludedFromStale(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h := loopLedgerTestHub(t, &now, nil)
	h.poolTriggerInterval = 0
	for _, m := range []loopMechanism{loopFleetStatus, loopPaneSweep, loopPoolTriggers, loopPoolExhaustedSweep} {
		if got := h.loopInterval(m); got != 0 {
			t.Fatalf("%s interval=%v, want 0 when disabled", m, got)
		}
	}
	now = now.Add(2 * time.Hour)
	for _, m := range h.loopsStaleHits(now) {
		if m == loopFleetStatus || m == loopPaneSweep || m == loopPoolTriggers || m == loopPoolExhaustedSweep {
			t.Fatalf("disabled loop %s flagged stale: %v", m, h.loopsStaleHits(now))
		}
	}
}

// An unreadable row is neither absent nor present: the write is skipped, the
// failure counts, and a later sweep retries the read instead of overwriting
// the day's counts with a restart-fresh ledger. A failed read backs off so
// the maintenance tick does not wait on hk every pass.
func TestLoopLedgerUnreadableRowSkipsWriteAndRetries(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	published := 0
	reads := 0
	readErr := errors.New("hk timeout")
	h := loopLedgerTestHub(t, &now, nil)
	h.loopsDocPublish = func(_ context.Context, _, _ string) error { published++; return nil }
	h.loopsDocExists = func(_ context.Context, _ string) (bool, error) { reads++; return false, readErr }
	ctx := context.Background()

	for _, m := range loopMechanisms {
		h.loopTick(m)
	}
	now = time.Date(2026, 11, 19, 0, 1, 0, 0, time.UTC)
	h.maybeWriteLoopsRow(ctx)
	if published != 0 {
		t.Fatalf("unreadable row published=%d, want 0", published)
	}
	h.maybeWriteLoopsRow(ctx)
	if reads != 1 {
		t.Fatalf("row reads=%d, want 1: an unreadable row backs off instead of reading every tick", reads)
	}
	readErr = nil
	now = now.Add(2 * loopsRowRetryAfter)
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

// An owed drain edge counts one made; sweeps while the line stays owed
// without progress count nothing, so an interval-gated line cannot trip the
// progress notice.
func TestLoopLedgerDrainOwedEdgeCountsOnce(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h := loopLedgerTestHub(t, &now, nil)
	h.reportRelayPath = r20LanesFile(t, `{"lanes":{"x-design":{"machine":"machine-a","pane":"w1:p1"}}}`)
	ctx := context.Background()
	h.drainWake = map[string]*drainWakeLane{"x-design": {known: true, drained: false}}
	h.sweepDrainWakes(ctx, now)
	snap := h.loopSnapshot()[loopLaneStallDrainWake]
	if snap.Made != 1 || snap.Done != 0 {
		t.Fatalf("owed edge made=%d done=%d, want 1 and 0", snap.Made, snap.Done)
	}
	// The line stays owed behind the interval gate: no further counts.
	h.drainWake["x-design"].lastSent = now
	h.sweepDrainWakes(ctx, now)
	snap = h.loopSnapshot()[loopLaneStallDrainWake]
	if snap.Made != 1 || snap.Done != 0 {
		t.Fatalf("gated sweep made=%d done=%d, want still 1 and 0", snap.Made, snap.Done)
	}
}

// A busy lane clears its owed line without a send: the clear counts as
// done, balancing the edge's made.
func TestLoopLedgerDrainBusyClearCountsDone(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h := loopLedgerTestHub(t, &now, nil)
	h.reportRelayPath = r20LanesFile(t, `{"lanes":{"x-design":{"machine":"machine-a","pane":"w1:p1"}}}`)
	ctx := context.Background()
	h.mu.Lock()
	h.nodes["machine-a"] = &hubNodeRecord{machineID: "machine-a", state: "connected", activeReported: true, activeJobs: map[string]HubActiveJob{"job-1": {JobID: "job-1", OwnerLane: "x-design"}}}
	h.mu.Unlock()
	h.drainWake = map[string]*drainWakeLane{"x-design": {known: true, drained: true, pending: true}}
	h.sweepDrainWakes(ctx, now)
	snap := h.loopSnapshot()[loopLaneStallDrainWake]
	if snap.Made != 0 || snap.Done != 1 {
		t.Fatalf("busy clear made=%d done=%d, want 0 and 1", snap.Made, snap.Done)
	}
}

// A steady population observed on every tick counts once: one pending spawn
// re-scanned 25 times makes 1, so the progress rule stays quiet.
func TestLoopLedgerSteadyPopulationCountsOnce(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h := loopLedgerTestHub(t, &now, nil)
	ctx := context.Background()
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: spawnJobsFixture{}, Out: out, Err: errout}, nil
	}
	req := durableSpawnRequest()
	raw, _ := json.Marshal(req)
	r := httptest.NewRequest("POST", "/v1/spawns", bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
	w := httptest.NewRecorder()
	h.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusAccepted {
		t.Fatalf("spawn submit=%d, want 202", w.Code)
	}
	for range 25 {
		h.spawnCoordinatorScan(ctx, func(hubSpawnTask) bool { return false })
	}
	snap := h.loopSnapshot()[loopSpawnCoordinator]
	if snap.Made != 1 || snap.Done != 0 {
		t.Fatalf("steady spawn made=%d done=%d, want 1 and 0", snap.Made, snap.Done)
	}
	found := false
	for _, m := range h.loopsProgressHits() {
		if m == loopSpawnCoordinator {
			found = true
		}
	}
	if found {
		t.Fatalf("steady population trips the progress rule: %+v", snap)
	}
}

// One long consult pending across many polls counts once: 20 polls of the
// same consult make 1, so the progress rule stays quiet.
func TestLoopLedgerLongConsultCountsOnce(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h := loopLedgerTestHub(t, &now, nil)
	ctx := context.Background()
	if _, err := h.storeConsult(ctx, hubConsult{
		ID: "c-long", IdemKey: "idem-long", State: consultQueued,
		RequestID: "missing-op", Created: now,
		ClaimUntil: now.Add(time.Hour), Deadline: now.Add(2 * time.Hour),
	}, false); err != nil {
		t.Fatalf("insert consult: %v", err)
	}
	for range 20 {
		h.reconcileConsults(ctx)
	}
	snap := h.loopSnapshot()[loopConsultCoordinator]
	if snap.Made != 1 || snap.Done != 0 {
		t.Fatalf("long consult made=%d done=%d, want 1 and 0", snap.Made, snap.Done)
	}
	found := false
	for _, m := range h.loopsProgressHits() {
		if m == loopConsultCoordinator {
			found = true
		}
	}
	if found {
		t.Fatalf("long consult trips the progress rule: %+v", snap)
	}
}

// An idle hub has no pending placement snapshots, but the placement pass
// still ticks: no stale notice for an idle placement loop.
func TestLoopLedgerIdlePlacementStillTicks(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h := loopLedgerTestHub(t, &now, nil)
	ctx := context.Background()
	h.flushPlacementRecords(ctx)
	if last := h.loopSnapshot()[loopPlacementRecords].LastTick; last.IsZero() {
		t.Fatalf("placement last_tick not recorded on an idle pass")
	}
	now = now.Add(10 * time.Second)
	h.flushPlacementRecords(ctx)
	for _, m := range h.loopsStaleHits(now) {
		if m == loopPlacementRecords {
			t.Fatalf("idle placement flagged stale: %v", h.loopsStaleHits(now))
		}
	}
}

// Each date's row carries only that date's counts: work done on D+1 never
// lands in D's row.
func TestLoopLedgerDayBoundaryCarriesOnlyItsDate(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	bodies := map[string]string{}
	h := loopLedgerTestHub(t, &now, nil)
	h.loopsDocPublish = func(_ context.Context, key, body string) error { bodies[key] = body; return nil }
	h.loopsDocExists = func(_ context.Context, _ string) (bool, error) { return false, nil }
	ctx := context.Background()

	h.loopTick(loopSpawnCoordinator)
	h.loopNoteMade(loopSpawnCoordinator, "r-1")
	h.loopNoteDone(loopSpawnCoordinator, "r-1")
	h.loopTick(loopConsultCoordinator)
	h.loopNoteMade(loopConsultCoordinator, "c-1")
	now = time.Date(2026, 11, 19, 0, 1, 0, 0, time.UTC)
	h.maybeWriteLoopsRow(ctx)
	var day18 loopsRowDoc
	if err := json.Unmarshal([]byte(bodies[loopsRowKey("2026-11-18")]), &day18); err != nil {
		t.Fatalf("day-18 body is not JSON: %v", err)
	}
	byMechanism := map[string]loopsRowEntry{}
	for _, row := range day18.Rows {
		byMechanism[row.Mechanism] = row
	}
	if got := byMechanism[string(loopSpawnCoordinator)]; got.Made != 1 || got.Done != 1 {
		t.Fatalf("day-18 spawn made=%d done=%d, want 1 and 1", got.Made, got.Done)
	}
	if got := byMechanism[string(loopConsultCoordinator)]; got.Made != 1 {
		t.Fatalf("day-18 consult made=%d, want 1", got.Made)
	}

	h.loopTick(loopSpawnCoordinator)
	h.loopNoteMade(loopSpawnCoordinator, "r-2")
	now = time.Date(2026, 11, 20, 0, 1, 0, 0, time.UTC)
	h.maybeWriteLoopsRow(ctx)
	var day19 loopsRowDoc
	if err := json.Unmarshal([]byte(bodies[loopsRowKey("2026-11-19")]), &day19); err != nil {
		t.Fatalf("day-19 body is not JSON: %v", err)
	}
	byMechanism = map[string]loopsRowEntry{}
	for _, row := range day19.Rows {
		byMechanism[row.Mechanism] = row
	}
	if got := byMechanism[string(loopSpawnCoordinator)]; got.Made != 1 || got.Done != 0 {
		t.Fatalf("day-19 spawn made=%d done=%d, want 1 and 0", got.Made, got.Done)
	}
	if got := byMechanism[string(loopConsultCoordinator)]; got.Made != 0 {
		t.Fatalf("day-19 consult made=%d, want 0", got.Made)
	}
}

// One transient silent error notices on its own date only: the next date
// starts clean even though the lifetime streak already ended.
func TestLoopLedgerTransientSilentErrorNoticesOnce(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h := loopLedgerTestHub(t, &now, nil)

	h.loopNoteSilentError(loopInbox, "hub_inbox.go:pendingBatches", errors.New("boom"))
	h.loopClearSilent("hub_inbox.go:pendingBatches")
	found := false
	for _, m := range h.loopsSilentHits() {
		if m == loopInbox {
			found = true
		}
	}
	if !found {
		t.Fatalf("no silent hit on the error's date")
	}
	now = time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	for _, m := range h.loopsSilentHits() {
		if m == loopInbox {
			t.Fatalf("silent hit leaks into the next date")
		}
	}
}

// A failed row PUT backs off like an unreadable row: the next tick makes
// no new PUT until the backoff has passed.
func TestLoopLedgerFailedPublishBacksOff(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	publishes := 0
	publishErr := errors.New("hk unreachable")
	h := loopLedgerTestHub(t, &now, nil)
	h.loopsDocPublish = func(_ context.Context, _, _ string) error { publishes++; return publishErr }
	h.loopsDocExists = func(_ context.Context, _ string) (bool, error) { return false, nil }
	ctx := context.Background()

	h.loopTick(loopSpawnCoordinator)
	now = time.Date(2026, 11, 19, 0, 1, 0, 0, time.UTC)
	h.maybeWriteLoopsRow(ctx)
	if publishes != 1 {
		t.Fatalf("publishes=%d, want 1", publishes)
	}
	h.maybeWriteLoopsRow(ctx)
	if publishes != 1 {
		t.Fatalf("publishes=%d after an immediate retry, want still 1", publishes)
	}
	publishErr = nil
	now = now.Add(2 * loopsRowRetryAfter)
	h.maybeWriteLoopsRow(ctx)
	if publishes != 2 {
		t.Fatalf("publishes=%d after the backoff, want 2", publishes)
	}
}

package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// A quota announcement after a local end (here a revoke) must not be written:
// as the job's newest end it would read as a live hand-over and mask the
// revoke (hk 4761, #593 round 6).
func TestQuotaStopAnnounceDoesNotMaskARevoke_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	if err := a.event(context.Background(), "j", "job.revoked", map[string]any{"pane_id": "w:p1"}); err != nil {
		t.Fatal(err)
	}
	before, _ := a.job("j")
	if err := a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"}); !errors.Is(err, errQuotaStopSuperseded) {
		t.Fatalf("announce after revoke: %v, want errQuotaStopSuperseded", err)
	}
	after, _ := a.job("j")
	if len(after.Events) != len(before.Events) {
		t.Fatalf("events grew after the revoke: %+v", after.Events[len(before.Events):])
	}
	if after.Ended != "job.revoked" {
		t.Fatalf("newest end = %q, want job.revoked", after.Ended)
	}
}

// The announcement waits for the job's end lock, so it orders against the
// other local ends and settlement.
func TestQuotaStopAnnounceWaitsForTheEndLock_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	unlock, err := LockJobEnd(filepath.Join(a.Jobs, "j"))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"})
	}()
	select {
	case err := <-done:
		unlock()
		t.Fatalf("the announcement ran under a held end lock: %v", err)
	case <-time.After(300 * time.Millisecond): // proves the writer waits: nothing more happens in this window
	}
	unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the announcement did not run after the end lock was released")
	}
}

// The operation-lock wait ends when its context does, not only at its bound.
func TestJobOperationLockWaitHonorsContext_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	unlock, err := TryLockJobOperation(filepath.Join(a.Jobs, "j"))
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = a.lockJobOperation(ctx, "j", quotaRecordLockWait)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("wait took %v; the context was not honored", took)
	}
}

func writeLedgerFile(t *testing.T, a *App, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(a.Jobs, "j", "events", name), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// A settled hk record is an end even before job.completed lands (a `done`
// that stopped between the two): no hand-over may follow it (#690 review).
func TestQuotaStopAnnounceRefusedAfterRecordSettled_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	if err := a.event(context.Background(), "j", eventRecordSettled, map[string]any{"pinned": true}); err != nil {
		t.Fatal(err)
	}
	if err := a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"}); !errors.Is(err, errQuotaStopSuperseded) {
		t.Fatalf("announce after record_settled: %v, want errQuotaStopSuperseded", err)
	}
}

// The check reads the ledger in sequence order, not by timestamp: a
// spawn recorded after a revoke under a clock stepped back is the
// current attempt, and the announcement goes ahead (#690 review).
func TestQuotaStopAnnounceReadsLedgerOrder_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	if err := a.event(context.Background(), "j", "job.revoked", map[string]any{"pane_id": "w:p1"}); err != nil {
		t.Fatal(err)
	}
	writeLedgerFile(t, a, "00090-job.spawned.json", `{"created_at":"2025-01-01T00:00:00Z","job_id":"j","kind":"job.spawned","payload":{"pane_id":"w:p1","label":"agent-a","role":"reviewer"},"seq":90}`)
	if err := a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"}); err != nil {
		t.Fatalf("announce on the re-spawned attempt: %v, want nil", err)
	}
}

// An unreadable ledger is an error, never permission to announce (#690 review).
func TestQuotaStopAnnounceFailsClosedOnUnreadableLedger_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	writeLedgerFile(t, a, "00090-job.revoked.json", `{not json`)
	err := a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"})
	if err == nil || errors.Is(err, errQuotaStopSuperseded) {
		t.Fatalf("announce on an unreadable ledger: %v, want a read error", err)
	}
}

// The end-lock wait is bounded even under a context that never ends: the
// quota-stop manager calls with its daemon context (#690 review).
func TestQuotaStopAnnounceEndLockWaitIsBounded_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	unlock, err := LockJobEnd(filepath.Join(a.Jobs, "j"))
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	start := time.Now()
	err = a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if took := time.Since(start); took > quotaRecordLockWait+5*time.Second {
		t.Fatalf("wait took %v", took)
	}
}

// A hub revocation that took the same sequence number as a spawn published
// before it sorts first by name; the tie is treated as an end (#690 review r2).
func TestQuotaStopAnnounceRefusedOnRevocationSpawnTie_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	writeLedgerFile(t, a, "00090-job.spawned.json", `{"created_at":"2026-01-01T00:00:02Z","job_id":"j","kind":"job.spawned","payload":{"pane_id":"w:p1","label":"agent-a","role":"reviewer"},"seq":90}`)
	writeLedgerFile(t, a, "00090-job.revoked.json", `{"created_at":"2026-01-01T00:00:03Z","job_id":"j","kind":"job.revoked","payload":{"pane_id":"w:p1"},"seq":90}`)
	if err := a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"}); !errors.Is(err, errQuotaStopSuperseded) {
		t.Fatalf("announce after a tied revocation: %v, want errQuotaStopSuperseded", err)
	}
}

// A reap stays an end across a stale re-prompt; only a new claim or spawn
// revives a reaped job (#690 review r2).
func TestQuotaStopAnnounceReapIsStickyAcrossReprompt_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	for _, k := range []string{"job.reaped", "job.reprompted"} {
		if err := a.event(context.Background(), "j", k, map[string]any{"pane_id": "w:p1"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"}); !errors.Is(err, errQuotaStopSuperseded) {
		t.Fatalf("announce after reap and re-prompt: %v, want errQuotaStopSuperseded", err)
	}
}

// A tied revocation stays an end even after a later-numbered spawn: with the
// revocation's sequence allocated outside the ledger lock, the order cannot
// show the spawn came after it (#690 review r4).
func TestQuotaStopAnnounceTieStaysAnEnd_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	writeLedgerFile(t, a, "00090-job.brief_sent.json", `{"created_at":"2026-01-01T00:00:02Z","job_id":"j","kind":"job.brief_sent","payload":{},"seq":90}`)
	writeLedgerFile(t, a, "00090-job.revoked.json", `{"created_at":"2026-01-01T00:00:03Z","job_id":"j","kind":"job.revoked","payload":{"pane_id":"w:p1"},"seq":90}`)
	writeLedgerFile(t, a, "00091-job.spawned.json", `{"created_at":"2026-01-01T00:00:01Z","job_id":"j","kind":"job.spawned","payload":{"pane_id":"w:p1","label":"agent-a","role":"reviewer"},"seq":91}`)
	if ended, err := a.attemptEndedBeforeAnnouncement("j", "w:p1"); err != nil || !ended {
		t.Fatalf("tied revocation then spawn: ended=%v err=%v, want ended", ended, err)
	}
}

// A revocation survives a re-prompt: it restores no ownership (#690 review r4).
func TestQuotaStopAnnounceRevokeIsStickyAcrossReprompt_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	for _, k := range []string{"job.revoked", "job.reprompted"} {
		if err := a.event(context.Background(), "j", k, map[string]any{"pane_id": "w:p1"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"}); !errors.Is(err, errQuotaStopSuperseded) {
		t.Fatalf("announce after revoke and re-prompt: %v, want errQuotaStopSuperseded", err)
	}
}

// A hand-over observed on one pane is not announced on a job now spawned on
// another pane (#690 review r4).
func TestQuotaStopAnnounceRefusesAnotherAttemptsPane_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	if err := a.event(context.Background(), "j", "job.spawned", map[string]any{"pane_id": "w:p2", "label": "agent-a", "role": "reviewer"}); err != nil {
		t.Fatal(err)
	}
	if err := a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"}); !errors.Is(err, errQuotaStopSuperseded) {
		t.Fatalf("announce for p1 on a job running on p2: %v, want errQuotaStopSuperseded", err)
	}
}

// A listed event that cannot be read is an error, not an absent ledger.
func TestQuotaStopAnnounceListedEventUnreadable_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	if err := os.Symlink(filepath.Join(a.Jobs, "j", "missing"), filepath.Join(a.Jobs, "j", "events", "00090-job.revoked.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.attemptEndedBeforeAnnouncement("j", "w:p1"); err == nil {
		t.Fatal("unreadable listed event: want an error")
	}
}

// An adopted spawn receipt is late bookkeeping for the revoked attempt and
// does not clear the revocation (#690 review r5).
func TestQuotaStopAnnounceRevokeIsStickyAcrossAdoptedSpawn_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	if err := a.event(context.Background(), "j", "job.revoked", map[string]any{"pane_id": "w:p1"}); err != nil {
		t.Fatal(err)
	}
	if err := a.event(context.Background(), "j", "job.spawned", map[string]any{"pane_id": "w:p1", "label": "agent-a", "role": "reviewer", "adopted_from": "brief_sent"}); err != nil {
		t.Fatal(err)
	}
	if err := a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"}); !errors.Is(err, errQuotaStopSuperseded) {
		t.Fatalf("announce after revoke and adopted spawn: %v, want errQuotaStopSuperseded", err)
	}
}

// After the hk record is written lost, an announcement that cannot read the
// ledger still releases the quota lease (#690 review r5).
func TestQuotaStopFinishReleasesLeaseOnUnreadableLedger_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	if pools, err := a.ledger().QuotaPools(context.Background(), "j"); err != nil || len(pools) == 0 {
		t.Fatalf("fixture lease: pools=%v err=%v", pools, err)
	}
	writeLedgerFile(t, a, "00090-job.note.json", `{not json`)
	if err := a.quotaStopFinish(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"}, true); err == nil {
		t.Fatal("finish on an unreadable ledger: want the read error")
	}
	if err := os.Remove(filepath.Join(a.Jobs, "j", "events", "00090-job.note.json")); err != nil {
		t.Fatal(err)
	}
	if pools, err := a.ledger().QuotaPools(context.Background(), "j"); err != nil || len(pools) != 0 {
		t.Fatalf("lease after finish: pools=%v err=%v, want released", pools, err)
	}
}

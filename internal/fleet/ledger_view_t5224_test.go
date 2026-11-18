package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// The ledger files below are numbered 90 and up so they follow the fixture's
// claim and spawn. Their timestamps are chosen against their sequence.
const (
	ledgerRevokedFarFuture = `{"created_at":"2099-01-01T00:00:00Z","job_id":"j","kind":"job.revoked","payload":{"pane_id":"w:p1"},"seq":90}`
	ledgerSpawnedEarly     = `{"created_at":"2000-01-01T00:00:01Z","job_id":"j","kind":"job.spawned","payload":{"pane_id":"w:p1","label":"agent-a","role":"implementer"},"seq":91}`
	ledgerQuotaLostEarly   = `{"created_at":"2000-01-01T00:00:02Z","job_id":"j","kind":"job.lost","payload":{"pane_id":"w:p1","reason":"quota_stop","pool":"claude"},"seq":92}`
)

// hubRevocationFile is the type-only record the node's dispatcher writes for
// a hub revocation (writeHubRevocation), with the hub's ownership epoch.
const hubRevocationFile = `{"type":"job.revoked","job_id":"j","epoch":3}`

// A clock stepped backwards stamps the attempt that follows a revocation
// earlier than the revocation. The ledger's sequence says the job was
// revoked, respawned and then announced a quota stop; the timestamp fold reads
// the revocation as the newest end and loses the hand-over: no watcher grace,
// no re-arm (hk 5224).
func TestClockRollbackKeepsTheHandOverOfTheLaterAttempt_t5224(t *testing.T) {
	a, _, h, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	a.QuotaHandOffGrace = 2 * time.Minute
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "600")
	writeLedgerFile(t, a, "00090-job.revoked.json", ledgerRevokedFarFuture)
	writeLedgerFile(t, a, "00091-job.spawned.json", ledgerSpawnedEarly)
	writeLedgerFile(t, a, "00092-job.lost.json", ledgerQuotaLostEarly)

	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	if stop, ok := announcedQuotaStop(j); !ok || stop.Pool != "claude" {
		t.Fatalf("announced stop = %+v, %v; want the claude hand-over of the respawned attempt", stop, ok)
	}

	var rearmed []string
	if err := a.RearmWatchers(func(id string) error { rearmed = append(rearmed, id); return nil }, func(string) {}); err != nil {
		t.Fatal(err)
	}
	if len(rearmed) != 1 {
		t.Fatalf("re-armed %v, want the announced job", rearmed)
	}

	// The watcher adopts the hand-over and, with nobody replacing the job,
	// ends it after the grace instead of returning at once.
	started := a.Now()
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}
	if a.Now().Sub(started) < 2*time.Minute {
		t.Fatalf("the watcher ended the job after %v, before the hand-off grace", a.Now().Sub(started))
	}
	if got := str(h.record(t, "j"), "state"); got != "lost" {
		t.Fatalf("hk state = %q, want lost after the grace", got)
	}
}

// A hub revocation that shares its sequence number with a spawn (a legacy
// artifact: it was allocated without the events lock) is an end of that
// attempt in every reader, however the two sort by name: the owner must not
// write the shared hk record and a quota hand-over must not be announced.
func TestRevocationTiedWithAnAttemptStartEndsTheAttempt_t5224(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	writeLedgerFile(t, a, "00090-job.spawned.json", `{"created_at":"2026-01-01T00:00:02Z","job_id":"j","kind":"job.spawned","payload":{"pane_id":"w:p1","label":"agent-a","role":"implementer"},"seq":90}`)
	writeLedgerFile(t, a, "00090-job.revoked.json", hubRevocationFile)

	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	if !j.Reassigned {
		t.Fatal("the tied hub revocation was cleared by the spawn it shares a number with: the owner would write the new owner's hk record")
	}
	if err := a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"}); !errors.Is(err, errQuotaStopSuperseded) {
		t.Fatalf("announce after a tied revocation: %v, want errQuotaStopSuperseded", err)
	}
}

// The tie is a fact about its own sequence number: a claim, reclaim or spawn
// at a strictly greater sequence is a real start and clears it. Vetoing every
// later attempt for a historical tie would leave the later attempt's quota
// stop unannounced and its lease held forever (the #690 r3 liveness cost).
func TestLaterStartClearsAHistoricalRevocationTie_t5224(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	writeLedgerFile(t, a, "00090-job.brief_sent.json", `{"created_at":"2026-01-01T00:00:02Z","job_id":"j","kind":"job.brief_sent","payload":{},"seq":90}`)
	writeLedgerFile(t, a, "00090-job.revoked.json", hubRevocationFile)
	// The respawn follows the tie in the ledger but carries an earlier time.
	writeLedgerFile(t, a, "00091-job.spawned.json", ledgerSpawnedEarly)

	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	if j.Reassigned {
		t.Fatal("the respawn at a greater sequence did not clear the tied revocation")
	}
	if err := a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"}); err != nil {
		t.Fatalf("announce on the respawned attempt: %v, want it to go ahead", err)
	}
	after, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := announcedQuotaStop(after); !ok {
		t.Fatal("the announced hand-over of the respawned attempt does not read as one")
	}
}

// A hub revocation that lands between the announcement's checks and its append
// must stop the append: the dispatcher publishes the revocation under the
// events lock without the end lock, so only a check under that lock sees it.
func TestQuotaAnnounceRacingARevocationAppendsNothing_t5224(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	lock, err := os.OpenFile(filepath.Join(a.Jobs, "j", ".wrk-events.lock"), os.O_WRONLY|os.O_CREATE, 0o666)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	before, _ := a.job("j")
	done := make(chan error, 1)
	go func() {
		done <- a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"})
	}()
	// Fixture pacing: the announcement takes the end and operation locks and
	// waits for the events lock; the dispatcher publishes while it waits.
	time.Sleep(300 * time.Millisecond)
	writeLedgerFile(t, a, "00090-job.revoked.json", hubRevocationFile)
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, errQuotaStopSuperseded) {
			t.Fatalf("announce raced by a revocation: %v, want errQuotaStopSuperseded", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the announcement did not finish after the events lock was released")
	}
	after, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Events) != len(before.Events)+1 {
		t.Fatalf("ledger has %d events, want the revocation alone after %d", len(after.Events), len(before.Events))
	}
	if _, ok := announcedQuotaStop(after); ok || !after.Reassigned {
		t.Fatalf("the revocation is masked: announced=%v reassigned=%v", ok, after.Reassigned)
	}
}

// The other publication order: the announcement lands first and the
// revocation after it ends the hand-over.
func TestRevocationAfterAnAnnouncementEndsTheHandOver_t5224(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	if err := a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"}); err != nil {
		t.Fatal(err)
	}
	writeLedgerFile(t, a, "00090-job.revoked.json", hubRevocationFile)
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := announcedQuotaStop(j); ok || !j.Reassigned {
		t.Fatalf("announced=%v reassigned=%v, want the revocation to end the hand-over", ok, j.Reassigned)
	}
}

// The watcher's resume marker is a decision on the snapshot it saw. A reap of
// the same attempt that followed the snapshot voids it: a job.reprompted after
// a reap would be a stale record the next reader folds as a live attempt.
func TestResumeMarkerIsVoidAfterAReap_t5224(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	announceQuotaStopEvent(t, a, "claude")
	stale, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.event(context.Background(), "j", "job.reaped", map[string]any{"pane_id": "w:p1"}); err != nil {
		t.Fatal(err)
	}

	ran := false
	deferred, err := a.endUnlessHubClose("j", stale, func(Job) error {
		ran = true
		return a.recordQuotaResume(context.Background(), stale)
	})
	if err != nil || !deferred || ran {
		t.Fatalf("endUnlessHubClose after a reap: deferred=%v ran=%v err=%v, want the stale decision dropped", deferred, ran, err)
	}
	// A writer that does not hold the end lock is stopped at the append.
	if err := a.recordQuotaResume(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	after, _ := a.job("j")
	for _, ev := range after.Events {
		if ev.Kind == "job.reprompted" {
			t.Fatalf("a resume marker was written after the reap: %+v", after.Events)
		}
	}
}

// A re-prompt starts a new attempt on the same pane, so a decision observed
// before it names an attempt that is gone: the retried announcement of a
// manager that crashed before it recorded its outcome is refused, and an
// observation of the current attempt goes ahead (#690 r4612664378).
func TestRepromptStartsANewAttemptIdentity_t5224(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	observed, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.event(context.Background(), "j", "job.reprompted", map[string]any{"pane_id": "w:p1", "reason": "quota_stop_resumed"}); err != nil {
		t.Fatal(err)
	}
	before, _ := a.job("j")

	if err := a.quotaStopAnnounce(context.Background(), observed, quotastop.Stop{Pool: "claude"}); !errors.Is(err, errQuotaStopSuperseded) {
		t.Fatalf("announce of the attempt before the re-prompt: %v, want errQuotaStopSuperseded", err)
	}
	if after, _ := a.job("j"); len(after.Events) != len(before.Events) {
		t.Fatalf("a stale announcement was appended: %+v", after.Events[len(before.Events):])
	}
	if err := a.quotaStopAnnounce(context.Background(), before, quotastop.Stop{Pool: "claude"}); err != nil {
		t.Fatalf("announce of the current attempt: %v, want it to go ahead", err)
	}
}

// The attempt identity the manager records with its decision is the view's.
func TestCurrentAttemptNamesTheNewestAttemptEvent_t5224(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	first, err := CurrentAttempt(a.Jobs, "j")
	if err != nil || first == "" {
		t.Fatalf("attempt %q err %v", first, err)
	}
	if err := a.event(context.Background(), "j", "job.reprompted", map[string]any{"pane_id": "w:p1"}); err != nil {
		t.Fatal(err)
	}
	second, err := CurrentAttempt(a.Jobs, "j")
	if err != nil || second == first {
		t.Fatalf("attempt after a re-prompt %q (was %q) err %v, want a new identity", second, first, err)
	}
	if none, err := CurrentAttempt(a.Jobs, "no-such-job"); err != nil || none != NoAttempt {
		t.Fatalf("job without a ledger: %q, %v", none, err)
	}
	// The announcement of the identity the manager recorded still goes ahead.
	if err := a.quotaStopAnnounceAttempt(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"}, second); err != nil {
		t.Fatal(err)
	}
	if err := a.quotaStopAnnounceAttempt(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"}, first); !errors.Is(err, errQuotaStopSuperseded) {
		t.Fatalf("announce of the earlier identity: %v, want errQuotaStopSuperseded", err)
	}
}

func mustAttempt(t *testing.T, a *App) string {
	t.Helper()
	attempt, err := CurrentAttempt(a.Jobs, "j")
	if err != nil {
		t.Fatal(err)
	}
	return attempt
}

// A decision the manager recorded without a verifiable attempt (a row from
// before attempts were bound) is dropped rather than announced for whatever
// attempt runs now; so is one bound to an attempt a re-prompt has replaced
// (#699 review).
func TestAnnounceQuotaStopRefusesAnUnboundOrStaleDecision_t5224(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	started := 0
	old := announceStartWatch
	announceStartWatch = func(*App, string) error { started++; return nil }
	t.Cleanup(func() { announceStartWatch = old })
	observed := mustAttempt(t, a)
	if err := a.event(context.Background(), "j", "job.reprompted", map[string]any{"pane_id": "w:p1"}); err != nil {
		t.Fatal(err)
	}
	before, _ := a.job("j")
	for name, attempt := range map[string]string{"unbound": "", "stale": observed} {
		if err := AnnounceQuotaStop(context.Background(), a.Jobs, "j", "w:p1", attempt, quotastop.Stop{Pool: "claude"}); err != nil {
			t.Fatalf("%s: %v, want the decision dropped without an error", name, err)
		}
	}
	after, _ := a.job("j")
	if len(after.Events) != len(before.Events) || started != 0 {
		t.Fatalf("events %d -> %d, watchers %d: want nothing written for an unbound or stale decision", len(before.Events), len(after.Events), started)
	}
}

// A ledger that has no attempt yet has its own identity: an announcement
// bound to it is refused once an attempt exists.
func TestNoAttemptIdentityIsNotAWildcard_t5224(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	if err := a.quotaStopAnnounceAttempt(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"}, NoAttempt); !errors.Is(err, errQuotaStopSuperseded) {
		t.Fatalf("announce bound to NoAttempt on a job with attempts: %v, want errQuotaStopSuperseded", err)
	}
}

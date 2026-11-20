package panewire

import (
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The wait-sync tests drive the same sighting/report paths the Sentry tests
// use; the fake handoffkeep answers the intake and waits routes the sync
// calls. settleOneTimeNotices is the barrier that ends on the worker having
// applied the batch and finished the hk writes it started.

func oneTimeWaitDialog(at time.Time) *fleet.Readiness {
	return readinessWith(at, "codex", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent, Dialog: fleet.DialogCodexHooksReview})
}

func oneTimeWaitClean(at time.Time) *fleet.Readiness {
	return readinessWith(at, "codex", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent})
}

func oneTimeWaitKey() oneTimeActionKey {
	return oneTimeActionKey{machine: "oci", kind: "codex", need: fleet.DialogCodexHooksReview}
}

// one standing need holds exactly one intake anchor and one open operator
// wait however often it is sighted, and the ids are durable on the row.
func TestOneTimeWaitOpensOncePerStandingNeed(t *testing.T) {
	h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
	transport := oneTimeSentry(t, h)
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	key := oneTimeWaitKey()
	ref := oneTimeWaitSourceRef(key)

	h.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
	h.noteReadinessActions("oci", oneTimeWaitDialog(start.Add(time.Minute)), start.Add(time.Minute))
	h.noteOneTimeActionRefusal("oci", "codex-sol", hubSpawnResult{RC: 90, StdoutTail: hooksTail})
	settleOneTimeNotices(t, h)

	anchors := fake.oneTimeAnchors(ref)
	if len(anchors) != 1 {
		t.Fatalf("intake anchors=%d, want 1", len(anchors))
	}
	anchor := anchors[0]
	if anchor.State != "triage" {
		t.Fatalf("anchor state=%q, want triage", anchor.State)
	}
	waits := fake.openOperatorWaits()
	if len(waits) != 1 {
		t.Fatalf("open operator waits=%d, want 1", len(waits))
	}
	wait := waits[0]
	if wait.TaskID != anchor.ID || wait.OnKind != "operator" || wait.OnRef != "" {
		t.Fatalf("wait=%+v anchor=%+v", wait, anchor)
	}
	if !strings.Contains(wait.Reason, "oci codex") || !strings.Contains(wait.Reason, `run codex once and choose "Trust all and continue"`) {
		t.Fatalf("wait reason=%q", wait.Reason)
	}
	// The agreed intake fields reached the wire.
	var intake map[string]any
	for _, call := range fake.calls {
		if call.Method == http.MethodPost && call.Path == "/v1/intake" {
			intake = call.Body
		}
	}
	if intake == nil {
		t.Fatal("no POST /v1/intake reached handoffkeep")
	}
	if intake["source"] != "panewire" || intake["source_ref"] != ref || intake["lane"] != "operator-desk" {
		t.Fatalf("intake body=%v", intake)
	}
	// The ids are durable on the need's row, not only in memory.
	h.oneTimeMu.Lock()
	state := h.oneTimeActions[key]
	h.oneTimeMu.Unlock()
	if state.WaitTaskID != anchor.ID || state.WaitID != wait.ID {
		t.Fatalf("memory ids task=%d wait=%d, want %d/%d", state.WaitTaskID, state.WaitID, anchor.ID, wait.ID)
	}
	var storedTask, storedWait int64
	if err := h.inbox.store.db.QueryRow(`SELECT wait_task_id,wait_id FROM one_time_action_notices WHERE machine=? AND kind=? AND need=?`, key.machine, key.kind, key.need).Scan(&storedTask, &storedWait); err != nil {
		t.Fatal(err)
	}
	if storedTask != anchor.ID || storedWait != wait.ID {
		t.Fatalf("durable ids task=%d wait=%d, want %d/%d", storedTask, storedWait, anchor.ID, wait.ID)
	}
	if got := oneTimeMessages(t, h, transport); len(got) != 1 {
		t.Fatalf("sentry messages=%q, want the one ACTION NEEDED", got)
	}
}

// The wait closes only after anti-flap clearing confirms the need stayed gone
// for a full cycle; a flap inside the cycle keeps it open. A need that
// returns opens a new wait on the same anchor.
func TestOneTimeWaitClosesAfterAntiFlapAndReopensOnReturn(t *testing.T) {
	h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
	transport := oneTimeSentry(t, h)
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	cycle := oneTimeActionClearCycle
	key := oneTimeWaitKey()
	ref := oneTimeWaitSourceRef(key)

	report := func(at time.Time, harness fleet.HarnessReadiness) {
		h.noteReadinessActions("oci", readinessWith(at, "codex", harness), at)
	}
	dialog := fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent, Dialog: fleet.DialogCodexHooksReview}
	clean := fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent}

	report(start, dialog)
	settleOneTimeNotices(t, h)
	anchor := fake.oneTimeAnchors(ref)[0]
	first := fake.openOperatorWaits()[0]

	// Gone, then back inside the cycle: the pending clear is cancelled and no
	// close call may have happened.
	gone := start.Add(time.Minute)
	report(gone, clean)
	report(gone.Add(time.Minute), dialog)
	settleOneTimeNotices(t, h)
	if n := fake.oneTimeCallCount(http.MethodPost, "/clear"); n != 0 {
		t.Fatalf("wait cleared before the anti-flap confirm: %d clear calls", n)
	}
	if got := fake.openOperatorWaits(); len(got) != 1 || got[0].ID != first.ID {
		t.Fatalf("open waits after the flap=%v", got)
	}

	// Gone for a full cycle: the same wait closes and the row drops.
	report(gone.Add(2*time.Minute), clean)
	report(gone.Add(2*time.Minute+cycle), clean)
	settleOneTimeNotices(t, h)
	if n := fake.oneTimeCallCount(http.MethodPost, "/clear"); n != 1 {
		t.Fatalf("clear calls=%d, want exactly 1", n)
	}
	if got := fake.openOperatorWaits(); len(got) != 0 {
		t.Fatalf("open waits after the confirmed clear=%v", got)
	}
	for _, wait := range fake.allWaits() {
		if wait.ID == first.ID && !wait.Cleared {
			t.Fatalf("first wait still open: %+v", wait)
		}
	}
	h.oneTimeMu.Lock()
	_, stillTold := h.oneTimeActions[key]
	h.oneTimeMu.Unlock()
	if stillTold {
		t.Fatal("the cleared need's row survived the wait close")
	}

	// The need returns: Sentry tells again and a new wait opens on the same
	// anchor — the old wait stays cleared as history.
	back := gone.Add(2*time.Minute + cycle + time.Minute)
	report(back, dialog)
	settleOneTimeNotices(t, h)
	if got := oneTimeMessages(t, h, transport); len(got) != 2 {
		t.Fatalf("sentry messages=%q, want the recurrence told again", got)
	}
	anchors := fake.oneTimeAnchors(ref)
	if len(anchors) != 1 || anchors[0].ID != anchor.ID {
		t.Fatalf("anchors after recurrence=%v, want the same anchor %d", anchors, anchor.ID)
	}
	waits := fake.openOperatorWaits()
	if len(waits) != 1 || waits[0].ID == first.ID || waits[0].TaskID != anchor.ID {
		t.Fatalf("waits after recurrence=%v, want one new open wait on anchor %d", waits, anchor.ID)
	}
	if len(fake.allWaits()) != 2 {
		t.Fatalf("wait rows=%d, want the cleared first plus the new open", len(fake.allWaits()))
	}
}

// A failed handoffkeep write leaves the Sentry notice untouched and retries
// on the next observation; the need is never marked handled by an hk error.
func TestOneTimeWaitHkFailureRetriesAndNeverBlocksSentry(t *testing.T) {
	h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
	transport := oneTimeSentry(t, h)
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)

	fake.mu.Lock()
	fake.waitStatus = http.StatusInternalServerError
	fake.mu.Unlock()
	h.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
	settleOneTimeNotices(t, h)

	if got := oneTimeMessages(t, h, transport); len(got) != 1 {
		t.Fatalf("sentry messages=%q, want the need told despite the hk failure", got)
	}
	if got := fake.openOperatorWaits(); len(got) != 0 {
		t.Fatalf("open waits=%v after a failed write", got)
	}
	key := oneTimeWaitKey()
	h.oneTimeMu.Lock()
	state := h.oneTimeActions[key]
	h.oneTimeMu.Unlock()
	if state.WaitTaskID != 0 || state.WaitID != 0 {
		t.Fatalf("wait ids recorded without a confirmed write: %+v", state)
	}

	// Intake failing is the same story on the other write: the flags land
	// before the sighting that would use them.
	fake.mu.Lock()
	fake.intakeStatus = http.StatusInternalServerError
	fake.waitStatus = 0
	fake.mu.Unlock()
	h.noteReadinessActions("mbp", oneTimeWaitDialog(start), start)
	settleOneTimeNotices(t, h)
	if got := oneTimeMessages(t, h, transport); len(got) != 2 {
		t.Fatalf("sentry messages=%q, want both needs told", got)
	}
	if n := fake.oneTimeCallCount(http.MethodPost, "/waits"); n != 1 {
		t.Fatalf("wait writes=%d, want only oci's earlier failed attempt retried once", n)
	}

	// hk recovers: the next sighting of each standing need lands its wait.
	fake.mu.Lock()
	fake.intakeStatus = 0
	fake.mu.Unlock()
	h.noteReadinessActions("oci", oneTimeWaitDialog(start.Add(time.Minute)), start.Add(time.Minute))
	h.noteReadinessActions("mbp", oneTimeWaitDialog(start.Add(time.Minute)), start.Add(time.Minute))
	settleOneTimeNotices(t, h)
	if got := fake.openOperatorWaits(); len(got) != 2 {
		t.Fatalf("open waits=%d, want both needs' waits retried open", len(got))
	}
	if got := oneTimeMessages(t, h, transport); len(got) != 2 {
		t.Fatalf("sentry messages=%q, hk recovery must not re-tell", got)
	}
}

// The wait ids live on the durable row: a restarted hub reloads them, keeps
// the same anchor and wait, and still closes the wait after anti-flap.
func TestOneTimeWaitSurvivesHubRestart(t *testing.T) {
	t.Setenv(crashReportDSNEnv, "")
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	fake, client, stop := newFakeHandoffkeep(t)
	defer stop()
	newHub := func() *HubServer {
		h, err := NewHubServer(HubServerConfig{
			Tokens:         map[string]string{"operator": t14OperatorToken},
			InboxStorePath: path,
			Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
			handoffkeep:    client,
		})
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	cycle := oneTimeActionClearCycle
	key := oneTimeWaitKey()
	ref := oneTimeWaitSourceRef(key)

	first := newHub()
	firstSentry := oneTimeSentry(t, first)
	first.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
	if got := oneTimeMessages(t, first, firstSentry); len(got) != 1 {
		t.Fatalf("first hub sent %q", got)
	}
	anchor := fake.oneTimeAnchors(ref)[0]
	wait := fake.openOperatorWaits()[0]
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := newHub()
	defer second.Close()
	secondSentry := oneTimeSentry(t, second)
	standing := start.Add(time.Minute)
	second.noteReadinessActions("oci", oneTimeWaitDialog(standing), standing)
	settleOneTimeNotices(t, second)
	if got := oneTimeMessages(t, second, secondSentry); len(got) != 0 {
		t.Fatalf("a restart told a standing need again: %q", got)
	}
	if anchors := fake.oneTimeAnchors(ref); len(anchors) != 1 || anchors[0].ID != anchor.ID {
		t.Fatalf("anchors after restart=%v, want the same anchor %d", anchors, anchor.ID)
	}
	if waits := fake.openOperatorWaits(); len(waits) != 1 || waits[0].ID != wait.ID {
		t.Fatalf("open waits after restart=%v, want wait %d", waits, wait.ID)
	}

	// Anti-flap clear on the restarted hub closes the same wait from the
	// reloaded ids.
	second.noteReadinessActions("oci", oneTimeWaitClean(start.Add(2*time.Minute)), start.Add(2*time.Minute))
	second.noteReadinessActions("oci", oneTimeWaitClean(start.Add(2*time.Minute+cycle)), start.Add(2*time.Minute+cycle))
	settleOneTimeNotices(t, second)
	if got := fake.openOperatorWaits(); len(got) != 0 {
		t.Fatalf("open waits after restart clear=%v", got)
	}
	if n := fake.oneTimeCallCount(http.MethodPost, "/clear"); n != 1 {
		t.Fatalf("clear calls=%d, want 1", n)
	}
}

// A need whose open write never confirmed still closes cleanly: the close
// resolves the anchor and wait through the same idempotent path, so no
// orphaned wait is left open in handoffkeep.
func TestOneTimeWaitCloseResolvesUnconfirmedOpen(t *testing.T) {
	h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
	oneTimeSentry(t, h)
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	cycle := oneTimeActionClearCycle

	fake.mu.Lock()
	fake.waitStatus = http.StatusInternalServerError
	fake.mu.Unlock()
	h.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
	settleOneTimeNotices(t, h)
	key := oneTimeWaitKey()
	h.oneTimeMu.Lock()
	state := h.oneTimeActions[key]
	h.oneTimeMu.Unlock()
	if state.WaitID != 0 {
		t.Fatalf("wait id recorded despite the failed write: %+v", state)
	}

	// The need clears for a full cycle while hk still fails the wait write;
	// once hk recovers the close resolves and ends whatever wait exists.
	h.noteReadinessActions("oci", oneTimeWaitClean(start.Add(time.Minute)), start.Add(time.Minute))
	h.noteReadinessActions("oci", oneTimeWaitClean(start.Add(time.Minute+cycle)), start.Add(time.Minute+cycle))
	settleOneTimeNotices(t, h)
	fake.mu.Lock()
	fake.waitStatus = 0
	fake.mu.Unlock()
	// One more gone report re-confirms the pending close and retries it.
	h.noteReadinessActions("oci", oneTimeWaitClean(start.Add(time.Minute+cycle+time.Minute)), start.Add(time.Minute+cycle+time.Minute))
	settleOneTimeNotices(t, h)

	if got := fake.openOperatorWaits(); len(got) != 0 {
		t.Fatalf("open waits=%v after the recovered close", got)
	}
	cleared := 0
	for _, wait := range fake.allWaits() {
		if wait.Cleared {
			cleared++
		}
	}
	if cleared == 0 {
		t.Fatal("no wait row was ever created and closed; the orphan path did not run")
	}
	h.oneTimeMu.Lock()
	_, stillTold := h.oneTimeActions[key]
	h.oneTimeMu.Unlock()
	if stillTold {
		t.Fatal("the need's row survived the recovered close")
	}
}

// The desk settling the anchor ends it: panewire asks again under a new
// source_ref and moves the wait, closing the superseded one so the need
// never holds two open waits.
func TestOneTimeWaitReanchorsAfterDeskSettlesAnchor(t *testing.T) {
	h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
	oneTimeSentry(t, h)
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	key := oneTimeWaitKey()
	ref := oneTimeWaitSourceRef(key)

	h.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
	settleOneTimeNotices(t, h)
	first := fake.oneTimeAnchors(ref)
	if len(first) != 1 {
		t.Fatalf("anchors=%d, want 1", len(first))
	}
	deadWait := fake.openOperatorWaits()[0]

	fake.settleOneTimeAnchor(ref, "dropped")
	h.noteReadinessActions("oci", oneTimeWaitDialog(start.Add(time.Minute)), start.Add(time.Minute))
	settleOneTimeNotices(t, h)

	anchors := fake.oneTimeAnchors(ref)
	if len(anchors) != 2 {
		t.Fatalf("anchors=%d, want the dead one plus a fresh anchor", len(anchors))
	}
	live := anchors[1]
	if live.State == "dropped" || live.ID == first[0].ID {
		t.Fatalf("live anchor=%+v dead=%+v", live, first[0])
	}
	if live.Ref == first[0].Ref {
		t.Fatalf("the settled anchor was reused: ref=%q", live.Ref)
	}
	waits := fake.openOperatorWaits()
	if len(waits) != 1 || waits[0].TaskID != live.ID {
		t.Fatalf("open waits=%v, want one on the live anchor %d", waits, live.ID)
	}
	for _, wait := range fake.allWaits() {
		if wait.ID == deadWait.ID && !wait.Cleared {
			t.Fatalf("the superseded wait is still open: %+v", wait)
		}
	}
}

// An anchor handoffkeep lost is intaken fresh under the same source_ref;
// the superseded wait's clear answers not_found and the new ids land.
func TestOneTimeWaitReintakesAfterAnchorLost(t *testing.T) {
	h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
	oneTimeSentry(t, h)
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	key := oneTimeWaitKey()
	ref := oneTimeWaitSourceRef(key)

	h.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
	settleOneTimeNotices(t, h)
	first := fake.oneTimeAnchors(ref)
	if len(first) != 1 {
		t.Fatalf("anchors=%d, want 1", len(first))
	}
	fake.dropOneTimeAnchor(ref)
	h.noteReadinessActions("oci", oneTimeWaitDialog(start.Add(time.Minute)), start.Add(time.Minute))
	settleOneTimeNotices(t, h)

	anchors := fake.oneTimeAnchors(ref)
	if len(anchors) != 1 || anchors[0].ID == first[0].ID {
		t.Fatalf("anchors=%v, want one fresh anchor after the loss", anchors)
	}
	waits := fake.openOperatorWaits()
	if len(waits) != 1 || waits[0].TaskID != anchors[0].ID {
		t.Fatalf("open waits=%v, want one on the fresh anchor %d", waits, anchors[0].ID)
	}
}

// With no handoffkeep client the wait sync is a no-op: nothing reaches a
// server and the durable row keeps zero wait ids (existing Sentry tests run
// this way).
func TestOneTimeWaitNoopsWithoutHandoffkeep(t *testing.T) {
	h := newOneTimeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	transport := oneTimeSentry(t, h)
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	h.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
	if got := oneTimeMessages(t, h, transport); len(got) != 1 {
		t.Fatalf("sentry messages=%q", got)
	}
	key := oneTimeWaitKey()
	h.oneTimeMu.Lock()
	state := h.oneTimeActions[key]
	h.oneTimeMu.Unlock()
	if state.WaitTaskID != 0 || state.WaitID != 0 {
		t.Fatalf("wait ids without handoffkeep: %+v", state)
	}
}

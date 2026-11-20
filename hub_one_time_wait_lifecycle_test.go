package panewire

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/mgh3326/panewire/internal/fleet"
)

// These tests pin the operator-wait lifecycle that review found unsafe: a row
// dropped while its wait is open, a replacement opened before the old wait was
// cleared, handoffkeep I/O on the Sentry worker, an anchor walk that could not
// pass eight settled anchors, a wait written without a durable row, and a close
// that created a wait for a need that never had one.

// oneTimeRowCount is the number of need rows the hub store holds.
func oneTimeRowCount(t *testing.T, h *HubServer) int {
	t.Helper()
	var n int
	if err := h.inbox.store.db.QueryRow(`SELECT COUNT(*) FROM one_time_action_notices`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// liveHkAnchor is the one anchor the fake holds in the triage state, copied.
func liveHkAnchor(fake *fakeHandoffkeep) fakeHkAnchor {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, anchor := range fake.intakeTasks {
		if anchor.State == "triage" {
			return *anchor
		}
	}
	return fakeHkAnchor{}
}

// settleHkAnchorByID settles exactly one anchor, leaving its chain alone.
func settleHkAnchorByID(fake *fakeHandoffkeep, id int64, state string) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, anchor := range fake.intakeTasks {
		if anchor.ID == id {
			anchor.State = state
		}
	}
}

// setFakeHk sets one of the fake's failure knobs under its lock.
func setFakeHk(fake *fakeHandoffkeep, set func(*fakeHandoffkeep)) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	set(fake)
}

// sentryCount is the number of Sentry events the transport has accepted.
func sentryCount(transport *spawnSentryTransport) int {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return len(transport.events)
}

// The need's wait clears before a replacement opens: when the old wait's clear
// fails, no second open wait may appear beside it.
func TestOneTimeWaitClearsSupersededWaitBeforeOpening(t *testing.T) {
	h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
	oneTimeSentry(t, h)
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)

	h.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
	settleOneTimeNotices(t, h)
	oldAnchor := liveHkAnchor(fake)
	oldWait := fake.openOperatorWaits()[0]

	setFakeHk(fake, func(f *fakeHandoffkeep) { f.clearStatus = http.StatusInternalServerError })
	settleHkAnchorByID(fake, oldAnchor.ID, "dropped")
	h.noteReadinessActions("oci", oneTimeWaitDialog(start.Add(time.Minute)), start.Add(time.Minute))
	settleOneTimeNotices(t, h)

	open := fake.openOperatorWaits()
	if len(open) != 1 || open[0].ID != oldWait.ID {
		t.Fatalf("open waits while the old clear fails=%+v, want only the old wait %d", open, oldWait.ID)
	}

	setFakeHk(fake, func(f *fakeHandoffkeep) { f.clearStatus = 0 })
	h.noteReadinessActions("oci", oneTimeWaitDialog(start.Add(2*time.Minute)), start.Add(2*time.Minute))
	settleOneTimeNotices(t, h)

	live := liveHkAnchor(fake)
	waits := fake.openOperatorWaits()
	if len(waits) != 1 || waits[0].TaskID != live.ID || waits[0].ID == oldWait.ID {
		t.Fatalf("open waits after the clear recovers=%+v, want one new wait on live anchor %d", waits, live.ID)
	}
	for _, wait := range fake.allWaits() {
		if wait.ID == oldWait.ID && !wait.Cleared {
			t.Fatalf("the superseded wait is still open: %+v", wait)
		}
	}
}

// A confirmed clear whose close fails keeps the close owed through a
// recurrence: the wait is still open, so a recurrence whose notice is rejected
// and a later clean confirm still close it.
func TestOneTimeWaitRecurrenceKeepsCloseObligation(t *testing.T) {
	h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
	oneTimeSentry(t, h)
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	cycle := oneTimeActionClearCycle
	report := func(at time.Time, harness fleet.HarnessReadiness) {
		h.noteReadinessActions("oci", readinessWith(at, "codex", harness), at)
	}
	dialog := fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent, Dialog: fleet.DialogCodexHooksReview}
	clean := fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent}

	report(start, dialog)
	settleOneTimeNotices(t, h)
	if got := fake.openOperatorWaits(); len(got) != 1 {
		t.Fatalf("open waits=%v, want the one standing wait", got)
	}

	// Confirmed gone, but the close's clear fails.
	setFakeHk(fake, func(f *fakeHandoffkeep) { f.clearStatus = http.StatusInternalServerError })
	gone := start.Add(time.Minute)
	report(gone, clean)
	report(gone.Add(cycle), clean)
	settleOneTimeNotices(t, h)
	if got := fake.openOperatorWaits(); len(got) != 1 {
		t.Fatalf("open waits after the failed close=%v, want the wait still open", got)
	}

	// The need returns and its notice is rejected, then the need goes again.
	h.oneTimeNotify = func(*sentry.Event) (bool, error) { return false, errors.New("sentry rejected the notice") }
	back := gone.Add(cycle + time.Minute)
	report(back, dialog)
	settleOneTimeNotices(t, h)
	setFakeHk(fake, func(f *fakeHandoffkeep) { f.clearStatus = 0 })
	report(back.Add(time.Minute), clean)
	report(back.Add(time.Minute+cycle), clean)
	settleOneTimeNotices(t, h)

	if got := fake.openOperatorWaits(); len(got) != 0 {
		t.Fatalf("open waits after the need cleared again=%v; the owed close was dropped", got)
	}
}

// The handoffkeep wait runs off the Sentry worker: a slow intake for one need
// leaves a second need's notice on time.
func TestOneTimeWaitHandoffkeepDoesNotBlockSentry(t *testing.T) {
	h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
	transport := oneTimeSentry(t, h)
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)

	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	releaseIntake := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseIntake)
	setFakeHk(fake, func(f *fakeHandoffkeep) {
		f.observe = func(method, path string) {
			if method == http.MethodPost && path == "/v1/intake" {
				enterOnce.Do(func() {
					close(entered)
					<-release
				})
			}
		}
	})

	h.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("no intake reached handoffkeep")
	}
	// The first need's intake is blocked in handoffkeep now.
	h.noteReadinessActions("mbp", oneTimeWaitDialog(start), start)
	if !waitUntil(10*time.Second, func() bool { return sentryCount(transport) == 2 }) {
		t.Fatalf("second need's notice was held behind the handoffkeep wait: %d sent", sentryCount(transport))
	}

	releaseIntake()
	settleOneTimeNotices(t, h)
	if got := fake.openOperatorWaits(); len(got) != 2 {
		t.Fatalf("open waits=%d after handoffkeep answers, want both needs' waits", len(got))
	}
}

// The walk along settled anchors keeps its position, so a need whose anchor was
// settled more times than one pass can walk still gets its wait on the live
// anchor.
func TestOneTimeWaitWalksPastNineSettledAnchors(t *testing.T) {
	h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
	oneTimeSentry(t, h)
	at := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)

	h.noteReadinessActions("oci", oneTimeWaitDialog(at), at)
	settleOneTimeNotices(t, h)
	for settled := 1; settled <= 9; settled++ {
		live := liveHkAnchor(fake)
		if live.ID == 0 {
			t.Fatalf("no live anchor after %d settled anchors", settled-1)
		}
		settleHkAnchorByID(fake, live.ID, "dropped")
		at = at.Add(time.Minute)
		h.noteReadinessActions("oci", oneTimeWaitDialog(at), at)
		settleOneTimeNotices(t, h)
	}

	live := liveHkAnchor(fake)
	waits := fake.openOperatorWaits()
	if live.ID == 0 || len(waits) != 1 || waits[0].TaskID != live.ID {
		t.Fatalf("after nine settled anchors live=%+v open waits=%+v, want one wait on the live anchor", live, waits)
	}
}

// No operator wait is written without a durable row. With delivery off there
// is no store, so nothing is written at all; a store that refuses the row
// blocks the wait until it accepts the row.
func TestOneTimeWaitNeedsDurableRowBeforeWait(t *testing.T) {
	t.Setenv(crashReportDSNEnv, "")
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)

	t.Run("delivery off has no store and no wait", func(t *testing.T) {
		fake, client, stop := newFakeHandoffkeep(t)
		defer stop()
		h, err := NewHubServer(HubServerConfig{
			Tokens:             map[string]string{"operator": t14OperatorToken},
			DeliveryPolicyPath: "off",
			InboxStorePath:     filepath.Join(t.TempDir(), "inbox.sqlite3"),
			Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
			handoffkeep:        client,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		transport := oneTimeSentry(t, h)
		h.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
		if !waitUntil(10*time.Second, func() bool { return sentryCount(transport) == 1 }) {
			t.Fatal("the need was not told")
		}
		settleOneTimeNotices(t, h)
		if n := fake.oneTimeCallCount(http.MethodPost, "/waits"); n != 0 {
			t.Fatalf("wait writes=%d with no durable row", n)
		}
		if n := fake.oneTimeCallCount(http.MethodPost, "/v1/intake"); n != 0 {
			t.Fatalf("intake writes=%d with no durable row", n)
		}
	})

	t.Run("a refused row blocks the wait until the store takes it", func(t *testing.T) {
		h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
		oneTimeSentry(t, h)
		db := h.inbox.store.db
		if _, err := db.Exec(`CREATE TRIGGER refuse_one_time_insert BEFORE INSERT ON one_time_action_notices BEGIN SELECT RAISE(ABORT, 'store refused'); END`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`CREATE TRIGGER refuse_one_time_update BEFORE UPDATE ON one_time_action_notices BEGIN SELECT RAISE(ABORT, 'store refused'); END`); err != nil {
			t.Fatal(err)
		}

		h.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
		settleOneTimeNotices(t, h)
		if n := fake.oneTimeCallCount(http.MethodPost, "/waits"); n != 0 {
			t.Fatalf("wait writes=%d while the store refused the row", n)
		}

		for _, drop := range []string{"refuse_one_time_insert", "refuse_one_time_update"} {
			if _, err := db.Exec(`DROP TRIGGER ` + drop); err != nil {
				t.Fatal(err)
			}
		}
		h.noteReadinessActions("oci", oneTimeWaitDialog(start.Add(time.Minute)), start.Add(time.Minute))
		settleOneTimeNotices(t, h)
		if got := fake.openOperatorWaits(); len(got) != 1 {
			t.Fatalf("open waits=%d after the store took the row, want 1", len(got))
		}
	})
}

// A wait is never dropped for want of a handoffkeep client: the rows keep their
// wait ids, and the close runs once a client returns.
func TestOneTimeWaitKeepsCleanupWithoutHandoffkeep(t *testing.T) {
	t.Setenv(crashReportDSNEnv, "")
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	fake, client, stop := newFakeHandoffkeep(t)
	defer stop()
	newHub := func(client *handoffkeepRelayClient) *HubServer {
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

	first := newHub(client)
	oneTimeSentry(t, first)
	first.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
	settleOneTimeNotices(t, first)
	if got := fake.openOperatorWaits(); len(got) != 1 {
		t.Fatalf("open waits=%d before the restart, want 1", len(got))
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	// This run has no handoffkeep client: the clear is confirmed, nothing is
	// sent, and the wait's row must survive.
	second := newHub(nil)
	oneTimeSentry(t, second)
	gone := start.Add(time.Minute)
	second.noteReadinessActions("oci", oneTimeWaitClean(gone), gone)
	second.noteReadinessActions("oci", oneTimeWaitClean(gone.Add(cycle)), gone.Add(cycle))
	settleOneTimeNotices(t, second)
	if n := oneTimeRowCount(t, second); n != 1 {
		t.Fatalf("rows=%d without a client, want the wait's row kept", n)
	}
	if n := fake.oneTimeCallCount(http.MethodPost, "/clear"); n != 0 {
		t.Fatalf("clear calls=%d without a client", n)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}

	// The client returns: the owed close runs with no new report.
	third := newHub(client)
	defer third.Close()
	settleOneTimeWaits(t, third)
	if got := fake.openOperatorWaits(); len(got) != 0 {
		t.Fatalf("open waits=%v after the client returned, want the owed close done", got)
	}
	if n := oneTimeRowCount(t, third); n != 0 {
		t.Fatalf("rows=%d after the close, want 0", n)
	}
}

// A confirmed clear whose close failed is retried at startup, not only when the
// next readiness report arrives.
func TestOneTimeWaitConfirmedCloseRetriesAtStartup(t *testing.T) {
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

	first := newHub()
	oneTimeSentry(t, first)
	first.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
	settleOneTimeNotices(t, first)
	setFakeHk(fake, func(f *fakeHandoffkeep) { f.clearStatus = http.StatusInternalServerError })
	gone := start.Add(time.Minute)
	first.noteReadinessActions("oci", oneTimeWaitClean(gone), gone)
	first.noteReadinessActions("oci", oneTimeWaitClean(gone.Add(cycle)), gone.Add(cycle))
	settleOneTimeNotices(t, first)
	if got := fake.openOperatorWaits(); len(got) != 1 {
		t.Fatalf("open waits=%d after the failed close, want 1", len(got))
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	setFakeHk(fake, func(f *fakeHandoffkeep) { f.clearStatus = 0 })
	second := newHub()
	defer second.Close()
	settleOneTimeWaits(t, second)
	if got := fake.openOperatorWaits(); len(got) != 0 {
		t.Fatalf("open waits=%v after restart, want the confirmed close retried at startup", got)
	}
}

// A close never writes a wait for a need that holds none: when the open was
// refused, nothing is open and the close must not create a wait to find one.
func TestOneTimeWaitCloseNeverCreatesForResolvedNeed(t *testing.T) {
	h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
	oneTimeSentry(t, h)
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	cycle := oneTimeActionClearCycle
	clean := fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent}

	setFakeHk(fake, func(f *fakeHandoffkeep) { f.waitStatus = http.StatusBadRequest })
	h.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
	settleOneTimeNotices(t, h)
	if got := fake.openOperatorWaits(); len(got) != 0 {
		t.Fatalf("open waits=%v after a refused open", got)
	}

	setFakeHk(fake, func(f *fakeHandoffkeep) { f.waitStatus = 0 })
	before := fake.oneTimeCallCount(http.MethodPost, "/waits")
	gone := start.Add(time.Minute)
	h.noteReadinessActions("oci", readinessWith(gone, "codex", clean), gone)
	h.noteReadinessActions("oci", readinessWith(gone.Add(cycle), "codex", clean), gone.Add(cycle))
	settleOneTimeNotices(t, h)
	if n := fake.oneTimeCallCount(http.MethodPost, "/waits") - before; n != 0 {
		t.Fatalf("the close wrote %d wait(s) for a need that never had one", n)
	}
	if got := fake.openOperatorWaits(); len(got) != 0 {
		t.Fatalf("open waits=%v after the close", got)
	}
}

package panewire

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The follow-ups on the operator's release of an unresolved ladder stage (hk
// 6137). The release's owner notice is durable: it is written with the release
// and relayed from the spawn notice outbox until the owner's lane takes it.

// releasedNotices collects the owner notices of ladder releases into seen. Reading
// the design lane drains it, so every notice read is kept here.
func releasedNotices(f *ladderCloseFixture, seen *[]string) {
	for _, text := range f.infoTexts() {
		if strings.Contains(text, "released the unresolved staged successor") {
			*seen = append(*seen, text)
		}
	}
}

// A release whose relay is down leaves its owner notice in the outbox, committed
// with the release. The flush delivers it once the relay answers, and a repeat of
// the release after delivery files nothing again.
func TestLadderReleaseNoticeIsDurableUntilRelayed(t *testing.T) {
	f := newLadderCloseFixture(t, "pw-task-6137-notice", true)
	stageID := f.stageLiveSuccessorWithClose(t, &fleet.LadderCloseBinding{Protocol: fleet.LadderCloseProtocolMarker}, true)
	eventID := ladderReleaseEventPrefix + stageID
	// The spawn coordinator runs the notice gate. Start it after the setup and let
	// its first pass settle, so the only thing that can wake the gate for the
	// release is the release's own commit (no earlier pending pass can deliver it).
	f.hub.startSpawnCoordinator(t.Context())
	gate := f.hub.inbox.store.readGate(readSpawnNotice)
	eventually(t, 5*time.Second, "the notice gate to settle its first pass", func() bool {
		gate.mu.Lock()
		defer gate.mu.Unlock()
		return gate.primed && gate.seen == gate.gen.Load()
	})
	// The relay is down: the hub stops between the release commit and the relay.
	f.hub.spawnNoticeRelayFilter = func(id string) bool { return id == eventID }
	recorder := releaseLadderHTTP(t, f.hub, stageID, t14OperatorToken, "", "relay down at release")
	if recorder.Code != http.StatusOK {
		t.Fatalf("release status=%d body=%s; want 200", recorder.Code, recorder.Body.String())
	}
	var owed int
	if err := f.hub.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_notice_outbox WHERE event_id=?`, eventID).Scan(&owed); err != nil || owed != 1 {
		t.Fatalf("outbox rows for the release notice=%d err=%v; want 1, committed with the release", owed, err)
	}
	var seen []string
	releasedNotices(f, &seen)
	if len(seen) != 0 {
		t.Fatalf("the notice reached the owner while the relay was down: %q", seen)
	}
	// The relay answers again: the outbox delivers the notice and clears its row.
	// No manual flush: the release's commit gives the spawn notice gate work. The
	// held row's retry (spawnNoticeRetry, 2s) and the coordinator's 2s tick put the
	// relay up to about 4s after the release; the 10s window is well under
	// readBackstop (1m), and without the wake the row waits for that backstop.
	f.hub.spawnNoticeRelayFilter = nil
	eventually(t, 10*time.Second, "the owner notice to be relayed once the relay answers", func() bool {
		releasedNotices(f, &seen)
		return len(seen) == 1
	})
	if !strings.Contains(seen[0], "Reason: relay down at release") {
		t.Fatalf("relayed notice %q does not carry the release's reason", seen[0])
	}
	// The owner's lane takes the notice inside the relay call, but the hub clears
	// the outbox row only after that call returns (routeLadderReleaseNotice). Wait
	// for the row to clear, the outcome asserted here.
	outboxRows := func() int {
		var n int
		if err := f.hub.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_notice_outbox WHERE event_id=?`, eventID).Scan(&n); err != nil {
			t.Errorf("counting outbox rows: %v", err)
		}
		return n
	}
	eventually(t, 10*time.Second, "the delivered notice's outbox row to clear", func() bool {
		return outboxRows() == 0
	})
	// A repeat answers the same release and, the notice being delivered, files
	// nothing more.
	again := releaseLadderHTTP(t, f.hub, stageID, t14OperatorToken, "", "a different reason")
	if again.Code != http.StatusOK {
		t.Fatalf("repeat status=%d body=%s; want 200", again.Code, again.Body.String())
	}
	f.hub.flushSpawnNotifications(t.Context())
	releasedNotices(f, &seen)
	if len(seen) != 1 {
		t.Fatalf("the release notice reached the owner %d times; want exactly 1", len(seen))
	}
}

// A holder the hub has already seen complete is not handed its role back, even
// while hk still says it is spawned. The release frees the role and answers 200;
// before the fix the decision was taken from hk alone and the release answered
// 409 holder_changed on every attempt.
func TestLadderReleaseFreesTheRoleOfAHolderTheHubSawComplete(t *testing.T) {
	f := newLadderCloseFixture(t, "pw-task-6137-completed", true)
	stageID := f.stageLiveSuccessorWithClose(t, &fleet.LadderCloseBinding{Protocol: fleet.LadderCloseProtocolMarker}, true)
	holder, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job)
	if err != nil {
		t.Fatal(err)
	}
	f.hub.mu.Lock()
	f.hub.jobs[f.job].Completed = true
	f.hub.mu.Unlock()
	recorder := releaseLadderHTTP(t, f.hub, stageID, t14OperatorToken, "", "holder completed in memory")
	if recorder.Code != http.StatusOK {
		t.Fatalf("release status=%d body=%s; want 200", recorder.Code, recorder.Body.String())
	}
	if rows, reservations := stagedRowCount(t, f.hub, stageID); rows != 0 || reservations != 0 {
		t.Fatalf("stage rows=%d reservations=%d; want both gone", rows, reservations)
	}
	if got := reservationTaskRole(t, f.hub, holder.Request.RequestID); got.Valid {
		t.Fatalf("a holder the hub saw complete got its role back: %+v", got)
	}
}

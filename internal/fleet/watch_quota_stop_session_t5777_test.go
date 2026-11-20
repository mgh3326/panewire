package fleet

import (
	"context"
	"testing"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// hk 5777: the hand-over's announcement starts the job's watcher in the pane's
// own herdr session. Built without it, the watcher read the default session and
// looked for a pane that lives in another one.
func TestAnnounceQuotaStopStartsTheWatcherInThePanesSession_t5777(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	var sessions []string
	old := announceStartWatch
	announceStartWatch = func(w *App, _ string) error { sessions = append(sessions, w.herdrSession()); return nil }
	t.Cleanup(func() { announceStartWatch = old })
	if e := AnnounceQuotaStop(context.Background(), a.Jobs, "j", "w:p1", "session-b", mustAttempt(t, a), quotastop.Stop{Pool: "claude"}); e != nil {
		t.Fatal(e)
	}
	if len(sessions) != 1 || sessions[0] != "session-b" {
		t.Fatalf("watcher sessions %v, want the pane's session-b", sessions)
	}
}

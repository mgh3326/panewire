package panewire

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// #572 r4576225996, r4576228677: a release whose end-lock wait expired
// answers journal_failed without a second, inline settlement under the
// acting lock; the daemon pass is left to settle.
func TestFailedReleaseSettlesNothingInline_t4632(t *testing.T) {
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, idlePaneHerdr())
	ledger := client.nodeHubClose.(*fakeHubClose)
	unlock, err := fleet.LockJobEnd(filepath.Join(inbox, "jobs", closeOpJob))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	var args nodeCloseOpRefArgs
	args.RequestID, args.Job = "op-9", closeOpJob
	if _, code := client.nodePaneCloseRelease(args); code != "journal_failed" {
		t.Fatalf("release under a held end lock: code=%q, want journal_failed", code)
	}
	if waited := time.Since(start); waited > hubCloseLockBudget+5*time.Second {
		t.Fatalf("release took %v", waited)
	}
	if len(ledger.settled) != 0 {
		t.Fatalf("settled inline after a failed release: %v", ledger.settled)
	}
	// A successful status still settles inline, once the end lock is free
	// (the step reconciles the fence under it, as the real settlement does).
	unlock()
	if _, code := client.nodePaneCloseStatus(args); code != "" {
		t.Fatalf("status code=%q", code)
	}
	if len(ledger.settled) != 1 {
		t.Fatalf("settled = %v, want one inline step after a status", ledger.settled)
	}
}

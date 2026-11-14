package fleet

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func seedCloseOp(t *testing.T, a *App, job string, op CloseOp) {
	t.Helper()
	root, err := os.OpenRoot(filepath.Join(a.Jobs, job))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := WriteCloseOp(root, op); err != nil {
		t.Fatal(err)
	}
}

// A pane the hub closed through a journalled close operation is the hub's to
// end: the watcher's missing-pane fallback must not write a plain lost record
// or release the lease while the operation is unreleased (only the ownership
// backstop lets it act), and acts at once on a released one (hk 4594).
func TestWatcherMissingPaneDefersToOpenCloseOp(t *testing.T) {
	previous := closeOpOwnershipTTL
	closeOpOwnershipTTL = 30 * time.Minute
	t.Cleanup(func() { closeOpOwnershipTTL = previous })
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "7200")
	for _, released := range []bool{false, true} {
		a := testApp(t)
		ledgerRunner(t, a)
		seedJob(t, a, "j", "implementer")
		seedLedger(t, a, "j")
		now := a.Now().UTC()
		op := CloseOp{RequestID: "hub-close-1", PaneID: "w:p1", State: CloseOpClosed, Created: now, Updated: now}
		if released {
			op.Released = &now
		}
		seedCloseOp(t, a, "j", op)
		started := a.Now()
		if e := a.watch(context.Background(), []string{"j"}); e != nil {
			t.Fatal(e)
		}
		if kinds := eventKindsOf(t, a, "j"); !slices.Contains(kinds, "job.lost") {
			t.Fatalf("released=%v: no loss recorded: %v", released, kinds)
		}
		waited := a.Now().Sub(started)
		if !released && waited < closeOpOwnershipTTL {
			t.Fatalf("watcher ended the job after %v under an open close operation", waited)
		}
		if released && waited >= closeOpOwnershipTTL {
			t.Fatalf("a released operation still held the watcher back for %v", waited)
		}
	}
}

// Which journal entries hold the watcher back: an unreleased close in any
// state but refused, within the ownership backstop; an unreadable journal
// holds it back too.
func TestCloseOpOwnsEnding(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	released := now
	cases := []struct {
		name string
		op   *CloseOp
		want bool
	}{
		{"no journal", nil, false},
		{"registered", &CloseOp{State: CloseOpRegistered, Updated: now}, true},
		{"closing", &CloseOp{State: CloseOpClosing, Updated: now}, true},
		{"closed", &CloseOp{State: CloseOpClosed, Updated: now}, true},
		{"gone", &CloseOp{State: CloseOpGone, Updated: now}, true},
		{"unknown", &CloseOp{State: CloseOpUnknown, Updated: now}, true},
		{"refused", &CloseOp{State: CloseOpRefused, Updated: now}, false},
		{"released", &CloseOp{State: CloseOpClosed, Updated: now, Released: &released}, false},
		{"past backstop", &CloseOp{State: CloseOpClosed, Updated: now.Add(-CloseOpOwnershipTTL)}, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			if test.op != nil {
				root, err := os.OpenRoot(dir)
				if err != nil {
					t.Fatal(err)
				}
				op := *test.op
				op.RequestID, op.PaneID, op.Created = "r1", "w:p1", now
				if err := WriteCloseOp(root, op); err != nil {
					t.Fatal(err)
				}
				root.Close()
				info, err := os.Stat(filepath.Join(dir, CloseOpDir, "r1.json"))
				if err != nil || info.Mode().Perm() != 0o600 {
					t.Fatalf("journal file mode: %v %v", info, err)
				}
			}
			if got := CloseOpOwnsEnding(dir, now); got != test.want {
				t.Fatalf("owns=%v want %v", got, test.want)
			}
		})
	}
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, CloseOpDir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, CloseOpDir, "r1.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !CloseOpOwnsEnding(dir, now) {
		t.Fatal("an unreadable journal must hold the watcher back")
	}
}

// An announced hand-over's grace fallback also waits for an open close
// operation: the hub writes the record with its successor pointer. Only the
// ownership backstop lets the fallback act without the hub's release.
func TestWatcherHandOffGraceDefersToOpenCloseOp(t *testing.T) {
	a, _, h, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	a.QuotaHandOffGrace = 2 * time.Minute
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "600")
	reset := a.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	if e := a.event(context.Background(), "j", "job.lost", map[string]any{"pane_id": "w:p1", "reason": "quota_stop", "pool": "claude", "reset_at": reset}); e != nil {
		t.Fatal(e)
	}
	previous := closeOpOwnershipTTL
	closeOpOwnershipTTL = 30 * time.Minute
	t.Cleanup(func() { closeOpOwnershipTTL = previous })
	now := a.Now().UTC()
	seedCloseOp(t, a, "j", CloseOp{RequestID: "hub-close-1", PaneID: "w:p1", State: CloseOpClosing, Created: now, Updated: now})
	started := a.Now()
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if waited := a.Now().Sub(started); waited < closeOpOwnershipTTL {
		t.Fatalf("grace fallback ended the job after %v under an open close operation", waited)
	}
	if rec := h.record(t, "j"); str(rec, "state") != "lost" {
		t.Fatalf("backstop did not end the job: %v", rec)
	}
}

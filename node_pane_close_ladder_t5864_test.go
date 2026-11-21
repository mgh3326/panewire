package panewire

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mgh3326/panewire/internal/jobowner"
)

// The ladder close's ownership fence and the fence op (hk 5864): the job's
// ownership lock is tried once and held through the close; one composite check
// (grant, revocation, an epoch-bound receipt) decides; a fence tombstones a K
// before any close runs.

func ladderJobs(root string) string { return filepath.Join(root, "jobs") }

// ladderReceiptAtEpoch rewrites the seeded spawn receipt so it names epoch.
func ladderReceiptAtEpoch(t *testing.T, root, epoch string) {
	t.Helper()
	body := `{"kind":"job.spawned","created_at":"2026-11-14T00:00:01Z","payload":{"pane_id":"w1:p1","label":"job-a","role":"reviewer","herdr_session":"fleet","epoch":"` + epoch + `"}}`
	path := filepath.Join(ladderJobs(root), closeOpJob, "events", closeOpAttempt)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func wantLadderRefused(t *testing.T, fake *nodeFakeHerdr, result map[string]any, code, reason string) {
	t.Helper()
	if code != "" || result["state"] != nodeLadderRefused || result["reason"] != reason {
		t.Fatalf("code=%q result=%v, want refused %q", code, result, reason)
	}
	if closes := fake.called("pane", "close"); closes != 0 {
		t.Fatalf("herdr closed the pane %d times", closes)
	}
}

// A held ownership lock is tried once: the result is unknown and nothing is
// closed. The record is final, so a repeat does not run the close either.
func TestNodeLadderCloseLockContentionIsUnknown(t *testing.T) {
	root := t.TempDir()
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	release, held, err := jobowner.TryLock(ladderJobs(root), closeOpJob)
	if err != nil || !held {
		t.Fatalf("held=%v err=%v", held, err)
	}
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	release()
	wantLadder(t, result, code, nodeLadderUnknown, false)
	if result["reason"] != ladderOwnerBusy || fake.called("pane", "close") != 0 {
		t.Fatalf("result=%v calls=%v", result, fake.calls)
	}
	result, code = nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderUnknown, true)
	if fake.called("pane", "close") != 0 {
		t.Fatalf("a repeat closed the pane: %v", fake.calls)
	}
}

// The lock is still held when herdr is asked to close the pane, and free after.
func TestNodeLadderCloseHoldsTheLockThroughTheClose(t *testing.T) {
	root := t.TempDir()
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	var contendedAtClose bool
	client.nodeHerdr = func(ctx context.Context, args ...string) ([]byte, error) {
		if len(args) > 1 && args[0] == "pane" && args[1] == "close" {
			release, free, err := jobowner.TryLock(ladderJobs(root), closeOpJob)
			contendedAtClose = err == nil && !free
			if free {
				release()
			}
		}
		return fake.run(ctx, args...)
	}
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderClosed, false)
	if !contendedAtClose {
		t.Fatal("the ownership lock was not held while herdr closed the pane")
	}
	release, free, err := jobowner.TryLock(ladderJobs(root), closeOpJob)
	if err != nil || !free {
		t.Fatalf("lock still held after the close: free=%v err=%v", free, err)
	}
	release()
}

// A hub revocation leaves the grant file as it was: the grant still equals the
// request's epoch, and the close is refused all the same.
func TestNodeLadderCloseRefusesARevokedGrantAtTheSameEpoch(t *testing.T) {
	root := t.TempDir()
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	if err := jobowner.Revoke(ladderJobs(root), closeOpJob, 2); err != nil {
		t.Fatal(err)
	}
	if epoch, err := jobowner.Epoch(ladderJobs(root), closeOpJob); err != nil || epoch != 1 {
		t.Fatalf("grant=%d err=%v, want the unchanged epoch 1", epoch, err)
	}
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadderRefused(t, fake, result, code, ladderRevoked)
}

// ABA: the job returns to this node at epoch 3 and the hub names epoch 3, but
// the receipt in the ledger was written at epoch 2. The grant matches; the
// receipt does not bind the epoch, so nothing is closed.
func TestNodeLadderCloseReceiptMustBindTheEpoch(t *testing.T) {
	for _, tc := range []struct {
		receipt, reason string
	}{{"2", ladderUnbound}, {"3", ""}} {
		root := t.TempDir()
		fake := idlePaneHerdr()
		client := ladderNode(t, fake, root)
		ladderReceiptAtEpoch(t, root, tc.receipt)
		if err := jobowner.Assign(ladderJobs(root), closeOpJob, 3); err != nil {
			t.Fatal(err)
		}
		args := ladderArgs(ladderRequest)
		args["epoch"] = 3
		result, code := nodeRun(t, client, nodeCommandLadderClose, args)
		if tc.reason == "" {
			wantLadder(t, result, code, nodeLadderClosed, false)
			continue
		}
		wantLadderRefused(t, fake, result, code, tc.reason)
	}
}

// A grant that is not the request's epoch is refused too, and a receipt for
// another pane is not bound.
func TestNodeLadderCloseRefusesAChangedGrantAndAForeignPane(t *testing.T) {
	root := t.TempDir()
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	if err := jobowner.Assign(ladderJobs(root), closeOpJob, 3); err != nil {
		t.Fatal(err)
	}
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadderRefused(t, fake, result, code, ladderGrantChanged)
	args := ladderArgs(ladderRequest2)
	args["epoch"], args["attempt"] = 3, "00009-job.spawned.json"
	result, code = nodeRun(t, client, nodeCommandLadderClose, args)
	wantLadderRefused(t, fake, result, code, ladderUnbound)
}

// A fence before the close writes the tombstone; the close that arrives later
// finds fenced and never runs, whichever request id it comes under.
func TestNodeLadderFenceBeforeTheCloseStopsIt(t *testing.T) {
	root := t.TempDir()
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	fence := map[string]any{"request_id": ladderRequest, "job": closeOpJob, "node": "machine-a", "epoch": 1, "attempt": closeOpAttempt, "pane_id": "w1:p1"}
	result, code := nodeRun(t, client, nodeCommandLadderFence, fence)
	wantLadder(t, result, code, nodeLadderFenced, false)
	result, code = nodeRun(t, client, nodeCommandLadderFence, fence)
	wantLadder(t, result, code, nodeLadderFenced, true)
	for _, id := range []string{ladderRequest, ladderRequest2} {
		result, code = nodeRun(t, client, nodeCommandLadderClose, ladderArgs(id))
		wantLadder(t, result, code, nodeLadderFenced, true)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("herdr was called: %v", fake.calls)
	}
}

// A fence after a final result returns that result, found by request id alone;
// a fence for a request id the node never saw has no K to fence.
func TestNodeLadderFenceAfterAResultReturnsIt(t *testing.T) {
	root := t.TempDir()
	fake := idlePaneHerdr()
	client := ladderNode(t, fake, root)
	result, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest))
	wantLadder(t, result, code, nodeLadderClosed, false)
	result, code = nodeRun(t, client, nodeCommandLadderFence, map[string]any{"request_id": ladderRequest})
	wantLadder(t, result, code, nodeLadderClosed, true)
	if _, code = nodeRun(t, client, nodeCommandLadderFence, map[string]any{"request_id": ladderRequest2}); code != "invalid_args" {
		t.Fatalf("code=%q", code)
	}
	if closes := fake.called("pane", "close"); closes != 1 {
		t.Fatalf("closes=%d", closes)
	}
}

// One request id names one K: reusing it for another K is refused, and the
// fence never overrides the K it was first used for.
func TestNodeLadderRequestIDNamesOneK(t *testing.T) {
	root := t.TempDir()
	client := ladderNode(t, idlePaneHerdr(), root)
	if _, code := nodeRun(t, client, nodeCommandLadderClose, ladderArgs(ladderRequest)); code != "" {
		t.Fatal(code)
	}
	args := ladderArgs(ladderRequest)
	args["epoch"] = 2
	if _, code := nodeRun(t, client, nodeCommandLadderClose, args); code != "invalid_args" {
		t.Fatalf("close code=%q", code)
	}
	if _, code := nodeRun(t, client, nodeCommandLadderFence, map[string]any{"request_id": ladderRequest, "job": closeOpJob, "node": "machine-a", "epoch": 2, "attempt": closeOpAttempt, "pane_id": "w1:p1"}); code != "invalid_args" {
		t.Fatalf("fence code=%q", code)
	}
}

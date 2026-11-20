package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// hk 5223, follow-up of hk 5211 (panewire#692). A sparse spawn left executing
// across a node restart is settled by the replay that reaches the settle path;
// a later list-less replay is answered by lookup. Both must give the same
// checkout_mode, and that mode must not be sparse: the hub records a missing
// mode as "full" (hub_spawn.go resolveSpawn).

// settledSparseRestart leaves a sparse spawn executing across a node restart:
// its lists are journaled, its job.spawned record says sparse, and the process
// that claimed it is gone. It returns the fixture and the replay forms of the
// request: withLists carries the sparse lists, stripped does not.
func settledSparseRestart(t *testing.T) (*settleFixture, hubOutboundMessage, hubOutboundMessage) {
	t.Helper()
	herdr := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "label-a", "working", "/work/a"))}, panes: settlePanes("w1:p1", "label-a")}
	f := newSettleFixture(t, herdr.run)
	if _, err := f.journal.db.ExecContext(t.Context(), `DELETE FROM spawn_inbox`); err != nil {
		t.Fatal(err)
	}
	withLists := f.orphan
	withLists.Checkout = &hubSpawnCheckout{Origin: "https://github.com/example/project.git", Mode: "detached", Sha: strings.Repeat("a", 40), Fetch: "refs/heads/main", Dir: "project.job-a", Exclude: []string{"Assets"}}
	stripped := withLists
	strippedCheckout := *withLists.Checkout
	strippedCheckout.Exclude, strippedCheckout.Include = nil, nil
	stripped.Checkout = &strippedCheckout
	if claimed, _, err := f.journal.claim(t.Context(), withLists); err != nil || !claimed {
		t.Fatalf("claim %v %v", claimed, err)
	}
	dir := filepath.Join(f.jobs, "jobs", "job-a", "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	event, _ := json.Marshal(map[string]any{"kind": "job.spawned", "payload": map[string]any{"pane_id": "w1:p1", "herdr_session": "worker", "host": "machine-a", "profile": "codex-terra", "model_id": "m", "role": "implementer", "cwd": "/work/a", "pool": "codex", "account_key": "codex:0123456789abcdef", "checkout_mode": nodeCheckoutSparse}})
	if err := os.WriteFile(filepath.Join(dir, "01.json"), event, 0600); err != nil {
		t.Fatal(err)
	}
	// The restart: the journal is closed and reopened, and nothing in this
	// process owns the executing row.
	f.client.releaseSpawnJournal()
	journal, err := f.client.acquireSpawnJournal()
	if err != nil {
		t.Fatal(err)
	}
	f.journal = journal
	return f, withLists, stripped
}

// hk5223Replay sends one replay of msg to peer and returns the answer.
func hk5223Replay(t *testing.T, f *settleFixture, peer *hubClientConnection, next func() hubSpawnResult, msg hubOutboundMessage) hubSpawnResult {
	t.Helper()
	f.client.handleHubSpawn(t.Context(), peer, msg)
	return next()
}

// The stripped replay that settles the row and the stripped replay that a
// lookup answers must agree, and neither reports sparse.
func TestHubSpawnSettleStrippedReplayMatchesLookup(t *testing.T) {
	f, _, stripped := settledSparseRestart(t)
	peer, next := settlePeer(t)
	peer.acceptsCheckoutSparse.Store(true)

	settled := hk5223Replay(t, f, peer, next, stripped)
	looked := hk5223Replay(t, f, peer, next, stripped)
	if settled.RC != 0 || looked.RC != 0 {
		t.Fatalf("stripped replays must land: settle rc=%d lookup rc=%d", settled.RC, looked.RC)
	}
	if settled.CheckoutMode != looked.CheckoutMode {
		t.Fatalf("settle path answered checkout_mode %q, lookup path %q", settled.CheckoutMode, looked.CheckoutMode)
	}
	if settled.CheckoutMode == nodeCheckoutSparse {
		t.Fatalf("a stripped replay must not report sparse: checkout_mode=%q", settled.CheckoutMode)
	}
	// The journal keeps the mode the row executed with; only the answer to a
	// list-less replay is filtered.
	if stored := f.stored(t); stored == nil || stored.CheckoutMode != nodeCheckoutSparse {
		t.Fatalf("journaled result must keep the executed mode %q, got %+v", nodeCheckoutSparse, stored)
	}
}

// Lists settle the row, then a list-less replay is answered by lookup: the
// list-less answer is the same as in TestHubSpawnSettleStrippedReplayMatchesLookup.
func TestHubSpawnSettleListsThenStrippedReplay(t *testing.T) {
	f, withLists, stripped := settledSparseRestart(t)
	peer, next := settlePeer(t)
	peer.acceptsCheckoutSparse.Store(true)

	listed := hk5223Replay(t, f, peer, next, withLists)
	relisted := hk5223Replay(t, f, peer, next, withLists)
	dropped := hk5223Replay(t, f, peer, next, stripped)
	if listed.CheckoutMode != nodeCheckoutSparse || relisted.CheckoutMode != nodeCheckoutSparse {
		t.Fatalf("replays with lists must report the executed mode %q: settle=%q lookup=%q", nodeCheckoutSparse, listed.CheckoutMode, relisted.CheckoutMode)
	}
	if dropped.CheckoutMode != "" {
		t.Fatalf("a list-less replay after a listed one must not report sparse: checkout_mode=%q", dropped.CheckoutMode)
	}
}

// A hub that does not list checkout_sparse_v1 gets no checkout_mode for either
// form of the replay, and both are still answered.
func TestHubSpawnSettleOldHubGetsNoCheckoutMode(t *testing.T) {
	f, withLists, stripped := settledSparseRestart(t)
	peer, next := settlePeer(t)

	for _, msg := range []hubOutboundMessage{stripped, withLists} {
		got := hk5223Replay(t, f, peer, next, msg)
		if got.RC != 0 || got.Pane != "w1:p1" || got.CheckoutMode != "" {
			t.Fatalf("old hub replay answered %+v, want rc 0 on w1:p1 with no checkout_mode", got)
		}
	}
}

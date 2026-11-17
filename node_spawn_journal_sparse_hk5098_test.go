package panewire

import (
	"strings"
	"testing"
)

// hk 5098: the sparse lists are not part of a spawn's replay identity. The hub
// adds or drops them with the target node's checkout_sparse_v1 capability, so a
// replay across that node's upgrade or rollback must still match its journaled
// request (hk 5098 review).
func TestSpawnFingerprintIgnoresTheSparseLists(t *testing.T) {
	checkout := &hubSpawnCheckout{Origin: "https://github.com/example/project.git", Mode: "detached", Sha: strings.Repeat("a", 40), Fetch: "refs/heads/main", Dir: "project.test-job"}
	withLists := *checkout
	withLists.Exclude, withLists.Include = []string{"Assets"}, []string{"Assets/Changed.prefab"}
	sparse := hubOutboundMessage{RequestID: "00000000-0000-4000-8000-000000000001", CWDKey: "project", BriefDoc: "briefs/job.md", Checkout: &withLists, WaitSeconds: 60}
	plain := sparse
	plain.Checkout = checkout
	if spawnFingerprint(sparse) != spawnFingerprint(plain) {
		t.Fatal("the sparse lists changed the replay identity")
	}
	// The fields that pick the worktree still do.
	moved := sparse
	movedCheckout := withLists
	movedCheckout.Sha = strings.Repeat("b", 40)
	moved.Checkout = &movedCheckout
	if spawnFingerprint(moved) == spawnFingerprint(sparse) {
		t.Fatal("a different commit did not change the replay identity")
	}
}

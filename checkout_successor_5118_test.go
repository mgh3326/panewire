package panewire

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A successor spec checkout (hk 5118) prepares a local branch at the pinned
// commit instead of a detached HEAD: the branch tracks origin/<branch>, so the
// successor continues it with a plain `git push`.
func TestNodePrepareSuccessorTracksOriginBranch5118(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	dir := filepath.Join(fixture.workRoot, "repo.job")
	args := fixture.prepareArgs(fleet.HubCheckoutSuccessor, "feat", fixture.feat, "", "repo.job")

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodePreparePrepared {
		t.Fatalf("prepare outcome=%v code=%q", result["outcome"], code)
	}
	if result["checkout_mode"] != nodeCheckoutBranch {
		t.Fatalf("checkout_mode=%v, want %q", result["checkout_mode"], nodeCheckoutBranch)
	}
	worktree, _ := result["worktree"].(map[string]any)
	if worktree["path"] != dir || worktree["branch"] != "feat" || worktree["head"] != fixture.feat {
		t.Fatalf("worktree=%v, want feat at %s in %s", worktree, fixture.feat, dir)
	}
	if got := nodeGit(t, dir, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"); got != "origin/feat" {
		t.Fatalf("upstream=%q, want origin/feat", got)
	}

	// A replay of the same request reuses the same branch checkout.
	result, code = nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodePrepareReused || result["checkout_mode"] != nodeCheckoutBranch {
		t.Fatalf("replay outcome=%v checkout_mode=%v code=%q", result["outcome"], result["checkout_mode"], code)
	}

	// The successor continues the branch: a plain push carries its commit.
	nodeGit(t, dir, "commit", "-q", "--allow-empty", "-m", "successor")
	pushed := nodeGit(t, dir, "rev-parse", "HEAD")
	nodeGit(t, dir, "push", "-q")
	if got := nodeGit(t, fixture.origin, "rev-parse", "feat"); got != pushed {
		t.Fatalf("origin/feat=%s, want the successor's %s", got, pushed)
	}
}

// Someone pushed after the brief: the commit the successor names is no longer
// origin's tip, so the node refuses before anything is prepared and leaves no
// worktree behind.
func TestNodePrepareSuccessorStaleRefuses5118(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	fixture.pushFixtureBranch(t, "feat", fixture.feat)
	dir := filepath.Join(fixture.workRoot, "repo.job")
	args := fixture.prepareArgs(fleet.HubCheckoutSuccessor, "feat", fixture.feat, "", "repo.job")

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodePrepareSuccessorStale {
		t.Fatalf("stale prepare outcome=%v code=%q, want %q", result["outcome"], code, nodePrepareSuccessorStale)
	}
	reason, _ := result["reason"].(string)
	if !strings.Contains(reason, "want="+fixture.feat) || !strings.Contains(reason, "origin=") {
		t.Fatalf("reason=%q, want the wanted and the origin commit", reason)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("a stale prepare left %s behind (err=%v)", dir, err)
	}
	if out := nodeGit(t, fixture.clone, "worktree", "list", "--porcelain"); strings.Contains(out, dir) {
		t.Fatalf("a stale prepare registered %s: %s", dir, out)
	}
}

// The branch another worktree of the clone holds is never forced or stolen.
func TestNodePrepareSuccessorBranchInUse5118(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	fixture.cloneRepo(t)
	held := filepath.Join(fixture.workRoot, "repo.parent")
	nodeGit(t, fixture.clone, "worktree", "add", "-q", held, "feat")
	dir := filepath.Join(fixture.workRoot, "repo.job")
	args := fixture.prepareArgs(fleet.HubCheckoutSuccessor, "feat", fixture.feat, "", "repo.job")

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodePrepareBranchInUse {
		t.Fatalf("in-use prepare outcome=%v code=%q, want %q", result["outcome"], code, nodePrepareBranchInUse)
	}
	if reason, _ := result["reason"].(string); reason != "worktree="+held {
		t.Fatalf("reason=%q, want worktree=%s", reason, held)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("an in-use refusal created %s (err=%v)", dir, err)
	}
	if got := nodeGit(t, held, "rev-parse", "--abbrev-ref", "HEAD"); got != "feat" {
		t.Fatalf("the held worktree moved to %q", got)
	}
}

// The refusal reaches the submitter with the reason the node gave: the hub
// answers checkout_refused:<outcome>: <reason>.
func TestSuccessorRefusalCarriesTheNodeReason5118(t *testing.T) {
	config := t3482SpawnConfig(t, filepath.Join(t.TempDir(), "work"))
	want := "want=" + strings.Repeat("a", 40) + " origin=" + strings.Repeat("b", 40)
	client := &HubClient{spawnConfigPath: config, spawnPrepare: func(context.Context, nodeWorktreePrepareArgs) (any, string) {
		return nodeWorktreePrepareResult{Outcome: nodePrepareSuccessorStale, Reason: want}, ""
	}}
	result := client.runHubSpawn(t.Context(), hubOutboundMessage{RequestID: t14RequestID(118), CWDKey: "repo-a", BriefInline: "brief", Checkout: t3482Checkout(), WaitSeconds: 1})
	if result.RC != 81 || result.Error != "checkout_refused:"+nodePrepareSuccessorStale+": "+want {
		t.Fatalf("rc=%d error=%q, want the stale refusal with its reason", result.RC, result.Error)
	}
}

// The hub accepts the successor checkout only in its one shape, and reads the
// node's answer for it.
func TestHubAcceptsTheSuccessorCheckout5118(t *testing.T) {
	ok := hubSpawnCheckout{Origin: "https://git.invalid/org/repo-a.git", Mode: fleet.HubCheckoutSuccessor, Branch: "wk/x", Sha: strings.Repeat("a", 40), Dir: "job-a-checkout"}
	if !validHubSpawnCheckout(ok) {
		t.Fatal("a well-formed successor checkout was refused")
	}
	for _, test := range []struct {
		name   string
		mutate func(*hubSpawnCheckout)
	}{
		{"no commit", func(c *hubSpawnCheckout) { c.Sha = "" }},
		{"no branch", func(c *hubSpawnCheckout) { c.Branch = "" }},
		{"a fetch ref", func(c *hubSpawnCheckout) { c.Fetch = "refs/heads/wk/x" }},
		{"a sparse list", func(c *hubSpawnCheckout) { c.Exclude = []string{"Assets"} }},
		{"verify_reuse", func(c *hubSpawnCheckout) { c.VerifyReuse = true }},
	} {
		bad := ok
		test.mutate(&bad)
		if validHubSpawnCheckout(bad) {
			t.Errorf("%s: accepted %+v", test.name, bad)
		}
	}
	if !validSpawnCheckoutMode(nodeCheckoutBranch) {
		t.Fatal("the hub must read the node's branch checkout_mode")
	}
}

// checkout_mode "branch" travels only to a hub that listed
// checkout_branch_v1, so an older hub never drops the whole answer.
func TestSpawnResultBranchModeNeedsItsHubFeature5118(t *testing.T) {
	result := hubSpawnResult{RequestID: t14RequestID(119), CheckoutMode: nodeCheckoutBranch}
	if got := spawnResultForHub(result, true, true, false); got.CheckoutMode != "" {
		t.Fatalf("old hub result = %+v, want no checkout_mode", got)
	}
	if got := spawnResultForHub(result, true, false, true); got.CheckoutMode != nodeCheckoutBranch {
		t.Fatalf("capable hub result = %+v, want checkout_mode branch", got)
	}
	// The sparse gate does not admit the branch value, and vice versa.
	sparse := hubSpawnResult{RequestID: t14RequestID(120), CheckoutMode: nodeCheckoutSparse}
	if got := spawnResultForHub(sparse, true, false, true); got.CheckoutMode != "" {
		t.Fatalf("sparse result without its feature = %+v", got)
	}
}

// A successor's refusals classify by what they say: a branch this host's clone
// holds moves an auto placement on, and a stale commit is the request's own
// staleness.
func TestSuccessorRefusalClassification5118(t *testing.T) {
	inUse := "checkout_refused:" + nodePrepareBranchInUse + ": worktree=/work/repo.parent"
	if !spawnRefusalTriesNextHost(inUse) {
		t.Fatal("branch_in_use must let auto placement try the next host")
	}
	stale := "checkout_refused:" + nodePrepareSuccessorStale + ": want=" + strings.Repeat("a", 40) + " origin=" + strings.Repeat("b", 40)
	if spawnRefusalTriesNextHost(stale) {
		t.Fatal("successor_stale is the same on every host")
	}
	if spawnRefusalReplayable(stale) {
		t.Fatal("replaying a stale successor meets the same stale commit")
	}
	if !spawnRefusalReplayable(inUse) {
		t.Fatal("branch_in_use clears when the holding worktree ends")
	}
	for _, code := range []string{nodePrepareSuccessorStale, nodePrepareBranchInUse} {
		if fix := nodeSpawnRefusalFix("checkout_refused:"+code+": detail=x", ""); fix == "" || strings.Contains(fix, "preflight --json") {
			t.Errorf("%s: fix=%q, want its own sentence", code, fix)
		}
	}
}

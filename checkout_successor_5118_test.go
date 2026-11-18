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

// The refusal reaches the submitter with the reason the node gave, as
// checkout_refused:<outcome>: <reason>, but only to a hub that listed
// checkout_branch_v1: an older hub's strict parser drops the whole answer for
// a value it does not know (hk 5118).
func TestSuccessorRefusalCarriesTheNodeReason5118(t *testing.T) {
	config := t3482SpawnConfig(t, filepath.Join(t.TempDir(), "work"))
	stale := "want=" + strings.Repeat("a", 40) + " origin=" + strings.Repeat("b", 40)
	for _, test := range []struct {
		name, outcome, reason string
		detail                bool
		want                  string
	}{
		{"a branch-capable hub reads the stale detail", nodePrepareSuccessorStale, stale, true, "checkout_refused:" + nodePrepareSuccessorStale + ": " + stale},
		{"a branch-capable hub reads the worktree that holds the branch", nodePrepareBranchInUse, "worktree=/work/repo.parent", true, "checkout_refused:" + nodePrepareBranchInUse + ": worktree=/work/repo.parent"},
		{"an older hub gets the outcome alone", nodePrepareSuccessorStale, stale, false, "checkout_refused:" + nodePrepareSuccessorStale},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &HubClient{spawnConfigPath: config, spawnPrepare: func(context.Context, nodeWorktreePrepareArgs) (any, string) {
				return nodeWorktreePrepareResult{Outcome: test.outcome, Reason: test.reason}, ""
			}}
			result := client.runHubSpawn(t.Context(), hubOutboundMessage{RequestID: t14RequestID(118), CWDKey: "repo-a", BriefInline: "brief", Checkout: t3482Checkout(), WaitSeconds: 1, checkoutBranchReply: test.detail})
			if result.RC != 81 || result.Error != test.want {
				t.Fatalf("rc=%d error=%q, want %q", result.RC, result.Error, test.want)
			}
		})
	}
}

// A detail the hub's bounded shape cannot carry is left out: the answer is the
// outcome alone, never a frame the hub would drop.
func TestSuccessorRefusalDropsADetailItCannotCarry5118(t *testing.T) {
	config := t3482SpawnConfig(t, filepath.Join(t.TempDir(), "work"))
	long := "worktree=/work/" + strings.Repeat("a", 512)
	for _, test := range []struct{ name, reason, want string }{
		{"a long worktree path", long, "checkout_refused:" + nodePrepareBranchInUse},
		{"a control byte", "worktree=/work/a\x00b", "checkout_refused:" + nodePrepareBranchInUse},
		{"a shape the hub does not know", "local=x expected=y relation=ahead", "checkout_refused:" + nodePrepareBranchInUse},
		{"a detail that fits", "worktree=/work/repo.parent", "checkout_refused:" + nodePrepareBranchInUse + ": worktree=/work/repo.parent"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &HubClient{spawnConfigPath: config, spawnPrepare: func(context.Context, nodeWorktreePrepareArgs) (any, string) {
				return nodeWorktreePrepareResult{Outcome: nodePrepareBranchInUse, Reason: test.reason}, ""
			}}
			result := client.runHubSpawn(t.Context(), hubOutboundMessage{RequestID: t14RequestID(122), CWDKey: "repo-a", BriefInline: "brief", Checkout: t3482Checkout(), WaitSeconds: 1, checkoutBranchReply: true})
			if result.RC != 81 || result.Error != test.want {
				t.Fatalf("rc=%d error=%q, want %q", result.RC, result.Error, test.want)
			}
			// Whatever the node built, the hub must still read the answer.
			if _, ok := parseHubSpawnResult(hubSpawnResultWire(t, result)); !ok {
				t.Fatalf("the hub dropped the answer: %q", result.Error)
			}
		})
	}
}

// A journaled result is replayed unchanged, so the detail is stripped at send
// time for a hub that did not list checkout_branch_v1.
func TestSuccessorRefusalDetailDoesNotReachAnOlderHub5118(t *testing.T) {
	stale := hubSpawnResult{RequestID: t14RequestID(123), RC: 81, Error: "checkout_refused:" + nodePrepareSuccessorStale + ": want=" + strings.Repeat("a", 40) + " origin=" + strings.Repeat("b", 40)}
	if got := spawnResultForHub(stale, true, true, false); got.Error != "checkout_refused:"+nodePrepareSuccessorStale {
		t.Fatalf("older hub result=%q, want the outcome alone", got.Error)
	}
	if got := spawnResultForHub(stale, true, true, true); got.Error != stale.Error {
		t.Fatalf("capable hub result=%q, want the detail", got.Error)
	}
	if got := spawnResultForHub(hubSpawnResult{Error: "spawn_failed: hooks_review"}, true, true, false); got.Error != "spawn_failed: hooks_review" {
		t.Fatalf("an unrelated error was rewritten to %q", got.Error)
	}
}

// The hub's result parser reads the detailed refusal from a branch-capable
// node and still refuses anything outside the bounded shape, so a frame it
// cannot trust is dropped whole.
func TestHubParsesTheSuccessorRefusalDetail5118(t *testing.T) {
	for _, test := range []struct {
		name, err string
		accepted  bool
	}{
		{"plain outcome", "checkout_refused:successor_stale", true},
		{"plain outcome with a dash", "checkout_refused:branch-elsewhere", true},
		{"stale detail", "checkout_refused:successor_stale: want=" + strings.Repeat("a", 40) + " origin=" + strings.Repeat("b", 40), true},
		{"stale with no origin branch", "checkout_refused:successor_stale: want=" + strings.Repeat("a", 40) + " origin=none", true},
		{"in-use detail", "checkout_refused:branch_in_use: worktree=/work/repo.parent", true},
		{"a long worktree path", "checkout_refused:branch_in_use: worktree=/work/" + strings.Repeat("a", 480), true},
		{"a short commit", "checkout_refused:successor_stale: want=abc origin=" + strings.Repeat("b", 40), false},
		{"an unknown detail", "checkout_refused:successor_stale: local=x expected=y relation=ahead", false},
		{"a control byte", "checkout_refused:branch_in_use: worktree=/work/a\x00b", false},
		{"a text refusal", "checkout refused: successor_stale", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := hubSpawnResult{RequestID: t14RequestID(121), RC: 81, Error: test.err}
			wire := hubSpawnResultWire(t, result)
			parsed, ok := parseHubSpawnResult(wire)
			if ok != test.accepted || (ok && parsed.Error != test.err) {
				t.Fatalf("accepted=%v parsed=%+v wire=%s", ok, parsed, wire)
			}
		})
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

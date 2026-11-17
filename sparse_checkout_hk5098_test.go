package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 5098: a spec spawn may ask for a sparse checkout. The node builds the
// gitignore pattern list itself and runs the three git commands against a real
// git in a temp dir; these tests drive that path through worktree.prepare, so
// what they assert is what lands on disk.

// seedSparseTree adds one commit with the trees the sparse tests cut and
// pushes it as pull/2/head. It returns the commit id.
func seedSparseTree(t *testing.T, fixture *nodeWorktreeFixture) string {
	t.Helper()
	seed := filepath.Join(fixture.root, "seed")
	nodeGit(t, seed, "checkout", "-q", "-b", "sparse", "main")
	for name, body := range map[string]string{
		"README.md":                 "# project\n",
		"src/app.go":                "package main\n",
		"Assets/Logo.bin":           "binary logo\n",
		"Assets/Changed.prefab":     "changed prefab\n",
		"Assets/a[1].txt":           "literal bracket file\n",
		"Assets/a1.txt":             "sibling without brackets\n",
		"Assets/b*.txt":             "literal star file\n",
		"Assets/b1.txt":             "sibling without a star\n",
		"docs/generated/big.md":     "generated\n",
		"docs/generated/deep/x.txt": "nested inside the excluded tree\n",
	} {
		path := filepath.Join(seed, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	nodeGit(t, seed, "add", "-A")
	nodeGit(t, seed, "commit", "-q", "-m", "sparse tree")
	nodeGit(t, seed, "push", "-q", "origin", "sparse", "sparse:refs/pull/2/head")
	return nodeGit(t, seed, "rev-parse", "sparse")
}

func sparsePrepareArgs(fixture *nodeWorktreeFixture, sha string, exclude, include []string) map[string]any {
	args := fixture.prepareArgs("detached", "", sha, "pull/2/head", "repo.job")
	if len(exclude) != 0 {
		args["exclude"] = exclude
	}
	if len(include) != 0 {
		args["include"] = include
	}
	return args
}

func TestSparseCheckoutLeavesExcludedTreesOffDisk(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	sha := seedSparseTree(t, fixture)
	client := fixture.client()
	args := sparsePrepareArgs(fixture, sha,
		[]string{"Assets", "docs/generated"},
		[]string{"Assets/Changed.prefab"})

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodePreparePrepared {
		t.Fatalf("prepare outcome=%v code=%q", result["outcome"], code)
	}
	if result["checkout_mode"] != nodeCheckoutSparse {
		t.Fatalf("checkout_mode=%v, want sparse", result["checkout_mode"])
	}
	dir := filepath.Join(fixture.workRoot, "repo.job")
	for _, absent := range []string{"Assets/Logo.bin", "Assets/a1.txt", "Assets/b1.txt", "docs/generated/big.md", "docs/generated/deep/x.txt"} {
		if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(absent))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s is on disk in a sparse checkout (err=%v)", absent, err)
		}
	}
	for _, present := range []string{"README.md", "src/app.go", "Assets/Changed.prefab"} {
		if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(present))); err != nil {
			t.Fatalf("%s is missing: %v", present, err)
		}
	}
	// Everything stays readable from objects.
	if body := nodeGit(t, dir, "show", "HEAD:Assets/Logo.bin"); body != "binary logo" {
		t.Fatalf("git show HEAD:Assets/Logo.bin = %q", body)
	}
	if head := nodeGit(t, dir, "rev-parse", "HEAD"); head != sha {
		t.Fatalf("HEAD=%s want %s", head, sha)
	}
	// A second prepare of the same spec is a reuse that still says sparse.
	if result, code = nodeRun(t, client, nodeCommandWorktreePrepare, args); code != "" || result["outcome"] != nodePrepareReused || result["checkout_mode"] != nodeCheckoutSparse {
		t.Fatalf("second prepare outcome=%v mode=%v code=%q", result["outcome"], result["checkout_mode"], code)
	}
}

func TestSparseCheckoutIncludesGlobCharactersLiterally(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	sha := seedSparseTree(t, fixture)
	args := sparsePrepareArgs(fixture, sha, []string{"Assets"}, []string{"Assets/a[1].txt", "Assets/b*.txt"})

	result, code := nodeRun(t, fixture.client(), nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodePreparePrepared || result["checkout_mode"] != nodeCheckoutSparse {
		t.Fatalf("prepare outcome=%v mode=%v code=%q", result["outcome"], result["checkout_mode"], code)
	}
	dir := filepath.Join(fixture.workRoot, "repo.job")
	for _, present := range []string{"Assets/a[1].txt", "Assets/b*.txt"} {
		if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(present))); err != nil {
			t.Fatalf("%s is missing: %v", present, err)
		}
	}
	// The include is literal: a sibling the glob would also match stays out.
	for _, absent := range []string{"Assets/a1.txt", "Assets/b1.txt"} {
		if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(absent))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s was checked out by a glob include (err=%v)", absent, err)
		}
	}
}

func TestNodeRefusesABadSparsePattern(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	sha := seedSparseTree(t, fixture)
	for _, tc := range []struct {
		name    string
		exclude []string
		include []string
	}{
		{"exclude starts with !", []string{"Assets", "!docs"}, nil},
		{"exclude starts with /", []string{"/Assets"}, nil},
		{"include with a parent segment", nil, []string{"Assets/../etc/passwd"}},
		{"include with a newline", nil, []string{"ok", "bad\npath"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := sparsePrepareArgs(fixture, sha, tc.exclude, tc.include)
			if _, code := nodeRun(t, fixture.client(), nodeCommandWorktreePrepare, args); code != "invalid_args" {
				t.Fatalf("code=%q, want invalid_args", code)
			}
			if entries, _ := filepath.Glob(filepath.Join(fixture.workRoot, "repo.job")); len(entries) != 0 {
				t.Fatalf("a refused sparse spec left a worktree behind: %v", entries)
			}
		})
	}
}

func TestNodeRefusesAnOverBoundSparseList(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	sha := seedSparseTree(t, fixture)
	exclude := make([]string, 65)
	for i := range exclude {
		exclude[i] = "dir" + strings.Repeat("x", i%5)
	}
	args := sparsePrepareArgs(fixture, sha, exclude, nil)
	if _, code := nodeRun(t, fixture.client(), nodeCommandWorktreePrepare, args); code != "invalid_args" {
		t.Fatalf("65 excludes: code=%q, want invalid_args", code)
	}
	include := make([]string, 501)
	for i := range include {
		include[i] = "file" + strings.Repeat("y", i%5)
	}
	args = sparsePrepareArgs(fixture, sha, nil, include)
	if _, code := nodeRun(t, fixture.client(), nodeCommandWorktreePrepare, args); code != "invalid_args" {
		t.Fatalf("501 includes: code=%q, want invalid_args", code)
	}
}

func TestSparseRequestOnAnOldGitPreparesTheFullCheckout(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	sha := seedSparseTree(t, fixture)
	client := fixture.client()
	base := defaultNodeCommandRunner()
	client.nodeGit = func(ctx context.Context, args ...string) ([]byte, error) {
		if len(args) == 1 && args[0] == "--version" {
			return []byte("git version 2.36.1\n"), nil
		}
		return base(ctx, args...)
	}
	args := sparsePrepareArgs(fixture, sha, []string{"Assets"}, []string{"Assets/Changed.prefab"})

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodePreparePrepared {
		t.Fatalf("prepare outcome=%v code=%q", result["outcome"], code)
	}
	if result["checkout_mode"] != nodeCheckoutFull {
		t.Fatalf("checkout_mode=%v, want full on a git without sparse-checkout", result["checkout_mode"])
	}
	dir := filepath.Join(fixture.workRoot, "repo.job")
	if _, err := os.Stat(filepath.Join(dir, "Assets", "Logo.bin")); err != nil {
		t.Fatalf("a node without the capability must get the whole tree: %v", err)
	}
}

func TestSparseFailureAfterAddLeavesNoWorktree(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	sha := seedSparseTree(t, fixture)
	client := fixture.client()
	client.nodeGitStdin = func(context.Context, string, ...string) ([]byte, error) {
		return nil, errors.New("sparse-checkout set failed")
	}
	args := sparsePrepareArgs(fixture, sha, []string{"Assets"}, nil)

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodeOutcomeGitFailed {
		t.Fatalf("prepare outcome=%v code=%q, want a git failure", result["outcome"], code)
	}
	if _, err := os.Stat(filepath.Join(fixture.workRoot, "repo.job")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a half-made sparse worktree was left behind: %v", err)
	}
	if listed := nodeGit(t, filepath.Join(fixture.workRoot, "repo"), "worktree", "list", "--porcelain"); strings.Contains(listed, "repo.job") {
		t.Fatalf("the failed worktree is still registered:\n%s", listed)
	}
}

func TestNodeHelloAdvertisesSparseOnlyOnNewGit(t *testing.T) {
	for _, tc := range []struct {
		version string
		want    bool
	}{
		{"git version 2.37.0\n", true},
		{"git version 2.39.5\n", true},
		{"git version 2.36.9\n", false},
		{"git version 2.31.1\n", false},
	} {
		t.Run(strings.TrimSpace(tc.version), func(t *testing.T) {
			base := defaultNodeCommandRunner()
			client := &HubClient{nodeGit: func(ctx context.Context, args ...string) ([]byte, error) {
				if len(args) == 1 && args[0] == "--version" {
					return []byte(tc.version), nil
				}
				return base(ctx, args...)
			}}
			capabilities := client.spawnHelloCapabilities(t.Context())
			got := false
			for _, capability := range capabilities {
				if capability == hubCapabilityCheckoutSparse {
					got = true
				}
			}
			if got != tc.want {
				t.Fatalf("capabilities=%v, want checkout_sparse_v1 present=%v", capabilities, tc.want)
			}
		})
	}
}

// A hub that does not list checkout_sparse_v1 would drop a sparse request's
// lists and run a full checkout without saying so, so the submitter refuses to
// send one there (hk 5098).
func TestSubmissionRequiresTheSparseFeature(t *testing.T) {
	request := durableSpawnRequest()
	request.Repository.Checkout = &fleet.HubCheckout{Origin: "https://github.com/example/project.git", Mode: "detached", Sha: strings.Repeat("a", 40), Fetch: "refs/heads/main", Dir: "project.test-job", Exclude: []string{"Assets"}}
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"include"`) || !strings.Contains(string(raw), `"exclude":["Assets"]`) {
		t.Fatalf("the sparse request's wire shape is wrong: %s", raw)
	}
	// The empty lists keep today's bytes: no key at all.
	plain := hubCheckoutJSON(t, fleet.HubCheckout{Origin: "https://github.com/example/project.git", Mode: "detached", Sha: strings.Repeat("a", 40), Fetch: "refs/heads/main", Dir: "project.test-job"})
	if strings.Contains(plain, "exclude") || strings.Contains(plain, "include") {
		t.Fatalf("empty lists changed the checkout's bytes: %s", plain)
	}
}

func hubCheckoutJSON(t *testing.T, checkout fleet.HubCheckout) string {
	t.Helper()
	raw, err := json.Marshal(checkout)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

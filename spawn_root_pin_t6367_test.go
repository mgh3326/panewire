package panewire

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// hk 6402 (T-TG2 of hk 4821): the job dir a checkout prepares into must be
// absent or a real directory, checked before the first git write at every N,
// and checked again as the same directory before a reuse merge writes into it.
// The admission key is a pure function of the request, so a link no longer
// hides a spawn from another's hold: it is refused instead.

// A link at <root>/job-a -> job-b names another job's worktree. prepare refuses
// it before any git command that writes, so job-b keeps its HEAD and files even
// though origin moved on in a way a reuse of job-b would have merged.
func TestPrepareRefusesLinkedJobDirBeforeMerge_t6367(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	fixture.cloneRepo(t)

	dirB := filepath.Join(fixture.workRoot, "job-b")
	nodeGit(t, fixture.clone, "worktree", "add", "-q", "-b", "job/b", dirB, "main")
	if err := os.WriteFile(filepath.Join(dirB, "payload.txt"), []byte("job-b\n"), 0600); err != nil {
		t.Fatal(err)
	}
	nodeGit(t, dirB, "add", "payload.txt")
	nodeGit(t, dirB, "commit", "-q", "-m", "job-b payload")
	headBefore := nodeGit(t, dirB, "rev-parse", "HEAD")
	contentBefore, err := os.ReadFile(filepath.Join(dirB, "payload.txt"))
	if err != nil {
		t.Fatal(err)
	}
	// Push the job-b commit, then one child on origin/job/b: a prepare that
	// reached the reuse merge through the link would fast-forward job-b to it.
	nodeGit(t, dirB, "push", "-q", "origin", "HEAD:refs/heads/job/b")
	fixture.pushFixtureBranch(t, "job/b", headBefore)
	// The clone's own objects must hold the expected commit for the reuse checks
	// that read it (the merge guard is what must refuse, not a missing object).
	nodeGit(t, dirB, "fetch", "-q", "origin")

	dirA := filepath.Join(fixture.workRoot, "job-a")
	if err := os.Symlink(dirB, dirA); err != nil {
		t.Fatal(err)
	}

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, fixture.prepareArgs("branch", "job/b", "", "", "job-a"))
	if code != "" || result["outcome"] != nodePrepareJobDirLink {
		t.Fatalf("prepare code=%q result=%v, want outcome %q", code, result, nodePrepareJobDirLink)
	}
	if got := nodeGit(t, dirB, "rev-parse", "HEAD"); got != headBefore {
		t.Fatalf("job-b HEAD moved to %s, want %s", got, headBefore)
	}
	contentAfter, err := os.ReadFile(filepath.Join(dirB, "payload.txt"))
	if err != nil || !bytes.Equal(contentBefore, contentAfter) {
		t.Fatalf("job-b payload changed: %q err=%v", contentAfter, err)
	}
	if info, err := os.Lstat(dirA); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("the refusal touched job-a (info=%v err=%v)", info, err)
	}
	if out := nodeGit(t, fixture.clone, "worktree", "list", "--porcelain"); strings.Contains(out, dirA) {
		t.Fatalf("the refused prepare registered %s: %s", dirA, out)
	}
}

// The dir can be swapped after prepare checked it and before a reuse merge
// writes into it. The merge re-checks the dir as the recorded directory (device
// and inode) and refuses, so the link's target is never fast-forwarded.
func TestJobDirSwappedBeforeReuseMergeRefused_t6367(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	fixture.cloneRepo(t)

	dirA := filepath.Join(fixture.workRoot, "job-a")
	nodeGit(t, fixture.clone, "worktree", "add", "-q", "-b", "job/b", dirA, "main")
	// The swap target is a real worktree of another clone, clean and one commit
	// behind origin/job/b, so a merge that followed the link would write there.
	dirB := filepath.Join(t.TempDir(), "job-b")
	nodeGit(t, fixture.root, "clone", "-q", fixture.origin, dirB)
	if err := os.WriteFile(filepath.Join(dirB, "payload.txt"), []byte("job-b\n"), 0600); err != nil {
		t.Fatal(err)
	}
	nodeGit(t, dirB, "add", "payload.txt")
	nodeGit(t, dirB, "commit", "-q", "-m", "job-b payload")
	headBefore := nodeGit(t, dirB, "rev-parse", "HEAD")
	contentBefore, err := os.ReadFile(filepath.Join(dirB, "payload.txt"))
	if err != nil {
		t.Fatal(err)
	}
	nodeGit(t, dirB, "push", "-q", "origin", "HEAD:refs/heads/job/b")
	fixture.pushFixtureBranch(t, "job/b", headBefore)
	// The clone's own objects must hold the expected commit for the reuse checks
	// that read it (the merge guard is what must refuse, not a missing object).
	nodeGit(t, dirB, "fetch", "-q", "origin")

	swapped := false
	client.nodePrepareJobDirChecked = func(dir string) {
		if swapped || dir != dirA {
			return
		}
		swapped = true
		if err := os.Rename(dirA, dirA+".held"); err != nil {
			t.Fatalf("rename job-a: %v", err)
		}
		if err := os.Symlink(dirB, dirA); err != nil {
			t.Fatalf("link job-a: %v", err)
		}
	}

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, fixture.prepareArgs("branch", "job/b", "", "", "job-a"))
	if !swapped {
		t.Fatal("the fixture never swapped the dir; the merge guard was not exercised")
	}
	if code != "" || result["outcome"] != nodePrepareJobDirLink {
		t.Fatalf("prepare code=%q result=%v, want outcome %q", code, result, nodePrepareJobDirLink)
	}
	if got := nodeGit(t, dirB, "rev-parse", "HEAD"); got != headBefore {
		t.Fatalf("the merge wrote into job-b: HEAD %s, want %s", got, headBefore)
	}
	contentAfter, err := os.ReadFile(filepath.Join(dirB, "payload.txt"))
	if err != nil || !bytes.Equal(contentBefore, contentAfter) {
		t.Fatalf("job-b payload changed: %q err=%v", contentAfter, err)
	}
}

// The wire entry takes only nodeWorktreePrepareArgs, and the op's field
// allowlist names exactly those keys: a root in the caller's JSON is refused,
// so it can never become the pin. A prepare that accepted the key and ignored
// it would succeed under spawn.json's root, which this refuses to see.
func TestWireWorktreePrepareCannotSetRoot_t6367(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	named := t.TempDir()
	args := fixture.prepareArgs("branch", "feat", "", "", "job-a")
	args["root"] = named
	if result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args); code != "invalid_args" {
		t.Fatalf("prepare with a caller root code=%q result=%v, want invalid_args", code, result)
	}
	if entries, err := os.ReadDir(named); err != nil || len(entries) != 0 {
		t.Fatalf("the caller-named root was used (entries=%d err=%v)", len(entries), err)
	}
	if _, err := os.Stat(filepath.Join(fixture.workRoot, "job-a")); !os.IsNotExist(err) {
		t.Fatalf("a refused prepare created a worktree (err=%v)", err)
	}
}

// The hub and the node read one key from the request. A linked job dir keeps
// that key at N=2, so the spawn is admitted beside another and refuses at
// prepare — it never waits for the link exclusive class that hk 6402 deletes.
func TestHubAndNodeKeysAgreeWithLinks_t6367(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	client.spawnConfigPath = t3482SpawnConfig(t, fixture.workRoot)
	client.spawnSlotsOverride = 2

	dirB := filepath.Join(fixture.workRoot, "job-b")
	if err := os.MkdirAll(dirB, 0700); err != nil {
		t.Fatal(err)
	}
	dirA := filepath.Join(fixture.workRoot, "job-a")
	if err := os.Symlink(dirB, dirA); err != nil {
		t.Fatal(err)
	}

	message := concurrencyMessage(2, "job-a")
	target := client.spawnTargetFor(message)
	if target == nil || target.Key == "" {
		t.Fatalf("a linked job dir lost its admission key: %+v", target)
	}
	if hub := hubSpawnOverlapKey(&hubSpawnRequest{Checkout: message.Checkout, Args: message.Args}); target.Key != hub {
		t.Fatalf("node key %q, hub key %q", target.Key, hub)
	}

	admission := client.nodeAdmission()
	releaseOther, _ := admission.tryAdmit(spawnOverlapKey("job-other", []string{"--job", "job-other"}), 2)
	if releaseOther == nil {
		t.Fatal("could not hold another slot")
	}
	release, changed := admission.tryAdmit(target.Key, 2)
	if release == nil {
		t.Fatalf("a linked job dir waited on another spawn (changed=%v)", changed)
	}
	defer release()
	defer releaseOther()

	result := client.runHubSpawn(t.Context(), message)
	if result.RC != 81 || result.Error != "checkout_refused:"+nodePrepareJobDirLink {
		t.Fatalf("result=%+v, want checkout_refused:%s", result, nodePrepareJobDirLink)
	}
}

// A non-ASCII job dir is refused where the hub validates a checkout: macOS
// maps the NFC and NFD spellings of one name to one inode, so widening the ASCII
// job-id grammar would let the two spellings share a directory (hk 4824, F5).
func TestNonASCIIJobDirRefused_t6367(t *testing.T) {
	const origin = "https://github.com/acme/repo.git"
	for _, dir := range []string{"caf\u00e9-review", "cafe\u0301-review"} {
		checkout := hubSpawnCheckout{Origin: origin, Mode: "branch", Branch: "feat", Dir: dir}
		if validHubSpawnCheckout(checkout) {
			t.Fatalf("job dir %q accepted at hub checkout validation", dir)
		}
	}
	if !validHubSpawnCheckout(hubSpawnCheckout{Origin: origin, Mode: "branch", Branch: "feat", Dir: "cafe-review"}) {
		t.Fatal("an ASCII job dir was refused")
	}
}

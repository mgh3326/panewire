package panewire

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// checkoutPullFixture is a bare origin, the checkout the node pulls, and a peer
// clone that pushes the commits the checkout has not seen yet.
type checkoutPullFixture struct {
	origin   string
	checkout string
	peer     string
	marker   string
}

// installScript records each run in the marker file and exits with
// PANEWIRE_TEST_INSTALL_EXIT, so a test reads both from outside the checkout.
const checkoutPullInstallScript = "#!/bin/sh\necho ran >> \"$PANEWIRE_TEST_INSTALL_MARKER\"\nexit \"${PANEWIRE_TEST_INSTALL_EXIT:-0}\"\n"

func newCheckoutPullFixture(t *testing.T) *checkoutPullFixture {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	root := t.TempDir()
	fixture := &checkoutPullFixture{
		origin:   filepath.Join(root, "origin.git"),
		checkout: filepath.Join(root, "rstack"),
		peer:     filepath.Join(root, "peer"),
		marker:   filepath.Join(root, "install-ran.txt"),
	}
	t.Setenv("PANEWIRE_TEST_INSTALL_MARKER", fixture.marker)
	t.Setenv("PANEWIRE_TEST_INSTALL_EXIT", "0")
	nodeGit(t, root, "init", "--bare", "-q", fixture.origin)
	nodeGit(t, root, "clone", "-q", fixture.origin, fixture.peer)
	fixture.pushPeer(t, "initial", map[string]string{"README.md": "rstack\n", "install.sh": checkoutPullInstallScript})
	nodeGit(t, root, "clone", "-q", fixture.origin, fixture.checkout)
	// No detached `git gc --auto` may run beside a case: it can hold a lock
	// that the next fetch or pull then trips over.
	for _, dir := range []string{fixture.origin, fixture.peer, fixture.checkout} {
		nodeGit(t, dir, "config", "gc.auto", "0")
	}
	return fixture
}

// pushPeer commits files in the peer clone and pushes them to origin. install.sh
// is written executable, as a repository's install script is.
func (fixture *checkoutPullFixture) pushPeer(t *testing.T, message string, files map[string]string) string {
	t.Helper()
	for name, body := range files {
		path := filepath.Join(fixture.peer, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		mode := os.FileMode(0o644)
		if filepath.Ext(name) == ".sh" || strings.HasPrefix(name, "bin/") {
			mode = 0o755
		}
		if err := os.WriteFile(path, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
	}
	nodeGit(t, fixture.peer, "add", "-A")
	nodeGit(t, fixture.peer, "commit", "-q", "-m", message)
	nodeGit(t, fixture.peer, "push", "-q", "origin", "HEAD:main")
	return nodeGit(t, fixture.peer, "rev-parse", "HEAD")
}

func (fixture *checkoutPullFixture) client() *HubClient {
	return &HubClient{nodeCheckouts: map[string]string{"rstack": fixture.checkout}}
}

func checkoutPullArgs(checkout, rev string) map[string]any {
	return map[string]any{"checkout": checkout, "rev": rev}
}

func checkoutPullRun(t *testing.T, client *HubClient, checkout, rev string) map[string]any {
	t.Helper()
	result, code := nodeRun(t, client, nodeCommandCheckoutPull, checkoutPullArgs(checkout, rev))
	if code != "" {
		t.Fatalf("checkout.pull code=%q", code)
	}
	return result
}

func checkoutPullMarkerRuns(t *testing.T, fixture *checkoutPullFixture) int {
	t.Helper()
	contents, err := os.ReadFile(fixture.marker)
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Count(string(contents), "ran")
}

func TestNodeCheckoutPullFastForward(t *testing.T) {
	fixture := newCheckoutPullFixture(t)
	before := nodeGit(t, fixture.checkout, "rev-parse", "HEAD")
	target := fixture.pushPeer(t, "next", map[string]string{"app.txt": "next\n"})

	result := checkoutPullRun(t, fixture.client(), "rstack", "")
	if result["outcome"] != "pulled" || result["from"] != before || result["to"] != target || result["install_ran"] != false || result["install_exit"] != float64(0) {
		t.Fatalf("result=%v want pulled %s -> %s without install", result, before, target)
	}
	if head := nodeGit(t, fixture.checkout, "rev-parse", "HEAD"); head != target {
		t.Fatalf("checkout HEAD=%s, want %s", head, target)
	}
	if runs := checkoutPullMarkerRuns(t, fixture); runs != 0 {
		t.Fatalf("install ran %d times for an app-only change", runs)
	}
}

func TestNodeCheckoutPullCurrent(t *testing.T) {
	fixture := newCheckoutPullFixture(t)
	head := nodeGit(t, fixture.checkout, "rev-parse", "HEAD")
	result := checkoutPullRun(t, fixture.client(), "rstack", head)
	if result["outcome"] != "current" || result["to"] != head || result["target_reached"] != true {
		t.Fatalf("result=%v, want current at %s with the target reached", result, head)
	}
}

// TestNodeCheckoutPullRefusals covers every refusal: each one leaves the
// checkout's branch and worktree as it was, even when origin is ahead.
func TestNodeCheckoutPullRefusals(t *testing.T) {
	cases := []struct {
		name    string
		reason  string
		arrange func(t *testing.T, fixture *checkoutPullFixture, checkout string) *HubClient
	}{
		{"dirty tracked", "dirty", func(t *testing.T, f *checkoutPullFixture, checkout string) *HubClient {
			if err := os.WriteFile(filepath.Join(checkout, "README.md"), []byte("local edit\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			return f.client()
		}},
		{"dirty untracked", "dirty", func(t *testing.T, f *checkoutPullFixture, checkout string) *HubClient {
			if err := os.WriteFile(filepath.Join(checkout, "scratch.txt"), []byte("x\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			return f.client()
		}},
		{"detached", "detached", func(t *testing.T, f *checkoutPullFixture, checkout string) *HubClient {
			nodeGit(t, checkout, "checkout", "-q", "--detach")
			return f.client()
		}},
		{"no upstream", "no-upstream", func(t *testing.T, f *checkoutPullFixture, checkout string) *HubClient {
			nodeGit(t, checkout, "branch", "--unset-upstream")
			return f.client()
		}},
		{"not fast-forward", "not-fast-forward", func(t *testing.T, f *checkoutPullFixture, checkout string) *HubClient {
			if err := os.WriteFile(filepath.Join(checkout, "local.txt"), []byte("local\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			nodeGit(t, checkout, "add", "local.txt")
			nodeGit(t, checkout, "commit", "-q", "-m", "local")
			f.pushPeer(t, "diverge", map[string]string{"remote.txt": "remote\n"})
			return f.client()
		}},
		{"unlisted name", "not-allow-listed", func(t *testing.T, f *checkoutPullFixture, checkout string) *HubClient {
			f.pushPeer(t, "next", map[string]string{"app.txt": "next\n"})
			return f.client()
		}},
		{"missing directory", "not-a-checkout", func(t *testing.T, f *checkoutPullFixture, checkout string) *HubClient {
			if err := os.RemoveAll(checkout); err != nil {
				t.Fatal(err)
			}
			f.pushPeer(t, "next", map[string]string{"app.txt": "next\n"})
			return f.client()
		}},
		{"symlink", "not-a-checkout", func(t *testing.T, f *checkoutPullFixture, checkout string) *HubClient {
			link := filepath.Join(filepath.Dir(checkout), "linked")
			if err := os.Symlink(checkout, link); err != nil {
				t.Fatal(err)
			}
			client := &HubClient{nodeCheckouts: map[string]string{"rstack": link}}
			f.pushPeer(t, "next", map[string]string{"app.txt": "next\n"})
			return client
		}},
		{"lock held", "busy", func(t *testing.T, f *checkoutPullFixture, checkout string) *HubClient {
			lock, err := nodeGitOpLockPath(checkout)
			if err != nil {
				t.Fatal(err)
			}
			release, err := tryNodeGitOpLock(lock)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(release)
			f.pushPeer(t, "next", map[string]string{"app.txt": "next\n"})
			client := f.client()
			client.nodeGitLockWait = 50 * time.Millisecond
			return client
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newCheckoutPullFixture(t)
			checkout := fixture.checkout
			client := tc.arrange(t, fixture, checkout)
			// The state the refusal must keep is the one after the arrange step
			// (a local commit, an edit), so HEAD is read once it is in place.
			before := ""
			if info, err := os.Stat(checkout); err == nil && info.IsDir() {
				before = nodeGit(t, checkout, "rev-parse", "HEAD")
			}
			name := "rstack"
			if tc.name == "unlisted name" {
				name = "other"
			}
			result := checkoutPullRun(t, client, name, "")
			if result["outcome"] != "refused" || result["reason"] != tc.reason {
				t.Fatalf("result=%v, want refused for %s", result, tc.reason)
			}
			if before != "" {
				if head := nodeGit(t, checkout, "rev-parse", "HEAD"); head != before {
					t.Fatalf("refused pull moved HEAD from %s to %s", before, head)
				}
			}
			if runs := checkoutPullMarkerRuns(t, fixture); runs != 0 {
				t.Fatalf("refused pull ran install %d times", runs)
			}
			// Only an interrupted pull names a revision in a refusal.
			if _, hasTo := result["to"]; hasTo && tc.reason != "pull-interrupted" {
				t.Fatalf("refusal %s carries to=%v; only pull-interrupted may", tc.reason, result["to"])
			}
		})
	}
}

func TestNodeCheckoutPullInstallOnlyForInstallPaths(t *testing.T) {
	cases := []struct {
		name    string
		files   map[string]string
		wantRun bool
	}{
		{"docs only", map[string]string{"docs/guide.md": "guide\n"}, false},
		{"install directory", map[string]string{"install/steps.txt": "step\n"}, true},
		{"bin directory", map[string]string{"bin/tool": "#!/bin/sh\n"}, true},
		{"install.sh itself", map[string]string{"install.sh": checkoutPullInstallScript + "# edited\n"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newCheckoutPullFixture(t)
			fixture.pushPeer(t, "change", tc.files)
			result := checkoutPullRun(t, fixture.client(), "rstack", "")
			if result["outcome"] != "pulled" {
				t.Fatalf("result=%v", result)
			}
			if got := result["install_ran"] == true; got != tc.wantRun {
				t.Fatalf("install_ran=%v, want %v (result %v)", got, tc.wantRun, result)
			}
			if runs := checkoutPullMarkerRuns(t, fixture); (runs == 1) != tc.wantRun {
				t.Fatalf("install marker runs=%d, want run=%v", runs, tc.wantRun)
			}
		})
	}
}

func TestNodeCheckoutPullInstallExitIsReported(t *testing.T) {
	fixture := newCheckoutPullFixture(t)
	fixture.pushPeer(t, "install change", map[string]string{"install/steps.txt": "step\n"})
	t.Setenv("PANEWIRE_TEST_INSTALL_EXIT", "3")
	result := checkoutPullRun(t, fixture.client(), "rstack", "")
	if result["outcome"] != "pulled" || result["install_ran"] != true || result["install_exit"] != float64(3) {
		t.Fatalf("result=%v, want pulled with install exit 3", result)
	}
}

// TestNodeCheckoutPullInstallTimeout: install.sh runs past its bound, is
// killed, and the pull still stands, reported as a timeout.
func TestNodeCheckoutPullInstallTimeout(t *testing.T) {
	fixture := newCheckoutPullFixture(t)
	fixture.pushPeer(t, "slow install", map[string]string{
		"install.sh":    "#!/bin/sh\nexec sleep 30\n",
		"install/x.txt": "x\n",
	})
	client := fixture.client()
	client.nodeInstallWait = 300 * time.Millisecond
	started := time.Now()
	result := checkoutPullRun(t, client, "rstack", "")
	if elapsed := time.Since(started); elapsed > 15*time.Second {
		t.Fatalf("install was not bounded: took %s", elapsed)
	}
	if result["outcome"] != "pulled" || result["install_ran"] != true || result["install_exit"] != float64(nodeCheckoutInstallExitKilled) || result["reason"] != nodeCheckoutReasonInstallLong {
		t.Fatalf("result=%v, want pulled with install-timeout", result)
	}
}

func TestNodeCheckoutPullTargetRev(t *testing.T) {
	fixture := newCheckoutPullFixture(t)
	fixture.pushPeer(t, "next", map[string]string{"app.txt": "next\n"})
	short := nodeGit(t, fixture.peer, "rev-parse", "--short=12", "HEAD")
	if result := checkoutPullRun(t, fixture.client(), "rstack", short); result["target_reached"] != true {
		t.Fatalf("result=%v, want target reached", result)
	}
	if result := checkoutPullRun(t, fixture.client(), "rstack", "deadbeef"); result["target_reached"] != false {
		t.Fatalf("result=%v, want target not reached for an unknown rev", result)
	}
}

func TestNodeCheckoutPullArgsAreValidated(t *testing.T) {
	for _, args := range []map[string]any{
		{"checkout": "../etc", "rev": ""},
		{"checkout": "Rstack", "rev": ""},
		{"checkout": "rstack", "rev": "zz"},
		{"checkout": "rstack"},
	} {
		raw, err := json.Marshal(args)
		if err != nil {
			t.Fatal(err)
		}
		if _, valid := parseNodeCommandArgs(nodeCommandCheckoutPull, raw); valid {
			t.Fatalf("args %s accepted", raw)
		}
	}
}

// checkoutSpawnJSON is a spawn.json that loads (enabled, workspace and session
// set) with the given extra members spliced in.
func checkoutSpawnJSON(extra string) string {
	base := `"enabled":true,"workspace":"w","herdr_session":"s"`
	if extra == "" {
		return "{" + base + "}"
	}
	return "{" + base + "," + extra + "}"
}

func TestNodeCheckoutAllowListFromSpawnConfig(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	dir := t.TempDir()
	write := func(body string) string {
		path := filepath.Join(dir, "spawn.json")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	defaults := map[string]string{"rstack": filepath.Join(home, ".agents", "rstack")}
	cases := []struct {
		name string
		body string
		want map[string]string
	}{
		{"checkouts key is the allow-list", checkoutSpawnJSON(`"checkouts":{"rstack":"/srv/rstack","docs":"/srv/docs"}`), map[string]string{"rstack": "/srv/rstack", "docs": "/srv/docs"}},
		{"no checkouts key gives the default", checkoutSpawnJSON(""), defaults},
		{"explicit empty list allows nothing", checkoutSpawnJSON(`"checkouts":{}`), map[string]string{}},
		{"unknown key ignored, allow-list still loads", checkoutSpawnJSON(`"checkouts":{"rstack":"/srv/rstack"},"bogus":1`), map[string]string{"rstack": "/srv/rstack"}},
		{"malformed known key allows nothing", checkoutSpawnJSON(`"checkouts":{"rstack":123}`), map[string]string{}},
		{"invalid name refuses the file", checkoutSpawnJSON(`"checkouts":{"Bad Name":"/srv/x"}`), map[string]string{}},
		{"relative path refuses the file", checkoutSpawnJSON(`"checkouts":{"rstack":"relative/path"}`), map[string]string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &HubClient{spawnConfigPath: write(tc.body)}
			got := client.nodeCheckoutAllowList()
			if len(got) != len(tc.want) {
				t.Fatalf("allow-list=%v, want %v", got, tc.want)
			}
			for name, path := range tc.want {
				if got[name] != path {
					t.Fatalf("allow-list[%s]=%q, want %q", name, got[name], path)
				}
			}
		})
	}

	// A checkouts key must not disable hub spawns: the file still loads.
	if _, err := loadHubSpawnConfig(write(checkoutSpawnJSON(`"checkouts":{"rstack":"/srv/rstack"}`))); err != nil {
		t.Fatalf("spawn.json with checkouts refused: %v", err)
	}
	// An unknown key elsewhere is ignored with a warning: the file still loads.
	if _, err := loadHubSpawnConfig(write(checkoutSpawnJSON(`"checkouts":{"rstack":"/srv/rstack"},"bogus":1`))); err != nil {
		t.Fatalf("unknown key refused: %v", err)
	}
	// A malformed value for a known key still refuses.
	_, err := loadHubSpawnConfig(write(checkoutSpawnJSON(`"checkouts":{"rstack":123}`)))
	var configErr *hubSpawnConfigError
	if !errors.As(err, &configErr) || configErr.Rule != "config_json_invalid" {
		t.Fatalf("malformed known key: err=%v, want config_json_invalid", err)
	}
	// An invalid checkouts entry refuses the file under its own rule.
	_, err = loadHubSpawnConfig(write(checkoutSpawnJSON(`"checkouts":{"rstack":"relative"}`)))
	if !errors.As(err, &configErr) || configErr.Rule != "checkouts_invalid" {
		t.Fatalf("relative checkout path: err=%v, want checkouts_invalid", err)
	}
	if got := (&HubClient{spawnConfigPath: filepath.Join(dir, "missing.json")}).nodeCheckoutAllowList(); got["rstack"] != defaults["rstack"] {
		t.Fatalf("missing spawn.json allow-list=%v", got)
	}
}

func TestNodeCheckoutHeartbeatRevisions(t *testing.T) {
	fixture := newCheckoutPullFixture(t)
	client := fixture.client()
	short := nodeGit(t, fixture.checkout, "rev-parse", "--short", "HEAD")
	revs := client.checkoutRevisionsForHeartbeat(t.Context())
	if rev, ok := revs["rstack"]; !ok || rev.Sha != short || rev.Branch != "main" {
		t.Fatalf("heartbeat revisions=%v, want rstack=%s@main", revs, short)
	}

	nodeGit(t, fixture.checkout, "checkout", "-q", "--detach")
	if rev := client.checkoutRevisionsForHeartbeat(t.Context())["rstack"]; rev.Branch != "" || rev.Sha != short {
		t.Fatalf("detached heartbeat revision=%+v, want sha %s and no branch", rev, short)
	}

	missing := &HubClient{nodeCheckouts: map[string]string{"rstack": filepath.Join(t.TempDir(), "absent")}}
	if revs := missing.checkoutRevisionsForHeartbeat(t.Context()); revs != nil {
		t.Fatalf("missing checkout reported %v", revs)
	}
}

func TestNodeCheckoutPullResultValidation(t *testing.T) {
	sha := strings.Repeat("a", 40)
	other := strings.Repeat("b", 40)
	valid := []nodeCheckoutPullResult{
		{Outcome: "pulled", Checkout: "rstack", From: sha, To: other},
		{Outcome: "pulled", Checkout: "rstack", From: sha, To: other, InstallRan: true, InstallExit: -1, Reason: "install-timeout"},
		{Outcome: "current", Checkout: "rstack", To: sha},
		{Outcome: "refused", Checkout: "rstack", Reason: "dirty"},
		{Outcome: "refused", Checkout: "rstack", Reason: "pull-interrupted", To: other},
	}
	for _, result := range valid {
		if !validNodeCheckoutPullResult(result) {
			t.Fatalf("rejected valid result %+v", result)
		}
	}
	invalid := []nodeCheckoutPullResult{
		{Outcome: "pulled", Checkout: "rstack", From: sha, To: sha},
		{Outcome: "current", Checkout: "rstack", To: sha, InstallRan: true},
		{Outcome: "refused", Checkout: "rstack", Reason: "made-up"},
		{Outcome: "refused", Checkout: "rstack"},
		{Outcome: "pulled", Checkout: "../x", From: sha, To: other},
	}
	for _, result := range invalid {
		if validNodeCheckoutPullResult(result) {
			t.Fatalf("accepted invalid result %+v", result)
		}
	}
}

// TestNodeCheckoutFailedPullNamesTheState: a failed pull is named from what it
// left in the checkout. Interrupted when HEAD moved or the worktree is dirty;
// not-fast-forward when nothing moved and the branch diverged.
func TestNodeCheckoutFailedPullNamesTheState(t *testing.T) {
	ctx := t.Context()
	t.Run("diverged and unchanged", func(t *testing.T) {
		fixture := newCheckoutPullFixture(t)
		nodeGit(t, fixture.checkout, "commit", "-q", "--allow-empty", "-m", "local")
		fixture.pushPeer(t, "remote", map[string]string{"remote.txt": "remote\n"})
		from := nodeGit(t, fixture.checkout, "rev-parse", "HEAD")
		reason, head := nodeCheckoutFailedPull(ctx, fixture.checkout, from)
		if reason != nodeCheckoutReasonNotFF || head != from {
			t.Fatalf("reason=%s head=%s, want not-fast-forward at %s", reason, head, from)
		}
	})
	t.Run("worktree dirty", func(t *testing.T) {
		fixture := newCheckoutPullFixture(t)
		from := nodeGit(t, fixture.checkout, "rev-parse", "HEAD")
		if err := os.WriteFile(filepath.Join(fixture.checkout, "README.md"), []byte("half applied\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		reason, head := nodeCheckoutFailedPull(ctx, fixture.checkout, from)
		if reason != nodeCheckoutReasonPullInterrupted || head != from {
			t.Fatalf("reason=%s head=%s, want pull-interrupted at %s", reason, head, from)
		}
	})
	t.Run("HEAD moved", func(t *testing.T) {
		fixture := newCheckoutPullFixture(t)
		from := nodeGit(t, fixture.checkout, "rev-parse", "HEAD")
		target := fixture.pushPeer(t, "next", map[string]string{"app.txt": "next\n"})
		nodeGit(t, fixture.checkout, "fetch", "-q")
		nodeGit(t, fixture.checkout, "reset", "-q", "--hard", target)
		reason, head := nodeCheckoutFailedPull(ctx, fixture.checkout, from)
		if reason != nodeCheckoutReasonPullInterrupted || head != target {
			t.Fatalf("reason=%s head=%s, want pull-interrupted at %s", reason, head, target)
		}
	})
}

// TestNodeCheckoutInstallKillsChildren: a child that install.sh starts must
// die with the script when the bound ends, not keep running after the result.
func TestNodeCheckoutInstallKillsChildren(t *testing.T) {
	fixture := newCheckoutPullFixture(t)
	late := fixture.marker + ".late"
	fixture.pushPeer(t, "install with a child", map[string]string{
		"install.sh":    "#!/bin/sh\n(sleep 2; echo late > \"$PANEWIRE_TEST_INSTALL_MARKER.late\") &\nsleep 30\n",
		"install/x.txt": "x\n",
	})
	client := fixture.client()
	client.nodeInstallWait = 300 * time.Millisecond
	result := checkoutPullRun(t, client, "rstack", "")
	if result["reason"] != nodeCheckoutReasonInstallLong {
		t.Fatalf("result=%v, want install-timeout", result)
	}
	// Proof that nothing happens later: the child would write after 2s, and
	// this wait is longer than that. It is a fixed window, not a poll.
	time.Sleep(2500 * time.Millisecond)
	if _, err := os.Stat(late); !os.IsNotExist(err) {
		t.Fatalf("install child kept running after the bound (stat err=%v)", err)
	}
}

// TestNodeCheckoutPullRenameOutOfInstallRunsInstall: a file moved out of
// install/ must still trigger install, since its old path changed too.
func TestNodeCheckoutPullRenameOutOfInstallRunsInstall(t *testing.T) {
	fixture := newCheckoutPullFixture(t)
	fixture.pushPeer(t, "add install file", map[string]string{"install/a.txt": "a\n"})
	if result := checkoutPullRun(t, fixture.client(), "rstack", ""); result["install_ran"] != true {
		t.Fatalf("setup pull result=%v, want install run", result)
	}
	if err := os.Remove(fixture.marker); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(fixture.peer, "docs"), 0o755); err != nil {
		t.Fatal(err)
	}
	nodeGit(t, fixture.peer, "mv", "install/a.txt", "docs/a.txt")
	nodeGit(t, fixture.peer, "commit", "-q", "-m", "move out of install")
	nodeGit(t, fixture.peer, "push", "-q", "origin", "HEAD:main")
	result := checkoutPullRun(t, fixture.client(), "rstack", "")
	if result["outcome"] != "pulled" || result["install_ran"] != true {
		t.Fatalf("result=%v, want pulled with install run for a rename out of install/", result)
	}
}

// TestNodeCheckoutPullInstallSeesQuotedPaths: git quotes unusual names unless
// the diff is -z, so a file under install/ with a non-ASCII name must still
// trigger install.
func TestNodeCheckoutPullInstallSeesQuotedPaths(t *testing.T) {
	fixture := newCheckoutPullFixture(t)
	fixture.pushPeer(t, "install with a non-ASCII name", map[string]string{"install/caf\u00e9.txt": "x\n"})
	result := checkoutPullRun(t, fixture.client(), "rstack", "")
	if result["install_ran"] != true {
		t.Fatalf("result=%v, want install run for install/café.txt", result)
	}
}

func TestInstallPathsChangedReadsNULList(t *testing.T) {
	cases := map[string]bool{
		"":                              false,
		"docs/a.md\x00":                 false,
		"docs/a.md\x00install/b\x00":    true,
		"bin/tool\x00":                  true,
		"installer/x\x00":               false,
		"\"install/caf\\303\\251\"\x00": false,
	}
	for names, want := range cases {
		if got := installPathsChanged(names); got != want {
			t.Fatalf("installPathsChanged(%q)=%v, want %v", names, got, want)
		}
	}
}

// TestNodeCheckoutPullKeepsIgnoredFiles: an incoming commit that tracks a path
// the checkout holds as an ignored local file must not overwrite it. The op
// refuses ignored-collision and leaves the local file and HEAD unchanged.
func TestNodeCheckoutPullKeepsIgnoredFiles(t *testing.T) {
	fixture := newCheckoutPullFixture(t)
	before := nodeGit(t, fixture.checkout, "rev-parse", "HEAD")
	fixture.pushPeer(t, "tracks a path the checkout ignores", map[string]string{"local.cfg": "tracked\n"})
	local := filepath.Join(fixture.checkout, "local.cfg")
	if err := os.WriteFile(local, []byte("node-local secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	excludeFile := filepath.Join(fixture.checkout, ".git", "info", "exclude")
	if err := os.WriteFile(excludeFile, []byte("local.cfg\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	result := checkoutPullRun(t, fixture.client(), "rstack", "")
	if result["outcome"] != "refused" || result["reason"] != nodeCheckoutReasonIgnoredCollision {
		t.Fatalf("result=%v, want refused ignored-collision", result)
	}
	if contents, err := os.ReadFile(local); err != nil || string(contents) != "node-local secret\n" {
		t.Fatalf("ignored file overwritten: %q err=%v", contents, err)
	}
	if head := nodeGit(t, fixture.checkout, "rev-parse", "HEAD"); head != before {
		t.Fatalf("refused pull moved HEAD from %s to %s", before, head)
	}
}

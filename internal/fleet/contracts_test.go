package fleet

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/arbiter"
)

func ledgerRunner(t *testing.T, a *App) *fakeRunner {
	t.Helper()
	f := &fakeRunner{}
	f.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			return Result{Out: `{"result":{"agents":[]}}`}
		}
		return Result{}
	}
	a.Runner = f
	return f
}
func seedJob(t *testing.T, a *App, id, role string) {
	putEvent(t, a, id, "job.claim", map[string]any{"owner_lane": "owner", "parent_lane": "parent", "task_id": "42", "t_level": "T1", "role": role})
	putEvent(t, a, id, "job.spawned", map[string]any{"pane_id": "w:p1", "tab_id": "w:t1", "label": "agent-a", "profile": "cmd-dsflash41", "family": "deepseek", "keep": false})
}

// seedLedger registers the job in the local ledger with a quota_pool record,
// as a live local spawn leaves it.
func seedLedger(t *testing.T, a *App, id string) {
	t.Helper()
	if _, e := a.ledger().Claim(context.Background(), arbiter.Claim{Job: id, Lane: "owner", T: "T1", AgentLabel: "agent-a"}); e != nil {
		t.Fatal(e)
	}
	if _, e := a.ledger().RecordQuotaPool(context.Background(), arbiter.QuotaRun{Job: id, Profile: "cmd-dsflash41", Gate: arbiter.Gate{Pool: "example"}}); e != nil {
		t.Fatal(e)
	}
}

// ledgerState is the job's local ledger state and quota pools.
func ledgerState(t *testing.T, a *App, id string) (string, []string) {
	t.Helper()
	v, e := a.ledger().JobGet(context.Background(), id)
	if e != nil {
		t.Fatal(e)
	}
	pools, e := a.ledger().QuotaPools(context.Background(), id)
	if e != nil {
		t.Fatal(e)
	}
	return v.State, pools
}

func eventKindsOf(t *testing.T, a *App, id string) []string {
	t.Helper()
	evs, e := a.events(id)
	if e != nil {
		t.Fatal(e)
	}
	var kinds []string
	for _, ev := range evs {
		kinds = append(kinds, ev.Kind)
	}
	return kinds
}

func TestR07ReapProtectsBuildersKeepAndRevivedJobs(t *testing.T) {
	for _, tt := range []struct {
		name, role            string
		keep, revive, include bool
		wantClose             bool
	}{{"implementer", "implementer", false, false, false, true}, {"builder default", "builder", false, false, false, false}, {"builder opt in", "builder", false, false, true, true}, {"keep", "implementer", true, false, true, false}, {"revived", "implementer", false, true, true, false}} {
		t.Run(tt.name, func(t *testing.T) {
			a := testApp(t)
			f := ledgerRunner(t, a)
			seedJob(t, a, "j", tt.role)
			putEvent(t, a, "j", "job.completed", map[string]any{})
			if tt.keep {
				putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "keep": true})
			}
			if tt.revive {
				putEvent(t, a, "j", "job.reclaim", map[string]any{"owner_lane": "owner", "task_id": "42"})
			}
			args := []string{"--job", "j", "--grace", "0s", "--apply"}
			if tt.include {
				args = append(args, "--include-builders")
			}
			a.Now = func() time.Time { return time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC) }
			if e := a.hygiene(context.Background(), "reap", args); e != nil {
				t.Fatal(e)
			}
			closed := false
			for _, c := range f.Calls {
				if strings.HasPrefix(strings.Join(c.Args, " "), "pane close") {
					closed = true
				}
			}
			if closed {
				t.Fatal("absent pane should not receive close")
			}
			reaped := slices.Contains(eventKindsOf(t, a, "j"), "job.reaped")
			if reaped != tt.wantClose {
				t.Fatalf("reaped=%t want %t", reaped, tt.wantClose)
			}
		})
	}
}
func TestR07KernelLockAndSecureFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "slot")
	a, e := secureOpen(path, true, nil)
	if e != nil {
		t.Fatal(e)
	}
	if !tryLock(a) {
		t.Fatal("first lock failed")
	}
	b, e := secureOpen(path, false, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	if tryLock(b) {
		t.Fatal("concurrent holder admitted")
	}
	a.Close()
	if !tryLock(b) {
		t.Fatal("dead holder did not release")
	}
	link := path + ".link"
	if e = os.Symlink(path, link); e != nil {
		t.Fatal(e)
	}
	if _, e = secureOpen(link, false, nil); e == nil {
		t.Fatal("symlink lock accepted")
	}
}
func TestR08RestoreVerifiesExistingReleasedPane(t *testing.T) {
	a := testApp(t)
	f := ledgerRunner(t, a)
	seedJob(t, a, "j", "implementer")
	seedLedger(t, a, "j")
	if e := a.releaseQuota(context.Background(), Job{ID: "j"}); e != nil {
		t.Fatal(e)
	}
	if st, _ := ledgerState(t, a, "j"); st != "released" {
		t.Fatalf("setup: state %s", st)
	}
	original := f.Do
	f.Do = func(c Command) Result {
		switch strings.Join(c.Args, " ") {
		case "agent list":
			return Result{Out: `{"result":{"agents":[{"pane_id":"w:p1","tab_id":"w:t1","name":"agent-a","agent_status":"working"}]}}`}
		}
		return original(c)
	}
	if e := a.lifecycle(context.Background(), "restore", []string{"j"}); e != nil {
		t.Fatal(e)
	}
	for _, c := range f.Calls {
		if strings.HasPrefix(strings.Join(c.Args, " "), "tab create") {
			t.Fatal("restore created a duplicate pane")
		}
	}
	if st, _ := ledgerState(t, a, "j"); st != "claimed" {
		t.Fatalf("restore did not reclaim the released job: state %s", st)
	}
	if kinds := eventKindsOf(t, a, "j"); !slices.Contains(kinds, "job.reclaim") {
		t.Fatalf("no reclaim event: %v", kinds)
	}
}
func TestDoneDeduplicatesAndSnapshotsChangedReport(t *testing.T) {
	a := testApp(t)
	useFakeHK(t).Handle(hkCommentAnswer)
	f := ledgerRunner(t, a)
	seedJob(t, a, "j", "implementer")
	path := filepath.Join(a.Jobs, "j", "report.md")
	if e := os.WriteFile(path, []byte("first\n"), 0600); e != nil {
		t.Fatal(e)
	}
	for n := 0; n < 2; n++ {
		if e := a.lifecycle(context.Background(), "done", []string{"j", "--report", path}); e != nil {
			t.Fatal(e)
		}
	}
	if e := os.WriteFile(path, []byte("second\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := a.lifecycle(context.Background(), "done", []string{"j", "--report", path}); e != nil {
		t.Fatal(e)
	}
	emits := 0
	for _, c := range f.Calls {
		if c.Name == "panewire" {
			emits++
		}
	}
	if emits != 2 {
		t.Fatalf("emits=%d", emits)
	}
	if b, e := os.ReadFile(filepath.Join(a.Jobs, "j", "report-r2.md")); e != nil || string(b) != "second\n" {
		t.Fatalf("snapshot %q %v", b, e)
	}
}
func TestR01R09R10R11ExternalServicesRemainExternal(t *testing.T) {
	for _, cmd := range []string{"assistant", "session-reap", "attach", "hk-desk", "joined", "round", "o3", "tester-eligible"} {
		t.Run(cmd, func(t *testing.T) {
			a := testApp(t)
			f := a.Runner.(*fakeRunner)
			if code(a.Run(context.Background(), []string{cmd})) != 2 {
				t.Fatal("unexpected replacement of external command")
			}
			if len(f.Calls) != 0 {
				t.Fatal("external mutation")
			}
		})
	}
}
func TestR03PlacementReadsCanonicalMachineAndFiltersDisconnected(t *testing.T) {
	raw := `{"decision":"host-b","candidates":[{"machine":"host-a","connected":false},{"machine":"host-b","connected":true},{"host":"host-c","reason":"not_accepting"},"host-d"]}`
	views, e := parseHubHosts(raw)
	if e != nil {
		t.Fatal(e)
	}
	if len(views) != 4 {
		t.Fatalf("expected 4 candidate views, got %v", views)
	}
	if views[0].Connected || !views[1].Connected || views[2].Reason != "not_accepting" {
		t.Fatalf("connectivity and acceptance must be preserved: %+v", views)
	}
	awake := []string{}
	for _, v := range views {
		if v.Connected && !strings.Contains(v.Reason, "not_accepting") {
			awake = append(awake, v.Name)
		}
	}
	if strings.Join(awake, ",") != "host-b,host-d" {
		t.Fatalf("%v", views)
	}
}
func TestR03PlacementReadsHubLoadRatio(t *testing.T) {
	raw := `{"decision":"host-a","candidates":[{"machine":"host-a","connected":true,"load_ratio":0.25,"reason":"available"},{"machine":"host-b","connected":true,"score":42.5}]}`
	views, e := parseHubHosts(raw)
	if e != nil {
		t.Fatal(e)
	}
	if views[1].Score == nil || *views[1].Score != 42.5 {
		t.Fatalf("score must be read from the hub: %+v", views[1])
	}
	if views[0].LoadRatio == nil || *views[0].LoadRatio != 0.25 {
		t.Fatalf("load_ratio must be read from the hub: %+v", views[0])
	}
	if views[0].Score != nil {
		t.Fatalf("an absent score stays nil: %+v", views[0])
	}
	if views[1].LoadRatio != nil {
		t.Fatalf("a missing reading stays unknown: %+v", views[1])
	}
}
func TestR04TaskStateAndBindingUseCLIResponse(t *testing.T) {
	for _, state := range []string{"merged", "dropped", "hold", "needs_decision"} {
		t.Run(state, func(t *testing.T) {
			a := testApp(t)
			a.Runner = &fakeRunner{Do: func(Command) Result {
				return Result{Out: fmt.Sprintf(`{"state":%q,"title":"task (grade B, T1)","refs":{}}`, state)}
			}}
			_, _, e := a.taskPrecheck(context.Background(), Spawn{Task: "42", Job: "j", Label: "agent"})
			if e == nil {
				t.Fatal("unavailable task accepted")
			}
		})
	}
}
func TestR05MergewakePolicyActingAndNoLegacyPins(t *testing.T) {
	b, e := os.ReadFile("../../.mergewake/policy.yaml")
	if e != nil {
		t.Fatal(e)
	}
	// Operator 10-30: mergewake owns land and watch for this repo.
	if !bytes.Contains(b, []byte("enabled: true")) {
		t.Fatal("mergewake acting land is not enabled")
	}
	for _, name := range []string{"gate_policy.json", "verdict.json"} {
		if _, e := os.Stat("../../" + name); !os.IsNotExist(e) {
			t.Fatal("legacy review state should not be created")
		}
	}
}
func TestR06R13QuotaPoolAndManualFallbackStayInQuotaTool(t *testing.T) {
	f := &fakeRunner{Do: func(Command) Result { return Result{Out: `{"pool":"operator-manual-provider","source":"manual"}`} }}
	q := CLIQuota{Runner: f, Binary: "quota"}
	out, e := q.Gate(context.Background(), QuotaRequest{Profile: "external-profile", Effort: "high", Role: "implementer"})
	if e != nil || !strings.Contains(out, "operator-manual-provider") {
		t.Fatalf("%s %v", out, e)
	}
	if len(f.Calls) != 1 {
		t.Fatal("panewire must not replace manual fallback")
	}
}
func TestR14MockIsolationRequiresExclusiveDescriptor(t *testing.T) {
	a := testApp(t)
	dir := t.TempDir()
	config := filepath.Join(dir, "mcp.json")
	t.Setenv("MOCK_MCP_PROFILE", "reviewed")
	t.Setenv("PANEWIRE_MOCK_MCP_CONFIG", config)
	for _, body := range []string{`{"mcpServers":{"x":{"url":"https://example.invalid"}}}`, `{"mcpServers":{"x":{"command":"relative"}}}`, `{"mcpServers":{}}`} {
		if e := os.WriteFile(config, []byte(body), 0600); e != nil {
			t.Fatal(e)
		}
		if _, e := a.mock(context.Background(), Spawn{Job: "j", CWD: dir, ExecutionLane: "mock"}, Seat{Kind: "claude"}); code(e) != 79 {
			t.Fatalf("unsafe mock accepted: %v", e)
		}
	}
}
func TestTrustGuardRejectsWritableBinary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tool")
	if e := os.WriteFile(path, []byte("fixture"), 0777); e != nil {
		t.Fatal(e)
	}
	if e := os.Chmod(path, 0777); e != nil {
		t.Fatal(e)
	}
	if code(TrustBinary(path, uint32(os.Getuid()))) != 70 {
		t.Fatal("writable binary accepted")
	}
	if e := os.Chmod(path, 0700); e != nil {
		t.Fatal(e)
	}
	if e := TrustBinary(path, uint32(os.Getuid())); e != nil {
		t.Fatal(e)
	}
}

// lockFixture creates a lock at mode, owned by uid:gid when chown is allowed.
func lockFixture(t *testing.T, mode os.FileMode, uid, gid int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "x.wrk.lock")
	if e := os.WriteFile(path, nil, 0600); e != nil {
		t.Fatal(e)
	}
	if uid >= 0 {
		if e := os.Chown(path, uid, gid); e != nil {
			t.Skipf("cannot chown to %d:%d: %v", uid, gid, e)
		}
	}
	if e := os.Chmod(path, mode); e != nil {
		t.Fatal(e)
	}
	return path
}

func lockMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, e := os.Stat(path)
	if e != nil {
		t.Fatal(e)
	}
	return info.Mode().Perm()
}

// ownGID is a group id that is ours, and is not 0 when we are root, so the
// fixture is an own lock and never the root:root shared shape.
func ownGID() int {
	if os.Getuid() == 0 {
		return 4242
	}
	return os.Getgid()
}

func TestSecureOpenRepairsOwnGroupWritableLock(t *testing.T) {
	path := lockFixture(t, 0664, os.Getuid(), ownGID())
	var warned []string
	f, e := secureOpen(path, true, func(format string, args ...any) {
		warned = append(warned, fmt.Sprintf(format, args...))
	})
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if got := lockMode(t, path); got != 0600 {
		t.Fatalf("mode %o, want 0600", got)
	}
	if len(warned) != 1 || !strings.Contains(warned[0], path) || !strings.Contains(warned[0], "0664") || !strings.Contains(warned[0], "0600") {
		t.Fatalf("repair warning %q", warned)
	}
	again, e := secureOpen(path, false, func(string, ...any) { t.Fatal("repaired lock warned again") })
	if e != nil {
		t.Fatal(e)
	}
	again.Close()
}

func TestSecureOpenRefusesOwnOtherWritableLock(t *testing.T) {
	path := lockFixture(t, 0666, os.Getuid(), ownGID())
	_, e := secureOpen(path, false, nil)
	if e == nil || !strings.Contains(e.Error(), path) || !strings.Contains(e.Error(), "other-write") || !strings.Contains(e.Error(), "chmod 600") {
		t.Fatalf("refusal %v", e)
	}
	if got := lockMode(t, path); got != 0666 {
		t.Fatalf("refused lock was chmod'ed to %o", got)
	}
}

func TestSecureOpenKeepsRootSharedLock(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("a root:root lock needs root to build")
	}
	path := lockFixture(t, 0664, 0, 0)
	f, e := secureOpen(path, false, func(string, ...any) { t.Fatal("shared lock repaired") })
	if e != nil {
		t.Fatal(e)
	}
	f.Close()
	if got := lockMode(t, path); got != 0664 {
		t.Fatalf("shared lock mode %o, want 0664", got)
	}
}

func TestSecureOpenRefusesForeignOwnedLock(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("a foreign-owned lock needs root to chown")
	}
	path := lockFixture(t, 0664, 4243, 4243)
	_, e := secureOpen(path, false, nil)
	if e == nil || !strings.Contains(e.Error(), "4243:4243") || !strings.Contains(e.Error(), "owned by another user") {
		t.Fatalf("refusal %v", e)
	}
	if got := lockMode(t, path); got != 0664 {
		t.Fatalf("foreign lock chmod'ed to %o", got)
	}
}

func TestSecureOpenRefusesHardLinkedLock(t *testing.T) {
	path := lockFixture(t, 0664, os.Getuid(), ownGID())
	if e := os.Link(path, path+".twin"); e != nil {
		t.Skipf("hard links unsupported: %v", e)
	}
	_, e := secureOpen(path, false, nil)
	if e == nil || !strings.Contains(e.Error(), "2 hard links") {
		t.Fatalf("refusal %v", e)
	}
	if got := lockMode(t, path); got != 0664 {
		t.Fatalf("hard-linked lock chmod'ed to %o", got)
	}
}

func TestLockRepairAndRefusalRules(t *testing.T) {
	const me = 1000
	reg := os.FileMode(0)
	for _, c := range []struct {
		name     string
		uid, gid uint32
		perm     os.FileMode
		nlink    uint64
		repair   bool
		refusal  string // substring; "" means accepted
	}{
		{"own 0664", me, me, 0664, 1, true, ""},
		{"own 0666", me, me, 0666, 1, false, "other-write"},
		{"own 0600", me, me, 0600, 1, false, ""},
		{"root:root 0664 as user", 0, 0, 0664, 1, false, ""},
		{"root:root 0664 as root", 0, 0, 0664, 1, false, ""},
		{"root:root 0666", 0, 0, 0666, 1, false, "pre-created root:root"},
		{"foreign 0664", 2000, 2000, 0664, 1, false, "uid 2000"},
		{"own hard-linked", me, me, 0664, 2, false, "2 hard links"},
	} {
		self := uint32(me)
		if c.name == "root:root 0664 as root" {
			self = 0
		}
		if got := lockRepairable(reg, c.nlink, c.uid, c.gid, c.perm, self); got != c.repair {
			t.Errorf("%s: repairable=%v want %v", c.name, got, c.repair)
		}
		if c.repair {
			continue
		}
		why := lockRefusal(reg, c.nlink, c.uid, c.gid, c.perm, self)
		if (c.refusal == "") != (why == "") || !strings.Contains(why, c.refusal) {
			t.Errorf("%s: refusal %q, want containing %q", c.name, why, c.refusal)
		}
	}
	if why := lockRefusal(os.ModeDir, 1, me, me, 0600, me); !strings.Contains(why, "not a regular file") {
		t.Errorf("directory refusal %q", why)
	}
}

func TestLockOwnerRule(t *testing.T) {
	const me = 1000
	for _, c := range []struct {
		uid, gid uint32
		perm     os.FileMode
		ok       bool
	}{
		{me, me, 0600, true},
		{me, me, 0660, false},
		{me, me, 0606, false},
		{0, 0, 0660, true}, // PA desktop shared lock (tmpfiles + ACL)
		{0, 0, 0600, true},
		{0, 0, 0666, false}, // other-writable
		{0, 5, 0660, false}, // root-owned but foreign group
		{2000, 2000, 0600, false},
	} {
		if got := lockOwnerOK(c.uid, c.gid, c.perm, me); got != c.ok {
			t.Fatalf("uid=%d gid=%d perm=%o: got %v want %v", c.uid, c.gid, c.perm, got, c.ok)
		}
	}
}

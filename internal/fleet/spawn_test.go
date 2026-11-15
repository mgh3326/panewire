package fleet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/arbiter"
)

type fakeQuota struct {
	LaunchValue Launch
	LaunchErr   error
	GateError   error
	GateDo      func(QuotaRequest) (string, error)
	CatalogRows []CatalogRow
	CatalogErr  error
	PoolViews   map[string]PoolView
	PoolsErr    error
	Calls       []QuotaRequest
	mu          sync.Mutex // delegate --panel asks the gate from several goroutines
}

func TestSpawnParseAcceptsAnyPurposeForAstra(t *testing.T) {
	for _, args := range [][]string{
		{"-m", "codex-astra", "--purpose", "design consult"},
		{"-m", "codex-astra", "--purpose", "architect"},
		{"-m", "codex-astra", "--operator-request", "X"},
		{"-m", "codex-sol", "--purpose", "design consult"},
	} {
		if _, err := parseSpawn(args); err != nil {
			t.Fatalf("%v: parse rejected: %v", args, err)
		}
	}
}

func TestParseRefusalDoesNotSubmit(t *testing.T) {
	a, s, runner, _ := cmdFixture(t)
	before := len(runner.Calls)
	err := a.Run(context.Background(), []string{"spawn", "-c", s.CWD, "-p", s.Prompt, "-w", s.Workspace, "-l", s.Label, "--job", s.Job, "--t", s.T, "--task", s.Task, "--owner", "parent", "--role", "implementer", "-m", "codex-astra", "--host", "local", "--override-reason", "fixture", "--containment", "hk:12", "--review-pass", "2"})
	if exitCode(err) != exitUsage || len(runner.Calls) != before {
		t.Fatalf("parse refusal submitted work: err=%v calls=%d before=%d", err, len(runner.Calls), before)
	}
}

func (q *fakeQuota) Launch(_ context.Context, r QuotaRequest) (Launch, error) {
	q.mu.Lock()
	q.Calls = append(q.Calls, r)
	q.mu.Unlock()
	return q.LaunchValue, q.LaunchErr
}
func (q *fakeQuota) Gate(_ context.Context, r QuotaRequest) (string, error) {
	q.mu.Lock()
	q.Calls = append(q.Calls, r)
	q.mu.Unlock()
	if q.GateDo != nil {
		return q.GateDo(r)
	}
	return "pool=example profile=cmd-dsflash41\n", q.GateError
}
func (q *fakeQuota) Catalog(context.Context) ([]CatalogRow, error) {
	if q.CatalogRows == nil && q.LaunchValue.Profile != "" {
		l := q.LaunchValue
		return []CatalogRow{{Profile: l.Profile, Effort: l.Effort, Grade: l.Grade, ModelID: l.ModelID, Gate: l.Gate, Pool: l.Pool}}, q.CatalogErr
	}
	return q.CatalogRows, q.CatalogErr
}
func (q *fakeQuota) Pools(context.Context) (map[string]PoolView, error) {
	return q.PoolViews, q.PoolsErr
}
func testApp(t *testing.T) *App {
	t.Helper()
	p, e := LoadPolicy("")
	if e != nil {
		t.Fatal(e)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	root := t.TempDir()
	if real, e := filepath.EvalSymlinks(root); e == nil {
		root = real
	}
	// HTTP producers must never inherit the developer's hk endpoint or token.
	t.Setenv("PANEWIRE_HK_ENV", filepath.Join(root, "absent-hk.env"))
	t.Setenv("HANDOFFKEEP_URL", "")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
	rearmFakeHK(t)
	nodeEnv := filepath.Join(root, "node.env")
	if e := os.WriteFile(nodeEnv, []byte("HUB_MACHINE_ID=test-host\n"), 0600); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PANEWIRE_NODE_ENV", nodeEnv)
	a := &App{Runner: &fakeRunner{}, Policy: p, Config: Config{}, Home: root, Jobs: filepath.Join(root, "work", "herdr-inbox", "jobs"), Bins: map[string]string{}, Out: &bytes.Buffer{}, Err: &bytes.Buffer{}, Now: func() time.Time { return now }, Sleep: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }}
	a.DecisionReply = func(context.Context, string, string, time.Time) (bool, error) { return false, nil }
	a.JobStore = newFakeJobStore()
	// The ledger clock runs a second ahead of the putEvent seeds (a.Now()+n ns),
	// so events the ledger writes sort after them.
	a.Arbiter = &arbiter.Store{DB: arbiter.StatePath(filepath.Join(root, ".local", "share")), Inbox: a.Jobs, Now: func() time.Time { return a.Now().Add(time.Second) }}
	return a
}
func putEvent(t *testing.T, a *App, job, kind string, p map[string]any) {
	t.Helper()
	dir := filepath.Join(a.Jobs, job, "events")
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	files, _ := os.ReadDir(dir)
	ev := map[string]any{"kind": kind, "payload": p, "created_at": a.Now().Add(time.Duration(len(files)) * time.Nanosecond).Format(time.RFC3339Nano)}
	b, _ := json.Marshal(ev)
	if e := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%04d-%s.json", len(files), kind)), b, 0600); e != nil {
		t.Fatal(e)
	}
}
func cmdFixture(t *testing.T) (*App, Spawn, *fakeRunner, *int) {
	t.Helper()
	a := testApp(t)
	a.Config.Local = map[string]any{"cmd_dsflash41": true}
	cwd := filepath.Join(a.Home, "tree")
	if e := os.MkdirAll(cwd, 0700); e != nil {
		t.Fatal(e)
	}
	prompt := filepath.Join(a.Home, "brief")
	if e := os.WriteFile(prompt, []byte(strings.Repeat("Please implement and verify the requested change.\n", 200)), 0600); e != nil {
		t.Fatal(e)
	}
	s := Spawn{CWD: cwd, Profile: "cmd-dsflash41", Prompt: prompt, Workspace: "workspace", Label: "worker-a", Job: "job-a", T: "T1", Task: "42", Role: "implementer", Host: "local", OverrideReason: "fixture", Containment: "hk:12"}
	a.Quota = &fakeQuota{LaunchValue: Launch{Profile: "cmd-dsflash41", ModelID: "provider/example-model", Effort: "high", Grade: "A+", Gate: "default"}}
	launched, detected := false, false
	submitted := false
	returns := 0
	token := ""
	banner := "# Command Code v1.0\n# models: example-model-(latest) with high effort · tag\n# " + cwd + "\n"
	useFakeHK(t).SetTask("42", `{"id":42,"title":"implement (grade B, T1)","state":"ready","refs":{}}`).Handle(hkClaimAnswer)
	f := &fakeRunner{}
	f.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch {
		case strings.HasPrefix(args, "tab create"):
			return Result{Out: `{"result":{"root_pane":{"pane_id":"w:p1"},"tab_id":"w:t1"}}`}
		case strings.HasPrefix(args, "pane process-info"):
			if !launched {
				return Result{Out: `{"result":{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":1,"argv0":"zsh"}]}}}`}
			}
			return Result{Out: `{"result":{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":2,"argv0":"cmd"}]}}}`}
		case strings.HasPrefix(args, "pane run") && len(c.Args) > 3 && c.Args[3] == "echo":
			token = c.Args[len(c.Args)-1]
			return Result{}
		case strings.HasPrefix(args, "pane run"):
			launched = true
			return Result{}
		case strings.HasPrefix(args, "agent wait") && strings.Contains(args, "--until idle"):
			// Real herdr: a CLI that reports "done" right after start never
			// becomes "idle"; waiting only for idle blocks until the timeout.
			t.Fatalf("agent wait must accept idle, done or blocked (no --until idle): %s", args)
			return Result{}
		case strings.HasPrefix(args, "agent wait") && launched && !detected:
			// Real herdr: the agent of a CLI started with pane run is detected a
			// moment later; until then agent wait answers agent_not_found at once.
			detected = true
			return Result{Code: 1, Out: `{"error":{"code":"agent_not_found"}}`}
		case strings.HasPrefix(args, "pane send-keys"):
			returns++
			if returns == 2 {
				submitted = true
			}
			return Result{}
		case strings.HasPrefix(args, "agent list"):
			status := "idle"
			if submitted {
				status = "working"
			}
			return Result{Out: fmt.Sprintf(`{"result":{"agents":[{"pane_id":"w:p1","tab_id":"w:t1","name":"worker-a","agent":"cmd","agent_status":%q,"cwd":%q,"state_change_seq":1}]}}`, status, cwd)}
		case strings.HasPrefix(args, "agent read") && !launched:
			// Real herdr: a shell-only pane has no agent yet (agent_not_found).
			return Result{Code: 1, Out: `{"error":{"code":"agent_not_found"}}`}
		case strings.HasPrefix(args, "agent read"), strings.HasPrefix(args, "pane read"):
			if !launched {
				return Result{Out: token + "\n"}
			}
			if !submitted {
				return Result{Out: banner + "[Pasted text #1 +200 lines]\n"}
			}
			return Result{Out: banner + "Working on the requested change\n"}
		}
		return Result{}
	}
	a.Runner = f
	return a, s, f, &returns
}
func TestCmdLongBriefSubmissionRequiresStatusAndRetriesOnce(t *testing.T) {
	a, s, f, returns := cmdFixture(t)
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	if *returns != 2 {
		t.Fatalf("submit keys=%d", *returns)
	}
	pastes := 0
	for _, c := range f.Calls {
		if len(c.Args) > 1 && c.Args[0] == "pane" && c.Args[1] == "send-text" {
			pastes++
			// The pane gets a short pointer; the full brief, preamble included,
			// is the file it names. Typed text past ~1 KB was lost on a remote host.
			path := filepath.Join(a.Jobs, s.Job, "brief-delivered.md")
			if len(c.Args[3]) > 400 || !strings.Contains(c.Args[3], path) || strings.Contains(c.Args[3], "Host safety (fleet-v1)") {
				t.Fatalf("typed text is not a short pointer to %s: %q", path, c.Args[3])
			}
			b, e := os.ReadFile(path)
			if e != nil || !strings.Contains(string(b), "Host safety (fleet-v1)") {
				t.Fatalf("delivered brief file: %v", e)
			}
			if raw, _ := os.ReadFile(s.Prompt); !strings.Contains(string(b), strings.TrimSpace(string(raw))) {
				t.Fatal("delivered brief file lacks the full brief")
			}
			sum := sha256.Sum256(b)
			if !strings.Contains(c.Args[3], fmt.Sprintf("%d bytes, sha256 %x", len(b), sum[:6])) {
				t.Fatalf("pointer does not name size and hash: %q", c.Args[3])
			}
		}
	}
	if pastes != 1 {
		t.Fatalf("pastes=%d", pastes)
	}
	if !strings.Contains(a.Out.(*bytes.Buffer).String(), "landed=yes") {
		t.Fatal("success absent")
	}
}
func TestSpawnSubmissionFailureCleansPaneAndLease(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	original := f.Do
	f.Do = func(c Command) Result {
		r := original(c)
		if strings.HasPrefix(strings.Join(c.Args, " "), "agent list") {
			r.Out = strings.ReplaceAll(r.Out, `"working"`, `"idle"`)
		}
		return r
	}
	e := a.Spawn(context.Background(), s)
	if code(e) != 76 {
		t.Fatalf("%v", e)
	}
	calls := ""
	for _, c := range f.Calls {
		calls += strings.Join(c.Args, " ") + "\n"
	}
	if !strings.Contains(calls, "pane close w:p1") {
		t.Fatal("pane cleanup missing")
	}
	if st, pools := ledgerState(t, a, s.Job); st != "released" || len(pools) != 0 {
		t.Fatalf("lease cleanup missing: %s %v", st, pools)
	}
	if hkClaimed(t) || slices.Contains(eventKindsOf(t, a, s.Job), "job.spawned") {
		t.Fatal("failed spawn committed")
	}
}
func TestSpawnDryRunShowsPreambleWithoutSideEffects(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.DryRun = true
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	if len(f.Calls) != 0 {
		t.Fatalf("dry run ran commands: %+v", f.Calls)
	}
	for _, r := range useFakeHK(t).Requests() {
		if r.Method != http.MethodGet {
			t.Fatalf("dry run wrote to hk: %s", r.Route())
		}
	}
	if !strings.Contains(a.Out.(*bytes.Buffer).String(), "fleet-v1") {
		t.Fatal("preamble missing")
	}
}
func TestSeatDefaultEffortResolvesBareProfile(t *testing.T) {
	a := testApp(t)
	// the retired launcher pinned DEFAULT_EFFORT=high for these seats; a bare -m must resolve the
	// same rung instead of passing an empty effort the catalog always refuses.
	for _, tc := range []struct{ seat, effort string }{
		{"devin-swe2", "high"},
		{"cmd-dsflash41", "high"},
	} {
		qr, ok := a.quotaRequest(Spawn{Profile: tc.seat})
		if !ok || qr.Effort != tc.effort {
			t.Fatalf("%s must default to effort %s: %+v", tc.seat, tc.effort, qr)
		}
	}
	qr, _ := a.quotaRequest(Spawn{Profile: "cmd-dsflash41", Effort: "max"})
	if qr.Effort != "max" {
		t.Fatalf("an explicit --effort must win over the seat default: %+v", qr)
	}
}

func TestSpawnDryRunLocalRefusesMissingCapability(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	s.Profile = "cc-glm" // a.Config.Local carries cmd_dsflash41 but not clinepass
	s.DryRun = true
	e := a.Spawn(context.Background(), s)
	if code(e) != 70 {
		t.Fatalf("a local dry-run on a host without the capability must refuse 70: %v", e)
	}
	if strings.Contains(a.Out.(*bytes.Buffer).String(), "dry-run host=") {
		t.Fatal("the preview must not promise a spawn Seat.Check would refuse")
	}
}

func TestSpawnExplicitProfileWithoutSeatRefusesBeforePlacement(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Profile = "cc-dsflash" // a legacy spelling with no seat policy
	s.DryRun = true
	e := a.Spawn(context.Background(), s)
	if code(e) != 70 {
		t.Fatalf("a -m profile with no seat policy must refuse 70: %v", e)
	}
	if len(f.Calls) != 0 {
		t.Fatalf("a missing seat must refuse before any dependency call: %v", f.Calls)
	}
	if strings.Contains(a.Out.(*bytes.Buffer).String(), "dry-run host=") {
		t.Fatal("dry-run must not preview a spawn that can never admit")
	}
}

// The local ledger is in-process: a spawn claims, records the quota run and
// logs its events through internal/arbiter and executes no `arbiter` command.
func TestR12LocalLedgerIsInProcess(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	for _, c := range f.Calls {
		if c.Name == "arbiter" {
			t.Fatalf("panewire executed arbiter: %v", c.Args)
		}
	}
	v, e := a.ledger().JobGet(context.Background(), s.Job)
	if e != nil || v.State != "leased" || v.AgentLabel != s.Label || v.Receipt["pane_id"] != "w:p1" {
		t.Fatalf("ledger job %+v %v", v, e)
	}
	pools, _ := a.ledger().QuotaPools(context.Background(), s.Job)
	if len(pools) != 1 || pools[0] != "example" {
		t.Fatalf("quota pools %v", pools)
	}
	kinds := eventKindsOf(t, a, s.Job)
	for _, want := range []string{"job.claim", "job.placement", "quota_pool.record", "job.spawned"} {
		if !slices.Contains(kinds, want) {
			t.Fatalf("missing %s in %v", want, kinds)
		}
	}
	if _, e := os.Stat(filepath.Join(a.Home, ".local", "share", "panewire", "state.db")); !os.IsNotExist(e) {
		t.Fatal("panewire must not create a second ledger")
	}
	if _, e := os.Stat(filepath.Join(a.Home, ".local", "share", "arbiter", "state.db")); e != nil {
		t.Fatalf("ledger state is not at the arbiter path: %v", e)
	}
}

// Non-test sources never name the arbiter binary: no exec, no ARBITER_BIN.
func TestNoArbiterExecInSources(t *testing.T) {
	files, e := filepath.Glob("*.go")
	if e != nil {
		t.Fatal(e)
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		b, _ := os.ReadFile(name)
		for _, bad := range []string{`"arbiter",`, `"arbiter")`, "ARBITER_BIN"} {
			if strings.Contains(string(b), bad) {
				t.Errorf("%s still refers to the arbiter binary (%s)", name, bad)
			}
		}
	}
}
func TestTaskClaimMustConfirmAtomicBinding(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	useFakeHK(t).Handle(func(r hkRequest) (int, string, bool) {
		if r.Route() != "POST /v1/tasks/42/claim" {
			return 0, "", false
		}
		return http.StatusOK, `{"id":42,"state":"developing","claimed_by":"worker-a","refs":{}}`, true
	})
	// hk 2928: the claim runs after the brief is delivered, so a half record
	// no longer fails the landed spawn; it must still never count as linked.
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatalf("landed spawn failed: %v", err)
	}
	events, err := a.ledger().Events(context.Background(), s.Job)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if ev.Kind == "job.task_link" && ev.Payload["status"] != "failed" {
			t.Fatalf("half-record admitted: %v", ev.Payload)
		}
	}
}

// A spawned pane carries its own hub lane, so `panewire inbox` run inside it
// scopes to that lane without --lane.
func TestSpawnPaneEnvironmentNamesItsLane(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	for _, c := range f.Calls {
		if !strings.HasPrefix(strings.Join(c.Args, " "), "tab create") {
			continue
		}
		var lanes []string
		for i, arg := range c.Args {
			if i > 0 && c.Args[i-1] == "--env" && strings.HasPrefix(arg, "PANEWIRE_LANE=") {
				lanes = append(lanes, arg)
			}
		}
		if len(lanes) != 1 || lanes[0] != "PANEWIRE_LANE="+s.Label {
			t.Fatalf("pane env PANEWIRE_LANE = %v, want exactly PANEWIRE_LANE=%s", lanes, s.Label)
		}
		return
	}
	t.Fatal("no tab created")
}

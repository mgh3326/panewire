package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDelegateRoutingRole(t *testing.T) {
	for role, want := range map[string]string{"reader": "implementer", "verifier": "verifier", "reviewer": "reviewer"} {
		d := delegateReq{o: delegateOpts{role: role}}
		if got := d.routingRole(); got != want {
			t.Errorf("%s routes to %q, want %q", role, got, want)
		}
	}
}

// delegateRows is a free SWE-2 rung and a paid Sonnet rung, both able to take a
// grade C task.
func delegateRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "devin-swe2", Effort: "high", Grade: "A+", Pool: "devin", Gate: "default", ModelID: "swe-2"},
		{Profile: "sonnet", Effort: "high", Grade: "C", Pool: "claude", Gate: "default", ModelID: "m"},
	}
}

// deadlineRunner is a fake process runner that records the context deadline of
// every call, so a test can see that a headless CLI call was time-bounded.
type deadlineRunner struct {
	mu        sync.Mutex
	do        func(Command) Result
	calls     []Command
	remaining map[int]time.Duration
}

func (r *deadlineRunner) Run(ctx context.Context, c Command) Result {
	r.mu.Lock()
	if d, ok := ctx.Deadline(); ok {
		if r.remaining == nil {
			r.remaining = map[int]time.Duration{}
		}
		r.remaining[len(r.calls)] = time.Until(d)
	}
	r.calls = append(r.calls, c)
	do := r.do
	r.mu.Unlock()
	return do(c)
}
func (r *deadlineRunner) named(name string) []Command {
	var out []Command
	for _, c := range r.calls {
		if c.Name == name {
			out = append(out, c)
		}
	}
	return out
}

type delegateEnv struct {
	a           *App
	runner      *deadlineRunner
	cwd         string
	prompt      string
	routingBody string
	// label is the name herdr reports for the delegate agent (single calls).
	mu    sync.Mutex
	label string
	// results overrides the result a family's seat files (default {"answer": ...});
	// failures makes a family's pane end in that error instead.
	results  map[string]map[string]any
	failures map[string]error
}

// rowQuota answers Launch from the catalog row of the requested rung, so each
// seat launches as itself.
type rowQuota struct{ *fakeQuota }

func (q rowQuota) Launch(ctx context.Context, r QuotaRequest) (Launch, error) {
	if _, err := q.fakeQuota.Launch(ctx, r); err != nil {
		return Launch{}, err
	}
	for _, row := range q.CatalogRows {
		if row.Profile == r.Profile && row.Effort == r.Effort {
			return Launch{Profile: row.Profile, ModelID: row.ModelID, Effort: row.Effort, Grade: row.Grade, Gate: row.Gate, Pool: row.Pool}, nil
		}
	}
	return Launch{}, fail(exitConfig, "no catalog row for %s@%s", r.Profile, r.Effort)
}

func newDelegateEnv(t *testing.T) *delegateEnv {
	t.Helper()
	return newDelegateEnvWith(t, delegateRows(), map[string]string{"devin-swe2": "devin", "sonnet": "claude"}, map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)}})
}

// newDelegateEnvWith builds the fixture for a catalog; gates maps each seat to
// its pool and every pool gets fresh presence.
func newDelegateEnvWith(t *testing.T, rows []CatalogRow, gates map[string]string, pools map[string]PoolView) *delegateEnv {
	t.Helper()
	var present []string
	for _, pool := range gates {
		if !slices.Contains(present, pool) {
			present = append(present, pool)
		}
	}
	slices.Sort(present)
	a, _, q := selFixture(t, "B", rows, pools, present)
	q.GateDo = gatePools(gates)
	a.Quota = rowQuota{q}
	a.Config.Local = map[string]any{"workspace": "ws"}
	e := &delegateEnv{a: a, cwd: filepath.Join(a.Home, "repo"), prompt: filepath.Join(a.Home, "ask.md")}
	// These tests exercise delegation mechanics with explicitly qualified
	// fixture profiles. Routing eligibility has separate live-v1 regressions.
	policy, err := parseRoutingJSON([]byte(routingBlockV1))
	if err != nil {
		t.Fatal(err)
	}
	var qualified []string
	for _, row := range rows {
		qualified = append(qualified, row.Profile)
	}
	for _, role := range []string{"implementer", "verifier"} {
		rule := policy.Roles[role]
		rule.Allowed = qualified
		policy.Roles[role] = rule
	}
	block, _ := json.Marshal(policy)
	e.routingBody = routingDoc(string(block))
	if err := os.MkdirAll(filepath.Join(e.cwd, "pkg"), 0700); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"main.go": "package main\n", "pkg/util.go": "package pkg\n"} {
		if err := os.WriteFile(filepath.Join(e.cwd, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(e.prompt, []byte("Where is the retry loop?\n"), 0600); err != nil {
		t.Fatal(err)
	}
	useFakeHK(t).SetTask("42", `{"id":42,"title":"work (grade B, T1)","state":"ready","refs":{}}`).Handle(func(r hkRequest) (int, string, bool) {
		if r.Route() != "GET /v1/documents/policy%2Fmodel-routing" {
			return 0, "", false
		}
		body, _ := json.Marshal(map[string]any{"key": routingDocKey, "body": e.routingBody})
		return http.StatusOK, string(body), true
	})
	e.runner = &deadlineRunner{do: func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch {
		case c.Name == "herdr" && strings.HasPrefix(args, "agent list"):
			e.mu.Lock()
			defer e.mu.Unlock()
			return Result{Out: fmt.Sprintf(`{"result":{"agents":[{"pane_id":"w:p1","tab_id":"w:t1","name":%q,"agent":"devin","agent_status":"idle"}]}}`, e.label)}
		case c.Name == "devin":
			return Result{Out: "The retry loop is in pkg/util.go.\n"}
		}
		return Result{}
	}}
	a.Runner = e.runner
	t.Setenv("HK_TASK_ID", "42")
	return e
}

// worker plays the delegate's pane for spawnFn: it runs the real selection for
// the Spawn it was given, then files an answer the way `panewire done --result`
// would, and returns the seats it was asked to run.
func (e *delegateEnv) worker(t *testing.T, answer string, seen *[]Spawn, chosen *[]string, during func(Spawn)) func(context.Context, *App, Spawn) error {
	return func(ctx context.Context, a *App, s Spawn) error {
		if s.ParentJob != "" { // Spawn resolves the parent before it selects
			s.Task, s.Inherited = "42", s.ParentJob
		}
		sel, err := a.selectProfile(ctx, s)
		if err != nil {
			return err
		}
		e.mu.Lock()
		*seen = append(*seen, s)
		*chosen = append(*chosen, sel.chosen.seatName)
		e.label = s.Label
		e.mu.Unlock()
		if err := e.failures[sel.chosen.seat.Family]; err != nil {
			return err
		}
		if during != nil {
			during(s)
		}
		putEvent(t, a, s.Job, "job.spawned", map[string]any{"pane_id": "w:p1", "tab_id": "w:t1", "label": s.Label, "profile": sel.chosen.seatName, "family": sel.chosen.seat.Family, "report": s.Report, "task_id": "42"})
		body := map[string]any{"answer": answer}
		if r, ok := e.results[sel.chosen.seat.Family]; ok {
			body = r
		}
		res, _ := json.Marshal(body)
		store := a.JobStore.(*fakeJobStore)
		if err := store.claimSpawned(ctx, s.Job, "42", "test-host"); err != nil {
			return err
		}
		_ = store.Put(ctx, s.Job, map[string]any{"state": "completed", "result": json.RawMessage(res)})
		putEvent(t, a, s.Job, "job.completed", map[string]any{"report": s.Report})
		fmt.Fprintf(a.Out, "OK pane=w:p1 job=%s\nwait job=%s outcome=completed\n", s.Job, s.Job)
		return nil
	}
}

func (e *delegateEnv) out() string  { return e.a.Out.(interface{ String() string }).String() }
func (e *delegateEnv) errs() string { return e.a.Err.(interface{ String() string }).String() }

func delegateArgs(e *delegateEnv, more ...string) []string {
	return append([]string{"--prompt", e.prompt, "--cwd", e.cwd}, more...)
}

// snapshot hashes every file under dir, so a test can tell that nothing was
// created, changed or removed.
func snapshot(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == ".git" {
			return fs.SkipDir // worktree bookkeeping lives here; the files are what must not change
		}
		if d.IsDir() {
			return nil
		}
		b, err := os.ReadFile(p)
		rel, _ := filepath.Rel(dir, p)
		out[rel] = fmt.Sprintf("%x", sha256.Sum256(b))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestDelegateReaderFollowsPolicyAndTakesFreeRungWhenAdmitted(t *testing.T) {
	e := newDelegateEnv(t)
	var seen []Spawn
	var chosen []string
	e.a.spawnFn = e.worker(t, "It is in pkg/util.go.", &seen, &chosen, nil)
	if err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "reader", "--timeout", "90s")); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || chosen[0] != "devin-swe2" {
		t.Fatalf("under the default policy the free SWE-2 rung wins over an unhurried paid pool: %v", chosen)
	}
	s := seen[0]
	if !s.Wait || s.WaitTimeout != "1m30s" || s.Profile != "auto" || s.Role != "worker" || s.Grade != "C" || s.Task != "42" || s.Host != "auto" {
		t.Fatalf("pane mode must go through spawn --wait with auto selection: %+v", s)
	}
	lines := strings.Split(strings.TrimSpace(e.out()), "\n")
	if last := lines[len(lines)-1]; last != "It is in pkg/util.go." {
		t.Fatalf("the answer must be the last thing printed: %q", e.out())
	}
	if !strings.Contains(lines[0], "mode=pane") || !strings.Contains(lines[0], "profile=devin-swe2") {
		t.Fatalf("header: %q", lines[0])
	}
	closes := 0
	for _, c := range e.runner.named("herdr") {
		if strings.Join(c.Args, " ") == "pane close w:p1" {
			closes++
		}
	}
	if closes != 1 {
		t.Fatalf("the pane must be closed once after the result is read, got %d", closes)
	}
	if strings.Contains(e.out(), "wait job=") || !strings.Contains(e.errs(), "wait job=") {
		t.Fatalf("spawn and wait chatter belongs on stderr, not stdout:\nout=%q\nerr=%q", e.out(), e.errs())
	}
}

func TestDelegateJSONOutput(t *testing.T) {
	e := newDelegateEnv(t)
	var seen []Spawn
	var chosen []string
	e.a.spawnFn = e.worker(t, "done", &seen, &chosen, nil)
	if err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "reader", "--json")); err != nil {
		t.Fatal(err)
	}
	var r DelegateResult
	if err := json.Unmarshal([]byte(e.out()), &r); err != nil {
		t.Fatalf("--json prints exactly one JSON document: %v\n%s", err, e.out())
	}
	if r.Answer != "done" || r.Job != "delegate_test-host_1" || r.Role != "reader" || r.Mode != "pane" || r.Profile != "devin-swe2" || r.Result["answer"] != "done" {
		t.Fatalf("%+v", r)
	}
}

func TestDelegateReviewerExcludesCallerFamily(t *testing.T) {
	e := newDelegateEnv(t)
	var seen []Spawn
	var chosen []string
	e.a.spawnFn = e.worker(t, "no findings", &seen, &chosen, nil)
	// SWE-2 is the free rung and a cognition seat: a cognition caller's review
	// must not run on it, a reader on the same family may.
	if err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "reviewer", "--family", "cognition")); err != nil {
		t.Fatal(err)
	}
	if err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "reader", "--family", "cognition")); err != nil {
		t.Fatal(err)
	}
	if want := []string{"sonnet", "devin-swe2"}; !slices.Equal(chosen, want) {
		t.Fatalf("reviewer must skip the caller's family, reader need not: %v want %v", chosen, want)
	}
	if seen[0].Role != "reviewer" || seen[0].ParentFamily != "cognition" {
		t.Fatalf("%+v", seen[0])
	}
}

func TestDelegateReviewerNeedsCallerFamily(t *testing.T) {
	e := newDelegateEnv(t)
	e.a.spawnFn = func(context.Context, *App, Spawn) error { t.Fatal("must refuse before spawning"); return nil }
	err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "reviewer"))
	if code(err) != exitUsage {
		t.Fatalf("a reviewer without a known caller family must refuse: %v", err)
	}
}

func TestDelegateInsideJobUsesParentFamilyAndIDs(t *testing.T) {
	e := newDelegateEnv(t)
	a := e.a
	putEvent(t, a, "task42-builder", "job.spawned", map[string]any{"pane_id": "w:p9", "label": "task42-builder", "profile": "devin-swe2", "family": "cognition", "task_id": "42"})
	t.Setenv("ARBITER_JOB", "task42-builder")
	var seen []Spawn
	var chosen []string
	a.spawnFn = e.worker(t, "ok", &seen, &chosen, nil)
	for i := 0; i < 2; i++ {
		if err := a.delegate(context.Background(), delegateArgs(e, "--role", "reviewer")); err != nil {
			t.Fatal(err)
		}
	}
	if seen[0].Job != "delegate-task42-builder-1" || seen[1].Job != "delegate-task42-builder-2" {
		t.Fatalf("job ids are delegate-<parent>-<n>: %q %q", seen[0].Job, seen[1].Job)
	}
	if seen[0].ParentJob != "task42-builder" || seen[0].Label != seen[0].Job {
		t.Fatalf("the delegation is a child job of the caller's job: %+v", seen[0])
	}
	if chosen[0] != "sonnet" {
		t.Fatalf("the parent job's family (cognition) must be excluded from the review: %v", chosen)
	}
}

// A top-level delegation on a host whose node id equals a parent job's id must
// not hand out the id a delegation under that parent gets on another host: both
// land in the one hk namespace.
func TestDelegateJobIDFormsDoNotCollide(t *testing.T) {
	hostA := testApp(t)
	nodeA := filepath.Join(t.TempDir(), "node.env")
	if e := os.WriteFile(nodeA, []byte("HUB_MACHINE_ID=task42-builder\n"), 0600); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PANEWIRE_NODE_ENV", nodeA)
	top, e := hostA.delegateJobID("")
	if e != nil {
		t.Fatal(e)
	}
	hostBDir := t.TempDir()
	hostB := &App{Jobs: filepath.Join(hostBDir, "jobs")}
	under, e := hostB.delegateJobID("task42-builder")
	if e != nil {
		t.Fatal(e)
	}
	if top == under {
		t.Fatalf("a top-level id and a parent-form id must never be equal: %q", top)
	}
}

// Two hosts share the hk job namespace but not their jobs dirs. A mkdir counter
// alone hands both of them delegate-1, and the second host's hk claim is then
// refused as a duplicate. Top-level delegate ids carry the reserving host's node
// id, so the claims never meet.
func TestDelegateJobIDsDoNotCollideAcrossHosts(t *testing.T) {
	ctx := context.Background()
	hk := newFakeJobStore()
	hostA := testApp(t) // node id test-host
	hostA.JobStore = hk
	hostBDir := t.TempDir()
	nodeB := filepath.Join(hostBDir, "node.env")
	if e := os.WriteFile(nodeB, []byte("HUB_MACHINE_ID=host-b\n"), 0600); e != nil {
		t.Fatal(e)
	}
	hostB := &App{Jobs: filepath.Join(hostBDir, "jobs"), JobStore: hk}

	var ids [][2]string // host, id
	for range 2 {
		id, e := hostA.delegateJobID("")
		if e != nil {
			t.Fatal(e)
		}
		ids = append(ids, [2]string{"test-host", id})
	}
	t.Setenv("PANEWIRE_NODE_ENV", nodeB)
	for range 2 {
		id, e := hostB.delegateJobID("")
		if e != nil {
			t.Fatal(e)
		}
		ids = append(ids, [2]string{"host-b", id})
	}
	seen := map[string]bool{}
	for _, c := range ids {
		if seen[c[1]] {
			t.Fatalf("job id %s reserved twice across hosts: %v", c[1], ids)
		}
		seen[c[1]] = true
		if e := hk.Claim(ctx, c[1], "7", c[0]); e != nil {
			t.Fatalf("hk refused the claim of %s on %s: %v", c[1], c[0], e)
		}
	}
}

func TestDelegateJobIDNeedsNodeIDOutsideJob(t *testing.T) {
	a := testApp(t)
	nodeless := filepath.Join(t.TempDir(), "node.env")
	if e := os.WriteFile(nodeless, []byte("HUB_TOKEN=x\n"), 0600); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PANEWIRE_NODE_ENV", nodeless)
	if _, e := a.delegateJobID(""); code(e) != exitConfig {
		t.Fatalf("a top-level delegate without a node id must refuse as config: %v", e)
	}
	if entries, _ := os.ReadDir(a.Jobs); len(entries) != 0 {
		t.Fatalf("a refused id must reserve nothing: %v", entries)
	}
	if id, e := a.delegateJobID("task42-builder"); e != nil || id != "delegate-task42-builder-1" {
		t.Fatalf("a job's delegations keep the parent form without a node id: %q %v", id, e)
	}
}

func TestDelegateBriefPreamblesPerRole(t *testing.T) {
	cases := []struct {
		role  string
		extra []string
		want  []string
		deny  []string
	}{
		{"verifier", nil, []string{"Run only the acceptance commands", "Never edit, create or delete any file"}, []string{"throwaway checkout"}},
		{"reader", nil, []string{"Never edit, create or delete any file", "read-only"}, []string{"throwaway checkout"}},
		{"reviewer", []string{"--family", "other"}, []string{"Never edit, create or delete any file", "ranked by severity"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.role, func(t *testing.T) {
			e := newDelegateEnv(t)
			var seen []Spawn
			var chosen []string
			var brief string
			e.a.spawnFn = e.worker(t, "ok", &seen, &chosen, func(s Spawn) {
				b, err := os.ReadFile(s.Prompt)
				if err != nil {
					t.Fatal(err)
				}
				brief = string(b)
			})
			if err := e.a.delegate(context.Background(), delegateArgs(e, append([]string{"--role", tc.role}, tc.extra...)...)); err != nil {
				t.Fatal(err)
			}
			for _, w := range tc.want {
				if !strings.Contains(brief, w) {
					t.Fatalf("%s brief lacks %q:\n%s", tc.role, w, brief)
				}
			}
			for _, d := range tc.deny {
				if strings.Contains(brief, d) {
					t.Fatalf("%s brief must not contain %q:\n%s", tc.role, d, brief)
				}
			}
			for _, w := range []string{"Where is the retry loop?", "panewire done delegate_test-host_1 --report", "--result"} {
				if !strings.Contains(brief, w) {
					t.Fatalf("the brief must carry the task and the filing steps (%q):\n%s", w, brief)
				}
			}
		})
	}
}

func TestDelegatePaneLeavesCwdUntouched(t *testing.T) {
	e := newDelegateEnv(t)
	before := snapshot(t, e.cwd)
	var seen []Spawn
	var chosen []string
	e.a.spawnFn = e.worker(t, "ok", &seen, &chosen, nil)
	for _, role := range []string{"reader", "verifier"} {
		if err := e.a.delegate(context.Background(), delegateArgs(e, "--role", role)); err != nil {
			t.Fatal(err)
		}
	}
	if after := snapshot(t, e.cwd); !equalMaps(before, after) {
		t.Fatalf("delegate changed --cwd:\nbefore=%v\nafter=%v", before, after)
	}
	if seen[0].CWD != e.cwd {
		t.Fatalf("a read-only delegate works in the caller's tree: %s", seen[0].CWD)
	}
}

func equalMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestDelegateFallsBackToReportWhenResultMissing(t *testing.T) {
	e := newDelegateEnv(t)
	e.a.spawnFn = func(ctx context.Context, _ *App, s Spawn) error {
		e.label = s.Label
		putEvent(t, e.a, s.Job, "job.spawned", map[string]any{"pane_id": "w:p1", "tab_id": "w:t1", "label": s.Label, "profile": "devin-swe2", "task_id": "42"})
		// The completion watcher finished the job from the report alone.
		if err := os.WriteFile(s.Report, []byte("answer from the report\n"), 0600); err != nil {
			t.Fatal(err)
		}
		putEvent(t, e.a, s.Job, "job.completed", map[string]any{"report": s.Report})
		return nil
	}
	if err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "reader")); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(strings.TrimSpace(e.out()), "answer from the report") {
		t.Fatalf("%q", e.out())
	}
}

func TestDelegateNoAnswerFails(t *testing.T) {
	e := newDelegateEnv(t)
	e.a.spawnFn = func(context.Context, *App, Spawn) error { return nil }
	err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "reader"))
	if code(err) != exitFailure {
		t.Fatalf("a job that finished with no answer must fail: %v", err)
	}
}

func TestDelegateWaitFailuresKeepTheirExitCodes(t *testing.T) {
	for _, c := range []int{exitWaitEscalated, exitWaitLost, exitWaitTimeout} {
		e := newDelegateEnv(t)
		e.a.spawnFn = func(context.Context, *App, Spawn) error { return fail(c, "wait ended") }
		if err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "reader")); code(err) != c {
			t.Fatalf("exit %d must pass through, got %v", c, err)
		}
		if !strings.Contains(e.errs(), "panewire reap") || strings.TrimSpace(e.out()) != "" {
			t.Fatalf("a failed delegation prints no answer and points at reap: out=%q err=%q", e.out(), e.errs())
		}
	}
}

func TestDelegateUsageRefusals(t *testing.T) {
	e := newDelegateEnv(t)
	e.a.spawnFn = func(context.Context, *App, Spawn) error { t.Fatal("must not spawn"); return nil }
	empty := filepath.Join(e.a.Home, "empty.md")
	if err := os.WriteFile(empty, []byte(" \n"), 0600); err != nil {
		t.Fatal(err)
	}
	cases := map[string][]string{
		"no role":         {"--prompt", e.prompt},
		"unknown role":    {"--role", "builder", "--prompt", e.prompt},
		"no prompt":       {"--role", "reader"},
		"empty prompt":    {"--role", "reader", "--prompt", empty},
		"bad grade":       {"--role", "reader", "--prompt", e.prompt, "--grade", "Z"},
		"bad timeout":     {"--role", "reader", "--prompt", e.prompt, "--timeout", "soon"},
		"cwd not a dir":   {"--role", "reader", "--prompt", e.prompt, "--cwd", e.prompt},
		"positional args": {"--role", "reader", "--prompt", e.prompt, "extra"},
	}
	for name, args := range cases {
		if err := e.a.delegate(context.Background(), args); code(err) != exitUsage {
			t.Errorf("%s: want exit %d, got %v", name, exitUsage, err)
		}
	}
	t.Setenv("HK_TASK_ID", "")
	if err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "reader")); code(err) != exitUsage {
		t.Errorf("no job or task to record the delegation under must refuse: %v", err)
	}
	e.a.Config.Local = map[string]any{}
	t.Setenv("HK_TASK_ID", "42")
	if err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "reader")); code(err) != exitUsage {
		t.Errorf("pane mode without a workspace must refuse: %v", err)
	}
}

func TestDelegateHeadlessRunsOneBoundedCLICall(t *testing.T) {
	e := newDelegateEnv(t)
	a := e.a
	before := snapshot(t, e.cwd)
	a.spawnFn = func(context.Context, *App, Spawn) error { t.Fatal("--headless must not spawn a pane"); return nil }
	if err := a.delegate(context.Background(), delegateArgs(e, "--role", "reader", "--headless", "--timeout", "2m")); err != nil {
		t.Fatal(err)
	}
	calls := e.runner.named("devin")
	if len(calls) != 1 {
		t.Fatalf("exactly one CLI call, got %d", len(calls))
	}
	c := calls[0]
	if c.Dir != e.cwd || !slices.Contains(c.Args, "swe-2") || !slices.Contains(c.Args, "-p") {
		t.Fatalf("the selected seat's CLI runs in --cwd with its model and one-shot flag: %+v", c)
	}
	if last := c.Args[len(c.Args)-1]; !strings.Contains(last, "Where is the retry loop?") || !strings.Contains(last, "You read.") {
		t.Fatalf("the brief goes in as the prompt: %q", last)
	}
	idx := -1
	for i, call := range e.runner.calls {
		if call.Name == "devin" {
			idx = i
		}
	}
	if d := e.runner.remaining[idx]; d <= 0 || d > 2*time.Minute {
		t.Fatalf("the CLI call must run under the --timeout deadline, remaining=%v", d)
	}
	lines := strings.Split(strings.TrimSpace(e.out()), "\n")
	if lines[len(lines)-1] != "The retry loop is in pkg/util.go." || !strings.Contains(lines[0], "mode=headless") {
		t.Fatalf("%q", e.out())
	}
	if !equalMaps(before, snapshot(t, e.cwd)) {
		t.Fatal("headless delegate changed --cwd")
	}
	rec := a.JobStore.(*fakeJobStore).record(t, "delegate_test-host_1")
	res, _ := rec["result"].(json.RawMessage)
	if rec["state"] != "completed" || !strings.Contains(string(res), "retry loop") || rec["profile"] != "devin-swe2" {
		t.Fatalf("the headless job completes its hk record with the result: %v", rec)
	}
	pools, err := a.ledger().QuotaPools(context.Background(), "delegate_test-host_1")
	if err != nil || len(pools) != 0 {
		t.Fatalf("done must release the quota lease: %v %v", pools, err)
	}
}

func TestDelegateHeadlessTimeoutAndFailureLoseTheJob(t *testing.T) {
	for _, rc := range []int{exitTimeout, 3} {
		e := newDelegateEnv(t)
		a := e.a
		base := e.runner.do
		e.runner.do = func(c Command) Result {
			if c.Name == "devin" {
				return Result{Code: rc, Err: "boom"}
			}
			return base(c)
		}
		err := a.delegate(context.Background(), delegateArgs(e, "--role", "reader", "--headless"))
		if code(err) != rc {
			t.Fatalf("exit %d must pass through: %v", rc, err)
		}
		if strings.TrimSpace(e.out()) != "" {
			t.Fatalf("a failed delegation prints no answer: %q", e.out())
		}
		if st := a.JobStore.(*fakeJobStore).record(t, "delegate_test-host_1")["state"]; st != "lost" {
			t.Fatalf("the failed job's record must be lost, got %v", st)
		}
		if pools, _ := a.ledger().QuotaPools(context.Background(), "delegate_test-host_1"); len(pools) != 0 {
			t.Fatalf("the lease must be released on failure: %v", pools)
		}
	}
}

func TestDelegateHeadlessKindWithoutOneShotRefusesBeforeClaiming(t *testing.T) {
	e := newDelegateEnv(t)
	q := e.a.Quota.(rowQuota).fakeQuota
	q.CatalogRows = []CatalogRow{{Profile: "kiro-glm", Effort: "high", Grade: "C", Pool: "claude", Gate: "default", ModelID: "glm"}}
	q.GateDo = gatePools(map[string]string{"kiro-glm": "claude"})
	e.routingBody = routingDoc(strings.ReplaceAll(routingBlockV1, `"sonnet", "haiku", "codex-luna", "devin-swe2-medium", "agy-flash38-high"`, `"kiro-glm"`))
	err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "reader", "--headless"))
	if code(err) != exitConfig || !strings.Contains(err.Error(), "no headless form") || len(e.a.JobStore.(*fakeJobStore).records) != 0 {
		t.Fatalf("an unsupported kind must be refused before any record is claimed: %v", err)
	}
}

func TestDelegateHeadlessReviewerExcludesCallerFamily(t *testing.T) {
	e := newDelegateEnv(t)
	base := e.runner.do
	e.runner.do = func(c Command) Result {
		if c.Name == "claude" {
			return Result{Out: "no findings\n"}
		}
		return base(c)
	}
	if err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "reviewer", "--headless", "--family", "cognition")); err != nil {
		t.Fatalf("a reviewer claims its job like any worker seat: %v", err)
	}
	if len(e.runner.named("devin")) != 0 || len(e.runner.named("claude")) != 1 {
		t.Fatal("the reviewer must not run on the caller's own family")
	}
	if st := e.a.JobStore.(*fakeJobStore).record(t, "delegate_test-host_1"); st["role"] != "reviewer" || st["state"] != "completed" {
		t.Fatalf("%v", st)
	}
}

func TestDelegateHeadlessInsideJobRecordsParent(t *testing.T) {
	e := newDelegateEnv(t)
	a := e.a
	putEvent(t, a, "task42-builder", "job.claim", map[string]any{"owner_lane": "lane-a", "task_id": "42"})
	putEvent(t, a, "task42-builder", "job.spawned", map[string]any{"pane_id": "w:p9", "label": "task42-builder", "profile": "sonnet", "family": "anthropic", "task_id": "42"})
	t.Setenv("ARBITER_JOB", "task42-builder")
	if err := a.delegate(context.Background(), delegateArgs(e, "--role", "reader", "--headless")); err != nil {
		t.Fatal(err)
	}
	rec := a.JobStore.(*fakeJobStore).record(t, "delegate-task42-builder-1")
	if rec["parent_job"] != "task42-builder" || rec["task_id"] != "42" || rec["owner_lane"] != "lane-a" {
		t.Fatalf("the delegation's job record must name the delegate id, parent job, task and owner lane: %v", rec)
	}
	selection, selectionOK := rec["selection"].(map[string]any)
	host, hostOK := selection["host"].(map[string]any)
	if !selectionOK || !hostOK || host["source"] != "delegate" {
		t.Fatalf("delegate spawned record must retain its selection: %v", rec["selection"])
	}
	j, err := a.job("delegate-task42-builder-1")
	if err != nil || j.Owner != "lane-a" || !j.Terminal || j.Task != "42" {
		t.Fatalf("local job record: %+v %v", j, err)
	}
	if e := os.Getenv("ARBITER_JOB"); e != "task42-builder" {
		t.Fatal("delegate must not change the caller's environment")
	}
}

// gitRunner sends git to the real binary (a throwaway repo in the test's temp
// dir) and everything else to the fake.
type gitRunner struct{ fake Runner }

func (g gitRunner) Run(ctx context.Context, c Command) Result {
	if c.Name == "git" {
		return ExecRunner{Timeout: time.Minute}.Run(ctx, c)
	}
	return g.fake.Run(ctx, c)
}

func gitIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(out)
}

func TestDelegatePatchRoleUsesTempWorktreeAndReturnsPatch(t *testing.T) {
	e := newDelegateEnv(t)
	a := e.a
	t.Setenv("TMPDIR", t.TempDir())
	gitIn(t, e.cwd, "init", "-q")
	gitIn(t, e.cwd, "add", "-A")
	gitIn(t, e.cwd, "commit", "-q", "-m", "init")
	// A tracked, uncommitted edit by the caller: the delegate must start from it.
	if err := os.WriteFile(filepath.Join(e.cwd, "main.go"), []byte("package main\n// wip\n"), 0600); err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, e.cwd)
	a.Runner = gitRunner{e.runner}
	var seen []Spawn
	var chosen []string
	var tree string
	var started string
	a.spawnFn = e.worker(t, "tried it; works", &seen, &chosen, func(s Spawn) {
		tree = s.CWD
		b, err := os.ReadFile(filepath.Join(tree, "main.go"))
		if err != nil {
			t.Fatal(err)
		}
		started = string(b)
		if err := os.WriteFile(filepath.Join(tree, "main.go"), []byte("package main\n// wip\nvar fixed = true\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(tree, "new.go"), []byte("package main\n"), 0600); err != nil {
			t.Fatal(err)
		}
	})
	if err := a.delegate(context.Background(), delegateArgs(e, "--role", "verifier", "--patch", "--json")); err != nil {
		t.Fatal(err)
	}
	var r DelegateResult
	if err := json.Unmarshal([]byte(e.out()), &r); err != nil {
		t.Fatal(err)
	}
	if tree == "" || tree == e.cwd || seen[0].Host != "local" {
		t.Fatalf("the delegate must work in a scratch tree on this host: tree=%q host=%q", tree, seen[0].Host)
	}
	if started != "package main\n// wip\n" {
		t.Fatalf("the scratch tree must start from the caller's uncommitted tracked state: %q", started)
	}
	if _, err := os.Stat(tree); !os.IsNotExist(err) {
		t.Fatalf("the temp worktree must be removed: %v", err)
	}
	if wt := gitIn(t, e.cwd, "worktree", "list", "--porcelain"); strings.Count(wt, "worktree ") != 1 {
		t.Fatalf("the worktree registration must be gone:\n%s", wt)
	}
	patch, err := os.ReadFile(r.Patch)
	if r.Patch == "" || err != nil {
		t.Fatalf("a patch path that exists must come back: %q %v", r.Patch, err)
	}
	if strings.Contains(string(patch), "+// wip") {
		t.Fatalf("the patch holds only the delegate's edits, not the caller's uncommitted ones:\n%s", patch)
	}
	if !strings.Contains(string(patch), "+var fixed = true") || !strings.Contains(string(patch), "new.go") {
		t.Fatalf("the patch must hold the delegate's edits:\n%s", patch)
	}
	if !equalMaps(before, snapshot(t, e.cwd)) {
		t.Fatal("the caller's tree must not change")
	}
	b, _ := os.ReadFile(filepath.Join(a.Jobs, r.Job, "brief.md"))
	if !strings.Contains(string(b), "throwaway checkout") || strings.Contains(string(b), "Run only the acceptance commands") {
		t.Fatalf("--patch swaps the read-only rule for the scratch-tree rule:\n%s", b)
	}
	// The patch applies cleanly to the caller's tree.
	gitIn(t, e.cwd, "apply", "--check", r.Patch)
}

func TestDelegatePatchWithoutChangesReturnsNoPatch(t *testing.T) {
	e := newDelegateEnv(t)
	t.Setenv("TMPDIR", t.TempDir())
	gitIn(t, e.cwd, "init", "-q")
	gitIn(t, e.cwd, "add", "-A")
	gitIn(t, e.cwd, "commit", "-q", "-m", "init")
	e.a.Runner = gitRunner{e.runner}
	var seen []Spawn
	var chosen []string
	e.a.spawnFn = e.worker(t, "nothing to change", &seen, &chosen, nil)
	if err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "reader", "--patch", "--json")); err != nil {
		t.Fatal(err)
	}
	var r DelegateResult
	if err := json.Unmarshal([]byte(e.out()), &r); err != nil || r.Patch != "" {
		t.Fatalf("no edits, no patch: %+v %v", r, err)
	}
}

func TestDelegatePatchOutsideGitRefuses(t *testing.T) {
	e := newDelegateEnv(t)
	t.Setenv("TMPDIR", t.TempDir())
	e.a.Runner = gitRunner{e.runner}
	e.a.spawnFn = func(context.Context, *App, Spawn) error { t.Fatal("must refuse before spawning"); return nil }
	if err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "reader", "--patch")); code(err) != exitUsage {
		t.Fatalf("%v", err)
	}
}

func TestSpawnGradeOverrideSkipsTaskMarker(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	useFakeHK(t).SetTask("42", `{"id":42,"title":"a task with no grade marker","state":"ready","refs":{}}`)
	if code(a.Spawn(context.Background(), s)) != exitSeatRefused {
		t.Fatal("without a grade marker or a stated grade the spawn must refuse")
	}
	s.Grade = "B"
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatalf("a stated grade stands in for the marker: %v", e)
	}
}

func TestDelegateDocumented(t *testing.T) {
	root := filepath.Join("..", "..")
	for _, f := range []string{"README.md", filepath.Join("docs", "config.md")} {
		b, err := os.ReadFile(filepath.Join(root, f))
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range []string{"panewire delegate", "Delegation"} {
			if !strings.Contains(string(b), w) {
				t.Errorf("%s must document %q", f, w)
			}
		}
	}
}

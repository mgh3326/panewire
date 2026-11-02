package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/mgh3326/panewire/internal/arbiter"
)

// fakeJobStore is an in-memory JobStore mirroring the hk write rules: a claim
// replays only an identical claim, and a taken id conflicts.
type fakeJobStore struct {
	mu      sync.Mutex
	records map[string]map[string]any
	calls   []string
	getErr  error
	putErr  error
	get404  int // remaining Get calls answered not_found (a lagging record)
}

func newFakeJobStore() *fakeJobStore { return &fakeJobStore{records: map[string]map[string]any{}} }

func (s *fakeJobStore) Claim(_ context.Context, jobID, taskID, host string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "claim "+jobID)
	if rec, ok := s.records[jobID]; ok {
		if scalar(rec["task_id"]) == taskID && str(rec, "host") == host {
			return nil
		}
		return errJobTaken
	}
	s.records[jobID] = map[string]any{"job_id": jobID, "task_id": taskID, "host": host, "state": "claimed"}
	return nil
}
func (s *fakeJobStore) Put(_ context.Context, jobID string, fields map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "put "+jobID)
	if s.putErr != nil {
		return s.putErr
	}
	rec, ok := s.records[jobID]
	if !ok {
		return errJobNotFound
	}
	for k, v := range fields {
		rec[k] = v
	}
	return nil
}
func (s *fakeJobStore) Get(_ context.Context, jobID string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "get "+jobID)
	if s.getErr != nil {
		return nil, s.getErr
	}
	if s.get404 > 0 {
		s.get404--
		return nil, errJobNotFound
	}
	rec, ok := s.records[jobID]
	if !ok {
		return nil, errJobNotFound
	}
	out := map[string]any{}
	for k, v := range rec {
		out[k] = v
	}
	return out, nil
}

func (s *fakeJobStore) record(t *testing.T, jobID string) map[string]any {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[jobID]
	if !ok {
		t.Fatalf("no record for %s", jobID)
	}
	return rec
}

// fakeHK is a hermetic hk HTTP server implementing the /v1/jobs contract:
// claim replay and 409 job_id_taken, the PUT field allowlist and forward-only
// states, 404 not_found. calls records the request order for assertions
// against runner calls.
type fakeHK struct {
	srv     *httptest.Server
	mu      sync.Mutex
	records map[string]map[string]any
	calls   []string
	missing map[string]int // job → remaining 404s before its record appears
	down    bool
}

var jobRecordFields = []string{"state", "role", "label", "family", "profile", "model", "pool", "owner_lane", "parent_lane", "parent_job", "pane_id", "worktree", "report_doc", "result"}

func newFakeHK(t *testing.T) *fakeHK {
	t.Helper()
	h := &fakeHK{records: map[string]map[string]any{}, missing: map[string]int{}}
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.serve(w, r)
	}))
	t.Cleanup(h.srv.Close)
	return h
}

func (h *fakeHK) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (h *fakeHK) fail(w http.ResponseWriter, status int, code string) {
	h.writeJSON(w, status, map[string]any{"error": code})
}

func (h *fakeHK) serve(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, r.Method+" "+r.URL.Path)
	if h.down {
		h.fail(w, 500, "unavailable")
		return
	}
	if r.URL.Path == "/v1/jobs" && r.Method == http.MethodPost {
		var body struct {
			JobID  string `json:"job_id"`
			TaskID any    `json:"task_id"`
			Host   string `json:"host"`
		}
		if e := json.NewDecoder(r.Body).Decode(&body); e != nil || body.JobID == "" || body.Host == "" {
			h.fail(w, 400, "invalid_context")
			return
		}
		if rec, ok := h.records[body.JobID]; ok {
			if scalar(rec["task_id"]) == scalar(body.TaskID) && str(rec, "host") == body.Host {
				h.writeJSON(w, 200, rec)
				return
			}
			h.fail(w, 409, "job_id_taken")
			return
		}
		rec := map[string]any{"job_id": body.JobID, "task_id": body.TaskID, "host": body.Host, "state": "claimed"}
		h.records[body.JobID] = rec
		h.writeJSON(w, 201, rec)
		return
	}
	if id, ok := strings.CutPrefix(r.URL.Path, "/v1/jobs/"); ok && id != "" {
		switch r.Method {
		case http.MethodGet:
			if h.missing[id] > 0 {
				h.missing[id]--
				h.fail(w, 404, "not_found")
				return
			}
			rec, ok := h.records[id]
			if !ok {
				h.fail(w, 404, "not_found")
				return
			}
			h.writeJSON(w, 200, rec)
		case http.MethodPut:
			rec, ok := h.records[id]
			if !ok {
				h.fail(w, 404, "not_found")
				return
			}
			if slices.Contains([]string{"completed", "escalated", "lost"}, str(rec, "state")) {
				h.fail(w, 409, "job_state_conflict")
				return
			}
			var fields map[string]any
			if e := json.NewDecoder(r.Body).Decode(&fields); e != nil {
				h.fail(w, 400, "invalid_context")
				return
			}
			for k := range fields {
				if !slices.Contains(jobRecordFields, k) {
					h.fail(w, 400, "invalid_context")
					return
				}
			}
			if st := str(fields, "state"); st != "" && !slices.Contains([]string{"claimed", "spawned", "completed", "escalated", "lost"}, st) {
				h.fail(w, 400, "invalid_context")
				return
			}
			if str(fields, "state") == "completed" && str(rec, "state") != "spawned" {
				h.fail(w, 409, "job_state_conflict")
				return
			}
			for k, v := range fields {
				rec[k] = v
			}
			h.writeJSON(w, 200, rec)
		default:
			h.fail(w, 405, "invalid_context")
		}
		return
	}
	h.fail(w, 404, "not_found")
}

func (h *fakeHK) client() JobStore { return newHKJobs(&hkQuota{base: h.srv.URL, token: "test-token"}) }

func (h *fakeHK) record(t *testing.T, id string) map[string]any {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	rec, ok := h.records[id]
	if !ok {
		t.Fatalf("no hk record for %s", id)
	}
	return rec
}

// claimJob writes a claimed record straight into the fake, as a successful
// POST /v1/jobs would have.
func (h *fakeHK) claimJob(id string, taskID any, host string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records[id] = map[string]any{"job_id": id, "task_id": taskID, "host": host, "state": "claimed"}
}

func hkCalls(h *fakeHK) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string{}, h.calls...)
}

// seqStore wraps a JobStore and appends each call to a shared sequence so a
// test can assert cross-boundary order (claim before arbiter, PUT before
// relay emit).
type seqStore struct {
	JobStore
	seq  *[]string
	name string
}

func (s seqStore) Claim(ctx context.Context, jobID, taskID, host string) error {
	*s.seq = append(*s.seq, s.name+" claim")
	return s.JobStore.Claim(ctx, jobID, taskID, host)
}
func (s seqStore) Put(ctx context.Context, jobID string, fields map[string]any) error {
	*s.seq = append(*s.seq, s.name+" put")
	return s.JobStore.Put(ctx, jobID, fields)
}
func (s seqStore) Get(ctx context.Context, jobID string) (map[string]any, error) {
	*s.seq = append(*s.seq, s.name+" get")
	return s.JobStore.Get(ctx, jobID)
}

// probeStore wraps a JobStore and records, at each call, whether the job
// already exists in the local ledger, so a test can assert cross-boundary
// order (hk claim before the local claim).
type probeStore struct {
	JobStore
	a      *App
	job    string
	ledger *[]string
}

func (p probeStore) mark(op string) {
	state := "ledger-empty"
	if _, e := p.a.ledger().JobGet(context.Background(), p.job); e == nil {
		state = "ledger-claimed"
	}
	*p.ledger = append(*p.ledger, "hk "+op+" "+state)
}
func (p probeStore) Claim(ctx context.Context, jobID, taskID, host string) error {
	p.mark("claim")
	return p.JobStore.Claim(ctx, jobID, taskID, host)
}
func (p probeStore) Put(ctx context.Context, jobID string, fields map[string]any) error {
	p.mark("put")
	return p.JobStore.Put(ctx, jobID, fields)
}
func (p probeStore) Get(ctx context.Context, jobID string) (map[string]any, error) {
	p.mark("get")
	return p.JobStore.Get(ctx, jobID)
}

// Acceptance 1: the hk claim is issued on the executing host before the
// local claim, and a taken id refuses the spawn with exit 74.
func TestJobClaimPrecedesLocalClaim(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	h := newFakeHK(t)
	var seq []string
	a.JobStore = probeStore{JobStore: h.client(), a: a, job: s.Job, ledger: &seq}
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	if len(seq) < 2 || seq[0] != "hk claim ledger-empty" || seq[1] != "hk get ledger-empty" {
		t.Fatalf("the hk claim must complete before the local claim: %v", seq)
	}
	if !slices.Contains(seq, "hk put ledger-claimed") {
		t.Fatalf("the spawned record is written after the local claim: %v", seq)
	}
	rec := h.record(t, s.Job)
	if str(rec, "host") != "test-host" || scalar(rec["task_id"]) != "42" {
		t.Fatalf("claim record wrong: %v", rec)
	}
}

func TestJobClaimTakenExits74BeforeLocalClaim(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	h := newFakeHK(t)
	h.claimJob(s.Job, float64(7), "other-host") // a different task and host took the id
	a.JobStore = h.client()
	if code(a.Spawn(context.Background(), s)) != 74 {
		t.Fatal("job_id_taken must exit 74")
	}
	if _, e := a.ledger().JobGet(context.Background(), s.Job); e == nil {
		t.Fatal("local claim ran after a refused hk claim")
	}
}

// Acceptance 2: the spawned PUT body carries only allowlisted keys and no
// absolute local path.
func TestSpawnedRecordAllowlistOnly(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	h := newFakeHK(t)
	a.JobStore = h.client()
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	rec := h.record(t, s.Job)
	if str(rec, "state") != "spawned" {
		t.Fatalf("state=%v", rec["state"])
	}
	allowed := append(append([]string{}, jobRecordFields...), "job_id", "task_id", "host")
	for k, v := range rec {
		if !slices.Contains(allowed, k) {
			t.Fatalf("non-allowlisted key %q in record", k)
		}
		b, _ := json.Marshal(v)
		if strings.Contains(string(b), a.Home) || strings.Contains(string(b), s.CWD) {
			t.Fatalf("absolute path leaked in %s=%s", k, b)
		}
	}
	for _, want := range []string{"role", "label", "family", "profile", "model", "pool", "owner_lane", "pane_id"} {
		if _, ok := rec[want]; !ok {
			t.Fatalf("missing spawned field %s", want)
		}
	}
	puts := 0
	for _, c := range hkCalls(h) {
		if strings.HasPrefix(c, "PUT /v1/jobs/") {
			puts++
		}
	}
	if puts != 1 {
		t.Fatalf("spawned PUTs=%d", puts)
	}
}

// Acceptance 3: done writes one completed PUT — with result when --result is
// given — and the relay emit is issued only after the PUT returns.
func TestDoneResultOnePutThenRelay(t *testing.T) {
	for _, withResult := range []bool{true, false} {
		a := testApp(t)
		f := ledgerRunner(t, a)
		h := newFakeHK(t)
		h.claimJob("j", float64(42), "test-host")
		h.records["j"]["state"] = "spawned"
		var seq []string
		a.JobStore = seqStore{JobStore: h.client(), seq: &seq, name: "hk"}
		seedJob(t, a, "j", "worker")
		origDo := f.Do
		f.Do = func(c Command) Result {
			if c.Name == "panewire" {
				seq = append(seq, "relay")
			}
			return origDo(c)
		}
		path := filepath.Join(a.Jobs, "j", "report.md")
		if e := os.WriteFile(path, []byte("done\n"), 0600); e != nil {
			t.Fatal(e)
		}
		args := []string{"j", "--report", path}
		if withResult {
			resultPath := filepath.Join(a.Jobs, "j", "findings.json")
			if e := os.WriteFile(resultPath, []byte(`{"findings":[{"severity":"low","path":"x.go"}]}`), 0600); e != nil {
				t.Fatal(e)
			}
			args = append(args, "--result", resultPath)
		}
		if e := a.lifecycle(context.Background(), "done", args); e != nil {
			t.Fatal(e)
		}
		puts := 0
		putAt, relayAt := -1, -1
		for i, c := range seq {
			if c == "hk put" {
				puts++
				putAt = i
			}
			if c == "relay" {
				relayAt = i
			}
		}
		if puts != 1 || relayAt < 0 {
			t.Fatalf("withResult=%t puts=%d relays=%d seq=%v", withResult, puts, relayAt, seq)
		}
		if putAt > relayAt {
			t.Fatalf("relay emitted before the record PUT: %v", seq)
		}
		rec := h.record(t, "j")
		if str(rec, "state") != "completed" || str(rec, "report_doc") == "" {
			t.Fatalf("record=%v", rec)
		}
		if withResult {
			res, ok := rec["result"].(map[string]any)
			if !ok || len(res["findings"].([]any)) != 1 {
				t.Fatalf("result not stored: %v", rec["result"])
			}
		} else if _, ok := rec["result"]; ok {
			t.Fatal("result written without --result")
		}
	}
}

func TestDoneRejectsNonObjectResult(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	seedJob(t, a, "j", "worker")
	path := filepath.Join(a.Jobs, "j", "report.md")
	if e := os.WriteFile(path, []byte("done\n"), 0600); e != nil {
		t.Fatal(e)
	}
	bad := filepath.Join(a.Jobs, "j", "findings.json")
	if e := os.WriteFile(bad, []byte(`["not","object"]`), 0600); e != nil {
		t.Fatal(e)
	}
	if code(a.lifecycle(context.Background(), "done", []string{"j", "--report", path, "--result", bad})) != 2 {
		t.Fatal("non-object result must refuse with exit 2")
	}
}

// A failed record write degrades to a warning and job.record_failed; the
// relay still goes out so a waiter falls back per the design.
func TestDoneRecordWriteFailureStillRelays(t *testing.T) {
	a := testApp(t)
	f := ledgerRunner(t, a)
	h := newFakeHK(t)
	h.down = true
	a.JobStore = h.client()
	seedJob(t, a, "j", "worker")
	emits := 0
	origDo := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "panewire" {
			emits++
		}
		return origDo(c)
	}
	path := filepath.Join(a.Jobs, "j", "report.md")
	if e := os.WriteFile(path, []byte("done\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := a.lifecycle(context.Background(), "done", []string{"j", "--report", path}); e != nil {
		t.Fatal(e)
	}
	if emits != 1 {
		t.Fatalf("emits=%d", emits)
	}
	evs, _ := a.events("j")
	found := false
	for _, ev := range evs {
		if ev.Kind == "job.record_failed" {
			found = true
		}
	}
	if !found {
		t.Fatal("job.record_failed event missing")
	}
}

// Acceptance 4: the watcher writes {state:"lost"} and escalation writes
// {state:"escalated"}.
func TestWatcherLostWritesJobRecord(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	h := newFakeHK(t)
	h.claimJob("j", float64(42), "test-host")
	h.records["j"]["state"] = "spawned"
	a.JobStore = h.client()
	seedJob(t, a, "j", "worker")
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if str(h.record(t, "j"), "state") != "lost" {
		t.Fatalf("record state=%v", h.record(t, "j")["state"])
	}
}

func TestEscalateWritesJobRecord(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	h := newFakeHK(t)
	h.claimJob("j", float64(42), "test-host")
	h.records["j"]["state"] = "spawned"
	a.JobStore = h.client()
	seedJob(t, a, "j", "worker")
	if e := a.lifecycle(context.Background(), "escalate", []string{"j", "--question", "which branch?"}); e != nil {
		t.Fatal(e)
	}
	if str(h.record(t, "j"), "state") != "escalated" {
		t.Fatalf("record state=%v", h.record(t, "j")["state"])
	}
}

// Acceptance 5: --parent-job resolves the local dir first, then hk with a
// bounded wait for a lagging record; hk down exits 75.
func TestParentResolveLocalFirst(t *testing.T) {
	a := testApp(t)
	h := newFakeHK(t)
	a.JobStore = h.client()
	putEvent(t, a, "pj", "job.claim", map[string]any{"owner_lane": "owner", "task_id": "11", "t_level": "T1", "role": "worker"})
	j, e := a.resolveParent(context.Background(), "pj")
	if e != nil {
		t.Fatal(e)
	}
	if j.Task != "11" {
		t.Fatalf("task=%q", j.Task)
	}
	for _, c := range hkCalls(h) {
		if strings.HasPrefix(c, "GET ") {
			t.Fatal("local parent hit hk")
		}
	}
}

func TestParentResolveFromHK(t *testing.T) {
	a := testApp(t)
	h := newFakeHK(t)
	h.claimJob("pj", float64(42), "test-host")
	h.records["pj"]["family"] = "anthropic"
	h.records["pj"]["owner_lane"] = "repo-session"
	a.JobStore = h.client()
	j, e := a.resolveParent(context.Background(), "pj")
	if e != nil {
		t.Fatal(e)
	}
	if j.Task != "42" || j.Family != "anthropic" || j.Owner != "repo-session" {
		t.Fatalf("%+v", j)
	}
}

func TestParentResolveLaggingRecord(t *testing.T) {
	a := testApp(t)
	h := newFakeHK(t)
	h.claimJob("pj", float64(42), "test-host")
	h.missing["pj"] = 3 // the record appears within the 30 s bound
	a.JobStore = h.client()
	j, e := a.resolveParent(context.Background(), "pj")
	if e != nil {
		t.Fatal(e)
	}
	if j.Task != "42" {
		t.Fatalf("%+v", j)
	}
	gets := 0
	for _, c := range hkCalls(h) {
		if strings.HasPrefix(c, "GET ") {
			gets++
		}
	}
	if gets != 4 {
		t.Fatalf("GETs=%d want 4 (3 misses then found)", gets)
	}
}

func TestParentResolveHKDownExits75(t *testing.T) {
	a := testApp(t)
	h := newFakeHK(t)
	h.down = true
	a.JobStore = h.client()
	if _, e := a.resolveParent(context.Background(), "pj"); e == nil {
		t.Fatal("hk down must not resolve")
	}
}

func TestSpawnParentJobHKDownExits75(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	h := newFakeHK(t)
	h.down = true
	a.JobStore = h.client()
	s.ParentJob = "pj"
	s.Task = ""
	s.Report = filepath.Join(a.Jobs, "job-a", "report.md")
	if code(a.Spawn(context.Background(), s)) != 75 {
		t.Fatal("unresolvable parent must exit 75")
	}
}

func TestSpawnParentJobResolvedOverHK(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	h := newFakeHK(t)
	h.claimJob("pj", float64(42), "test-host")
	h.records["pj"]["family"] = "anthropic"
	a.JobStore = h.client()
	s.ParentJob = "pj"
	s.Task = "" // comes from the parent record
	s.Report = filepath.Join(a.Jobs, "job-a", "report.md")
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	// The spawn succeeding proves the parent resolved over hk: the task
	// binding, owner and family all came from the record.
	rec := h.record(t, "job-a")
	if scalar(rec["task_id"]) != "42" {
		t.Fatalf("task_id=%v", rec["task_id"])
	}
	if str(rec, "parent_job") != "pj" {
		t.Fatalf("spawned record=%v", h.record(t, "job-a"))
	}
}

// Acceptance 6: --wait on a delegated job answers from the hk record alone
// (the W4 rule), each row with no local events and no lane inbox.
func TestWaitDelegatedHKRows(t *testing.T) {
	for _, tt := range []struct {
		name  string
		state string
		want  int
	}{{name: "completed with result", state: "completed", want: 0},
		{name: "completed no result", state: "completed", want: 0},
		{name: "escalated", state: "escalated", want: 83},
		{name: "lost", state: "lost", want: 84},
		{name: "still spawned", state: "spawned", want: 85},
		{name: "no record", state: "", want: 85}} {
		t.Run(tt.name, func(t *testing.T) {
			a := testApp(t)
			h := newFakeHK(t)
			a.JobStore = h.client()
			if tt.state != "" {
				h.claimJob("j", float64(42), "test-host")
				h.records["j"]["state"] = tt.state
				h.records["j"]["report_doc"] = "reports/j/r.md"
			}
			if tt.name == "completed with result" {
				h.records["j"]["result"] = map[string]any{"findings": []any{}}
			}
			s := Spawn{Job: "j", Role: "worker", WaitTimeout: "5s"}
			e := a.waitJob(context.Background(), s, true)
			if code(e) != tt.want {
				t.Fatalf("state %q: exit %d want %d (%v)", tt.state, code(e), tt.want, e)
			}
		})
	}
}

// A refused task id never reaches the wire as task_id=0.
func TestJobClaimRejectsNonNumericTaskBeforePost(t *testing.T) {
	h := newFakeHK(t)
	e := h.client().Claim(context.Background(), "j", "not-a-number", "test-host")
	if e == nil {
		t.Fatal("non-numeric task must refuse")
	}
	for _, c := range hkCalls(h) {
		if strings.HasPrefix(c, "POST ") {
			t.Fatal("a refused claim was still POSTed")
		}
	}
}

// The hk-unavailable warning has its own one-shot flag: a prior unrelated
// wait warning must not suppress it.
func TestWaitDelegatedHKWarnSurvivesEarlierWarnings(t *testing.T) {
	a := testApp(t)
	a.Config.Hub = map[string]any{"hub_url": "https://hub.invalid"}
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		return Result{Code: 1, Err: "inbox down"}
	}}
	h := newFakeHK(t)
	h.down = true
	a.JobStore = h.client()
	s := Spawn{Job: "j", Role: "worker", WaitTimeout: "5s"}
	if code(a.waitJob(context.Background(), s, true)) != 85 {
		t.Fatal("unreadable hk must bound and fall back")
	}
	errs := a.Err.(interface{ String() string }).String()
	if !strings.Contains(errs, "hk-unavailable") || !strings.Contains(errs, "lane inbox read failed") {
		t.Fatalf("warnings=%q", errs)
	}
}

func TestWaitDelegatedHKUnavailableWarnsOnce(t *testing.T) {
	a := testApp(t)
	h := newFakeHK(t)
	h.down = true
	a.JobStore = h.client()
	s := Spawn{Job: "j", Role: "worker", WaitTimeout: "5s"}
	if code(a.waitJob(context.Background(), s, true)) != 85 {
		t.Fatal("unreadable hk must bound and fall back")
	}
	if strings.Count(a.Err.(interface{ String() string }).String(), "hk-unavailable") != 1 {
		t.Fatalf("warns=%q", a.Err)
	}
}

// Unconfigured or unreachable hk fails the claim closed, as the task claim
// does.
func TestJobClaimFailsClosedWithoutHK(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	a.JobStore = failingJobStore{}
	if code(a.Spawn(context.Background(), s)) != 70 {
		t.Fatal("hk down must fail the spawn closed")
	}
}

func TestJobClaimFailsClosedWithoutNodeID(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	t.Setenv("PANEWIRE_NODE_ENV", filepath.Join(t.TempDir(), "missing.env"))
	if code(a.Spawn(context.Background(), s)) != 70 {
		t.Fatal("no node id must refuse the claim")
	}
}

type failingJobStore struct{}

func (failingJobStore) Claim(context.Context, string, string, string) error {
	return errors.New("handoffkeep request failed")
}
func (failingJobStore) Put(context.Context, string, map[string]any) error {
	return errors.New("handoffkeep request failed")
}
func (failingJobStore) Get(context.Context, string) (map[string]any, error) {
	return nil, errors.New("handoffkeep request failed")
}

// An identical claim replayed onto a finished record is a used id, not a
// fresh claim: refuse with 74 and never reach the local ledger.
func TestJobClaimReplayOnTerminalRecordExits74(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	store := newFakeJobStore()
	store.records[s.Job] = map[string]any{"job_id": s.Job, "task_id": s.Task, "host": "test-host", "state": "completed", "result": map[string]any{"findings": []any{}}}
	a.JobStore = store
	if code(a.Spawn(context.Background(), s)) != 74 {
		t.Fatal("replay onto a completed record must exit 74")
	}
	if _, e := a.ledger().JobGet(context.Background(), s.Job); e == nil {
		t.Fatal("local claim ran after a used id")
	}
	if st := str(store.records[s.Job], "state"); st != "completed" {
		t.Fatalf("finished record touched: %s", st)
	}
}

// A verified pre-launch abort preserves the identical claim for a safe retry.
func TestSpawnAbortAfterClaimMarksRetryable(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	store := newFakeJobStore()
	a.JobStore = store
	// A ledger path under a regular file cannot be created: the local claim fails.
	a.Arbiter.DB = filepath.Join(a.Home, "node.env", "arbiter", "state.db")
	if a.Spawn(context.Background(), s) == nil {
		t.Fatal("spawn must fail when the local claim fails")
	}
	if st := str(store.records[s.Job], "state"); st != "claimed" {
		t.Fatalf("aborted claim state = %q, want claimed for retry", st)
	}
}

// claimLocal registers the spawn's job in the ledger as an earlier spawn would.
func claimLocal(t *testing.T, a *App, s Spawn) {
	t.Helper()
	if _, e := a.ledger().Claim(context.Background(), arbiter.Claim{Job: s.Job, Lane: "x", T: "T1", AgentLabel: "y"}); e != nil {
		t.Fatal(e)
	}
}

// releaseLocal leaves the job released, as done/reap do: quota record, then release.
func releaseLocal(t *testing.T, a *App, s Spawn) {
	t.Helper()
	if _, e := a.ledger().RecordQuotaPool(context.Background(), arbiter.QuotaRun{Job: s.Job, Profile: "p", Gate: arbiter.Gate{Pool: "example"}}); e != nil {
		t.Fatal(e)
	}
	if e := a.ledger().ReleaseQuotaPool(context.Background(), s.Job, "example", false); e != nil {
		t.Fatal(e)
	}
}

// A concurrent duplicate that loses at the local claim leaves the live spawn's
// record alone.
func TestSpawnDuplicateAtLedgerKeepsRecord(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	store := newFakeJobStore()
	store.records[s.Job] = map[string]any{"job_id": s.Job, "task_id": s.Task, "host": "test-host", "state": "claimed"}
	a.JobStore = store
	claimLocal(t, a, s)
	if code(a.Spawn(context.Background(), s)) != 74 {
		t.Fatal("a duplicate must exit 74")
	}
	if st := str(store.records[s.Job], "state"); st != "claimed" {
		t.Fatalf("duplicate rewrote the live record to %q", st)
	}
}

// A released job has no receipt (or a stale one); either way the same job id
// is reclaimed in place and its history kept.
func TestSpawnReclaimsReleasedJob(t *testing.T) {
	for name, withReceipt := range map[string]bool{"no receipt": false, "stale receipt": true} {
		t.Run(name, func(t *testing.T) {
			a, s, _, _ := cmdFixture(t)
			claimLocal(t, a, s)
			if withReceipt {
				if _, e := a.ledger().AppendEvent(context.Background(), s.Job, "job.spawned", map[string]any{"pane_id": "old:p"}, nil); e != nil {
					t.Fatal(e)
				}
			}
			releaseLocal(t, a, s)
			if e := a.Spawn(context.Background(), s); e != nil {
				t.Fatalf("released job not reclaimed: %v", e)
			}
			kinds := eventKindsOf(t, a, s.Job)
			if !slices.Contains(kinds, "job.reclaim") || slices.Index(kinds, "job.claim") != 0 {
				t.Fatalf("no reclaim over the original claim: %v", kinds)
			}
			if st, pools := ledgerState(t, a, s.Job); st != "leased" || len(pools) != 1 {
				t.Fatalf("after reclaim: %s %v", st, pools)
			}
		})
	}
}

// A duplicate of a live job with a receipt is refused (74); a receipt the
// ledger cannot read fails closed with the unreadable-state error (75) and
// never reclaims.
func TestSpawnDuplicateReceiptShapes(t *testing.T) {
	for name, tc := range map[string]struct {
		payload map[string]any
		rc      int
	}{
		"valid receipt": {map[string]any{"pane_id": "w:p", "label": "y"}, exitDuplicate},
		"no pane id":    {map[string]any{"label": "y"}, exitStateConflict},
		"empty pane id": {map[string]any{"pane_id": ""}, exitStateConflict},
		"label number":  {map[string]any{"pane_id": "w:p", "label": 7}, exitStateConflict},
	} {
		t.Run(name, func(t *testing.T) {
			a, s, _, _ := cmdFixture(t)
			claimLocal(t, a, s)
			if _, e := a.ledger().AppendEvent(context.Background(), s.Job, "job.spawned", tc.payload, nil); e != nil {
				t.Fatal(e)
			}
			e := a.Spawn(context.Background(), s)
			if got := code(e); got != tc.rc {
				t.Fatalf("rc = %d (%v), want %d", got, e, tc.rc)
			}
			if tc.rc == exitStateConflict && !strings.Contains(e.Error(), "duplicate job state unreadable") {
				t.Fatalf("error = %v, want the existing unreadable-state error", e)
			}
			if st, _ := a.ledger().Status(context.Background(), s.Job, ""); len(st.Jobs) != 1 || st.Jobs[0].State != "claimed" {
				t.Fatalf("a refused duplicate changed the job: %+v", st.Jobs)
			}
		})
	}
}

func TestDurableReadsReceiptFromLedger(t *testing.T) {
	a := testApp(t)
	ctx := context.Background()
	if _, e := a.ledger().Claim(ctx, arbiter.Claim{Job: "j", Lane: "l", T: "T1"}); e != nil {
		t.Fatal(e)
	}
	d, e := a.durable(ctx, "j")
	if e != nil || d.State != "claimed" || d.Receipt != nil || d.Label != "j" {
		t.Fatalf("%+v %v", d, e)
	}
	if _, e = a.ledger().AppendEvent(ctx, "j", "job.spawned", map[string]any{"pane_id": "w:p", "label": "lab"}, nil); e != nil {
		t.Fatal(e)
	}
	d, e = a.durable(ctx, "j")
	if e != nil || str(d.Receipt, "pane_id") != "w:p" || str(d.Receipt, "label") != "lab" {
		t.Fatalf("%+v %v", d, e)
	}
	if _, e = a.durable(ctx, "nope"); code(e) != arbiter.ExitNotFound {
		t.Fatalf("unknown job: %v", e)
	}
}

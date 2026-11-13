package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
	"github.com/mgh3326/panewire/internal/quotastop"
)

func TestCreditExhaustedExcludedFromSelection(t *testing.T) {
	a, s := creditFixture(t, balance(0, 24*time.Hour), riskPool(20, 100), "", nil)
	plan, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	row := candOutcome(plan.record(), "omp-opus", "high")
	if row["outcome"] != "dropped:credit_exhausted" {
		t.Fatalf("zero credit outcome = %v, want dropped:credit_exhausted", row)
	}
}

func TestOmpCreditRejectionStopsJobButQuotedAnswerDoesNot(t *testing.T) {
	for _, tc := range []struct {
		name, message string
		want          bool
	}{
		{"rejection", `{"role":"assistant","stopReason":"error","errorStatus":400,"provider":"anthropic","completedAt":1794461272051,"content":[],"errorMessage":"400 {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"Your credit balance is too low to access the Anthropic API\"}}"}`, true},
		{"generic provider error type", `{ "role":"assistant", "stopReason":"error", "errorStatus":400, "provider":"anthropic", "completedAt":1794461272051, "errorMessage":"400 {\"error\":{\"type\":\"billing_error\",\"message\":\"Your credit balance is too low to access the Anthropic API\"}}" }`, true},
		{"quoted answer", `{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"API Error: 400 Your credit balance is too low to access the Anthropic API"}]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := testApp(t)
			dir := filepath.Join(a.Home, ".omp", "agent", "sessions", "test")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "session.jsonl")
			content := "{\"type\":\"session\",\"cwd\":\"/tmp/project\"}\n{\"type\":\"message\",\"message\":" + tc.message + "}\n"
			if err := os.WriteFile(path, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			job := Job{Pane: "p", SpawnRole: "worker", Receipt: map[string]any{"pool": "anthropic-api", "cwd": "/tmp/project", "harness_session": map[string]any{"kind": "omp", "id": path}}}
			stop, ok := a.quotaStopped(context.Background(), job, Agent{Status: "idle"}, &quotaStopWatch{})
			if ok != tc.want || ok && stop.Pool != "anthropic-api" {
				t.Fatalf("stop = %+v, %t; want pool anthropic-api, %t", stop, ok, tc.want)
			}
		})
	}
}

func TestCreditRejectionPublishesBeforeQuotaStop(t *testing.T) {
	hk := &hkBenchFake{}
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/observation") && attempts.Add(1) == 1 {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		hk.ServeHTTP(w, r)
	}))
	defer srv.Close()
	a, _, jobs, report := quotaStopFixture(t, "worker", "idle", "")
	if err := os.WriteFile(report, []byte("partial report"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HANDOFFKEEP_URL", "https://hk.test.invalid")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-token")
	a.Config.Quota = map[string]any{"url": srv.URL, "accounts": map[string]any{"anthropic-api": "0123456789abcdef"}}
	dir := filepath.Join(a.Home, ".omp", "agent", "sessions", "test")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "session.jsonl")
	rejected := a.Now()
	message := `{"type":"session","cwd":"/tmp/project"}` + "\n" + fmt.Sprintf(`{"type":"message","message":{"role":"assistant","stopReason":"error","errorStatus":400,"provider":"anthropic","completedAt":%d,"content":[],"errorMessage":"400 {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"Your credit balance is too low to access the Anthropic API\"}}"}}`, a.Now().UnixMilli()) + "\n"
	if err := os.WriteFile(path, []byte(message), 0600); err != nil {
		t.Fatal(err)
	}
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "role": "worker", "pool": "anthropic-api", "cwd": "/tmp/project", "harness_session": map[string]any{"kind": "omp", "id": path}})
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 2 || len(hk.manual) != 1 || len(hk.manual[0].Buckets) != 1 || hk.manual[0].Buckets[0].UsedPct != 100 || !hk.manual[0].MeasuredAt.Equal(rejected) {
		t.Fatalf("shared observations = %+v", hk.manual)
	}
	if got := str(jobs.record(t, "j"), "end_reason"); got != "quota_stop pool=anthropic-api" {
		t.Fatalf("end_reason = %q, want quota_stop pool=anthropic-api", got)
	}
	if p := lostPayload(t, a); p == nil || str(p, "reason") != "quota_stop" {
		t.Fatalf("quota stop event = %v", p)
	}
}

func TestCreditPlanShowsExhaustionTime(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a, _ := creditFixture(t, func(now time.Time) *CreditBalance {
		return &CreditBalance{LeftPct: 0, MeasuredAt: at, ExpiresAt: now.Add(time.Hour)}
	}, riskPool(20, 100), "", nil)
	var output bytes.Buffer
	a.Out = &output
	_ = a.planCmd(context.Background(), []string{"--grade", "A", "--t", "T1"})
	if !strings.Contains(output.String(), "reason=credit_exhausted since=2026-01-01T00:00:00Z") {
		t.Fatalf("plan missing closure and time: %s", output.String())
	}
}

func TestCreditClosureSurvivesHubFailureAndReopensOnNewEntry(t *testing.T) {
	a, s := creditBalanceSelectionApp(t, nil)
	rejected := a.Now().Add(-time.Hour)
	expiry := a.Now().Add(24 * time.Hour)
	current := shared.Observation{Kind: "manual", Window: "credit", MeasuredAt: rejected, ExpiresAt: &expiry, Buckets: []shared.Bucket{{Window: "credit", UsedPct: 100, Scope: "account"}}}
	a.SharedLatest = func(context.Context, string) (shared.Latest, error) {
		return shared.Latest{Manual: []shared.Observation{current}}, nil
	}
	a.State = &fakeStateReader{err: errors.New("hub restarted")}
	for i := 0; i < 2; i++ {
		a.fleetLog = nil
		sel, err := a.selectProfile(context.Background(), s)
		if err != nil {
			t.Fatal(err)
		}
		row := candOutcome(sel.record(), "omp-luna", "high")
		if row["outcome"] != "dropped:credit_exhausted" || row["since"] != rejected.Format(time.RFC3339Nano) {
			t.Fatalf("closed pool: %v", row)
		}
	}
	current.MeasuredAt = rejected.Add(time.Minute)
	current.Buckets[0].UsedPct = 30
	a.fleetLog = nil
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if row := candOutcome(sel.record(), "omp-luna", "high"); row["outcome"] == "dropped:credit_exhausted" {
		t.Fatalf("new credit did not reopen pool: %v", row)
	}
}

// orderedCreditStore models hk's measured_at conditional latest pointer.
type orderedCreditStore struct {
	latest *shared.Observation
}

func (s *orderedCreditStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/lease"):
		var req struct {
			Holder string `json:"holder"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		writeJSONBody(w, http.StatusOK, map[string]any{"holder": req.Holder, "acquired": true, "expires_at": "2099-01-01T00:00:00Z"})
	case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/lease"):
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/observation"):
		var req struct {
			Observation shared.Observation `json:"observation"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid", 400)
			return
		}
		if s.latest == nil || !req.Observation.MeasuredAt.Before(s.latest.MeasuredAt) {
			s.latest = &req.Observation
		}
		writeJSONBody(w, http.StatusCreated, req.Observation)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/latest"):
		manual := []shared.Observation{}
		if s.latest != nil {
			manual = append(manual, *s.latest)
		}
		writeJSONBody(w, http.StatusOK, shared.Latest{Manual: manual})
	default:
		http.Error(w, "unexpected", 500)
	}
}

func TestCreditReplayDoesNotUndoNewManualEntry(t *testing.T) {
	store := &orderedCreditStore{}
	srv := httptest.NewServer(store)
	defer srv.Close()
	a := benchFakeApp(t, srv)
	at := a.Now().Add(-time.Hour)
	stop := quotastop.Stop{Pool: "anthropic-api", CreditExhaustedAt: at}
	job := Job{}
	if err := a.publishCreditRejection(context.Background(), job, stop); err != nil {
		t.Fatal(err)
	}
	if store.latest == nil || store.latest.Buckets[0].UsedPct != 100 {
		t.Fatalf("closure = %+v", store.latest)
	}
	manual := ManualObservation{Pool: "anthropic-api", Window: "credit", Reason: "top up", MeasuredAt: at.Add(time.Minute).Format(time.RFC3339Nano), Credit: &ManualCredit{RemainingUSD: 50, TotalUSD: 100, ExpiresOn: a.Now().Add(24 * time.Hour).Format("2006-01-02")}}
	if _, err := a.addManual(context.Background(), manual, 0, a.Now()); err != nil {
		t.Fatal(err)
	}
	if err := a.publishCreditRejection(context.Background(), job, stop); err != nil {
		t.Fatal(err)
	}
	if store.latest == nil || store.latest.Buckets[0].UsedPct != 50 {
		t.Fatalf("old rejection replaced new credit: %+v", store.latest)
	}
}

func TestOmpCreditOpenAIProviderError(t *testing.T) {
	a := testApp(t)
	dir := filepath.Join(a.Home, ".omp", "agent", "sessions", "test")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "session.jsonl")
	header := "{\"type\":\"title\",\"v\":1}\n{\"type\":\"session\",\"cwd\":\"/tmp/project\"}\n"
	rejection := `{"type":"message","timestamp":"2026-01-01T00:00:00Z","message":{"role":"assistant","stopReason":"error","errorStatus":429,"provider":"openai","errorMessage":"429 {\"error\":{\"code\":\"insufficient_quota\",\"message\":\"Quota exceeded\"}}"}}` + "\n"
	job := Job{Receipt: map[string]any{"pool": "openai-api", "cwd": "/tmp/project", "harness_session": map[string]any{"kind": "omp", "id": path}}}
	for _, tc := range []struct {
		name, suffix string
		want         bool
	}{
		{"structured rejection", "", true},
		{"later assistant answer", `{"type":"message","message":{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"insufficient_quota"}]}}` + "\n", false},
		{"later incomplete record", `{"type":"message"`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(header+rejection+tc.suffix), 0600); err != nil {
				t.Fatal(err)
			}
			stop, ok := ompCreditStop(a.Home, job)
			if ok != tc.want {
				t.Fatalf("stop = %+v, %t; want %t", stop, ok, tc.want)
			}
			if ok && (!stop.CreditExhaustedAt.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) || stop.Pool != "openai-api") {
				t.Fatalf("wrong rejection: %+v", stop)
			}
		})
	}
}

func TestOmpCreditSessionID(t *testing.T) {
	a := testApp(t)
	dir := filepath.Join(a.Home, ".omp", "agent", "sessions", "test")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	data := `{"type":"title","v":1}` + "\n" + `{"type":"session","id":"session-123","cwd":"/tmp/project"}` + "\n" + `{"type":"message","timestamp":"2026-01-01T00:00:00Z","message":{"role":"assistant","stopReason":"error","errorStatus":429,"provider":"openai","errorMessage":"429 {\"error\":{\"code\":\"insufficient_quota\"}}"}}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, "one.jsonl"), []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	job := Job{Receipt: map[string]any{"pool": "openai-api", "cwd": "/tmp/project", "harness_session": map[string]any{"kind": "omp", "id": "session-123"}}}
	if _, ok := ompCreditStop(a.Home, job); !ok {
		t.Fatal("session ID did not find its unique rejection")
	}
	if err := os.WriteFile(filepath.Join(dir, "two.jsonl"), []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	if _, ok := ompCreditStop(a.Home, job); ok {
		t.Fatal("ambiguous session ID produced a stop")
	}
}

func creditWatchSession(t *testing.T, a *App, role string) {
	t.Helper()
	dir := filepath.Join(a.Home, ".omp", "agent", "sessions", "test")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "session.jsonl")
	data := `{"type":"session","cwd":"/tmp/project"}` + "\n" + fmt.Sprintf(`{"type":"message","message":{"role":"assistant","stopReason":"error","errorStatus":400,"provider":"anthropic","completedAt":%d,"errorMessage":"400 {\"error\":{\"type\":\"invalid_request_error\",\"message\":\"Your credit balance is too low\"}}"}}`, a.Now().UnixMilli()) + "\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "role": role, "pool": "anthropic-api", "cwd": "/tmp/project", "harness_session": map[string]any{"kind": "omp", "id": path}})
}

func TestCreditPublicationFailureIsBoundedAndRearmed(t *testing.T) {
	hk := &hkBenchFake{}
	var fail atomic.Bool
	fail.Store(true)
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/observation") {
			attempts.Add(1)
			if fail.Load() {
				http.Error(w, "unavailable", 503)
				return
			}
		}
		hk.ServeHTTP(w, r)
	}))
	defer srv.Close()
	a, _, jobs, _ := quotaStopFixture(t, "worker", "idle", "")
	t.Setenv("HANDOFFKEEP_URL", "https://hk.test.invalid")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-token")
	a.Config.Quota = map[string]any{"url": srv.URL, "accounts": map[string]any{"anthropic-api": "0123456789abcdef"}}
	creditWatchSession(t, a, "worker")
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("publication attempts = %d, want 3", attempts.Load())
	}
	if got := str(jobs.record(t, "j"), "end_reason"); got != "quota_stop pool=anthropic-api" {
		t.Fatalf("end reason = %q", got)
	}
	job, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := pendingCreditRejection(job); !ok {
		t.Fatal("failed publication was not recorded for recovery")
	}
	fail.Store(false)
	started := 0
	if err := a.rearmWatchers(func(id string) error { started++; return a.watch(context.Background(), []string{id, "--rearmed"}) }, func(string) {}, true); err != nil {
		t.Fatal(err)
	}
	if started != 1 || len(hk.manual) != 1 {
		t.Fatalf("rearm started=%d, published=%d", started, len(hk.manual))
	}
	job, err = a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := pendingCreditRejection(job); ok {
		t.Fatal("published rejection remained pending")
	}
}

func TestCreditPermanentPublishFailureDoesNotRearmUntilAccountIsKnown(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "worker", "idle", "")
	creditWatchSession(t, a, "worker")
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}

	starts := 0
	for range 3 {
		if err := a.rearmWatchers(func(string) error {
			starts++
			return nil
		}, func(string) {}, true); err != nil {
			t.Fatal(err)
		}
	}
	if starts != 0 {
		t.Fatalf("pending permanent rejection rearmed %d times without an identifiable account", starts)
	}
}

func TestCreditCounselPublishesWithoutEndingJob(t *testing.T) {
	hk := &hkBenchFake{}
	srv := httptest.NewServer(hk)
	defer srv.Close()
	a, _, jobs, _ := quotaStopFixture(t, "counsel", "idle", "")
	t.Setenv("HANDOFFKEEP_URL", "https://hk.test.invalid")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-token")
	a.Config.Quota = map[string]any{"url": srv.URL, "accounts": map[string]any{"anthropic-api": "0123456789abcdef"}}
	creditWatchSession(t, a, "counsel")
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}
	if len(hk.manual) != 1 {
		t.Fatalf("published observations = %d, want 1", len(hk.manual))
	}
	if got := str(jobs.record(t, "j"), "state"); got != "spawned" {
		t.Fatalf("counsel job ended: %q", got)
	}
	if p := lostPayload(t, a); p != nil {
		t.Fatalf("counsel quota handoff = %v", p)
	}
}

func TestCreditHandOffRecognizesResumedPane(t *testing.T) {
	seq := int64(1)
	h := quotaHandOff{stop: quotastop.Stop{Pool: "anthropic-api", CreditExhaustedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}, seq: &seq, active: true}
	if h.unchanged(Agent{Status: "working", StateChangeSeq: &seq}) {
		t.Fatal("working pane considered unchanged")
	}
	next := int64(2)
	if h.unchanged(Agent{Status: "idle", StateChangeSeq: &next}) {
		t.Fatal("changed pane considered unchanged")
	}
}

func TestCreditSpawnReceiptKeepsAdmittedAccount(t *testing.T) {
	a, s, _ := ompSpawnFixture(t)
	const admitted = "anthropic-api:0123456789abcdef"
	q := a.Quota.(*fakeQuota)
	q.GateDo = func(QuotaRequest) (string, error) {
		return `{"schema":"scopefuel.gate.v1","provider_id":"anthropic-api","profile":"omp-opus","ok":true,"exit_code":0,"reading_account_key":"` + admitted + `"}`, nil
	}
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	job, err := a.job(s.Job)
	if err != nil {
		t.Fatal(err)
	}
	if got := str(job.Receipt, "account_key"); got != admitted {
		t.Fatalf("spawn receipt account = %q, want admitted %q", got, admitted)
	}
}

func TestCreditWatcherResumedDuringHandoff(t *testing.T) {
	hk := &hkBenchFake{}
	srv := httptest.NewServer(hk)
	defer srv.Close()
	a, runner, jobs, _ := quotaStopFixture(t, "worker", "idle", "")
	t.Setenv("HANDOFFKEEP_URL", "https://hk.test.invalid")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-token")
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "600")
	a.Config.Quota = map[string]any{"url": srv.URL, "accounts": map[string]any{"anthropic-api": "0123456789abcdef"}}
	a.QuotaHandOffGrace = 2 * time.Minute
	creditWatchSession(t, a, "worker")
	original := runner.Do
	lists := 0
	runner.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			lists++
			if lists > 1 {
				return Result{Out: `{"result":{"agents":[{"pane_id":"w:p1","agent_status":"working","state_change_seq":8}]}}`}
			}
		}
		return original(c)
	}
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}
	if got := str(jobs.record(t, "j"), "state"); got == "lost" {
		t.Fatal("resumed job ended lost")
	}
	if !slices.Contains(eventKindsOf(t, a, "j"), "job.reprompted") {
		t.Fatal("resumed job was not reprompted")
	}
}

func TestCreditPendingPublicationKeepsJobDirectory(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	putEvent(t, r.a, "j1", "job.credit_rejection", map[string]any{"pool": "anthropic-api", "at": r.a.Now().Format(time.RFC3339Nano)})
	r.age("j1")
	out := r.pass(true)
	wantKeep(t, r, out, "j1", "credit-publication-pending")
}

func TestCreditSpawnRejectsMismatchedAdmittedAccount(t *testing.T) {
	a, s, _ := ompSpawnFixture(t)
	q := a.Quota.(*fakeQuota)
	q.GateDo = func(QuotaRequest) (string, error) {
		return `{"schema":"scopefuel.gate.v1","provider_id":"anthropic-api","profile":"omp-opus","ok":true,"exit_code":0,"reading_account_key":"openai-api:0123456789abcdef"}`, nil
	}
	if err := a.Spawn(context.Background(), s); err == nil || !strings.Contains(err.Error(), "reading account") {
		t.Fatalf("mismatched admitted account error = %v", err)
	}
}

func TestCreditTerminalPaneStillPublishes(t *testing.T) {
	hk := &hkBenchFake{}
	srv := httptest.NewServer(hk)
	defer srv.Close()
	a, _, _, _ := quotaStopFixture(t, "worker", "idle", "")
	t.Setenv("HANDOFFKEEP_URL", "https://hk.test.invalid")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-token")
	a.Config.Quota = map[string]any{"url": srv.URL, "accounts": map[string]any{"anthropic-api": "0123456789abcdef"}}
	creditWatchSession(t, a, "worker")
	if err := a.Sleep(context.Background(), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	putEvent(t, a, "j", "job.completed", map[string]any{})
	job, err := a.job("j")
	if err != nil || !job.Terminal {
		t.Fatalf("fixture is not terminal: %+v, %v", job, err)
	}
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}
	if len(hk.manual) != 1 {
		t.Fatalf("terminal job published %d observations, want 1", len(hk.manual))
	}
}

func TestCreditCounselFailureKeepsJobLive(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unavailable", 503) }))
	defer srv.Close()
	a, _, jobs, _ := quotaStopFixture(t, "counsel", "idle", "")
	t.Setenv("HANDOFFKEEP_URL", "https://hk.test.invalid")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-token")
	a.Config.Quota = map[string]any{"url": srv.URL, "accounts": map[string]any{"anthropic-api": "0123456789abcdef"}}
	creditWatchSession(t, a, "counsel")
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}
	if got := str(jobs.record(t, "j"), "state"); got != "spawned" {
		t.Fatalf("counsel state = %q, want spawned", got)
	}
}

func TestCreditEscalationReleasesWhenHerdrUnavailable(t *testing.T) {
	a, runner, _, _ := quotaStopFixture(t, "worker", "idle", "")
	putEvent(t, a, "j", "job.escalate", map[string]any{})
	runner.Do = func(c Command) Result {
		if c.Name == "herdr" {
			return Result{Code: 1, Err: "offline"}
		}
		return Result{}
	}
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatalf("escalation failed when herdr unavailable: %v", err)
	}
	if st, _ := ledgerState(t, a, "j"); st != "released" {
		t.Fatalf("quota state = %s, want released", st)
	}
}

func TestCreditEscalatedJobDoesNotAnnounceStop(t *testing.T) {
	hk := &hkBenchFake{}
	srv := httptest.NewServer(hk)
	defer srv.Close()
	a, _, _, _ := quotaStopFixture(t, "worker", "idle", "")
	t.Setenv("HANDOFFKEEP_URL", "https://hk.test.invalid")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-token")
	a.Config.Quota = map[string]any{"url": srv.URL, "accounts": map[string]any{"anthropic-api": "0123456789abcdef"}}
	creditWatchSession(t, a, "worker")
	putEvent(t, a, "j", "job.escalate", map[string]any{})
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}
	if p := lostPayload(t, a); p != nil {
		t.Fatalf("escalated job announced quota stop: %v", p)
	}
	if len(hk.manual) != 1 {
		t.Fatalf("escalated job published %d observations, want 1", len(hk.manual))
	}
	if state, _ := ledgerState(t, a, "j"); state != "released" {
		t.Fatalf("escalated job quota state = %s, want released", state)
	}
}

package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

type placementHK struct {
	mu                 sync.Mutex
	server             *httptest.Server
	requests           [][]byte
	rows               map[string][]byte
	status             int
	statusFirst        int
	beforeFirstRefusal func()
	loseAck            bool
	refusalBody        string
}

func placementServer(t *testing.T, a *App) *placementHK {
	t.Helper()
	h := &placementHK{rows: map[string][]byte{}}
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/placements" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+hkSvcTestToken {
			t.Error("existing hk bearer credential was not used")
		}
		b, _ := io.ReadAll(r.Body)
		// Pin the producer to hk's wire fields, including the candidate
		// allowlist. Local-only diagnostics such as load_ratio are not API
		// fields (hk rejects unknown JSON keys).
		var wire map[string]json.RawMessage
		_ = json.Unmarshal(b, &wire)
		allowed := strings.Fields("job_id attempt task_id decided_at tool_version source ranking state_source outcome reason candidates chosen_host family model role profile pool pane_id repo account_key")
		for key := range wire {
			if !slices.Contains(allowed, key) {
				t.Errorf("unknown placement wire field %q", key)
			}
		}
		var candidates []map[string]json.RawMessage
		_ = json.Unmarshal(wire["candidates"], &candidates)
		allowed = strings.Fields("host outcome reason score signal_age_s cpu_busy_short cpu_busy_5m mem_available swap_in_mbps power_source bridged_jobs active_jobs tasks task_slots power")
		for _, candidate := range candidates {
			for key := range candidate {
				if !slices.Contains(allowed, key) {
					t.Errorf("unknown placement candidate wire field %q", key)
				}
			}
		}
		var rec placementRecord
		if json.Unmarshal(b, &rec) != nil {
			t.Error("invalid placement body")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		h.requests = append(h.requests, b)
		if h.statusFirst != 0 && len(h.requests) == 1 {
			if h.beforeFirstRefusal != nil {
				h.beforeFirstRefusal()
			}
			w.WriteHeader(h.statusFirst)
			body := h.refusalBody
			if body == "" {
				body = `{"error":"invalid_context"}`
			}
			_, _ = w.Write([]byte(body))
			return
		}
		if h.status != 0 {
			w.WriteHeader(h.status)
			return
		}
		key := fmt.Sprintf("%s/%d", rec.JobID, rec.Attempt)
		if old, ok := h.rows[key]; ok && !bytes.Equal(old, b) {
			w.WriteHeader(http.StatusConflict)
			return
		}
		h.rows[key] = b
		if h.loseAck {
			h.loseAck = false
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(h.server.Close)
	// The svc-panewire credential points at the test's spawn-path fake hk
	// (tasks, routing); hosts.toml [quota] url overrides it for placements.
	useFakeHK(t)
	a.Config.Quota = map[string]any{"url": h.server.URL}
	return h
}

func (h *placementHK) snapshot() ([][]byte, map[string][]byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	rows := map[string][]byte{}
	for k, v := range h.rows {
		rows[k] = v
	}
	return append([][]byte{}, h.requests...), rows
}

func onePlacement(t *testing.T, h *placementHK) placementRecord {
	t.Helper()
	requests, rows := h.snapshot()
	if len(requests) != 1 || len(rows) != 1 {
		t.Fatalf("want exactly one request/row, got %d/%d", len(requests), len(rows))
	}
	var rec placementRecord
	if err := json.Unmarshal(requests[0], &rec); err != nil {
		t.Fatal(err)
	}
	for _, bad := range append(fakeSecrets, hkSvcTestToken, "brief text", "decided_on", "login", "@example.invalid") {
		if bytes.Contains(requests[0], []byte(bad)) {
			t.Fatalf("private field or value %q reached placement payload", bad)
		}
	}
	return rec
}

func placementSpool(t *testing.T, a *App) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(a.stateDir(), "placements", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// Exercise real Spawn control flow with a fake terminal/quota gate and HTTP
// hk. A telemetry outage must never alter that control flow.
func TestPlacementRecordsEveryOutcomeAndSource(t *testing.T) {
	for _, source := range []string{"auto", "forced"} {
		for _, outcome := range []string{"placed", "refused", "failed"} {
			t.Run(source+"/"+outcome, func(t *testing.T) {
				a, s, f, _ := cmdFixture(t)
				h := placementServer(t, a)
				a.Config.Local["machine_id"] = "host-a"
				a.LoadRatio = func(context.Context) (float64, error) { return 0.2, nil }
				a.Source = &fakePlacementSource{hosts: []HostView{{Name: "host-a", Connected: true, Score: ptrF(120), Signals: PlacementSignals{CPUBusyShort: ptrF(0.2), MemAvailable: ptrF(0.7), PowerSource: "ac"}}}}
				if source == "auto" {
					s.Host = "auto"
				}
				if outcome == "refused" {
					a.Config.Local["spawn"] = false
				}
				base := f.Do
				f.Do = func(c Command) Result {
					args := strings.Join(c.Args, " ")
					if outcome == "failed" && strings.HasPrefix(args, "tab create") {
						return Result{Code: 1, Err: fakeSecretText}
					}
					if c.Name == "git" && strings.Contains(args, "rev-parse --show-toplevel") {
						return Result{Out: s.CWD + "/repo-a\n"}
					}
					return base(c)
				}
				err := a.Spawn(context.Background(), s)
				if (err == nil) != (outcome == "placed") {
					t.Fatalf("spawn outcome: %v", err)
				}
				r := onePlacement(t, h)
				if r.JobID != s.Job || r.Attempt != 1 || r.TaskID != 42 || r.Source != source || r.Outcome != outcome || r.Profile != s.Profile || r.Role != "worker" || r.Family == "" || r.Repo != "repo-a" {
					t.Fatalf("placement: %+v", r)
				}
				if _, err := time.Parse(time.RFC3339Nano, r.DecidedAt); err != nil {
					t.Fatal(err)
				}
				if outcome == "placed" && (r.Host != "host-a" || r.Model != "provider/example-model" || r.PaneID != "w:p1") {
					t.Fatalf("execution metadata: %+v", r)
				}
				if source == "auto" && (len(r.Candidates) == 0 || r.Candidates[0].Score == nil || r.Candidates[0].CPUBusyShort == nil || r.Candidates[0].Reason == "") {
					t.Fatalf("candidate evidence: %+v", r.Candidates)
				}
				if files := placementSpool(t, a); len(files) != 0 {
					t.Fatalf("delivered record still spooled: %v", files)
				}
			})
		}
	}
}

func TestPlacementOutageSpoolsAndNextInvocationRetries(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	h := placementServer(t, a)
	h.status = http.StatusServiceUnavailable
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatalf("telemetry outage failed the spawn: %v", err)
	}
	files := placementSpool(t, a)
	if len(files) != 1 {
		t.Fatalf("want one durable record: %v", files)
	}
	info, _ := os.Stat(files[0])
	if info.Mode().Perm() != 0600 {
		t.Fatalf("spool permissions: %v", info.Mode())
	}
	h.mu.Lock()
	h.status = 0
	h.mu.Unlock()
	// A new App represents the next CLI process; even help drains the spool.
	next := &App{Home: a.Home, Config: a.Config, Out: io.Discard, Err: io.Discard, Now: a.Now}
	if err := next.Run(context.Background(), []string{"help"}); err != nil {
		t.Fatal(err)
	}
	requests, rows := h.snapshot()
	if len(requests) != 2 || len(rows) != 1 || !bytes.Equal(requests[0], requests[1]) || len(placementSpool(t, a)) != 0 {
		t.Fatalf("retry changed or lost the payload: %d requests, %d rows", len(requests), len(rows))
	}
}

func TestPlacementAmbiguousDeliveryReplaysIdenticalPayload(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	h := placementServer(t, a)
	h.loseAck = true
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	a.flushPlacements()
	requests, rows := h.snapshot()
	if len(requests) != 2 || len(rows) != 1 || !bytes.Equal(requests[0], requests[1]) || len(placementSpool(t, a)) != 0 {
		t.Fatalf("same immutable key/payload must replay as one row: %d/%d", len(requests), len(rows))
	}
}

func TestPlacementConflictLoggedAndDropped(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	h := placementServer(t, a)
	h.status = http.StatusConflict
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	a.flushPlacements()
	requests, _ := h.snapshot()
	if len(requests) != 1 || len(placementSpool(t, a)) != 0 || !strings.Contains(a.Err.(*bytes.Buffer).String(), "HTTP 409") {
		t.Fatalf("conflict was not logged and dropped: requests=%d warnings=%s", len(requests), a.Err)
	}
}

func TestPlacementReviewUsesOwningTask(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	h := placementServer(t, a)
	// These are the mergewake review request's task and author metadata.
	s.Role, s.AuthorFamily, s.BuilderJob = "reviewer", "anthropic", "author-job"
	s.Report = filepath.Join(a.Jobs, s.Job, "report.md")
	t.Setenv("HK_TASK_ID", "99")
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	r := onePlacement(t, h)
	if r.Role != "review" || r.TaskID != 42 {
		t.Fatalf("review must name the request's task, not the caller's: %+v", r)
	}
}

func TestPlacementDeliveryTimeoutAndCancellationDoNotFailSpawn(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The handler has no detached fixture process to clean up.
		select {
		case <-r.Context().Done():
		case <-time.After(4 * time.Second):
		}
	}))
	t.Cleanup(server.Close)
	// The svc-panewire credential points at the test's spawn-path fake hk
	// (tasks, routing); hosts.toml [quota] url overrides it for placements.
	useFakeHK(t)
	a.Config.Quota = map[string]any{"url": server.URL}
	start := time.Now()
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second || len(placementSpool(t, a)) != 1 {
		t.Fatalf("delivery exceeded the two-second budget or lost the spool: %s", elapsed)
	}
}

func TestPlacementAttemptsNeverReuseDeliveredKeys(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	h := placementServer(t, a)
	a.Config.Local["spawn"] = false
	for i := 0; i < 2; i++ {
		if a.Spawn(context.Background(), s) == nil {
			t.Fatal("fixture should refuse")
		}
	}
	_, rows := h.snapshot()
	if len(rows) != 2 || rows["job-a/1"] == nil || rows["job-a/2"] == nil {
		t.Fatalf("separate decisions reused a key: %v", rows)
	}
}

func TestPlacementDryRunAndDelegatedExecutorDoNotProduceRows(t *testing.T) {
	for _, mode := range []string{"dry-run", "ssh-executor", "hub-executor"} {
		t.Run(mode, func(t *testing.T) {
			a, s, _, _ := cmdFixture(t)
			h := placementServer(t, a)
			s.DryRun = mode == "dry-run"
			s.Delegated = mode != "dry-run"
			if mode == "hub-executor" {
				s.Delegated = true
			}
			if err := a.Spawn(context.Background(), s); err != nil {
				t.Fatal(err)
			}
			requests, _ := h.snapshot()
			if len(requests) != 0 || len(placementSpool(t, a)) != 0 {
				t.Fatal("preview or executor duplicated the initiator's decision")
			}
		})
	}
}

func TestPlacementSignalsDecodeFromBothHubSources(t *testing.T) {
	const body = `{"candidates":[{"machine":"host-b","score":123,"signals":{"cpu_busy_short":0.2,"mem_available":0.6,"power_source":"ac","task_slots":3,"unknown":"ignored"}}]}`
	hosts, err := parseHubHosts(body)
	if err != nil || len(hosts) != 1 || hosts[0].Signals.CPUBusyShort == nil || *hosts[0].Signals.CPUBusyShort != 0.2 {
		t.Fatalf("direct placement dropped signals: %+v %v", hosts, err)
	}
	a := testApp(t)
	var snap FleetSnapshot
	data := fmt.Sprintf(`{"v":1,"at":%q,"states":[{"host":"host-b","kind":"publisher","publisher":{"connected":true,"last_seen":%q}},{"host":"host-b","kind":"load","measured_at":%q,"load":{"connected":true,"score":123,"signals":{"cpu_busy_short":0.2,"mem_available":0.6,"power_source":"ac","task_slots":3}}}]}`, a.Now().Format(time.RFC3339), a.Now().Format(time.RFC3339), a.Now().Format(time.RFC3339))
	if err := json.Unmarshal([]byte(data), &snap); err != nil {
		t.Fatal(err)
	}
	a.Config.Hub = map[string]any{"hub_url": "https://hub.invalid"}
	a.fleetLog = &fleetStateLog{snapshot: &snap, fetchedAt: a.Now()}
	stateHosts, why := a.hubHosts(context.Background())
	if why != "" || len(stateHosts) != 1 || !reflect.DeepEqual(hosts[0].Signals, stateHosts[0].Signals) {
		t.Fatalf("fleet snapshot dropped signals: %+v %s", stateHosts, why)
	}
}

func remotePlacementFixture(t *testing.T, transport, source, outcome string, metadata bool) (*App, Spawn) {
	t.Helper()
	execution := placementExecution{Host: "host-b", Family: "anthropic", Model: "executing-model", Profile: "sonnet", Pool: "claude", Role: "worker", PaneID: "w:remote", Repo: "repo-a", AccountKey: "claude:fedcba9876543210"}
	line := "OK pane=w:remote job=job-a"
	if metadata {
		line += " placement=" + execution.receipt()
	}
	result := Result{Out: line}
	if outcome == "failed" {
		result = Result{Code: exitConfig, Out: "FAILED placement=" + execution.receipt(), Err: fakeSecretText}
	}
	if outcome == "contact-refused" {
		result = Result{Code: exitFailure, Err: "remote refused before launching"}
	}
	if outcome == "retryable-failed" {
		result = Result{Code: exitFailure, Out: "FAILED placement=" + execution.receipt()}
	}
	src := &fakePlacementSource{hosts: []HostView{{Name: "host-b", Connected: true, Score: ptrF(123), LoadRatio: ptrF(0.2)}}}
	a, s, _ := remoteFixture(t, src, &fakeWaker{}, "2026-01-01T00:00:00Z", func(args string) Result {
		if strings.Contains(args, "_spawn-stdin") {
			return result
		}
		return Result{}
	})
	s.Label, s.Workspace, s.OverrideReason, s.T, s.Containment = "worker-a", "workspace", "fixture", "T1", "hk:12"
	s.Prompt = filepath.Join(a.Home, "brief.md")
	if err := os.WriteFile(s.Prompt, []byte("brief text"), 0600); err != nil {
		t.Fatal(err)
	}
	q := a.Quota.(*fakeQuota)
	q.LaunchValue = Launch{Profile: "sonnet", Pool: "claude", ModelID: "initiator-model", Effort: "high", Grade: "A", Gate: "default"}
	if outcome == "refused" {
		q.GateDo = func(QuotaRequest) (string, error) {
			return "", fail(exitSeatRefused, "quota refused: %s", fakeSecretText)
		}
	}
	a.Config.Hosts = map[string]map[string]any{"host-b": {"cwd_map": map[string]any{s.CWD: "~/work/repo-a"}}}
	a.Config.HostOrder = []string{"host-b"}
	if source == "forced" {
		s.Host = "host-b"
	}
	if transport == "hub" {
		a.Config.Hosts["host-b"]["via"] = "hub"
		events := []string{}
		a.Hub = &fakeHub{t: t, events: &events, node: func(r HubSpawnRequest) (HubSpawnReply, error) {
			return HubSpawnReply{RequestID: r.RequestID, Machine: r.Machine, JobID: s.Job, Pane: execution.PaneID, RC: result.Code, StdoutTail: result.Out}, nil
		}}
	}
	return a, s
}

func TestPlacementRemoteTransportsRecordEveryOutcomeAndSource(t *testing.T) {
	for _, transport := range []string{"ssh", "hub"} {
		for _, source := range []string{"auto", "forced"} {
			for _, outcome := range []string{"placed", "refused", "failed"} {
				t.Run(transport+"/"+source+"/"+outcome, func(t *testing.T) {
					a, s := remotePlacementFixture(t, transport, source, outcome, true)
					h := placementServer(t, a)
					err := a.Spawn(context.Background(), s)
					if (err == nil) != (outcome == "placed") {
						t.Fatalf("unexpected spawn outcome: %v", err)
					}
					r := onePlacement(t, h)
					if r.Source != source || r.Outcome != outcome || r.TaskID != 42 || r.Role != "worker" {
						t.Fatalf("remote decision: %+v", r)
					}
					if outcome == "placed" && (r.Host != "host-b" || r.Model != "executing-model" || r.AccountKey != "claude:fedcba9876543210" || r.PaneID != "w:remote") {
						t.Fatalf("must record the executing session, not initiator-model: %+v", r)
					}
					if outcome == "failed" && r.Host != "host-b" {
						t.Fatalf("failed remote launch lost the contacted host: %+v", r)
					}
					if outcome == "failed" && (r.Family != "anthropic" || r.Pool != "claude" || r.Model != "executing-model") {
						t.Fatalf("accepted failure receipt lost execution metadata: %+v", r)
					}
					if outcome == "failed" && source == "auto" {
						chosen := slices.ContainsFunc(r.Candidates, func(c placementCandidateRecord) bool { return c.Host == "host-b" && c.Outcome == "chosen" })
						if !chosen {
							t.Fatalf("failed remote launch marked its host as skipped: %+v", r.Candidates)
						}
					}
				})
			}
		}
	}
}

func TestPlacementLegacyRemoteMetadataWaitsForHKAndFreezes(t *testing.T) {
	a, s := remotePlacementFixture(t, "ssh", "auto", "placed", false)
	h := placementServer(t, a)
	s.AccountKey = "claude:0123456789abcdef"
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	requests, _ := h.snapshot()
	if len(requests) != 0 || len(placementSpool(t, a)) != 1 {
		t.Fatal("an old executor's incomplete metadata must stay spooled")
	}
	b, err := os.ReadFile(placementSpool(t, a)[0])
	if err != nil {
		t.Fatal(err)
	}
	var pending pendingPlacement
	if err := json.Unmarshal(b, &pending); err != nil {
		t.Fatal(err)
	}
	if pending.Record.Family != "anthropic" || pending.Record.Pool != "claude" || !pending.ResolveJob {
		t.Fatalf("legacy receipt discarded known family/pool before resolution: %+v", pending)
	}
	store := a.JobStore.(*fakeJobStore)
	store.records[s.Job] = map[string]any{"family": "anthropic", "model": "final-model", "profile": "sonnet", "pool": "claude", "pane_id": "w:remote"}
	h.loseAck = true
	a.flushPlacements()
	store.records[s.Job]["model"] = "later-change"
	a.flushPlacements()
	requests, rows := h.snapshot()
	if len(rows) != 1 || len(requests) != 2 || !bytes.Equal(requests[0], requests[1]) || !bytes.Contains(requests[0], []byte("final-model")) {
		t.Fatalf("legacy execution metadata was guessed or changed on retry: %q", requests)
	}
	var r placementRecord
	_ = json.Unmarshal(requests[0], &r)
	if r.Role == "" || r.Family == "" || r.TaskID == 0 || r.Host == "" {
		t.Fatalf("resolved placed record misses required metadata: %+v", r)
	}
	if r.AccountKey != s.AccountKey {
		t.Fatalf("legacy executor lost the admitted account: got %q, want %q", r.AccountKey, s.AccountKey)
	}
}

func TestPlacementContactIsNotLaunchAcceptance(t *testing.T) {
	for _, transport := range []string{"ssh", "hub"} {
		for _, source := range []string{"auto", "forced"} {
			t.Run(transport+"/"+source, func(t *testing.T) {
				a, s := remotePlacementFixture(t, transport, source, "contact-refused", false)
				h := placementServer(t, a)
				if err := a.Spawn(context.Background(), s); err == nil {
					t.Fatal("remote refusal unexpectedly succeeded")
				}
				r := onePlacement(t, h)
				if r.Outcome != "refused" || r.Host != "" || slices.ContainsFunc(r.Candidates, func(c placementCandidateRecord) bool { return c.Outcome == "chosen" }) {
					t.Fatalf("contact without acceptance was counted as a launch: outcome=%s chosen_host=%q candidates=%+v", r.Outcome, r.Host, r.Candidates)
				}
				if !slices.ContainsFunc(r.Candidates, func(c placementCandidateRecord) bool { return c.Host == "host-b" && c.Reason != "" }) {
					t.Fatalf("contacted destination missing from refusal trace: %+v", r.Candidates)
				}
			})
		}
	}
}

func TestPlacementRetryDoesNotLeakLaunchStateIntoLocalRefusal(t *testing.T) {
	for _, outcome := range []string{"contact-refused", "retryable-failed"} {
		for _, transport := range []string{"ssh", "hub"} {
			t.Run(transport+"/"+outcome, func(t *testing.T) {
				a, s := remotePlacementFixture(t, transport, "auto", outcome, false)
				h := placementServer(t, a)
				a.Config.Local["spawn"] = true
				a.LoadRatio = func(context.Context) (float64, error) { return 0.9, nil }
				if err := os.MkdirAll(s.CWD, 0700); err != nil {
					t.Fatal(err)
				}
				checks := 0
				useFakeHK(t).Handle(func(r hkRequest) (int, string, bool) {
					if r.Route() != "GET /v1/tasks/42" {
						return 0, "", false
					}
					checks++
					if checks > 1 {
						return http.StatusServiceUnavailable, `{"error":"local precheck unavailable"}`, true
					}
					return 0, "", false
				})
				if err := a.Spawn(context.Background(), s); exitCode(err) != exitConfig || checks != 2 {
					t.Fatalf("fixture did not reach local pre-launch refusal: checks=%d err=%v", checks, err)
				}
				r := onePlacement(t, h)
				if r.Outcome != "refused" || r.Host != "" || slices.ContainsFunc(r.Candidates, func(c placementCandidateRecord) bool { return c.Outcome == "chosen" }) {
					t.Fatalf("previous candidate leaked into final refusal: outcome=%s chosen_host=%q candidates=%+v", r.Outcome, r.Host, r.Candidates)
				}
			})
		}
	}
}

func TestPlacementExecutorEmitsFailureOnlyAfterLaunchAttempt(t *testing.T) {
	for _, delegated := range []bool{false, true} {
		for _, started := range []bool{false, true} {
			t.Run(fmt.Sprintf("delegated=%t/started=%t", delegated, started), func(t *testing.T) {
				a, s, f, _ := cmdFixture(t)
				s.Delegated = delegated
				if !delegated {
					s.Delegated = true
				}
				a.Config.Local["spawn"] = started
				base := f.Do
				f.Do = func(c Command) Result {
					if c.Name == "herdr" && len(c.Args) >= 2 && c.Args[0] == "tab" && c.Args[1] == "create" {
						return Result{Code: 1, Err: fakeSecretText}
					}
					return base(c)
				}
				if err := a.Spawn(context.Background(), s); err == nil {
					t.Fatal("fixture did not refuse/fail")
				}
				out := a.Out.(*bytes.Buffer).String()
				d := &placementDecision{}
				d.remoteReceipt(a.machine(), out)
				if d.started != started || d.placed || (started && (d.execution.Host != a.machine() || d.execution.Model != "provider/example-model")) {
					t.Fatalf("failure receipt did not describe actual launch phase: started=%t metadata=%+v stdout=%q", d.started, d.execution, out)
				}
			})
		}
	}
}

func TestPlacementWakeRetryDoesNotChooseRejectedHost(t *testing.T) {
	for _, transport := range []string{"ssh", "hub"} {
		for _, outcome := range []string{"contact-refused", "retryable-failed"} {
			t.Run(transport+"/"+outcome, func(t *testing.T) {
				a, s := remotePlacementFixture(t, transport, "auto", outcome, false)
				h := placementServer(t, a)
				src := a.Source.(*fakePlacementSource)
				src.hosts[0].Connected = false
				a.Config.Hosts["host-b"]["wake"] = "panewire"
				w := &fakeWaker{onWake: func(string) { src.hosts[0].Connected = true }}
				a.Wake = w
				if err := a.Spawn(context.Background(), s); err == nil || len(w.calls) != 1 {
					t.Fatalf("fixture did not reach the wake refusal: wakes=%v err=%v", w.calls, err)
				}
				r := onePlacement(t, h)
				if r.Outcome != "refused" || r.Host != "" || slices.ContainsFunc(r.Candidates, func(c placementCandidateRecord) bool { return c.Outcome == "chosen" }) {
					t.Fatalf("wake retry leaked launch state: outcome=%s chosen_host=%q candidates=%+v", r.Outcome, r.Host, r.Candidates)
				}
			})
		}
	}
}

func TestPlacementDeliverySkipsStrayEntriesWithoutUsingBudget(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	h := placementServer(t, a)
	dir := filepath.Join(a.stateDir(), "placements")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		path := filepath.Join(dir, fmt.Sprintf("000-stray-%02d", i))
		var err error
		switch i % 3 {
		case 0:
			err = os.WriteFile(path, []byte("not a record"), 0600)
		case 1:
			err = os.Mkdir(path+".json", 0700)
		case 2:
			err = os.Symlink("absent.json", path+".json")
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	_ = onePlacement(t, h)
}

func TestPlacementDeliveryProgressesPastUnresolvablePrefix(t *testing.T) {
	a := testApp(t)
	h := placementServer(t, a)
	for i := 0; i < 32; i++ {
		p := pendingPlacement{Record: placementRecord{
			JobID: fmt.Sprintf("a-unresolved-%02d", i), TaskID: 42, DecidedAt: a.Now().Format(time.RFC3339Nano),
			Source: "auto", Ranking: "unknown", StateSource: "unknown", Outcome: "placed", Candidates: []placementCandidateRecord{},
			placementExecution: placementExecution{Host: "host-b", Role: "worker"},
		}, ResolveJob: true}
		if err := a.spoolPlacement(&p); err != nil {
			t.Fatal(err)
		}
	}
	ready := pendingPlacement{Record: placementRecord{
		JobID: "z-ready", TaskID: 42, DecidedAt: a.Now().Format(time.RFC3339Nano),
		Source: "auto", Ranking: "unknown", StateSource: "unknown", Outcome: "placed", Candidates: []placementCandidateRecord{},
		placementExecution: placementExecution{Host: "host-b", Family: "anthropic", Role: "worker", Model: "model-a", PaneID: "w:remote"},
	}}
	if err := a.spoolPlacement(&ready); err != nil {
		t.Fatal(err)
	}
	a.flushPlacements()
	if requests, _ := h.snapshot(); len(requests) != 0 {
		t.Fatal("first pass exceeded the 32-record attempt budget")
	}
	// Progress must survive a new CLI process, not just another call on App.
	next := &App{Home: a.Home, Config: a.Config, Out: io.Discard, Err: io.Discard, Now: a.Now, JobStore: a.JobStore}
	if err := next.Run(context.Background(), []string{"help"}); err != nil {
		t.Fatal(err)
	}
	if _, rows := h.snapshot(); rows["z-ready/1"] == nil {
		t.Fatalf("healthy placement is stuck behind unresolved jobs: %d rows", len(rows))
	}
}

func TestPlacementAutomaticModelSelection(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	h := placementServer(t, a)
	a.fleetLog = &fleetStateLog{hosts: &StateUse{Source: stateSourceDirect, Reason: stateAbsent}}
	s.Profile, s.Host, s.OverrideReason = "auto", "auto", ""
	q := a.Quota.(*fakeQuota)
	q.LaunchValue.Pool = "claude"
	q.PoolViews = map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)}}
	q.GateDo = gatePools(map[string]string{"cmd-dsflash41": "claude"})
	const key = "claude:0123456789abcdef"
	a.Source = &fakePlacementSource{
		presence: []PresenceRecord{{HostID: a.machine(), AccountKey: key, LastSeen: a.Now()}},
		accounts: map[string]SharedAccount{key: {Key: key, Observed: true, Fresh: true, Quota: &PoolView{Status: "ok", Class: "spend", Blocking: ptrF(10)}}},
	}
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	r := onePlacement(t, h)
	if r.Source != "auto" || r.Outcome != "placed" || r.Model != "provider/example-model" || r.AccountKey != key || r.Profile != "cmd-dsflash41" || r.StateSource != "direct" || r.Reason != stateAbsent {
		t.Fatalf("automatic selection lost executing metadata: %+v", r)
	}
}

func TestPlacementAccountLoginAndSecretStayOutOfPayload(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	h := placementServer(t, a)
	s.AccountKey = "fixture-login token=tok3nVALUEfake99"
	if err := a.Spawn(context.Background(), s); err == nil {
		t.Fatal("an unbound account must refuse admission")
	}
	r := onePlacement(t, h)
	if !strings.HasPrefix(r.AccountKey, "account:") || r.Outcome != "refused" {
		t.Fatalf("the raw account key must become an opaque hash: %+v", r)
	}
}

// A refused delivery names hk's status so a token-scope refusal can be told
// from an outage; the record stays spooled for the next pass.
func TestPlacementDeliveryRefusalNamesTheStatus(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	h := placementServer(t, a)
	h.status = http.StatusUnauthorized
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	a.flushPlacements()
	if warnings := a.Err.(*bytes.Buffer).String(); !strings.Contains(warnings, "hk HTTP 401") || len(placementSpool(t, a)) == 0 {
		t.Fatalf("refusal must name HTTP 401 and keep the record spooled: warnings=%s spool=%d", warnings, len(placementSpool(t, a)))
	}
}

func TestPlacementPermanentRefusalQuarantinesAndContinues(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusNotFound, http.StatusUnprocessableEntity} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			a := testApp(t)
			h := placementServer(t, a)
			h.statusFirst = status
			for _, id := range []string{"a", "b", "c"} {
				p := pendingPlacement{Record: placementRecord{JobID: id}}
				if err := a.spoolPlacement(&p); err != nil {
					t.Fatal(err)
				}
			}
			a.flushPlacements()
			requests, rows := h.snapshot()
			if len(requests) != 3 || len(rows) != 2 {
				t.Fatalf("want 3 requests and 2 delivered rows, got %d/%d", len(requests), len(rows))
			}
			rejected := filepath.Join(a.stateDir(), "placements-rejected", "a.1.json")
			info, err := os.Stat(rejected)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != 0600 {
				t.Fatalf("mode %v", info.Mode())
			}
			if got := placementSpool(t, a); len(got) != 0 {
				t.Fatalf("remaining spool: %v", got)
			}
			cursor, err := os.ReadFile(filepath.Join(a.stateDir(), "placement-cursor"))
			if err != nil || string(cursor) != "c.1.json" {
				t.Fatalf("cursor %q: %v", cursor, err)
			}
			warnings := a.Err.(*bytes.Buffer).String()
			if !strings.Contains(warnings, "a.1.json") || !strings.Contains(warnings, fmt.Sprint(status)) || !strings.Contains(warnings, "invalid_context") {
				t.Fatalf("warning: %s", warnings)
			}
			a.flushPlacements()
			requests, _ = h.snapshot()
			if len(requests) != 3 {
				t.Fatalf("rejected record retried: %d", len(requests))
			}
		})
	}
}

func TestPlacementNotFoundRefusalQuarantinesAndContinues(t *testing.T) {
	a := testApp(t)
	h := placementServer(t, a)
	h.statusFirst = http.StatusNotFound
	h.refusalBody = `{"error":"not_found"}`
	for _, id := range []string{"a", "b", "c"} {
		p := pendingPlacement{Record: placementRecord{JobID: id}}
		if err := a.spoolPlacement(&p); err != nil {
			t.Fatal(err)
		}
	}
	a.flushPlacements()
	requests, rows := h.snapshot()
	if len(requests) != 3 || len(rows) != 2 {
		t.Fatalf("want 3 requests and 2 delivered rows, got %d/%d", len(requests), len(rows))
	}
	rejected := filepath.Join(a.stateDir(), "placements-rejected", "a.1.json")
	info, err := os.Stat(rejected)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("mode %v", info.Mode())
	}
	if got := placementSpool(t, a); len(got) != 0 {
		t.Fatalf("remaining spool: %v", got)
	}
	cursor, err := os.ReadFile(filepath.Join(a.stateDir(), "placement-cursor"))
	if err != nil || string(cursor) != "c.1.json" {
		t.Fatalf("cursor %q: %v", cursor, err)
	}
	warnings := a.Err.(*bytes.Buffer).String()
	if !strings.Contains(warnings, "a.1.json") || !strings.Contains(warnings, "404") || !strings.Contains(warnings, "not_found") {
		t.Fatalf("warning: %s", warnings)
	}
	a.flushPlacements()
	requests, _ = h.snapshot()
	if len(requests) != 3 {
		t.Fatalf("rejected record retried: %d", len(requests))
	}
}

func TestPlacementTransientRefusalStopsDelivery(t *testing.T) {
	for _, status := range []int{401, 403, 408, 429, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			a := testApp(t)
			h := placementServer(t, a)
			h.statusFirst = status
			for _, id := range []string{"a", "b", "c"} {
				p := pendingPlacement{Record: placementRecord{JobID: id}}
				if err := a.spoolPlacement(&p); err != nil {
					t.Fatal(err)
				}
			}
			cursorPath := filepath.Join(a.stateDir(), "placement-cursor")
			if err := os.WriteFile(cursorPath, []byte("0"), 0600); err != nil {
				t.Fatal(err)
			}
			a.flushPlacements()
			requests, rows := h.snapshot()
			if len(requests) != 1 || len(rows) != 0 || len(placementSpool(t, a)) != 3 {
				t.Fatalf("requests=%d rows=%d spool=%v", len(requests), len(rows), placementSpool(t, a))
			}
			cursor, err := os.ReadFile(cursorPath)
			if err != nil || string(cursor) != "0" {
				t.Fatalf("cursor %q: %v", cursor, err)
			}
			if _, err := os.Stat(filepath.Join(a.stateDir(), "placements-rejected")); !os.IsNotExist(err) {
				t.Fatalf("unexpected quarantine: %v", err)
			}
		})
	}
}

func TestPlacementQuarantineCapDropsOldest(t *testing.T) {
	a := testApp(t)
	h := placementServer(t, a)
	h.statusFirst = 400
	dir := filepath.Join(a.stateDir(), "placements-rejected")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		path := filepath.Join(dir, fmt.Sprintf("old-%04d.json", i))
		if err := os.WriteFile(path, []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
		stamp := time.Unix(int64(i+1), 0)
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	p := pendingPlacement{Record: placementRecord{JobID: "new"}}
	if err := a.spoolPlacement(&p); err != nil {
		t.Fatal(err)
	}
	a.flushPlacements()
	entries, err := os.ReadDir(dir)
	records := 0
	for _, e := range entries {
		// Rejection-reason sidecars travel with their record and are not counted.
		if !strings.HasSuffix(e.Name(), ".reason") {
			records++
		}
	}
	if err != nil || records != 1000 {
		t.Fatalf("records=%d err=%v", records, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "new.1.json.reason")); err != nil {
		t.Fatalf("the new rejection has no reason sidecar: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "old-0000.json")); !os.IsNotExist(err) {
		t.Fatalf("oldest retained: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "new.1.json")); err != nil {
		t.Fatal(err)
	}
	if warnings := a.Err.(*bytes.Buffer).String(); !strings.Contains(warnings, "dropped 1 oldest") {
		t.Fatalf("warnings: %s", warnings)
	}
}

func TestPlacementConcurrentQuarantineContinuesDelivery(t *testing.T) {
	a := testApp(t)
	h := placementServer(t, a)
	h.statusFirst = 400
	for _, id := range []string{"a", "b"} {
		p := pendingPlacement{Record: placementRecord{JobID: id}}
		if err := a.spoolPlacement(&p); err != nil {
			t.Fatal(err)
		}
	}
	h.beforeFirstRefusal = func() {
		dir := filepath.Join(a.stateDir(), "placements-rejected")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Error(err)
			return
		}
		if err := os.Rename(filepath.Join(a.stateDir(), "placements", "a.1.json"), filepath.Join(dir, "a.1.json")); err != nil {
			t.Error(err)
		}
	}
	a.flushPlacements()
	requests, rows := h.snapshot()
	if len(requests) != 2 || len(rows) != 1 || len(placementSpool(t, a)) != 0 {
		t.Fatalf("requests=%d rows=%d spool=%v", len(requests), len(rows), placementSpool(t, a))
	}
	if warnings := a.Err.(*bytes.Buffer).String(); strings.Contains(warnings, "could not be quarantined") {
		t.Fatalf("false warning: %s", warnings)
	}
}

// A refused record keeps hk's reason: in the warning (one line, bounded) and
// in a sidecar next to the quarantined record, which the cap drops with it.
func TestPlacementRejectionKeepsHKReason(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	h := placementServer(t, a)
	h.statusFirst = http.StatusBadRequest
	h.refusalBody = `{"error":"invalid_context","reason":"task_id 42 not found\nin this store"}`
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	a.flushPlacements()
	if warnings := a.Err.(*bytes.Buffer).String(); !strings.Contains(warnings, "hk HTTP 400 invalid_context: task_id 42 not found in this store") {
		t.Fatalf("warning lacks the flattened reason: %s", warnings)
	}
	dir := filepath.Join(a.stateDir(), "placements-rejected")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var sidecar []byte
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".reason") {
			sidecar, _ = os.ReadFile(filepath.Join(dir, e.Name()))
		}
	}
	var note map[string]any
	if json.Unmarshal(sidecar, &note) != nil || note["reason"] != "task_id 42 not found in this store" || note["error"] != "invalid_context" || note["status"] != float64(400) {
		t.Fatalf("sidecar = %s", sidecar)
	}
}

func TestBackendReasonIsBoundedToOneLine(t *testing.T) {
	long := strings.Repeat("é", 300) + "\nsecond line"
	got := backendReason(long)
	if strings.Contains(got, "\n") || len(got) > backendReasonMaxBytes+len("…") {
		t.Fatalf("reason not bounded/flattened: %d bytes", len(got))
	}
}

func TestCreditBandRefusalRetainsRoleDeniedTriesAsNoRung(t *testing.T) {
	got, ok := creditBandRefusal(map[string]any{"admission_outcome": "no_rung", "outcome": "all_refused", "tried": []any{map[string]any{"outcome_code": outcomeNoRoleCandidate, "result": "refused"}}})
	if !ok || got != "all_refused" {
		t.Fatalf("credit role refusal=(%q,%v), want retained all_refused row", got, ok)
	}
}

package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func t12Float(value float64) *float64 { return &value }
func t12Int(value int) *int           { return &value }

func TestT12HostLoadHeartbeatAndNodesSurface(t *testing.T) {
	legacy, ok := decodeHubHeartbeatPayload([]byte(`{"status":"alive","host_load":{"load1":1,"load5":2,"swap_used_gb":3,"worker_procs":4}}`))
	if !ok || legacy.HostLoad == nil || legacy.HostLoad.Load15 != nil || legacy.HostLoad.NCPU != nil {
		t.Fatalf("legacy four-key host_load=%+v ok=%t", legacy.HostLoad, ok)
	}
	full := []byte(`{"status":"alive","host_load":{"load1":1,"load5":2,"swap_used_gb":3,"worker_procs":4,"load15":null,"ncpu":null}}`)
	heartbeat, ok := decodeHubHeartbeatPayload(full)
	if !ok || heartbeat.HostLoad == nil || heartbeat.HostLoad.Load15 != nil || heartbeat.HostLoad.NCPU != nil {
		t.Fatalf("six-key null host_load=%+v ok=%t", heartbeat.HostLoad, ok)
	}
	for _, payload := range [][]byte{
		[]byte(`{"status":"alive","host_load":{"load1":1,"load5":2,"swap_used_gb":3,"worker_procs":4,"load15":5}}`),
		[]byte(`{"status":"alive","host_load":{"load1":1,"load5":2,"swap_used_gb":3,"worker_procs":4,"load15":5,"ncpu":8,"extra":1}}`),
		[]byte(`{"status":"alive","host_load":{"load1":1,"load5":2,"swap_used_gb":3,"worker_procs":4,"unknown":1}}`),
	} {
		if _, ok := decodeHubHeartbeatPayload(payload); ok {
			t.Fatalf("invalid host_load shape accepted: %s", payload)
		}
	}

	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "operator-token", "host-a": "node-token", "host-b": "node-token-b"}})
	if err != nil {
		t.Fatal(err)
	}
	hub.nodes["host-a"] = &hubNodeRecord{machineID: "host-a", hostLoad: heartbeat.HostLoad, remoteMeta: map[string]string{}}
	hub.nodes["host-b"] = &hubNodeRecord{machineID: "host-b", remoteMeta: map[string]string{}}
	recording := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/nodes", nil)
	request.Header.Set(hubAuthorizationHeader, "Bearer operator-token")
	hub.Handler().ServeHTTP(recording, request)
	if recording.Code != http.StatusOK {
		t.Fatalf("nodes status=%d body=%s", recording.Code, recording.Body.String())
	}
	var result struct {
		Nodes []json.RawMessage `json:"nodes"`
	}
	if err := json.Unmarshal(recording.Body.Bytes(), &result); err != nil || len(result.Nodes) != 2 {
		t.Fatalf("nodes decode=%v result=%s", err, recording.Body.String())
	}
	var withLoad, withoutLoad map[string]json.RawMessage
	for _, raw := range result.Nodes {
		var node map[string]json.RawMessage
		if err := json.Unmarshal(raw, &node); err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte(`"machine_id":"host-a"`)) {
			withLoad = node
		} else {
			withoutLoad = node
		}
	}
	if string(withoutLoad["load"]) != "null" {
		t.Fatalf("node without host_load did not serialize load:null: %s", withoutLoad["load"])
	}
	if !bytes.Contains(withLoad["load"], []byte(`"load15":null`)) || !bytes.Contains(withLoad["load"], []byte(`"ncpu":null`)) || !bytes.Contains(withLoad["load"], []byte(`"load1":1`)) || !bytes.Contains(withLoad["load"], []byte(`"load5":2`)) {
		t.Fatalf("node CPU projection lost explicit null/value fields: %s", withLoad["load"])
	}
}

func t12FixtureBlock(t *testing.T, path, header string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_, remaining, found := strings.Cut(string(contents), header+"\n")
	if !found {
		t.Fatalf("fixture %s lacks %q", path, header)
	}
	if next := strings.Index(remaining, "\n### "); next >= 0 {
		remaining = remaining[:next]
	}
	return strings.TrimSpace(remaining)
}

func TestT12HostLoadCollectorsUseFixturesAndRetainPartialCPU(t *testing.T) {
	macPath := filepath.Join("testdata", "memory", "macos-sysctl.txt")
	macRun := func(_ context.Context, argv ...string) ([]byte, error) {
		switch strings.Join(argv, " ") {
		case "sysctl -n vm.loadavg":
			return []byte(t12FixtureBlock(t, macPath, "### command: sysctl -n vm.loadavg")), nil
		case "sysctl -n vm.swapusage":
			return []byte(t12FixtureBlock(t, macPath, "### command: sysctl -n vm.swapusage")), nil
		case "sysctl -n hw.ncpu":
			return []byte(t12FixtureBlock(t, macPath, "### command: sysctl -n hw.ncpu")), nil
		default:
			return nil, errors.New("unexpected fixture command")
		}
	}
	mac, err := collectDarwinHostLoad(t.Context(), macRun)
	if err != nil || mac.Load15 == nil || *mac.Load15 != 3.9 || mac.NCPU == nil || *mac.NCPU != 8 {
		t.Fatalf("Darwin fixture load=%+v err=%v", mac, err)
	}
	macPartial, err := collectDarwinHostLoad(t.Context(), func(ctx context.Context, argv ...string) ([]byte, error) {
		if strings.Join(argv, " ") == "sysctl -n hw.ncpu" {
			return nil, errors.New("fixture ncpu unavailable")
		}
		return macRun(ctx, argv...)
	})
	if err != nil || macPartial.NCPU != nil || macPartial.Load15 == nil {
		t.Fatalf("Darwin partial CPU collection=%+v err=%v", macPartial, err)
	}

	linuxPath := filepath.Join("testdata", "load", "linux-x86_64-loadavg.txt")
	linuxLoads := t12FixtureBlock(t, linuxPath, "### file: /proc/loadavg")
	linuxNCPU := t12FixtureBlock(t, linuxPath, "### command: nproc")
	meminfo := "SwapTotal: 16777216 kB\nSwapFree: 8388608 kB\n"
	linux, err := collectLinuxHostLoad(func(path string) ([]byte, error) {
		switch path {
		case "/proc/loadavg":
			return []byte(linuxLoads), nil
		case "/proc/meminfo":
			return []byte(meminfo), nil
		default:
			return nil, errors.New("unexpected fixture file")
		}
	}, func(_ context.Context, argv ...string) ([]byte, error) {
		if strings.Join(argv, " ") != "nproc" {
			return nil, errors.New("unexpected fixture command")
		}
		return []byte(linuxNCPU), nil
	})
	if err != nil || linux.Load15 == nil || *linux.Load15 != 8.5 || linux.NCPU == nil || *linux.NCPU != 8 {
		t.Fatalf("Linux fixture load=%+v err=%v", linux, err)
	}
	linuxPartial, err := collectLinuxHostLoad(func(path string) ([]byte, error) {
		if path == "/proc/loadavg" {
			return []byte(linuxLoads), nil
		}
		return []byte(meminfo), nil
	}, func(context.Context, ...string) ([]byte, error) { return nil, errors.New("fixture nproc unavailable") })
	if err != nil || linuxPartial.NCPU != nil || linuxPartial.Load15 == nil {
		t.Fatalf("Linux partial CPU collection=%+v err=%v", linuxPartial, err)
	}
}

func TestT12ConsoleLoadDoesNotAffectPlacement(t *testing.T) {
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "operator-token", "host-a": "node-token", "host-b": "node-token-b"}})
	if err != nil {
		t.Fatal(err)
	}
	hub.nodes["host-a"] = &hubNodeRecord{machineID: "host-a", accepting: true, state: "connected", agent: &hubAgent{}, activeJobs: map[string]HubActiveJob{}}
	hub.nodes["host-b"] = &hubNodeRecord{machineID: "host-b", accepting: true, state: "connected", agent: &hubAgent{}, activeJobs: map[string]HubActiveJob{}}
	policy := PlacementPolicy{LocalMachine: "host-a", SpillTargets: []string{"host-b"}, MaxActiveJobs: 5, LoadRatio: .5}
	metrics := placementMetrics{load: map[string]float64{"host-a": .1, "host-b": .2}, throttled: map[string]bool{}}
	now := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	before := hub.makePlacement(policy, metrics, "prometheus", now)
	hub.nodes["host-a"].hostLoad = &HubHostLoad{Load1: .1, Load5: .2, Load15: t12Float(99), NCPU: t12Int(1)}
	hub.nodes["host-b"].hostLoad = &HubHostLoad{Load1: .1, Load5: .2, Load15: t12Float(0), NCPU: t12Int(64)}
	after := hub.makePlacement(policy, metrics, "prometheus", now)
	beforeJSON, _ := json.Marshal(struct {
		Decision   string               `json:"decision"`
		Candidates []PlacementCandidate `json:"candidates"`
		Reason     string               `json:"reason"`
	}{before.Decision, before.Candidates, before.Reason})
	afterJSON, _ := json.Marshal(struct {
		Decision   string               `json:"decision"`
		Candidates []PlacementCandidate `json:"candidates"`
		Reason     string               `json:"reason"`
	}{after.Decision, after.Candidates, after.Reason})
	if !bytes.Equal(beforeJSON, afterJSON) {
		t.Fatalf("console CPU load changed placement: before=%s after=%s", beforeJSON, afterJSON)
	}
}

func TestT12ActiveJobsAPIAndHeartbeatMetadata(t *testing.T) {
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "operator-token", "host-a": "node-token", "host-b": "node-token-b"}})
	if err != nil {
		t.Fatal(err)
	}
	hub.jobs["job-a"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-a", AgentLabel: "worker-a", Epoch: 1, OwnerLane: "lane-a", Pane: "pane-a", Tier: "T2", Role: "worker", StartedAt: "2026-09-05T00:00:00Z", LastEventKind: "job.spawned", LastEventAt: "2026-09-05T00:01:00Z"}, Node: "host-a", Orphaned: true}
	hub.jobs["job-complete"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-complete", AgentLabel: "worker-a", Epoch: 1}, Node: "host-a", Completed: true}

	request := httptest.NewRequest(http.MethodGet, "/v1/jobs?machine=host-a", nil)
	request.Header.Set(hubAuthorizationHeader, "Bearer operator-token")
	recording := httptest.NewRecorder()
	hub.Handler().ServeHTTP(recording, request)
	if recording.Code != http.StatusOK {
		t.Fatalf("jobs status=%d body=%s", recording.Code, recording.Body.String())
	}
	var jobs struct {
		Jobs []hubConsoleJob `json:"jobs"`
	}
	if err := json.Unmarshal(recording.Body.Bytes(), &jobs); err != nil || len(jobs.Jobs) != 1 || jobs.Jobs[0].JobID != "job-a" || jobs.Jobs[0].Machine != "host-a" || jobs.Jobs[0].Pane != "pane-a" || jobs.Jobs[0].Tier != "T2" || jobs.Jobs[0].Role != "worker" {
		t.Fatalf("active jobs=%+v err=%v", jobs, err)
	}
	if orphaned := hub.orphanedJobs(); len(orphaned) != 1 || orphaned[0].JobID != "job-a" {
		t.Fatalf("orphan view does not share active definition: %+v", orphaned)
	}
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/v1/jobs", nil),
		httptest.NewRequest(http.MethodGet, "/v1/jobs?machine=bad/id", nil),
	} {
		want := http.StatusUnauthorized
		if strings.Contains(request.URL.RawQuery, "bad") {
			request.Header.Set(hubAuthorizationHeader, "Bearer operator-token")
			want = http.StatusBadRequest
		}
		response := httptest.NewRecorder()
		hub.Handler().ServeHTTP(response, request)
		if response.Code != want {
			t.Fatalf("jobs authorization/filter status=%d want=%d", response.Code, want)
		}
	}

	legacy, ok := decodeHubHeartbeatPayload([]byte(`{"status":"alive","active_jobs":[{"job_id":"job-old","agent_label":"worker-a","last_event_seq":1,"epoch":1}]}`))
	if !ok || len(legacy.ActiveJobs) != 1 {
		t.Fatalf("legacy active_jobs=%+v ok=%t", legacy.ActiveJobs, ok)
	}
	expanded, ok := decodeHubHeartbeatPayload([]byte(`{"status":"alive","active_jobs":[{"job_id":"job-new","agent_label":"worker-a","last_event_seq":1,"epoch":1,"owner_lane":"lane-a","pane":"pane-a","tier":"T2","role":"worker","started_at":"2026-09-05T00:00:00Z","last_event_kind":"job.spawned","last_event_at":"2026-09-05T00:01:00Z"}]}`))
	if !ok || len(expanded.ActiveJobs) != 1 || expanded.ActiveJobs[0].Pane != "pane-a" || expanded.ActiveJobs[0].Tier != "T2" {
		t.Fatalf("expanded active_jobs=%+v ok=%t", expanded.ActiveJobs, ok)
	}
	partial, ok := decodeHubHeartbeatPayload([]byte(`{"status":"alive","active_jobs":[{"job_id":"job-partial","agent_label":"worker-a","last_event_seq":1,"epoch":1,"pane":"","tier":"T9","role":"other","started_at":"nope","last_event_kind":"Bad","last_event_at":"nope"}]}`))
	if !ok || len(partial.ActiveJobs) != 1 || partial.ActiveJobs[0].Pane != "" || partial.ActiveJobs[0].Tier != "" || partial.ActiveJobs[0].Role != "" || partial.ActiveJobs[0].StartedAt != "" || partial.ActiveJobs[0].LastEventKind != "" || partial.ActiveJobs[0].LastEventAt != "" {
		t.Fatalf("bad optional active metadata did not drop fields: %+v ok=%t", partial.ActiveJobs, ok)
	}
}

func TestT12JobsCLIListsAndFiltersActiveRegistry(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/jobs" || request.URL.Query().Get("machine") != "host-a" || request.Header.Get(hubAuthorizationHeader) != "Bearer operator-token" {
			http.Error(writer, "unexpected jobs request", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(writer).Encode(struct {
			Jobs []hubConsoleJob `json:"jobs"`
		}{Jobs: []hubConsoleJob{{Machine: "host-a", JobID: "job-a", OwnerLane: "lane-a", Pane: "pane-a", Tier: "T2", Role: "worker"}}})
	}))
	defer server.Close()
	tokenPath := filepath.Join(t.TempDir(), "operator.env")
	if err := os.WriteFile(tokenPath, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN=operator-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := runJobsCLI([]string{"jobs", "--hub-url", server.URL, "--hub-token-env", tokenPath, "--machine", "host-a"}, &stdout, &stderr, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true})
	if code != ExitOK || !strings.Contains(stdout.String(), "MACHINE\tJOB_ID") || !strings.Contains(stdout.String(), "host-a\tjob-a\tlane-a\tpane-a\tT2\tworker") {
		t.Fatalf("jobs CLI code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestT12ScannerCarriesConsoleMetadataAndCompletionRemovesIt(t *testing.T) {
	root, events := t.TempDir(), ""
	events = filepath.Join(root, "jobs", "job-scan", "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(events, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("00001-job.claimed.json", `{"type":"job.claimed","created_at":"2026-09-05T00:00:00Z","agent_label":"worker-a","owner_lane":"lane-a","role":"worker","t_level":"T2","epoch":1}`)
	write("00002-job.spawned.json", `{"type":"job.spawned","created_at":"2026-09-05T00:01:00Z","pane_id":"pane-a"}`)
	write("00003-job.progress.json", `{"type":"job.progress","created_at":"2026-09-05T00:02:00Z"}`)
	jobs := scanHubActiveJobs(root)
	if len(jobs) != 1 || jobs[0].OwnerLane != "lane-a" || jobs[0].Pane != "pane-a" || jobs[0].Tier != "T2" || jobs[0].Role != "worker" || jobs[0].StartedAt != "2026-09-05T00:00:00Z" || jobs[0].LastEventKind != "job.progress" || jobs[0].LastEventAt != "2026-09-05T00:02:00Z" {
		t.Fatalf("scanned console metadata=%+v", jobs)
	}
	write("00004-job.completed.json", `{"type":"job.completed","created_at":"2026-09-05T00:03:00Z"}`)
	if jobs := scanHubActiveJobs(root); len(jobs) != 0 {
		t.Fatalf("completed job remained active: %+v", jobs)
	}
}

func TestT12SinkRelayAndLaneTextLimits(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-web":{"machine":"host-a","pane":"pane-a","sink":true},"lane-a":{"machine":"host-a","pane":"pane-a"}}}`, client, nil)
	source := &hubAgent{persisted: make(chan hubRelayPersistedEvent, 4), relays: make(chan hubRelayInjectEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{machineID: "host-a", agent: source, remoteMeta: map[string]string{}}
	seen := r20t5Subscribe(t, hub)
	sinkText := strings.Repeat("x", laneEventTextLimitSink)
	payload, _ := json.Marshal(map[string]any{"type": "event", "kind": "lane.event", "payload": map[string]any{"owner_lane": "lane-web", "event_id": "sink-1", "text": sinkText, "epoch": 1}})
	hub.handleAgentMessage("host-a", "test", source, payload)
	if got := fake.sequence(); len(got) != 2 || got[0] != "POST /v1/relay/events" || got[1] != "POST /v1/relay/events/101/delivered" {
		t.Fatalf("sink persist/delivered sequence=%v", got)
	}
	if acks := drainPersisted(source); len(acks) != 1 || acks[0].Kind != "lane.event" {
		t.Fatalf("sink persisted acknowledgements=%+v", acks)
	}
	if injected := drainRelays(source); injected != 0 {
		t.Fatalf("sink queued pane injections=%d", injected)
	}
	hub.mu.Lock()
	pending, timeouts := len(hub.r19a.relayPending), len(hub.r19a.relayTimeouts)
	hub.mu.Unlock()
	if pending != 0 || timeouts != 0 || len(seen("relay.unrouted")) != 0 || len(seen("lane.event")) != 1 {
		t.Fatalf("sink bookkeeping pending=%d timeouts=%d unrouted=%d inbound=%d", pending, timeouts, len(seen("relay.unrouted")), len(seen("lane.event")))
	}
	hub.replayUndeliveredLaneEvents(context.Background())
	if injected := drainRelays(source); injected != 0 {
		t.Fatalf("delivered sink replay injected=%d", injected)
	}

	tooLong := hubJobEventPayload{OwnerLane: "lane-a", EventID: "normal-too-long", Text: strings.Repeat("x", laneEventTextLimit+1), Epoch: 1}
	hub.relayLaneEvent(tooLong, source)
	if fake.rowCount() != 1 || len(seen("relay.rejected")) != 1 || !bytes.Contains(seen("relay.rejected")[0], []byte(`"reason":"text_too_long"`)) {
		t.Fatalf("non-sink long text persisted/rejection rows=%d rejected=%s", fake.rowCount(), seen("relay.rejected"))
	}
}

func TestT12SinkRouteValidationKeepsSinkAndRejectsBadPaneRoute(t *testing.T) {
	path := r20LanesFile(t, `{"lanes":{"lane-web":{},"lane-a":{"machine":"host-a","pane":"pane-a"}}}`)
	routes := loadReportRelayRoutes(path)
	if !routes["lane-web"].Sink || routes["lane-web"].Machine != "" || routes["lane-web"].Pane != "" {
		t.Fatalf("sink routes were not retained/normalized: %+v", routes)
	}
	if route, ok := routes["lane-a"]; !ok || route.Sink || route.Machine != "host-a" || route.Pane != "pane-a" {
		t.Fatalf("ordinary pane route changed: %+v", route)
	}
}

func TestT12EmitSinkLimitTruncatesAtNode(t *testing.T) {
	inbox := t.TempDir()
	var stderr bytes.Buffer
	code := runEmitCLI([]string{"--kind", "lane.event", "--lane", "lane-web", "--event-id", "sink-truncate", "--text", strings.Repeat("가", 4000), "--sink", "--inbox-root", inbox}, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")})
	if code != ExitOK {
		t.Fatalf("sink emit code=%d stderr=%q", code, stderr.String())
	}
	entries, err := os.ReadDir(filepath.Join(inbox, "events-lane"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("sink event files=%+v err=%v", entries, err)
	}
	contents, err := os.ReadFile(filepath.Join(inbox, "events-lane", entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	var event emitRecord
	if json.Unmarshal(contents, &event) != nil || len(event.Text) != laneEventTextLimitSink || !event.Truncated || !strings.HasSuffix(event.Text, "[truncated]") {
		t.Fatalf("sink truncation record=%+v bytes=%d", event, len(event.Text))
	}
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-web":{"sink":true}}}`, client, nil)
	seen := r20t5Subscribe(t, hub)
	hub.relayLaneEvent(hubJobEventPayload{OwnerLane: event.OwnerLane, EventID: event.EventID, Text: event.Text, Truncated: event.Truncated, Epoch: event.Epoch}, &hubAgent{persisted: make(chan hubRelayPersistedEvent, 1)})
	if fake.rowCount() != 1 || len(seen("relay.truncated")) != 1 {
		t.Fatalf("sink truncation was not persisted/broadcast: rows=%d broadcasts=%d", fake.rowCount(), len(seen("relay.truncated")))
	}
}

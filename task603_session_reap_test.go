package panewire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// #603 session-reap stage 1 (report only). The herdr fixtures in
// testdata/task603 are this repo's sanitized copy of a live agent.list,
// pane.list and tab.list capture: every key and value type is as observed
// (human sessions carry no "name" key at all), only paths, UUIDs, titles and
// ids were replaced. The jobs inbox reuses the observed event shapes: arbiter
// envelopes for claim/quota/spawn receipts and wrk's flat completed/joined/
// lost records.

var t603Now = time.Date(2026, 9, 23, 13, 0, 0, 0, time.UTC)

func t603HerdrResults(t *testing.T) map[string]any {
	t.Helper()
	results := map[string]any{}
	for method, file := range map[string]string{"agent.list": "herdr-agent-list.json", "pane.list": "herdr-pane-list.json", "tab.list": "herdr-tab-list.json"} {
		raw, err := os.ReadFile(filepath.Join("testdata", "task603", file))
		if err != nil {
			t.Fatal(err)
		}
		var decoded map[string]any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		results[method] = decoded
	}
	return results
}

// t603FixtureReport runs the production collection path end to end: a fake
// herdr socket answering with the fixture, the fixture jobs inbox, and
// collectSessionReapReport.
func t603FixtureReport(t *testing.T) (SessionReapReport, *t501HerdrServer) {
	t.Helper()
	herdr := t501NewHerdr(t, t603HerdrResults(t), nil)
	report := collectSessionReapReport(t.Context(), herdr.path, filepath.Join("testdata", "task603", "jobs"), 10*time.Minute, t603Now)
	return report, herdr
}

func t603RowsByPane(rows []SessionReapRow) map[string]SessionReapRow {
	byPane := make(map[string]SessionReapRow, len(rows))
	for _, row := range rows {
		byPane[row.PaneID] = row
	}
	return byPane
}

func TestT603FixtureJudgment(t *testing.T) {
	report, herdr := t603FixtureReport(t)
	if !report.Observed || !report.JobsReadable || report.Truncated || report.GraceSeconds != 600 || report.Schema != 1 {
		t.Fatalf("report header = %+v", report)
	}
	type verdict struct{ class, reason, job string }
	want := map[string]verdict{
		// observed sessions
		"w6:p1":  {sessionReapClassHeld, sessionReapReasonLabelUnknown, "400-old-20260901"}, // human session on a pane id an old job once used
		"w6:p3":  {sessionReapClassHeld, fleetCensusReasonNoTerminal, "consult-projects-20260923d"},
		"w2:p2":  {sessionReapClassHeld, fleetCensusReasonNoTerminal, "598-verify-20260923-1500"},
		"w2:p3":  {sessionReapClassBuilderTaskGate, "", "529-deploy-view-20260923-1535"},
		"w2:p1":  {sessionReapClassHeld, "status=working", "597-grade-table-20260923-2100"},
		"w2:p4":  {sessionReapClassHeld, "status=working", "603-session-reap-20260923-2100"},
		"w2:p25": {sessionReapClassCandidate, "", "601-verify-20260923-1100"},
		"w2:p26": {sessionReapClassHeld, fleetCensusReasonProtected, "consult-arch-20260923"},
		"w2:p27": {sessionReapClassHeld, sessionReapReasonLabelMismatch, "590-verify-20260923-1000"},
		"w2:p28": {sessionReapClassHeld, sessionReapReasonLabelUnknown, "591-impl-20260923-1000"},
		"w2:p29": {sessionReapClassHeld, sessionReapReasonProtectedRole, "checker-2-20260923"},
		"w2:p30": {sessionReapClassHeld, sessionReapReasonLabelConflict, "595-conflict-20260923"},
		"w2:p31": {sessionReapClassHeld, sessionReapReasonPaneAmbiguous, "596-second-20260923"},
		"w2:p32": {sessionReapClassBuilderTaskGate, "", "599-merged-builder-20260923"},
	}
	got := make(map[string]verdict, len(report.Rows))
	for _, row := range report.Rows {
		got[row.PaneID] = verdict{row.Class, row.Reason, row.JobID}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("verdicts\n got=%v\nwant=%v", got, want)
	}
	wantSummary := SessionReapSummary{Panes: 20, NoJob: 6, Candidate: 1, BuilderTaskGate: 2, Held: 11}
	if report.Summary != wantSummary {
		t.Fatalf("summary = %+v, want %+v", report.Summary, wantSummary)
	}
	// Rows are ordered candidates first so byte truncation drops held rows.
	if report.Rows[0].Class != sessionReapClassCandidate || report.Rows[1].Class != sessionReapClassBuilderTaskGate || report.Rows[2].Class != sessionReapClassBuilderTaskGate {
		t.Fatalf("row order = %+v", report.Rows[:3])
	}
	candidate := t603RowsByPane(report.Rows)["w2:p25"]
	if candidate.AgentName != "t601-verify" || candidate.Role != "worker" || candidate.TerminalKind != "job.completed" || candidate.Status != "idle" || candidate.TabID != "w2:t25" || candidate.OwnerLane != "b601-builder" {
		t.Fatalf("candidate row = %+v", candidate)
	}
	if builder := t603RowsByPane(report.Rows)["w2:p32"]; builder.Role != "builder" {
		t.Fatalf("captain claim must surface as builder role, got %+v", builder)
	}
	// Read-only: the collection issues exactly the three list reads.
	methods := herdr.Methods()
	sort.Strings(methods)
	if !reflect.DeepEqual(methods, []string{"agent.list", "pane.list", "tab.list"}) {
		t.Fatalf("herdr methods = %v", methods)
	}
	// The report the node would send is accepted by the hub decoder as is.
	payload, ok := marshalSessionReapReport(report)
	if !ok {
		t.Fatal("fixture report does not fit the wire budget")
	}
	if decoded, valid := decodeSessionReapReport(payload); !valid || !reflect.DeepEqual(decoded, report) {
		t.Fatalf("hub decode valid=%t\n got=%+v\nwant=%+v", valid, decoded, report)
	}
}

// Human and resident sessions: every observed pane without a spawn receipt —
// operator desk, director, and the unnamed shells — is never a row, and no
// unnamed pane is ever a candidate even when an old receipt names its id.
func TestT603HumanAndResidentSessionsNeverCandidates(t *testing.T) {
	report, _ := t603FixtureReport(t)
	rows := t603RowsByPane(report.Rows)
	for _, human := range []string{"w6:p2", "w3:p1", "w3:p2", "w4:p1", "w5:p1", "w5:p2"} {
		if row, listed := rows[human]; listed {
			t.Errorf("human session %s became a row: %+v", human, row)
		}
	}
	for _, row := range report.Rows {
		if row.AgentName == "" && row.Class != sessionReapClassHeld {
			t.Errorf("unnamed pane %s is %s", row.PaneID, row.Class)
		}
	}
}

// t603Inbox builds a small inbox from the observed event shapes.
func t603Inbox(t *testing.T, jobs map[string][]map[string]any) string {
	t.Helper()
	root := t.TempDir()
	for job, events := range jobs {
		for index, document := range events {
			raw, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			kind, _ := document["kind"].(string)
			t501RawJobEvent(t, root, job, fmt.Sprintf("%05d-%s.json", index+1, kind), raw)
		}
	}
	return root
}

func t603Claim(job, label, role, at string, keep bool) map[string]any {
	payload := map[string]any{"agent_label": label, "owner_lane": "b-owner", "parent_lane": nil, "role": role, "t_level": "T2"}
	if keep {
		payload["keep"] = true
	}
	return map[string]any{"created_at": at, "job_id": job, "kind": "job.claim", "payload": payload, "seq": 1}
}

func t603Spawned(job, label, pane, tab, at string) map[string]any {
	return map[string]any{"created_at": at, "job_id": job, "kind": "job.spawned", "payload": map[string]any{"label": label, "pane_id": pane, "profile": "opus", "tab_id": tab, "workspace": "w1"}, "seq": 2}
}

func t603Completed(job, label, pane, at string) map[string]any {
	return map[string]any{"kind": "job.completed", "job_id": job, "owner_lane": "b-owner", "label": label, "pane_id": pane, "host": "node-a", "report_path": "/home/operator/r.md", "report_last_line": "done", "epoch": 1, "created_at": at}
}

func t603View(panes ...[4]string) fleetCensusLocalView {
	view := fleetCensusLocalView{agents: map[string]fleetCensusPaneObs{}, tabs: map[string]*int{}, tabsOK: true, paneListOK: true}
	for _, pane := range panes {
		obs := fleetCensusPaneObs{PaneID: pane[0], TabID: pane[1], AgentName: pane[2], Status: pane[3], HasAgent: true, WorkspaceID: "w1"}
		view.agents[pane[0]] = obs
		view.panes = append(view.panes, obs)
		one := 1
		view.tabs[pane[1]] = &one
	}
	return view
}

func t603Judge(t *testing.T, jobs map[string][]map[string]any, view fleetCensusLocalView, grace time.Duration, now time.Time) map[string]SessionReapRow {
	t.Helper()
	scans, ok := scanFleetCensusJobs(t603Inbox(t, jobs))
	if !ok {
		t.Fatal("inbox unreadable")
	}
	rows, _ := judgeSessionReap(scans, view, grace, now)
	return t603RowsByPane(rows)
}

// AC2: the grace boundary is wrk's `age < grace`. One second short of the
// grace holds the pane; exactly the grace makes it a candidate.
func TestT603GraceBoundary(t *testing.T) {
	grace := 10 * time.Minute
	for _, test := range []struct {
		name      string
		completed time.Time
		class     string
		reason    string
	}{
		{"one second before grace", t603Now.Add(-grace + time.Second), sessionReapClassHeld, fleetCensusReasonWithinGrace},
		{"exactly at grace", t603Now.Add(-grace), sessionReapClassCandidate, ""},
		{"one second after grace", t603Now.Add(-grace - time.Second), sessionReapClassCandidate, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			at := test.completed.Format(time.RFC3339)
			rows := t603Judge(t, map[string][]map[string]any{
				"701-verify": {t603Claim("701-verify", "t701", "worker", "2026-09-23T09:00:00Z", false), t603Spawned("701-verify", "t701", "w1:p1", "w1:t1", "2026-09-23T09:00:00Z"), t603Completed("701-verify", "t701", "w1:p1", at)},
			}, t603View([4]string{"w1:p1", "w1:t1", "t701", "idle"}), grace, t603Now)
			if row := rows["w1:p1"]; row.Class != test.class || row.Reason != test.reason {
				t.Fatalf("row = %+v, want class=%s reason=%q", row, test.class, test.reason)
			}
		})
	}
}

// R1 counterexample from the AC review: a later claim under the same label
// with no spawn receipt may be the session now on this reused pane id.
func TestT603LabelReusedHolds(t *testing.T) {
	jobs := map[string][]map[string]any{
		"702-old": {t603Claim("702-old", "t702", "worker", "2026-09-23T09:00:00Z", false), t603Spawned("702-old", "t702", "w1:p1", "w1:t1", "2026-09-23T09:00:00Z"), t603Completed("702-old", "t702", "w1:p1", "2026-09-23T10:00:00Z")},
		"702-new": {t603Claim("702-new", "t702", "worker", "2026-09-23T11:00:00Z", false)},
	}
	rows := t603Judge(t, jobs, t603View([4]string{"w1:p1", "w1:t1", "t702", "idle"}), 10*time.Minute, t603Now)
	if row := rows["w1:p1"]; row.Class != sessionReapClassHeld || row.Reason != sessionReapReasonLabelReused || row.JobID != "702-old" {
		t.Fatalf("row = %+v", row)
	}
	// Without the later receiptless claim the same pane is a candidate, so
	// the hold above is caused by the reuse and nothing else.
	delete(jobs, "702-new")
	if row := t603Judge(t, jobs, t603View([4]string{"w1:p1", "w1:t1", "t702", "idle"}), 10*time.Minute, t603Now)["w1:p1"]; row.Class != sessionReapClassCandidate {
		t.Fatalf("control row = %+v", row)
	}
}

// keep is sticky and only a JSON true counts, mirroring wrk's `is True`.
func TestT603KeepMarker(t *testing.T) {
	for _, test := range []struct {
		name  string
		claim map[string]any
		held  bool
	}{
		{"claim keep true", t603Claim("703-a", "t703", "worker", "2026-09-23T09:00:00Z", true), true},
		{"no keep", t603Claim("703-a", "t703", "worker", "2026-09-23T09:00:00Z", false), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows := t603Judge(t, map[string][]map[string]any{
				"703-a": {test.claim, t603Spawned("703-a", "t703", "w1:p1", "w1:t1", "2026-09-23T09:00:00Z"), t603Completed("703-a", "t703", "w1:p1", "2026-09-23T10:00:00Z")},
			}, t603View([4]string{"w1:p1", "w1:t1", "t703", "idle"}), 10*time.Minute, t603Now)
			row := rows["w1:p1"]
			if test.held != (row.Class == sessionReapClassHeld && row.Reason == fleetCensusReasonProtected) || (!test.held && row.Class != sessionReapClassCandidate) {
				t.Fatalf("row = %+v, want held=%t", row, test.held)
			}
		})
	}
	// A kept builder is held too; builders never reach the reused wrk
	// pipeline, so this is the judgment's own keep gate.
	builderClaim := t603Claim("703-b", "b703", "builder", "2026-09-23T09:00:00Z", false)
	builderSpawn := t603Spawned("703-b", "b703", "w1:p2", "w1:t2", "2026-09-23T09:00:00Z")
	builderSpawn["payload"].(map[string]any)["keep"] = true
	rows := t603Judge(t, map[string][]map[string]any{"703-b": {builderClaim, builderSpawn}}, t603View([4]string{"w1:p2", "w1:t2", "b703", "idle"}), 10*time.Minute, t603Now)
	if row := rows["w1:p2"]; row.Class != sessionReapClassHeld || row.Reason != fleetCensusReasonProtected {
		t.Fatalf("kept builder row = %+v", row)
	}
	delete(builderSpawn["payload"].(map[string]any), "keep")
	rows = t603Judge(t, map[string][]map[string]any{"703-b": {builderClaim, builderSpawn}}, t603View([4]string{"w1:p2", "w1:t2", "b703", "idle"}), 10*time.Minute, t603Now)
	if row := rows["w1:p2"]; row.Class != sessionReapClassBuilderTaskGate {
		t.Fatalf("unkept builder control row = %+v", row)
	}
	for raw, want := range map[string]bool{"true": true, " true ": true, `"true"`: false, "1": false, "false": false, "null": false, "": false} {
		if got := fleetCensusTrue(json.RawMessage(raw)); got != want {
			t.Errorf("fleetCensusTrue(%q) = %t, want %t", raw, got, want)
		}
	}
}

// Unobservable inputs yield an empty report, never a partial one.
func TestT603UnobservableReportsNothing(t *testing.T) {
	down := t501NewHerdr(t, t603HerdrResults(t), map[string]bool{"agent.list": true})
	report := collectSessionReapReport(t.Context(), down.path, filepath.Join("testdata", "task603", "jobs"), 10*time.Minute, t603Now)
	if report.Observed || len(report.Rows) != 0 || report.Summary != (SessionReapSummary{}) {
		t.Fatalf("herdr down report = %+v", report)
	}
	up := t501NewHerdr(t, t603HerdrResults(t), nil)
	report = collectSessionReapReport(t.Context(), up.path, filepath.Join(t.TempDir(), "missing"), 10*time.Minute, t603Now)
	if report.JobsReadable || len(report.Rows) != 0 || report.Summary != (SessionReapSummary{}) {
		t.Fatalf("inbox missing report = %+v", report)
	}
	for _, report := range []SessionReapReport{
		{Schema: 1, GeneratedAt: t603Now.Format(time.RFC3339), Observed: false, JobsReadable: true, Rows: []SessionReapRow{}},
	} {
		payload, _ := marshalSessionReapReport(report)
		if _, valid := decodeSessionReapReport(payload); !valid {
			t.Fatalf("honest empty report rejected: %s", payload)
		}
	}
}

// I6: the periodic report is off unless an operator sets a valid interval.
func TestT603ReporterDefaultOff(t *testing.T) {
	for _, value := range []string{"", " ", "off", "0", "0s", "-1m", "banana", "10"} {
		if interval, enabled := sessionReapReportInterval(value); enabled {
			t.Errorf("interval %q enabled the reporter (%s)", value, interval)
		}
	}
	if interval, enabled := sessionReapReportInterval("5s"); !enabled || interval != time.Minute {
		t.Errorf("5s = %s %t, want clamped to 1m", interval, enabled)
	}
	if interval, enabled := sessionReapReportInterval("15m"); !enabled || interval != 15*time.Minute {
		t.Errorf("15m = %s %t", interval, enabled)
	}
	t.Setenv(sessionReapIntervalEnv, "")
	client := &HubClient{events: make(chan hubClientEvent, 1)}
	if startSessionReapReporter(t.Context(), client, "/nonexistent.sock", t.TempDir(), nil) {
		t.Fatal("reporter started with the interval unset")
	}
	if len(client.events) != 0 {
		t.Fatal("reporter queued an event while off")
	}
	if got := sessionReapGrace(""); got != 10*time.Minute {
		t.Errorf("default grace = %s", got)
	}
	if got := sessionReapGrace("2h"); got != 2*time.Hour {
		t.Errorf("grace 2h = %s", got)
	}
	// Beyond what the hub decoder accepts falls back instead of producing
	// reports the hub would silently drop.
	for value, want := range map[string]time.Duration{"30d": 30 * 24 * time.Hour, "31d": 10 * time.Minute, "45d": 10 * time.Minute, "-5m": 10 * time.Minute} {
		if got := sessionReapGrace(value); got != want {
			t.Errorf("grace %s = %s, want %s", value, got, want)
		}
	}
	for _, grace := range []time.Duration{sessionReapGrace("30d"), sessionReapGrace("31d")} {
		payload, _ := marshalSessionReapReport(SessionReapReport{Schema: 1, GeneratedAt: t603Now.Format(time.RFC3339), GraceSeconds: int64(grace / time.Second), Observed: true, JobsReadable: true, Rows: []SessionReapRow{}})
		if _, valid := decodeSessionReapReport(payload); !valid {
			t.Errorf("hub rejects a report with grace %s", grace)
		}
	}
}

// AC3: no pane-, tab-, or process-ending call anywhere in the session-reap
// sources. The check is scoped to the files this change added.
func TestT603NoClosingCodePath(t *testing.T) {
	forbidden := regexp.MustCompile(`(?i)"(pane|tab|agent|workspace)\.(close|kill|stop|remove|delete)"|\b(tab|pane)\s+close\b|CloseTab|ClosePane|KillPane|syscall\.Kill|os\.FindProcess|exec\.Command|send-keys|send_keys|agent\.send|pane\.send`)
	for _, file := range []string{"session_reap.go", "hub_session_reap.go"} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if match := forbidden.Find(raw); match != nil {
			t.Errorf("%s contains a closing/process call: %q", file, match)
		}
		if bytes.Contains(raw, []byte("client.Call(")) {
			t.Errorf("%s calls herdr directly; only readFleetCensusLocal may", file)
		}
	}
}

func t603Hub(t *testing.T, lanes string) (*HubServer, *hubAgent) {
	t.Helper()
	config := HubServerConfig{Tokens: map[string]string{"operator": "op-token", "node-a": "node-token"}, Now: func() time.Time { return t603Now }}
	if lanes != "" {
		path := filepath.Join(t.TempDir(), "lanes.json")
		if err := os.WriteFile(path, []byte(lanes), 0o600); err != nil {
			t.Fatal(err)
		}
		config.ReportRelayPath = path
	}
	hub, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{}
	hub.nodes["node-a"] = &hubNodeRecord{machineID: "node-a", agent: agent, state: "connected"}
	return hub, agent
}

func t603Envelope(t *testing.T, report SessionReapReport) []byte {
	t.Helper()
	payload, ok := marshalSessionReapReport(report)
	if !ok {
		t.Fatal("report does not fit")
	}
	raw, err := json.Marshal(hubClientWireEvent(hubClientEvent{Kind: sessionReapEventKind, Payload: payload}))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func t603GetSessionReap(t *testing.T, hub *HubServer, token string) (int, []HubSessionReapNode) {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/v1/session-reap", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	writer := httptest.NewRecorder()
	hub.Handler().ServeHTTP(writer, request)
	var body struct {
		Nodes []HubSessionReapNode `json:"nodes"`
	}
	if writer.Code == http.StatusOK {
		if err := json.Unmarshal(writer.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
	}
	return writer.Code, body.Nodes
}

func TestT603HubStoresAndServesReport(t *testing.T) {
	report, _ := t603FixtureReport(t)
	hub, agent := t603Hub(t, "")
	hub.handleAgentMessage("node-a", "127.0.0.1", agent, t603Envelope(t, report))
	if hub.unknownMessages != 0 {
		t.Fatalf("valid report counted unknown (%d)", hub.unknownMessages)
	}
	if code, _ := t603GetSessionReap(t, hub, "node-token"); code != http.StatusUnauthorized {
		t.Fatalf("node token read = %d, want 401", code)
	}
	code, nodes := t603GetSessionReap(t, hub, "op-token")
	if code != http.StatusOK || len(nodes) != 1 || nodes[0].MachineID != "node-a" || nodes[0].Stale || nodes[0].State != "connected" || !nodes[0].ReceivedAt.Equal(t603Now) {
		t.Fatalf("GET = %d %+v", code, nodes)
	}
	if !reflect.DeepEqual(nodes[0].Report, report) {
		t.Fatalf("served report differs\n got=%+v\nwant=%+v", nodes[0].Report, report)
	}
	hub.nodes["node-a"].state = "stale"
	if _, nodes := t603GetSessionReap(t, hub, "op-token"); len(nodes) != 1 || !nodes[0].Stale {
		t.Fatal("report from a stale node not marked stale")
	}
}

// The hub only ever downgrades: a pane another lane routes to is held, and
// unreadable lanes hold every non-held row.
func TestT603HubLaneRouteDowngrade(t *testing.T) {
	report, _ := t603FixtureReport(t)
	hub, agent := t603Hub(t, `{"lanes":{"checker-9":{"machine":"node-a","pane":"w2:p25"},"b529-deploy-view":{"machine":"node-a","pane":"w2:p3"},"sink-lane":{"machine":"node-a","pane":"w2:p32","sink":true}}}`)
	hub.handleAgentMessage("node-a", "127.0.0.1", agent, t603Envelope(t, report))
	_, nodes := t603GetSessionReap(t, hub, "op-token")
	if len(nodes) != 1 {
		t.Fatalf("hub did not keep the report: nodes=%+v unknown=%d", nodes, hub.unknownMessages)
	}
	rows := t603RowsByPane(nodes[0].Report.Rows)
	if row := rows["w2:p25"]; row.Class != sessionReapClassHeld || row.Reason != sessionReapReasonLaneRoute {
		t.Fatalf("resident-routed candidate = %+v", row)
	}
	if row := rows["w2:p3"]; row.Class != sessionReapClassBuilderTaskGate {
		t.Fatalf("a builder's own lane must not hold it: %+v", row)
	}
	if row := rows["w2:p32"]; row.Class != sessionReapClassBuilderTaskGate {
		t.Fatalf("a sink lane must not hold a pane: %+v", row)
	}
	if summary := nodes[0].Report.Summary; summary.Candidate != 0 || summary.Held != 12 || summary.BuilderTaskGate != 2 {
		t.Fatalf("summary after downgrade = %+v", summary)
	}
	// The stored copy is untouched: the downgrade is a read-time projection.
	if stored := hub.sessionReap["node-a"]; stored == nil || len(stored.report.Rows) == 0 || stored.report.Rows[0].Class != sessionReapClassCandidate {
		t.Fatal("downgrade mutated the stored report")
	}
	broken, brokenAgent := t603Hub(t, `{"lanes":`)
	broken.handleAgentMessage("node-a", "127.0.0.1", brokenAgent, t603Envelope(t, report))
	_, nodes = t603GetSessionReap(t, broken, "op-token")
	if len(nodes) != 1 {
		t.Fatalf("hub with broken lanes did not keep the report: %+v", nodes)
	}
	for _, row := range nodes[0].Report.Rows {
		if row.Class != sessionReapClassHeld {
			t.Fatalf("unreadable lanes left %+v unheld", row)
		}
	}
}

func TestT603HubRejectsMalformedReports(t *testing.T) {
	report, _ := t603FixtureReport(t)
	valid, _ := marshalSessionReapReport(report)
	mutate := func(edit func(map[string]any)) []byte {
		var document map[string]any
		_ = json.Unmarshal(valid, &document)
		edit(document)
		raw, _ := json.Marshal(document)
		return raw
	}
	firstRow := func(document map[string]any) map[string]any {
		return document["rows"].([]any)[0].(map[string]any)
	}
	for name, payload := range map[string][]byte{
		"unknown top field":      mutate(func(d map[string]any) { d["close"] = true }),
		"schema 2":               mutate(func(d map[string]any) { d["schema"] = 2 }),
		"unknown row field":      mutate(func(d map[string]any) { firstRow(d)["action"] = "close" }),
		"unknown class":          mutate(func(d map[string]any) { firstRow(d)["class"] = "close-now" }),
		"candidate with reason":  mutate(func(d map[string]any) { firstRow(d)["reason"] = "x" }),
		"candidate without name": mutate(func(d map[string]any) { delete(firstRow(d), "agent_name") }),
		"candidate builder role": mutate(func(d map[string]any) { firstRow(d)["role"] = "builder" }),
		"candidate working":      mutate(func(d map[string]any) { firstRow(d)["status"] = "working" }),
		"candidate no terminal": mutate(func(d map[string]any) {
			r := firstRow(d)
			delete(r, "terminal_kind")
			delete(r, "terminal_at")
			delete(r, "terminal_age_seconds")
		}),
		"summary does not add up": mutate(func(d map[string]any) { d["summary"].(map[string]any)["no_job"] = 7.0 }),
		"rows beyond summary": mutate(func(d map[string]any) {
			d["summary"].(map[string]any)["candidate"] = 0.0
			d["summary"].(map[string]any)["no_job"] = 7.0
		}),
		"rows while unobserved": mutate(func(d map[string]any) { d["observed"] = false }),
		"duplicate pane": mutate(func(d map[string]any) {
			rows := d["rows"].([]any)
			rows[1].(map[string]any)["pane_id"] = firstRow(d)["pane_id"]
		}),
		"null field":       mutate(func(d map[string]any) { d["rows"] = nil }),
		"bad generated_at": mutate(func(d map[string]any) { d["generated_at"] = "yesterday" }),
	} {
		if _, valid := decodeSessionReapReport(payload); valid {
			t.Errorf("%s: accepted", name)
		}
	}
	hub, agent := t603Hub(t, "")
	raw, _ := json.Marshal(hubClientWireEvent(hubClientEvent{Kind: sessionReapEventKind, Payload: mutate(func(d map[string]any) { d["schema"] = 2 })}))
	hub.handleAgentMessage("node-a", "127.0.0.1", agent, raw)
	if hub.unknownMessages != 1 || len(hub.sessionReap) != 0 {
		t.Fatalf("invalid report: unknown=%d stored=%d", hub.unknownMessages, len(hub.sessionReap))
	}
	// A transient connection (CLI probe) may not publish a report.
	agent.transient = true
	hub.handleAgentMessage("node-a", "127.0.0.1", agent, t603Envelope(t, report))
	if len(hub.sessionReap) != 0 || hub.unknownMessages != 2 {
		t.Fatalf("transient report: unknown=%d stored=%d", hub.unknownMessages, len(hub.sessionReap))
	}
}

// Truncation keeps the report inside the hub message budget and drops held
// rows before candidates.
func TestT603ReportByteBudget(t *testing.T) {
	report := SessionReapReport{Schema: 1, GeneratedAt: t603Now.Format(time.RFC3339), GraceSeconds: 600, Observed: true, JobsReadable: true, Rows: []SessionReapRow{}}
	long := strings.Repeat("n", 500)
	age := int64(3600)
	for index := 0; index < 90; index++ {
		row := SessionReapRow{PaneID: "w1:p" + strings.Repeat("9", index%5+1) + string(rune('a'+index%26)) + string(rune('a'+index/26)), Status: "idle", JobID: "job-" + string(rune('a'+index%26)) + string(rune('a'+index/26)), AgentName: long, Class: sessionReapClassHeld, Reason: "status=idle"}
		if index == 0 {
			row.Class, row.Reason, row.Role, row.TerminalKind, row.TerminalAt, row.TerminalAgeSeconds = sessionReapClassCandidate, "", "worker", "job.completed", t603Now.Format(time.RFC3339), &age
			report.Summary.Candidate++
		} else {
			report.Summary.Held++
		}
		report.Rows = append(report.Rows, row)
	}
	report.Summary.Panes = report.Summary.Candidate + report.Summary.Held
	payload, ok := marshalSessionReapReport(report)
	if !ok {
		t.Fatal("marshal failed")
	}
	decoded, valid := decodeSessionReapReport(payload)
	if !valid || !decoded.Truncated || len(decoded.Rows) == 0 || len(decoded.Rows) >= 64 || decoded.Rows[0].Class != sessionReapClassCandidate {
		t.Fatalf("valid=%t truncated=%t rows=%d", valid, decoded.Truncated, len(decoded.Rows))
	}
	envelope, _ := json.Marshal(hubClientWireEvent(hubClientEvent{Kind: sessionReapEventKind, Payload: payload}))
	if len(envelope) >= hubMaxMessageBytes {
		t.Fatalf("envelope %d bytes exceeds budget", len(envelope))
	}
}

// The one-shot CLI prints the local judgment and sends nothing anywhere.
func TestT603CLI(t *testing.T) {
	herdr := t501NewHerdr(t, t603HerdrResults(t), nil)
	var stdout, stderr bytes.Buffer
	code := runSessionReapCLI([]string{"--json", "--jobs-root", filepath.Join("testdata", "task603", "jobs"), "--herdr-socket", herdr.path}, &stdout, &stderr, hubCLIDeps{Now: func() time.Time { return t603Now }})
	if code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	var report SessionReapReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil || report.Summary.Candidate != 1 {
		t.Fatalf("json output err=%v summary=%+v", err, report.Summary)
	}
	stdout.Reset()
	code = runSessionReapCLI([]string{"--jobs-root", filepath.Join("testdata", "task603", "jobs"), "--herdr-socket", herdr.path}, &stdout, io.Discard, hubCLIDeps{Now: func() time.Time { return t603Now }})
	if code != 0 || !strings.Contains(stdout.String(), "candidate pane=w2:p25 name=t601-verify job=601-verify-20260923-1100 role=worker status=idle reason=-") {
		t.Fatalf("text output code=%d:\n%s", code, stdout.String())
	}
	for _, args := range [][]string{{"--apply"}, {"--grace"}, {"--grace", "soon"}, {"positional"}, {"--json", "--json"}} {
		if code := runSessionReapCLI(args, io.Discard, io.Discard, hubCLIDeps{}); code != ExitUsage {
			t.Errorf("args %v = %d, want usage", args, code)
		}
	}
}

// A builder later reclaimed with a role outside wrk's vocabulary (or none)
// is held as protected-role: the latest role is checked before the sticky
// builder marking can route the pane to the builder task gate.
func TestT603LatestRoleOutsideVocabularyBeatsStickyBuilder(t *testing.T) {
	for _, latest := range []string{"resident", ""} {
		reclaim := t603Claim("704-b", "b704", latest, "2026-09-23T09:30:00Z", false)
		reclaim["kind"] = "job.reclaim"
		jobs := map[string][]map[string]any{"704-b": {
			t603Claim("704-b", "b704", "builder", "2026-09-23T09:00:00Z", false),
			reclaim,
			t603Spawned("704-b", "b704", "w1:p1", "w1:t1", "2026-09-23T10:00:00Z"),
		}}
		rows := t603Judge(t, jobs, t603View([4]string{"w1:p1", "w1:t1", "b704", "idle"}), 10*time.Minute, t603Now)
		if row := rows["w1:p1"]; row.Class != sessionReapClassHeld || row.Reason != sessionReapReasonProtectedRole {
			t.Fatalf("latest role %q: row = %+v", latest, row)
		}
	}
	// Control: a builder reclaimed as captain (the legacy alias) still
	// reaches the builder task gate.
	reclaim := t603Claim("704-b", "b704", "captain", "2026-09-23T09:30:00Z", false)
	reclaim["kind"] = "job.reclaim"
	jobs := map[string][]map[string]any{"704-b": {t603Claim("704-b", "b704", "builder", "2026-09-23T09:00:00Z", false), reclaim, t603Spawned("704-b", "b704", "w1:p1", "w1:t1", "2026-09-23T10:00:00Z")}}
	if row := t603Judge(t, jobs, t603View([4]string{"w1:p1", "w1:t1", "b704", "idle"}), 10*time.Minute, t603Now)["w1:p1"]; row.Class != sessionReapClassBuilderTaskGate {
		t.Fatalf("captain control row = %+v", row)
	}
}

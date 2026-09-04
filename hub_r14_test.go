package panewire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHubR14OrphanRecoveryRedispatchAndEpochFence(t *testing.T) {
	clock := time.Date(2026, 9, 2, 6, 0, 0, 0, time.UTC)
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{"operator": r6OperatorToken, "node-a": r6NodeAToken, "node-b": r6NodeBToken, "node-c": "node-c-token-123456"},
		Now:    func() time.Time { return clock }, GracePeriod: 2 * time.Second, OrphanGrace: 2 * time.Second, KeepaliveInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	old := &hubAgent{revocations: make(chan hubJobRevokedEvent, 1)}
	hub.connect("node-a", "r14", "fixture", old, false)
	first := HubActiveJob{JobID: "recover-me", AgentLabel: "wrk-a", LastEventSeq: 1, Epoch: 1}
	hub.observeActiveJobs("node-a", []HubActiveJob{first}, clock)
	hub.disconnect("node-a", old)
	clock = clock.Add(2*time.Second - time.Nanosecond)
	r14Sweep(hub)
	if got := hub.orphanedJobs(); len(got) != 0 {
		t.Fatalf("job orphaned before grace: %+v", got)
	}
	clock = clock.Add(time.Nanosecond)
	r14Sweep(hub)
	if got := hub.orphanedJobs(); len(got) != 1 || got[0].JobID != first.JobID {
		t.Fatalf("missing orphan: %+v", got)
	}
	r14Sweep(hub)
	if countR14UIEvents(hub, "orphaned") != 1 {
		t.Fatalf("orphan was duplicated: %+v", hub.uiEvents)
	}
	hub.connect("node-a", "r14", "fixture", old, false)
	// Mutant F1: deleting the active-return recovery branch leaves this orphan
	// eligible for reassignment and turns the assertion red.
	hub.observeActiveJobs("node-a", []HubActiveJob{first}, clock)
	if got := hub.orphanedJobs(); len(got) != 0 || hub.jobs[first.JobID].Orphaned {
		t.Fatalf("working return left sticky orphan: jobs=%+v record=%+v", got, hub.jobs[first.JobID])
	}
	if countR14UIEvents(hub, "recovered") != 1 {
		t.Fatalf("working return did not record recovery: %+v", hub.uiEvents)
	}
	// Terminal return is separately retained for the original completion case.
	hub.disconnect("node-a", old)
	clock = clock.Add(2 * time.Second)
	r14Sweep(hub)
	hub.connect("node-a", "r14", "fixture", old, false)
	hub.observeActiveJobs("node-a", nil, clock) // local completion removed it from the active scan
	if countR14UIEvents(hub, "recovered") != 2 {
		t.Fatalf("reconnect completion did not recover orphan: %+v", hub.uiEvents)
	}

	second := HubActiveJob{JobID: "move-me", AgentLabel: "wrk-a", LastEventSeq: 2, Epoch: 1}
	hub.connect("node-a", "r14", "fixture", old, false)
	hub.observeActiveJobs("node-a", []HubActiveJob{second}, clock)
	hub.disconnect("node-a", old)
	clock = clock.Add(2 * time.Second)
	r14Sweep(hub)
	result, ok := hub.reassignJob(second.JobID, "node-b")
	if !ok || result.From != "node-a" || result.To != "node-b" || result.Epoch != 2 {
		t.Fatalf("bad reassignment: result=%+v ok=%t", result, ok)
	}
	newOwner := &hubAgent{revocations: make(chan hubJobRevokedEvent, 4), assignments: make(chan hubJobAssignedEvent, 4)}
	hub.connect("node-b", "r14", "fixture", newOwner, false)
	hub.sendHeartbeatDirectives("node-b", newOwner)
	// Mutant F2: deleting assigned-epoch delivery leaves the new owner unable
	// to adopt the hub fence and makes this assertion red.
	select {
	case assigned := <-newOwner.assignments:
		if assigned.JobID != second.JobID || assigned.Epoch != 2 {
			t.Fatalf("bad assigned epoch: %+v", assigned)
		}
	default:
		t.Fatal("new owner was not sent hub-issued epoch")
	}
	hub.observeActiveJobs("node-b", []HubActiveJob{{JobID: second.JobID, AgentLabel: "wrk-b", LastEventSeq: 3, Epoch: 2}}, clock)
	// New-owner metadata must never erase the predecessor fence.
	if got := hub.jobs[second.JobID].FencedNodes["node-a"]; got != 2 {
		t.Fatalf("new owner heartbeat erased A fence: %+v", hub.jobs[second.JobID])
	}
	reconnected := &hubAgent{revocations: make(chan hubJobRevokedEvent, 1)}
	hub.connect("node-a", "r14", "fixture", reconnected, false)
	hub.observeActiveJobs("node-a", []HubActiveJob{second}, clock)
	hub.observeActiveJobs("node-a", []HubActiveJob{{JobID: second.JobID, AgentLabel: "wrk-a", LastEventSeq: 99, Epoch: 99}}, clock)
	if job := hub.jobs[second.JobID]; job.Node != "node-b" || job.Epoch != 2 {
		t.Fatalf("node self-promoted its epoch: %+v", job)
	}
	select {
	case revoked := <-reconnected.revocations:
		if revoked.JobID != second.JobID || revoked.Epoch != 2 {
			t.Fatalf("bad revocation: %+v", revoked)
		}
	default:
		t.Fatal("old epoch was not revoked")
	}
	// A fenced heartbeat may be alive, but it cannot become B's liveness.
	hub.disconnect("node-a", reconnected)
	clock = clock.Add(10 * time.Second)
	r14Sweep(hub)
	if got := hub.orphanedJobs(); len(got) != 0 || hub.jobs[second.JobID].Node != "node-b" || hub.jobs[second.JobID].Epoch != 2 {
		t.Fatalf("fenced A heartbeat affected B liveness: orphans=%+v job=%+v", got, hub.jobs[second.JobID])
	}
	if hub.observeJobCompletion("node-a", hubJobEventPayload{JobID: second.JobID, Epoch: 1}, clock) {
		t.Fatal("stale epoch completion was accepted")
	}
	if !hub.observeJobCompletion("node-b", hubJobEventPayload{JobID: second.JobID, Epoch: 2}, clock) {
		t.Fatal("new owner epoch N+1 completion was rejected")
	}

	// A→B→A makes A the current owner again. The stale A epoch must be
	// rejected by the epoch check itself, not merely by the node-owner check.
	chain := HubActiveJob{JobID: "chain-me", AgentLabel: "wrk-a", LastEventSeq: 1, Epoch: 1}
	hub.connect("node-a", "r14", "fixture", old, false)
	hub.observeActiveJobs("node-a", []HubActiveJob{chain}, clock)
	hub.disconnect("node-a", old)
	clock = clock.Add(2 * time.Second)
	r14Sweep(hub)
	if _, ok := hub.reassignJob(chain.JobID, "node-b"); !ok {
		t.Fatal("A→B reassignment failed")
	}
	hub.connect("node-b", "r14", "fixture", newOwner, false)
	hub.observeActiveJobs("node-b", []HubActiveJob{{JobID: chain.JobID, AgentLabel: "wrk-b", LastEventSeq: 2, Epoch: 2}}, clock)
	hub.disconnect("node-b", newOwner)
	clock = clock.Add(2 * time.Second)
	r14Sweep(hub)
	if result, ok := hub.reassignJob(chain.JobID, "node-a"); !ok || result.Epoch != 3 {
		t.Fatalf("B→A reassignment failed: %+v ok=%t", result, ok)
	}
	if hub.observeJobCompletion("node-a", hubJobEventPayload{JobID: chain.JobID, Epoch: 1}, clock) {
		t.Fatal("current owner accepted its old epoch completion")
	}

	// A→B→C preserves both historical fences, and B's N+1 completion remains
	// rejected once C owns N+2.
	third := HubActiveJob{JobID: "three-hop", AgentLabel: "wrk-a", LastEventSeq: 1, Epoch: 1}
	hub.connect("node-a", "r14", "fixture", old, false)
	hub.observeActiveJobs("node-a", []HubActiveJob{third}, clock)
	hub.disconnect("node-a", old)
	clock = clock.Add(2 * time.Second)
	r14Sweep(hub)
	_, _ = hub.reassignJob(third.JobID, "node-b")
	hub.connect("node-b", "r14", "fixture", newOwner, false)
	hub.observeActiveJobs("node-b", []HubActiveJob{{JobID: third.JobID, AgentLabel: "wrk-b", LastEventSeq: 2, Epoch: 2}}, clock)
	hub.disconnect("node-b", newOwner)
	clock = clock.Add(2 * time.Second)
	r14Sweep(hub)
	if result, ok := hub.reassignJob(third.JobID, "node-c"); !ok || result.Epoch != 3 {
		t.Fatalf("B→C reassignment failed: %+v ok=%t", result, ok)
	}
	if got := hub.jobs[third.JobID].FencedNodes; got["node-a"] != 3 || got["node-b"] != 3 {
		t.Fatalf("redispatch history was not retained: %+v", got)
	}
	fencedB := &hubAgent{revocations: make(chan hubJobRevokedEvent, 2)}
	hub.connect("node-b", "r14", "fixture", fencedB, false)
	found := false
	for len(fencedB.revocations) > 0 {
		revoked := <-fencedB.revocations
		if revoked.JobID == third.JobID && revoked.Epoch == 3 {
			found = true
		}
	}
	if !found {
		t.Fatal("B did not receive pending N+2 revocation")
	}
	if hub.observeJobCompletion("node-b", hubJobEventPayload{JobID: third.JobID, Epoch: 2}, clock) {
		t.Fatal("B completion at N+1 was accepted after N+2 reassignment")
	}
}

func TestHubR14ReassignImmediatelyDeliversAssignedEpoch(t *testing.T) {
	clock := time.Date(2026, 9, 4, 3, 10, 0, 0, time.UTC)
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": r6OperatorToken, "node-a": r6NodeAToken, "node-b": r6NodeBToken}, Now: func() time.Time { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	hub.jobs["immediate-assignment"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "immediate-assignment", AgentLabel: "wrk-a", LastEventSeq: 1, Epoch: 1}, Node: "node-a", Orphaned: true, FencedNodes: make(map[string]uint64)}
	recipient := &hubAgent{assignments: make(chan hubJobAssignedEvent, 1)}
	hub.connect("node-b", "r14", "fixture", recipient, false)
	// Mutant M5b: deleting reassignJob's immediate queueAssignment leaves this
	// receive empty even though the heartbeat re-delivery path still exists.
	if result, ok := hub.reassignJob("immediate-assignment", "node-b"); !ok || result.Epoch != 2 {
		t.Fatalf("reassignment failed: result=%+v ok=%t", result, ok)
	}
	select {
	case assigned := <-recipient.assignments:
		if assigned.JobID != "immediate-assignment" || assigned.Epoch != 2 {
			t.Fatalf("bad immediate assignment: %+v", assigned)
		}
	default:
		t.Fatal("reassign did not immediately deliver assigned epoch")
	}
}

func TestHubR14HeartbeatJobMetadataIsBoundedAndBodyFree(t *testing.T) {
	jobs := make([]HubActiveJob, 40)
	for i := range jobs {
		jobs[i] = HubActiveJob{JobID: fmt.Sprintf("job-%02d", i), AgentLabel: "wrk-a", LastEventSeq: uint64(i + 1), Epoch: 1}
	}
	payload, err := json.Marshal(hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}, ActiveJobs: jobs})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := decodeHubHeartbeatPayload(payload); ok {
		t.Fatal("40 distinct active jobs was accepted")
	}
	payload, err = json.Marshal(hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}, ActiveJobs: jobs[:32]})
	if err != nil || func() bool { _, ok := decodeHubHeartbeatPayload(payload); return !ok }() {
		t.Fatalf("valid 32-job envelope was rejected: err=%v", err)
	}
	if _, ok := decodeHubHeartbeatPayload([]byte(`{"status":"alive","checks":{},"active_jobs":[{"job_id":"job-a","agent_label":"wrk-a","last_event_seq":1,"epoch":1,"brief":"do not transport this"}]}`)); ok {
		t.Fatal("brief body carrier was accepted")
	}
}

func TestHubR14JobEndpointsRequireOperator(t *testing.T) {
	clock := time.Date(2026, 9, 2, 7, 0, 0, 0, time.UTC)
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": r6OperatorToken, "node-a": r6NodeAToken, "node-b": r6NodeBToken}, Now: func() time.Time { return clock }, GracePeriod: time.Second, OrphanGrace: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{revocations: make(chan hubJobRevokedEvent, 1)}
	hub.connect("node-a", "r14", "fixture", agent, false)
	hub.observeActiveJobs("node-a", []HubActiveJob{{JobID: "operator-gate", AgentLabel: "wrk-a", LastEventSeq: 1, Epoch: 1}}, clock)
	hub.disconnect("node-a", agent)
	clock = clock.Add(time.Second)
	r14Sweep(hub)
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	body := []byte(`{"job_id":"operator-gate","to":"node-b"}`)
	for _, endpoint := range []string{"/v1/jobs/orphaned", "/v1/jobs/reassign"} {
		method, requestBody := http.MethodGet, bytes.NewReader(nil)
		if endpoint == "/v1/jobs/reassign" {
			method, requestBody = http.MethodPost, bytes.NewReader(body)
		}
		request, err := http.NewRequest(method, server.URL+endpoint, requestBody)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set(hubAuthorizationHeader, "Bearer "+r6NodeAToken)
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatalf("node token was accepted for %s: %d", endpoint, response.StatusCode)
		}
	}
	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/jobs/reassign", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(hubAuthorizationHeader, "Bearer "+r6OperatorToken)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("operator reassignment rejected: %d", response.StatusCode)
	}
}

func TestHubR14RevocationRetriesUntilLocalAck(t *testing.T) {
	clock := time.Date(2026, 9, 2, 8, 0, 0, 0, time.UTC)
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": r6OperatorToken, "node-a": r6NodeAToken, "node-b": r6NodeBToken}, Now: func() time.Time { return clock }, GracePeriod: time.Second, OrphanGrace: time.Second, KeepaliveInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	old := &hubAgent{revocations: make(chan hubJobRevokedEvent, 1)}
	hub.connect("node-a", "r14", "fixture", old, false)
	job := HubActiveJob{JobID: "retry-me", AgentLabel: "wrk-a", LastEventSeq: 1, Epoch: 1}
	hub.observeActiveJobs("node-a", []HubActiveJob{job}, clock)
	hub.disconnect("node-a", old)
	clock = clock.Add(time.Second)
	r14Sweep(hub)
	if _, ok := hub.reassignJob(job.JobID, "node-b"); !ok {
		t.Fatal("reassignment failed")
	}
	// Simulate a saturated per-agent channel at reconnect. The hub must retain
	// the pending marker and retry it on the next heartbeat.
	full := &hubAgent{revocations: make(chan hubJobRevokedEvent, 1)}
	full.revocations <- hubJobRevokedEvent{Type: "fixture"}
	hub.connect("node-a", "r14", "fixture", full, false)
	if pending := hub.pendingRevocations["node-a"][job.JobID]; pending.Epoch != 2 {
		t.Fatalf("full channel lost pending revocation: %+v", hub.pendingRevocations)
	}
	<-full.revocations
	hub.observeActiveJobs("node-a", []HubActiveJob{job}, clock)
	select {
	case revoked := <-full.revocations:
		if revoked.JobID != job.JobID || revoked.Epoch != 2 {
			t.Fatalf("retry delivered wrong revocation: %+v", revoked)
		}
	default:
		t.Fatal("pending revocation was not retried")
	}
	if !hub.acknowledgeRevocation("node-a", hubJobEventPayload{JobID: job.JobID, Epoch: 2}) {
		t.Fatal("local marker acknowledgement rejected")
	}
	if _, exists := hub.pendingRevocations["node-a"]; exists {
		t.Fatalf("ack did not clear pending marker: %+v", hub.pendingRevocations)
	}
}

func TestHubR14LocalScannerAndRevocationAreMetadataOnly(t *testing.T) {
	root := t.TempDir()
	events := filepath.Join(root, "jobs", "job-a", "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(events, "00001-job.claimed.json"), []byte(`{"type":"job.claimed","agent_label":"wrk-a","epoch":1,"push_sha":"abcdef1","brief":"secret brief text"}`), 0600); err != nil {
		t.Fatal(err)
	}
	jobs := scanHubActiveJobs(root)
	if len(jobs) != 1 || jobs[0].JobID != "job-a" || jobs[0].AgentLabel != "wrk-a" || strings.Contains(string(mustR14JSON(t, jobs)), "secret brief") {
		t.Fatalf("scanner leaked or lost metadata: %+v", jobs)
	}
	if err := writeHubRevocation(root, hubJobRevokedEvent{Type: "job.revoked", JobID: "job-a", Epoch: 2}); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(events)
	if err != nil || len(entries) != 2 || !strings.HasSuffix(entries[1].Name(), "job.revoked.json") {
		t.Fatalf("revocation event missing: entries=%v err=%v", entries, err)
	}
	contents, err := os.ReadFile(filepath.Join(events, entries[1].Name()))
	if err != nil || strings.Contains(string(contents), "secret brief") || !strings.Contains(string(contents), `"epoch":2`) {
		t.Fatalf("revocation was not metadata-only: %q err=%v", contents, err)
	}
	if err := os.WriteFile(filepath.Join(events, "00003-job.completed.json"), []byte(`{"type":"job.completed","epoch":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{jobsInboxRoot: root, completedJobs: make(map[string]uint64)}
	eventsOut := client.jobCompletionEvents()
	if len(eventsOut) != 1 || eventsOut[0].Kind != "job.completed" || !strings.Contains(string(eventsOut[0].Payload), `"epoch":2`) {
		t.Fatalf("completion producer missing or malformed: %+v", eventsOut)
	}
	if repeated := client.jobCompletionEvents(); len(repeated) != 0 {
		t.Fatalf("completion producer duplicated terminal event: %+v", repeated)
	}
}

func TestHubJobScannerReadsArbiterEnvelopeAndKeepsFlatCompatibility(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Format(time.RFC3339)
	envelope := filepath.Join(root, "jobs", "job-20260101-0000", "events")
	if err := os.MkdirAll(envelope, 0700); err != nil {
		t.Fatal(err)
	}
	// This is the arbiter.record_event shape and key order, not a scanner-only fixture.
	if err := os.WriteFile(filepath.Join(envelope, "00001-job.claim.json"), []byte(`{"created_at":"`+now+`","job_id":"job-20260101-0000","kind":"job.claim","payload":{"agent_label":"wrk-a","owner_lane":"lane-a","t_level":"T1"},"seq":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	jobs := scanHubActiveJobs(root)
	if len(jobs) != 1 || jobs[0].AgentLabel != "wrk-a" || jobs[0].Epoch != 1 {
		t.Fatalf("M1/M4: envelope claim was not active with default epoch 1: %+v", jobs)
	}
	flat := filepath.Join(root, "jobs", "job-flat", "events")
	if err := os.MkdirAll(flat, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(flat, "00001-job.claimed.json"), []byte(`{"type":"job.claimed","agent_label":"wrk-a","epoch":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if got := scanHubActiveJobs(root); len(got) != 2 {
		t.Fatalf("flat compatibility claim disappeared: %+v", got)
	}
	if err := os.WriteFile(filepath.Join(envelope, "00002-job.completed.json"), []byte(`{"created_at":"`+now+`","job_id":"job-20260101-0000","kind":"job.completed","payload":{"agent_label":"wrk-a","owner_lane":"lane-a","label":"wrk-a","report_path":"report.md","report_last_line":"VERDICT: DONE","host":"host-a"},"seq":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	completed := scanHubCompletedJobs(root)
	if len(completed) != 1 || completed[0].OwnerLane != "lane-a" || completed[0].ReportPath != "report.md" {
		t.Fatalf("envelope completion was not reported: %+v", completed)
	}
}

func TestHubActiveScannerNewestFirstAndAgeCut(t *testing.T) {
	root := t.TempDir()
	old := time.Now().UTC().Add(-73 * time.Hour).Format(time.RFC3339)
	fresh := time.Now().UTC().Format(time.RFC3339)
	legacyLast := time.Now().UTC().Add(2 * time.Hour).Format(time.RFC3339)
	for i := 0; i < 1000; i++ {
		name := fmt.Sprintf("job-legacy-%04d", i)
		dir := filepath.Join(root, "jobs", name, "events")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		// The last event is new, but the still-open claim is beyond the replay age.
		if err := os.WriteFile(filepath.Join(dir, "00001-job.claim.json"), []byte(`{"created_at":"`+old+`","kind":"job.claim","payload":{"agent_label":"wrk-a"},"seq":1}`), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "00002-note.json"), []byte(`{"created_at":"`+legacyLast+`","kind":"note","payload":{},"seq":2}`), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 40; i++ {
		name := fmt.Sprintf("job-a-fresh-%04d", i)
		dir := filepath.Join(root, "jobs", name, "events")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "00001-job.claim.json"), []byte(`{"created_at":"`+fresh+`","kind":"job.claim","payload":{"agent_label":"wrk-a"},"seq":1}`), 0600); err != nil {
			t.Fatal(err)
		}
	}
	dir := filepath.Join(root, "jobs", "job-z-newest", "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	newest := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	if err := os.WriteFile(filepath.Join(dir, "00001-job.claim.json"), []byte(`{"created_at":"`+newest+`","job_id":"job-z-newest","kind":"job.claim","payload":{"agent_label":"wrk-a","owner_lane":"lane-a","t_level":"T1"},"seq":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	jobs := scanHubActiveJobs(root)
	found := false
	for _, job := range jobs {
		found = found || job.JobID == "job-z-newest"
	}
	if len(jobs) != 32 || !found {
		t.Fatalf("M2/M3: newest valid job was displaced by legacy entries: %+v", jobs)
	}
}

func TestHubCompletedScannerAgeCut(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "jobs", "job-old", "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-73 * time.Hour).Format(time.RFC3339)
	if err := os.WriteFile(filepath.Join(dir, "00001-job.completed.json"), []byte(`{"created_at":"`+old+`","kind":"job.completed","epoch":1,"payload":{"owner_lane":"lane-a","report_path":"report.md"},"seq":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if got := scanHubCompletedJobs(root); len(got) != 0 {
		t.Fatalf("completed age cut removed (restart would replay): %+v", got)
	}
}

func countR14UIEvents(hub *HubServer, phase string) int {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	count := 0
	for _, event := range hub.uiEvents {
		if event.Kind == "job" && event.Phase == phase {
			count++
		}
	}
	return count
}

// Direct hub-agent fixtures have no websocket. Suppress unrelated server
// keepalive writes so these tests exercise only job state transitions.
func r14Sweep(hub *HubServer) {
	hub.mu.Lock()
	for _, node := range hub.nodes {
		node.lastKeepaliveSent = hub.now().UTC()
	}
	hub.mu.Unlock()
	hub.Sweep()
}

func mustR14JSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHubR14OrphanRecoveryRedispatchAndEpochFence(t *testing.T) {
	clock := time.Date(2026, 9, 2, 6, 0, 0, 0, time.UTC)
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{"operator": r6OperatorToken, "node-a": r6NodeAToken, "node-b": r6NodeBToken},
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
	hub.Sweep()
	if got := hub.orphanedJobs(); len(got) != 0 {
		t.Fatalf("job orphaned before grace: %+v", got)
	}
	clock = clock.Add(time.Nanosecond)
	hub.Sweep()
	if got := hub.orphanedJobs(); len(got) != 1 || got[0].JobID != first.JobID {
		t.Fatalf("missing orphan: %+v", got)
	}
	hub.Sweep()
	if countR14UIEvents(hub, "orphaned") != 1 {
		t.Fatalf("orphan was duplicated: %+v", hub.uiEvents)
	}
	hub.connect("node-a", "r14", "fixture", old, false)
	hub.observeActiveJobs("node-a", nil, clock) // local completion removed it from the active scan
	if countR14UIEvents(hub, "recovered") != 1 {
		t.Fatalf("reconnect completion did not recover orphan: %+v", hub.uiEvents)
	}

	second := HubActiveJob{JobID: "move-me", AgentLabel: "wrk-a", LastEventSeq: 2, Epoch: 1}
	hub.connect("node-a", "r14", "fixture", old, false)
	hub.observeActiveJobs("node-a", []HubActiveJob{second}, clock)
	hub.disconnect("node-a", old)
	clock = clock.Add(2 * time.Second)
	hub.Sweep()
	result, ok := hub.reassignJob(second.JobID, "node-b")
	if !ok || result.From != "node-a" || result.To != "node-b" || result.Epoch != 2 {
		t.Fatalf("bad reassignment: result=%+v ok=%t", result, ok)
	}
	reconnected := &hubAgent{revocations: make(chan hubJobRevokedEvent, 1)}
	hub.connect("node-a", "r14", "fixture", reconnected, false)
	hub.observeActiveJobs("node-a", []HubActiveJob{second}, clock)
	select {
	case revoked := <-reconnected.revocations:
		if revoked.JobID != second.JobID || revoked.Epoch != 2 {
			t.Fatalf("bad revocation: %+v", revoked)
		}
	default:
		t.Fatal("old epoch was not revoked")
	}
	if hub.observeJobCompletion("node-a", hubJobEventPayload{JobID: second.JobID, Epoch: 1}, clock) {
		t.Fatal("stale epoch completion was accepted")
	}
}

func TestHubR14HeartbeatJobMetadataIsBoundedAndBodyFree(t *testing.T) {
	jobs := make([]HubActiveJob, 33)
	for i := range jobs {
		jobs[i] = HubActiveJob{JobID: "job-" + strings.Repeat("x", 2) + string(rune('a'+i%26)), AgentLabel: "wrk-a", Epoch: 1}
	}
	payload, err := json.Marshal(hubHeartbeatPayload{Status: "alive", ActiveJobs: jobs})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := decodeHubHeartbeatPayload(payload); ok {
		t.Fatal("more than 32 active jobs was accepted")
	}
	if _, ok := decodeHubHeartbeatPayload([]byte(`{"status":"alive","active_jobs":[{"job_id":"job-a","agent_label":"wrk-a","last_event_seq":1,"epoch":1,"brief":"do not transport this"}]}`)); ok {
		t.Fatal("brief body carrier was accepted")
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

func mustR14JSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

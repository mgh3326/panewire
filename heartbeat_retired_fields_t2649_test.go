package panewire

import (
	"encoding/json"
	"testing"
)

// Acceptance 3: hub and nodes deploy in either order, so a heartbeat with the
// retired fields and one without them both decode.

const heartbeatOldNodeT2649 = `{
	"status": "alive",
	"checks": {"service": "ok"},
	"quota": {"status": "ok"},
	"load_error": "any text\nincluding what the old decoder refused",
	"holds_active": true,
	"host_load": {"load1": 1.5, "load5": 2.5, "load15": 3.5, "ncpu": 8, "swap_used_gb": 0.5, "worker_procs": 2},
	"host_memory": {"free_pct": 40, "compressed_mb": 10, "swap_used_mb": 20, "psi_some_avg10": 0.1, "source": "proc_meminfo", "swap_in_per_s": 1, "pressure": 0},
	"active_jobs": [{"job_id": "job-a", "agent_label": "agent-a", "last_event_seq": 7, "push_sha": "abcdef1", "epoch": 1}]
}`

const heartbeatNewNodeT2649 = `{
	"status": "alive",
	"checks": {"service": "ok"},
	"host_load": {"load5": 2.5, "ncpu": 8, "swap_used_gb": 0.5, "worker_procs": 2},
	"host_memory": {"free_pct": 40, "source": "proc_meminfo"},
	"active_jobs": [{"job_id": "job-a", "agent_label": "agent-a", "epoch": 1}]
}`

func TestHeartbeatOldNodeWithRetiredFieldsDecodes(t *testing.T) {
	heartbeat, ok := decodeHubHeartbeatPayload([]byte(heartbeatOldNodeT2649))
	if !ok {
		t.Fatal("an old node's heartbeat was rejected")
	}
	if heartbeat.HostLoad == nil || heartbeat.HostLoad.Load5 != 2.5 || heartbeat.HostLoad.WorkerProcs != 2 {
		t.Fatalf("host_load=%+v", heartbeat.HostLoad)
	}
	if heartbeat.HostMemory == nil || *heartbeat.HostMemory.FreePct != 40 {
		t.Fatalf("host_memory=%+v", heartbeat.HostMemory)
	}
	if len(heartbeat.ActiveJobs) != 1 || heartbeat.ActiveJobs[0].JobID != "job-a" || heartbeat.ActiveJobs[0].Epoch != 1 {
		t.Fatalf("active_jobs=%+v", heartbeat.ActiveJobs)
	}
}

func TestHeartbeatNewNodeWithoutRetiredFieldsDecodes(t *testing.T) {
	heartbeat, ok := decodeHubHeartbeatPayload([]byte(heartbeatNewNodeT2649))
	if !ok {
		t.Fatal("a heartbeat without the retired fields was rejected")
	}
	if heartbeat.HostLoad == nil || heartbeat.HostLoad.Load5 != 2.5 || heartbeat.HostLoad.Load1 != nil || heartbeat.HostLoad.Load15 != nil {
		t.Fatalf("host_load=%+v", heartbeat.HostLoad)
	}
	if heartbeat.HostMemory == nil || heartbeat.HostMemory.SwapUsedMB != nil || heartbeat.HostMemory.CompressedMB != nil || heartbeat.HostMemory.PSISomeAvg10 != nil {
		t.Fatalf("host_memory=%+v", heartbeat.HostMemory)
	}
	if len(heartbeat.ActiveJobs) != 1 || heartbeat.ActiveJobs[0].LastEventSeq != 0 {
		t.Fatalf("active_jobs=%+v", heartbeat.ActiveJobs)
	}
	// A retired measurement stays unavailable on the console, not a made-up zero.
	view := hubNodeLoadFromHostLoad(heartbeat.HostLoad)
	if view == nil || view.Load1 != nil || view.Load5 == nil || *view.Load5 != 2.5 {
		t.Fatalf("console load=%+v", view)
	}
}

// What stays required is what a reader still needs.
func TestHeartbeatStillRejectsWhatReadersNeed(t *testing.T) {
	for name, payload := range map[string]string{
		"unknown top-level field": `{"status":"alive","unexpected":true}`,
		"load without load5":      `{"status":"alive","host_load":{"swap_used_gb":0,"worker_procs":0}}`,
		"unknown load field":      `{"status":"alive","host_load":{"load5":1,"swap_used_gb":0,"worker_procs":0,"x":1}}`,
		"memory without free_pct": `{"status":"alive","host_memory":{"source":"proc_meminfo"}}`,
		"unknown memory field":    `{"status":"alive","host_memory":{"free_pct":1,"source":"proc_meminfo","x":1}}`,
		"job without epoch":       `{"status":"alive","active_jobs":[{"job_id":"job-a","agent_label":"agent-a","x":1}]}`,
		"unknown job field":       `{"status":"alive","active_jobs":[{"job_id":"job-a","agent_label":"agent-a","epoch":1,"x":1}]}`,
	} {
		if _, ok := decodeHubHeartbeatPayload([]byte(payload)); ok {
			t.Errorf("%s was accepted", name)
		}
	}
}

// A node's own heartbeat no longer carries the fields with no reader, and still
// carries what an older hub requires.
func TestNodeHeartbeatDropsUnreadFieldsKeepsOldHubRequirements(t *testing.T) {
	load1 := 1.5
	payload, err := json.Marshal(hubHeartbeatPayload{Status: "alive", HostLoad: &HubHostLoad{Load1: &load1, Load5: 2}, ActiveJobs: []HubActiveJob{{JobID: "job-a", AgentLabel: "agent-a", LastEventSeq: 3, Epoch: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"quota", "load_error", "holds_active"} {
		if _, present := fields[name]; present {
			t.Errorf("heartbeat still carries %s: %s", name, payload)
		}
	}
	var load map[string]json.RawMessage
	var jobs []map[string]json.RawMessage
	if json.Unmarshal(fields["host_load"], &load) != nil || json.Unmarshal(fields["active_jobs"], &jobs) != nil {
		t.Fatalf("payload=%s", payload)
	}
	if _, present := load["load1"]; !present {
		t.Errorf("an older hub requires load1: %s", payload)
	}
	if _, present := jobs[0]["last_event_seq"]; !present {
		t.Errorf("an older hub requires last_event_seq: %s", payload)
	}
	if _, present := jobs[0]["push_sha"]; present {
		t.Errorf("heartbeat still carries push_sha: %s", payload)
	}
}

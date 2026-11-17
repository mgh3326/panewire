package panewire

import (
	"path/filepath"
	"testing"
)

// #662 review: a node re-registering a changed capability on the same
// connection keeps the active jobs its heartbeat reported, and gains the
// capability; placement never sees its pools as free in between.
func TestSameConnectionHelloKeepsActiveJobs_t4632(t *testing.T) {
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	agent := &hubAgent{}
	h.connect("machine-a", "v", "addr", agent, true, hubSpawnCapabilities)
	h.mu.Lock()
	h.nodes["machine-a"].activeJobs["job-1"] = HubActiveJob{JobID: "job-1", AgentLabel: "job-1", Epoch: 1}
	h.nodes["machine-a"].hostMemory = &HubHostMemory{}
	h.nodes["machine-a"].powerSource = "battery"
	since := h.nodes["machine-a"].connectedSince
	h.mu.Unlock()
	h.connect("machine-a", "v", "addr", agent, true, append(append([]string(nil), hubSpawnCapabilities...), hubCapabilityCloseOps))
	h.mu.Lock()
	defer h.mu.Unlock()
	record := h.nodes["machine-a"]
	if _, ok := record.activeJobs["job-1"]; !ok {
		t.Fatalf("active jobs %v lost across a same-connection hello", record.activeJobs)
	}
	if record.hostMemory == nil || record.powerSource != "battery" || !record.connectedSince.Equal(since) {
		t.Fatalf("admission telemetry reset by a same-connection hello: memory=%v power=%q since=%v", record.hostMemory, record.powerSource, record.connectedSince)
	}
	if !record.capabilities[hubCapabilityCloseOps] {
		t.Fatal("re-registration did not record close_ops_v1")
	}
}

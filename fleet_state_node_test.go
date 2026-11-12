package panewire

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fsWriteJobEvent(t *testing.T, root, job, name, body string) {
	t.Helper()
	events := filepath.Join(root, "jobs", job, "events")
	if err := os.MkdirAll(events, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(events, name), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// fsRunNode runs a real node client against the test hub until the test ends.
// The node gets a private HOME, so its default spawn.json and hk credential
// paths never reach a real one (hk 3570); configure may still name files.
func fsRunNode(t *testing.T, server string, configure func(*HubClient)) *HubClient {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	client, err := NewHubClient(HubClientConfig{
		URL: r6WSURL(server, ""), MachineID: "node-a", Token: fsNodeAToken, Accepting: true,
		AllowInsecureForTests: true, PingInterval: 20 * time.Millisecond, Warn: func(string) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	client.statePublish = true
	configure(client)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return client
}

// The node refreshes its configured pools on a schedule and the hub holds the
// reading under the node's own host.
func TestFleetStateNodeSkipsUnmeasurableQuota(t *testing.T) {
	hub, server := fsHub(t, "")
	var calls, warnings atomic.Int32
	fsRunNode(t, server.URL, func(client *HubClient) {
		client.stateQuotaPools = []string{"grok"}
		client.stateQuotaInterval = 10 * time.Second
		client.warn = func(message string) {
			if strings.Contains(message, "grok") && strings.Contains(message, "unmeasurable") {
				warnings.Add(1)
			}
		}
	})
	eventually(t, 3*time.Second, "unmeasurable quota warning", func() bool { return warnings.Load() == 1 })
	if calls.Load() != 0 {
		t.Fatalf("node executed external quota CLI %d times", calls.Load())
	}
	hub.fleetState.mu.Lock()
	defer hub.fleetState.mu.Unlock()
	for key := range hub.fleetState.latest {
		if key.kind == fleetStateKindQuota {
			t.Fatalf("unmeasurable quota was published: %+v", key)
		}
	}
}

func TestQuotaRefreshOpSkipsUnmeasurablePool(t *testing.T) {
	client := &HubClient{}
	var calls int
	var warnings []string
	client.warn = func(message string) { warnings = append(warnings, message) }
	result, code := nodeRun(t, client, nodeCommandQuotaRefresh, map[string]any{"pool": "grok"})
	if code != "refresh_failed" || result != nil {
		t.Fatalf("unmeasurable op result=%v code=%q", result, code)
	}
	if calls != 0 || len(warnings) != 1 || !strings.Contains(warnings[0], "grok") || !strings.Contains(warnings[0], "측정 소스가 없습니다") {
		t.Fatalf("external calls=%d warnings=%q", calls, warnings)
	}
}

func TestParseFleetQuotaPools(t *testing.T) {
	if pools, ok := parseFleetQuotaPools(" claude, codex,claude ,"); !ok || strings.Join(pools, ",") != "claude,codex" {
		t.Fatalf("pools=%v ok=%v", pools, ok)
	}
	if _, ok := parseFleetQuotaPools("claude;rm"); ok {
		t.Fatal("invalid pool accepted")
	}
}

// hk 2649: a node no longer publishes spawn values, whatever jobs it runs.
func TestFleetStateNodeDoesNotPublishSpawn(t *testing.T) {
	hub, server := fsHub(t, "")
	inbox := t.TempDir()
	fsWriteJobEvent(t, inbox, "job-e2e", "00001-job.claim.json", `{"type":"job.claim","epoch":1,"agent_label":"agent-a"}`)
	fsRunNode(t, server.URL, func(client *HubClient) { client.jobsInboxRoot = inbox })
	eventually(t, 5*time.Second, "job reached the hub", func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		record := hub.nodes["node-a"]
		return record != nil && record.activeJobs["job-e2e"].JobID != ""
	})
	for _, message := range fsSnapshot(t, server).States {
		if message.Kind == fleetStateKindSpawn {
			t.Fatalf("a spawn value was published: %+v", message)
		}
	}
}

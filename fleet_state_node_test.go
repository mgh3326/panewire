package panewire

import (
	"context"
	"encoding/json"
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

const fsPaceleverFresh = `{"id":"claude","buckets":[{"label":"5h","window":"5h","used_pct":42,"resets_at":"2026-11-01T05:00:00.5Z","scope":{"kind":"account"},"horizon":"short"},{"label":"7d opus","window":"7d","used_pct":null,"scope":{"kind":"model","name":"opus"}}],"pool_class":"spend","source":"handoffkeep","stale":false,"fetched_at":1761962400.25,"account_fp":"0123456789abcdef","account_fp_kind":"account"}`

func TestFleetQuotaFromPacelever(t *testing.T) {
	publish, ok := fleetQuotaFromPacelever("claude", []byte(fsPaceleverFresh))
	if !ok {
		t.Fatal("fresh reading refused")
	}
	quota := publish.Quota
	if publish.Key != fsAccountKey || quota.AccountKey != fsAccountKey || quota.Pool != "claude" || quota.Source != "handoffkeep" || !publish.MeasuredAt.Equal(time.UnixMilli(1761962400250).UTC()) {
		t.Fatalf("publish = %+v quota=%+v", publish, quota)
	}
	if len(quota.Buckets) != 1 || quota.Buckets[0].Window != "5h" || *quota.Buckets[0].UsedPct != 42 || quota.Buckets[0].ResetsAt != "2026-11-01T05:00:00Z" || quota.Buckets[0].Scope != "account" {
		t.Fatalf("buckets = %+v (the unmeasured bucket must be left out)", quota.Buckets)
	}
	published := strings.Replace(fsPaceleverFresh, `"account_fp":"0123456789abcdef","account_fp_kind":"account"`, `"publish_fp":"fedcba9876543210","publish_fp_kind":"account"`, 1)
	if publish, ok := fleetQuotaFromPacelever("claude", []byte(published)); !ok || publish.Key != "claude:fedcba9876543210" {
		t.Fatalf("publish_fp reading key=%q ok=%v", publish.Key, ok)
	}
	for name, raw := range map[string]string{
		"stale":           strings.Replace(fsPaceleverFresh, `"stale":false`, `"stale":true`, 1),
		"error":           strings.Replace(fsPaceleverFresh, `"stale":false`, `"stale":false,"error":"usage HTTP 500"`, 1),
		"other pool":      strings.Replace(fsPaceleverFresh, `"id":"claude"`, `"id":"codex"`, 1),
		"no account":      strings.Replace(fsPaceleverFresh, `"account_fp_kind":"account"`, `"account_fp_kind":"machine"`, 1),
		"no fetched_at":   strings.Replace(fsPaceleverFresh, `"fetched_at":1761962400.25,`, ``, 1),
		"bad resets_at":   strings.Replace(fsPaceleverFresh, `2026-11-01T05:00:00.5Z`, `tomorrow`, 1),
		"no measured bkt": strings.Replace(fsPaceleverFresh, `"used_pct":42`, `"used_pct":null`, 1),
		"huge fetched_at": strings.Replace(fsPaceleverFresh, `"fetched_at":1761962400.25`, `"fetched_at":1e300`, 1),
		"not json":        `pacelever: unknown provider`,
	} {
		if _, ok := fleetQuotaFromPacelever("claude", []byte(raw)); ok {
			t.Errorf("%s reading accepted", name)
		}
	}
}

// The node refreshes its configured pools on a schedule and the hub holds the
// reading under the node's own host.
func TestFleetStateNodeSkipsUnmeasurableQuota(t *testing.T) {
	hub, server := fsHub(t, "")
	var calls, warnings atomic.Int32
	fsRunNode(t, server.URL, func(client *HubClient) {
		client.stateQuotaPools = []string{"grok"}
		client.stateQuotaInterval = 10 * time.Second
		client.nodePacelever = func(context.Context, ...string) ([]byte, error) { calls.Add(1); return nil, nil }
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
	client.nodePacelever = func(context.Context, ...string) ([]byte, error) { calls++; return nil, nil }
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

// Review #142: an all-null reading is refused by the node itself, not only by
// the hub's decoder.
func TestFleetQuotaAllUnmeasuredBucketsRefused(t *testing.T) {
	var reading paceleverReading
	raw := strings.Replace(fsPaceleverFresh, `"used_pct":42`, `"used_pct":null`, 1)
	if json.Unmarshal([]byte(raw), &reading) != nil || len(reading.Buckets) != 2 {
		t.Fatal("fixture")
	}
	if _, ok := fleetQuotaFromPacelever("claude", []byte(raw)); ok {
		t.Fatal("a reading with no measured bucket was published")
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

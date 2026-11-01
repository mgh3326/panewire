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
func fsRunNode(t *testing.T, server string, configure func(*HubClient)) *HubClient {
	t.Helper()
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

// Acceptance 4, end to end: a job runtug spawned on the node appears on the
// hub stream as spawn start, and as spawn end once it completes, with job,
// profile and host.
func TestFleetStateNodePublishesSpawnStartEnd(t *testing.T) {
	_, server := fsHub(t, "")
	events, _ := fsStream(t, server, fsOperator())
	inbox := t.TempDir()
	fsWriteJobEvent(t, inbox, "job-e2e", "00001-job.claim.json", `{"type":"job.claim","epoch":1,"agent_label":"agent-a"}`)
	fsWriteJobEvent(t, inbox, "job-e2e", "00002-job.spawned.json", `{"kind":"job.spawned","payload":{"profile":"builder-opus","host":"node-a","launch_profile":"opus@high"}}`)
	fsRunNode(t, server.URL, func(client *HubClient) { client.jobsInboxRoot = inbox })

	start := fsNext(t, events, "spawn start", func(e fsEvent) bool { return e.message.Kind == fleetStateKindSpawn })
	if spawn := start.message.Spawn; start.message.Host != "node-a" || spawn.Phase != "start" || spawn.Job != "job-e2e" || spawn.Profile != "builder-opus" || spawn.Host != "node-a" {
		t.Fatalf("start = %+v spawn=%+v", start.message, spawn)
	}
	fsWriteJobEvent(t, inbox, "job-e2e", "00003-job.completed.json", `{"type":"job.completed","epoch":1}`)
	end := fsNext(t, events, "spawn end", func(e fsEvent) bool {
		return e.message.Kind == fleetStateKindSpawn && e.message.Spawn.Phase == "end"
	})
	if spawn := end.message.Spawn; spawn.Job != "job-e2e" || spawn.Profile != "builder-opus" || spawn.Host != "node-a" {
		t.Fatalf("end spawn=%+v", spawn)
	}
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
func TestFleetStateNodePublishesQuota(t *testing.T) {
	hub, server := fsHub(t, "")
	var calls atomic.Int32
	fsRunNode(t, server.URL, func(client *HubClient) {
		client.stateQuotaPools = []string{"claude"}
		client.stateQuotaInterval = 10 * time.Second
		client.nodePacelever = func(_ context.Context, args ...string) ([]byte, error) {
			calls.Add(1)
			if strings.Join(args, " ") != "refresh claude" {
				t.Errorf("pacelever argv = %q", args)
			}
			return []byte(fsPaceleverFresh), nil
		}
	})
	eventually(t, 3*time.Second, "quota from node", func() bool {
		message, ok := fsLatest(hub, "node-a", fleetStateKindQuota, fsAccountKey)
		return ok && message.Quota.Buckets[0].UsedPct != nil && *message.Quota.Buckets[0].UsedPct == 42
	})
	if calls.Load() < 1 {
		t.Fatal("pacelever was not run")
	}
}

func TestFleetSpawnsOffWithoutStatePublish(t *testing.T) {
	client := &HubClient{events: make(chan hubClientEvent, 4), warn: func(string) {}}
	client.publishFleetSpawns([]HubActiveJob{{JobID: "job-a", Epoch: 1}}, time.Now())
	if len(client.events) != 0 {
		t.Fatal("spawn state published without --state-publish")
	}
	client.statePublish = true
	client.publishFleetSpawns([]HubActiveJob{{JobID: "job-a", Epoch: 1}}, time.Now())
	client.publishFleetSpawns([]HubActiveJob{{JobID: "job-a", Epoch: 1}}, time.Now())
	if len(client.events) != 1 {
		t.Fatalf("an unchanged running job was re-announced: %d events", len(client.events))
	}
	client.resyncFleetState()
	client.publishFleetSpawns([]HubActiveJob{{JobID: "job-a", Epoch: 1}}, time.Now())
	client.publishFleetSpawns(nil, time.Now())
	if len(client.events) != 3 {
		t.Fatalf("resync start + end: %d events, want 3", len(client.events))
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

// Review #142: a job whose start was dropped by the full queue announces the
// start before its end.
func TestFleetSpawnEndNeverPrecedesStart(t *testing.T) {
	client := &HubClient{events: make(chan hubClientEvent, 1), warn: func(string) {}, statePublish: true}
	client.events <- hubClientEvent{Kind: "note"} // queue full: the start is dropped
	client.publishFleetSpawns([]HubActiveJob{{JobID: "job-a", Epoch: 1}}, time.Now())
	if running := client.fleetSpawns["job-a"]; running.announced {
		t.Fatal("start counted as announced although the queue dropped it")
	}
	<-client.events
	client.events = make(chan hubClientEvent, 4)
	client.publishFleetSpawns(nil, time.Now()) // the job ended before a retry
	var phases []string
	for len(client.events) > 0 {
		event := <-client.events
		var publish FleetStatePublish
		if json.Unmarshal(event.Payload, &publish) != nil || publish.Spawn == nil {
			t.Fatalf("event %+v", event)
		}
		phases = append(phases, publish.Spawn.Phase)
	}
	if strings.Join(phases, ",") != "start,end" {
		t.Fatalf("phases = %v, want start,end", phases)
	}
	if _, kept := client.fleetSpawns["job-a"]; kept {
		t.Fatal("ended job kept")
	}
}

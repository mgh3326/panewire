package panewire

import (
	"context"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

// hk 7125: a burst of spawns to one node that has no spawn slots. The first
// spawn dispatches and holds the node's only slot; each later spawn waits
// behind it. Each spawn's first pass records the queue it joined, so the later
// ones carry queued counts 1..N-1, and the first carries 0.
func TestSpawnBurstQueueTagsLaterSpawns_t7125(t *testing.T) {
	h := asyncSpawnHub(t)
	const n = 3
	base := time.Date(2026, 11, 28, 9, 0, 0, 0, time.UTC)
	ctx := t.Context()
	var tasks []hubSpawnTask
	for i := 1; i <= n; i++ {
		// Created in order: spawn 1 is the oldest, so it is the first in the queue.
		tasks = append(tasks, bindDrainRow(t, h.inbox.store, drainRow{n: i, created: base.Add(time.Duration(i) * time.Second)}, base))
	}

	for i, task := range tasks {
		// Spawn 1 is sent and holds the node's one slot, so each later spawn is
		// busy on its first pass: one dispatch in flight, itself not counted.
		dispatching := i == 0
		inFlight := 0
		if i > 0 {
			inFlight = 1
		}
		h.recordSpawnQueueAtFirstPass(ctx, task, inFlight, dispatching)

		open, queued, recorded, err := h.inbox.store.spawnDispatchQueue(ctx, task.Request.RequestID, task.Attempt)
		if err != nil {
			t.Fatal(err)
		}
		if !recorded {
			t.Fatalf("spawn %d: first pass recorded nothing", i+1)
		}
		if open != 1 {
			t.Errorf("spawn %d: node_open_dispatches = %d, want 1", i+1, open)
		}
		if want := i; queued != want {
			t.Errorf("spawn %d: node_queued_spawns = %d, want %d", i+1, queued, want)
		}
	}
}

// hk 7125: the first pass decides the record. A later pass that sees a
// different queue does not move it, so the landed trace shows the queue the
// spawn joined.
func TestSpawnQueueRecordedOnlyOnFirstPass_t7125(t *testing.T) {
	h := asyncSpawnHub(t)
	base := time.Date(2026, 11, 28, 9, 0, 0, 0, time.UTC)
	ctx := t.Context()
	task := bindDrainRow(t, h.inbox.store, drainRow{n: 1, created: base}, base)
	h.recordSpawnQueueAtFirstPass(ctx, task, 0, false)
	// A later pass after the queue drained would count differently.
	h.recordSpawnQueueAtFirstPass(ctx, task, 4, true)

	open, queued, recorded, err := h.inbox.store.spawnDispatchQueue(ctx, task.Request.RequestID, task.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	if !recorded || open != 0 || queued != 0 {
		t.Fatalf("record = open %d queued %d recorded %v, want the first pass's 0 0 true", open, queued, recorded)
	}
}

// hk 7125: a landing recorded after the dispatch has returned still carries the
// queue tags, because the count lives on the operation, not in memory.
func TestSpawnLandingAfterDispatchCarriesQueue_t7125(t *testing.T) {
	h := asyncSpawnHub(t)
	transport := &spawnSentryTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{Dsn: "https://public@example.invalid/1", EnableTracing: true, TracesSampleRate: 1, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	h.spawnSentry = client

	accepted := time.Date(2026, 11, 28, 9, 0, 0, 0, time.UTC)
	landedAt := accepted.Add(time.Minute)
	h.now = func() time.Time { return landedAt }

	req := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", accepted)
	if err != nil {
		t.Fatal(err)
	}
	bindTo := hubSpawnRequest{RequestID: t14RequestID(9891), Machine: "machine-a", Args: []string{"--job", "test-job"}, WaitSeconds: 300}
	task, err = h.inbox.store.bindSpawn(t.Context(), req.RequestID, task.Attempt, bindTo, accepted.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	// The first pass runs on the bound attempt, before the send.
	h.recordSpawnQueueAtFirstPass(t.Context(), task, 0, true)
	sent := sentSpawnFixture(t, h.inbox.store, task)
	landed := hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Profile: "model-a", Role: "implementer", StageMS: map[string]int64{"launch": 1000, "landing": 1000}}
	if _, err = h.inbox.store.completeSpawn(t.Context(), sent, landed, landedAt); err != nil {
		t.Fatal(err)
	}
	h.recordSpawnTrace(sent, landed)

	event := spawnTransactionNamed(t, transport, "runtug.spawn")
	if got := event.Tags["node_open_dispatches"]; got != "1" {
		t.Fatalf("node_open_dispatches = %q, want 1 (the send counts itself)", got)
	}
	if got := event.Tags["node_queued_spawns"]; got != "0" {
		t.Fatalf("node_queued_spawns = %q, want 0", got)
	}
}

// The store keeps the first record: a second write for the same attempt is a no-op.
func TestSpawnDispatchQueueWriteOnlyUnrecorded_t7125(t *testing.T) {
	h := asyncSpawnHub(t)
	base := time.Date(2026, 11, 28, 9, 0, 0, 0, time.UTC)
	ctx := context.Background()
	task := bindDrainRow(t, h.inbox.store, drainRow{n: 2, created: base}, base)
	if err := h.inbox.store.recordSpawnDispatchQueue(ctx, task.Request.RequestID, task.Attempt, 1, 2); err != nil {
		t.Fatal(err)
	}
	if err := h.inbox.store.recordSpawnDispatchQueue(ctx, task.Request.RequestID, task.Attempt, 9, 9); err != nil {
		t.Fatal(err)
	}
	open, queued, recorded, err := h.inbox.store.spawnDispatchQueue(ctx, task.Request.RequestID, task.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	if !recorded || open != 1 || queued != 2 {
		t.Fatalf("record = %d,%d recorded %v, want 1,2 true", open, queued, recorded)
	}
}

package panewire

import (
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

// hk 6074: the landed attempt's trace starts at acceptance and carries a span
// for each attempt the hub renewed before it, and a queue span for each wait no
// step accounts for. Here an attempt is refused after 120 s, the request waits
// 130 s for a node slot, and the next attempt lands 40 s after it started. The
// transaction must run from acceptance to landed, and the queue must be a span.
func TestSpawnTraceRefusedAttemptsAndQueueWait_t6074(t *testing.T) {
	h := asyncSpawnHub(t)
	transport := &spawnSentryTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{Dsn: "https://public@example.invalid/1", EnableTracing: true, TracesSampleRate: 1, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	h.spawnSentry = client

	accepted := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	refusedAt := accepted.Add(120 * time.Second)
	boundAt := refusedAt.Add(130 * time.Second)
	landedAt := boundAt.Add(40 * time.Second)
	h.now = func() time.Time { return landedAt }

	req := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", accepted)
	if err != nil {
		t.Fatal(err)
	}
	// Attempt 1: bound, then refused as an unstarted timeout at 120 s. The
	// hub renews the request, so attempt 2 opens at that same instant.
	first := hubSpawnRequest{RequestID: t14RequestID(9861), Machine: "machine-a", Args: []string{"--job", "test-job"}, WaitSeconds: 300}
	task, err = h.inbox.store.bindSpawn(t.Context(), req.RequestID, task.Attempt, first, accepted.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	// Bound but never sent: a timeout renews only an attempt the node never
	// started (writeSpawnOutcome, spawnAttemptEndedUnstarted).
	refused := hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a", Retryable: true, StageMS: map[string]int64{"claim": 5_000, "placement": 20_000}}
	if changed, err := h.inbox.store.completeSpawn(t.Context(), task, refused, refusedAt); err != nil || !changed {
		t.Fatalf("refusal renewed: changed=%v err=%v", changed, err)
	}
	// Attempt 2: bound at 250 s (the 130 s queue wait), sent, then landed.
	renewed, err := h.inbox.store.spawnTask(t.Context(), req.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.Attempt != 2 {
		t.Fatalf("attempt after renewal = %d, want 2", renewed.Attempt)
	}
	second := hubSpawnRequest{RequestID: t14RequestID(9862), Machine: "machine-a", Args: []string{"--job", "test-job"}, WaitSeconds: 300}
	bound, err := h.inbox.store.bindSpawn(t.Context(), req.RequestID, renewed.Attempt, second, boundAt)
	if err != nil {
		t.Fatal(err)
	}
	sent := sentSpawnFixture(t, h.inbox.store, bound)
	// The steps run after the bind and fit in the 40 s from bind to landed, so
	// the queue is the 130 s before the bind (window minus the steps' sum).
	landed := hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Profile: "model-a", Role: "implementer", StageMS: map[string]int64{"launch": 20_000, "landing": 20_000}}
	if _, err = h.inbox.store.completeSpawn(t.Context(), sent, landed, landedAt); err != nil {
		t.Fatal(err)
	}
	h.recordSpawnTrace(sent, landed)

	transport.mu.Lock()
	defer transport.mu.Unlock()
	var tx *sentry.Event
	for _, e := range transport.events {
		if e.Type == "transaction" && e.Transaction == "runtug.spawn" && e.Tags["rc"] == "0" {
			tx = e
		}
	}
	if tx == nil {
		t.Fatalf("no landed runtug.spawn transaction among %d events", len(transport.events))
	}
	if got, want := tx.StartTime, accepted; !got.Equal(want) {
		t.Fatalf("trace starts %v, want acceptance %v", got, want)
	}
	if got, want := tx.Timestamp, landedAt; !got.Equal(want) {
		t.Fatalf("trace ends %v, want landed %v", got, want)
	}

	// The refused attempt is a window from acceptance to its refusal, and the
	// 130 s queue wait is the landed attempt's window before its first step.
	var refusedWindow, queueWait *sentry.Span
	for _, span := range tx.Spans {
		switch {
		case span.Op == "attempt" && span.Tags["attempt"] == "1":
			refusedWindow = span
		case span.Op == "queue" && span.StartTime.Equal(refusedAt):
			queueWait = span
		}
	}
	if refusedWindow == nil {
		t.Fatalf("no span for the refused attempt; spans=%d", len(tx.Spans))
	}
	if !refusedWindow.StartTime.Equal(accepted) || !refusedWindow.EndTime.Equal(refusedAt) {
		t.Fatalf("refused attempt window %v..%v, want %v..%v", refusedWindow.StartTime, refusedWindow.EndTime, accepted, refusedAt)
	}
	if refusedWindow.Tags["reason"] != "timeout" {
		t.Fatalf("refused attempt reason = %q, want the bounded code", refusedWindow.Tags["reason"])
	}
	if queueWait == nil {
		t.Fatalf("no queue span for the 130 s wait from %v", refusedAt)
	}
	if got, want := queueWait.EndTime.Sub(queueWait.StartTime), 130*time.Second; got != want {
		t.Fatalf("queue wait lasted %v, want %v", got, want)
	}

	// The windows cover acceptance to landed: the refused attempt, the queue
	// wait, and the landed attempt's own steps add up to the whole trace.
	var landedSteps time.Duration
	for _, span := range tx.Spans {
		if span.Op == "launch" || span.Op == "landing" {
			if span.StartTime.Before(boundAt) {
				t.Fatalf("landed step %q starts %v, before its bind %v", span.Op, span.StartTime, boundAt)
			}
			landedSteps += span.EndTime.Sub(span.StartTime)
		}
	}
	if got, want := refusedWindow.EndTime.Sub(refusedWindow.StartTime)+queueWait.EndTime.Sub(queueWait.StartTime)+landedSteps, landedAt.Sub(accepted); got != want {
		t.Fatalf("windows cover %v, want acceptance to landed %v", got, want)
	}
}

// hk 6074: trace_history is read and written by every renewal and by the landed
// trace, so a hub on Postgres or MySQL, whose schema comes only from the
// migration files, needs the column there too (see the hk 5776 check).
func TestSpawnTraceHistoryColumnHasAMigrationOnEveryDialect_t6074(t *testing.T) {
	postgres := columnsOf(t, migrationSources(t, hubPostgresMigrations, "hub_postgres_migrations/*.sql"))
	mysql := columnsOf(t, migrationSources(t, hubMySQLMigrations, "hub_mysql_migrations/*.sql"))
	for name, tables := range map[string]map[string]map[string]bool{"postgres": postgres, "mysql": mysql} {
		if !tables["spawn_operations"]["trace_history"] {
			t.Errorf("%s migrations do not add spawn_operations.trace_history", name)
		}
	}
}

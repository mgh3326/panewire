package panewire

import (
	"context"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

// hk 7112: the queue of an attempt is its window minus the sum of the chain
// steps, and the steps are laid back to back. Known durations give exact
// spans: the queue is 10 s minus 6 s, and the checkout sub-stages are drawn
// inside checkout without being counted a second time.
func TestSpawnTraceQueueIsWindowLessSumOfSteps_t7112(t *testing.T) {
	transport := &spawnSentryTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{Dsn: "https://public@example.invalid/1", EnableTracing: true, TracesSampleRate: 1, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	open := time.Date(2026, 11, 28, 9, 0, 0, 0, time.UTC)
	end := open.Add(10 * time.Second)
	stages := map[string]int64{
		"claim": 1000, "placement": 500, "checkout": 2000,
		checkoutStageLockWait: 300, checkoutStageFetch: 800, checkoutStageWorktreeAdd: 900,
		"launch": 1500, "submit": 400, "landing": 600,
	}
	tx := sentry.StartTransaction(sentry.SetHubOnContext(context.Background(), sentry.NewHub(client, sentry.NewScope())), "queue-test", sentry.WithSpanSampled(sentry.SampledTrue))
	tx.StartTime = open
	layoutSpawnWindow(tx, open, end, stages)
	tx.EndTime = end
	tx.Finish()

	event := spawnTransactionNamed(t, transport, "queue-test")
	spans := map[string][]*sentry.Span{}
	for _, span := range event.Spans {
		spans[span.Op] = append(spans[span.Op], span)
	}
	at := func(ms int64) time.Time { return open.Add(time.Duration(ms) * time.Millisecond) }
	want := map[string][2]int64{
		"queue":                  {0, 4000},
		"placement":              {4000, 4500},
		"checkout":               {4500, 6500},
		"claim":                  {6500, 7500},
		"launch":                 {7500, 9000},
		"submit":                 {9000, 9400},
		"landing":                {9400, 10000},
		checkoutStageLockWait:    {4500, 4800},
		checkoutStageFetch:       {4800, 5600},
		checkoutStageWorktreeAdd: {5600, 6500},
	}
	for op, bounds := range want {
		got := spans[op]
		if len(got) != 1 {
			t.Fatalf("%q: %d spans, want 1", op, len(got))
		}
		if !got[0].StartTime.Equal(at(bounds[0])) || !got[0].EndTime.Equal(at(bounds[1])) {
			t.Errorf("%q spans %v..%v, want %v..%v", op, got[0].StartTime.Sub(open), got[0].EndTime.Sub(open), time.Duration(bounds[0])*time.Millisecond, time.Duration(bounds[1])*time.Millisecond)
		}
	}
	if len(event.Spans) != len(want) {
		t.Fatalf("%d spans, want %d (no step drawn twice)", len(event.Spans), len(want))
	}
}

// hk 7112: a renewed attempt keeps its window, and the queue inside it is that
// window minus the steps the node reported for it. The landed window opens
// where the renewed one ended.
func TestSpawnTraceRenewedAttemptWindowUnchanged_t7112(t *testing.T) {
	transport := &spawnSentryTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{Dsn: "https://public@example.invalid/1", EnableTracing: true, TracesSampleRate: 1, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 11, 28, 9, 0, 0, 0, time.UTC)
	refusedAt := created.Add(120 * time.Second)
	tx := sentry.StartTransaction(sentry.SetHubOnContext(context.Background(), sentry.NewHub(client, sentry.NewScope())), "renewed-test", sentry.WithSpanSampled(sentry.SampledTrue))
	tx.StartTime = created
	history := []spawnTraceAttempt{{Attempt: 1, EndMS: refusedAt.UnixMilli(), Reason: "timeout", StageMS: map[string]int64{"claim": 5000, "placement": 20000}}}
	open, _ := layoutRenewedAttempts(tx, created, history)
	tx.EndTime = refusedAt
	tx.Finish()

	if !open.Equal(refusedAt) {
		t.Fatalf("landed window opens %v, want the renewed end %v", open, refusedAt)
	}
	event := spawnTransactionNamed(t, transport, "renewed-test")
	var attempt, queue *sentry.Span
	for _, span := range event.Spans {
		switch span.Op {
		case "attempt":
			attempt = span
		case "queue":
			queue = span
		}
	}
	if attempt == nil || !attempt.StartTime.Equal(created) || !attempt.EndTime.Equal(refusedAt) {
		t.Fatalf("attempt window is not %v..%v: %+v", created, refusedAt, attempt)
	}
	if queue == nil || queue.EndTime.Sub(queue.StartTime) != 95*time.Second {
		t.Fatalf("queue inside the renewed attempt = %+v, want 95 s (120 s less 25 s of steps)", queue)
	}
}

// hk 7112, hk 7125: the landed trace carries the node's queue as the attempt's
// first scheduling pass recorded it, so a burst on one node is visible on the trace.
func TestSpawnTraceTagsNodeOpenDispatches_t7112(t *testing.T) {
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
	bindTo := hubSpawnRequest{RequestID: t14RequestID(9871), Machine: "machine-a", Args: []string{"--job", "test-job"}, WaitSeconds: 300}
	task, err = h.inbox.store.bindSpawn(t.Context(), req.RequestID, task.Attempt, bindTo, accepted.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	sent := sentSpawnFixture(t, h.inbox.store, task)
	landed := hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Profile: "model-a", Role: "implementer", StageMS: map[string]int64{"launch": 1000, "landing": 1000}}
	if _, err = h.inbox.store.completeSpawn(t.Context(), sent, landed, landedAt); err != nil {
		t.Fatal(err)
	}
	// The first pass counted three open dispatches on the node, itself included,
	// and two operations ahead of it.
	if err := h.inbox.store.recordSpawnDispatchQueue(t.Context(), req.RequestID, sent.Node.RequestID, sent.Attempt, 3, 2); err != nil {
		t.Fatal(err)
	}
	h.recordSpawnTrace(sent, landed)

	event := spawnTransactionNamed(t, transport, "runtug.spawn")
	if got := event.Tags["node_open_dispatches"]; got != "3" {
		t.Fatalf("node_open_dispatches = %q, want 3", got)
	}
	if got := event.Tags["node_queued_spawns"]; got != "2" {
		t.Fatalf("node_queued_spawns = %q, want 2", got)
	}
}

// spawnTransactionNamed returns the one transaction event named name that the
// transport received, failing the test when there is none.
func spawnTransactionNamed(t *testing.T, transport *spawnSentryTransport, name string) *sentry.Event {
	t.Helper()
	transport.mu.Lock()
	defer transport.mu.Unlock()
	for _, e := range transport.events {
		if e.Type == "transaction" && e.Transaction == name {
			return e
		}
	}
	t.Fatalf("no transaction %q among %d events", name, len(transport.events))
	return nil
}

// hk 7112 (review): only the landed trace carries the first send's count; a
// refused outcome of the same request does not.
func TestSpawnDispatchOpenTagOnlyOnLandedTrace_t7112(t *testing.T) {
	h := asyncSpawnHub(t)
	transport := &spawnSentryTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{Dsn: "https://public@example.invalid/1", EnableTracing: true, TracesSampleRate: 1, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	h.spawnSentry = client

	accepted := time.Date(2026, 11, 28, 9, 0, 0, 0, time.UTC)
	refusedAt := accepted.Add(time.Minute)
	h.now = func() time.Time { return refusedAt }

	req := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", accepted)
	if err != nil {
		t.Fatal(err)
	}
	bindTo := hubSpawnRequest{RequestID: t14RequestID(9881), Machine: "machine-a", Args: []string{"--job", "test-job"}, WaitSeconds: 300}
	task, err = h.inbox.store.bindSpawn(t.Context(), req.RequestID, task.Attempt, bindTo, accepted.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	sent := sentSpawnFixture(t, h.inbox.store, task)
	refused := hubSpawnOutcome{RC: 1, Stage: "launch", Host: "machine-a", Reason: "spawn_failed:launch_refused", StageMS: map[string]int64{"launch": 1000}}
	if _, err = h.inbox.store.completeSpawn(t.Context(), sent, refused, refusedAt); err != nil {
		t.Fatal(err)
	}
	if err := h.inbox.store.recordSpawnDispatchQueue(t.Context(), req.RequestID, sent.Node.RequestID, sent.Attempt, 3, 2); err != nil {
		t.Fatal(err)
	}
	h.recordSpawnTrace(sent, refused)

	event := spawnTransactionNamed(t, transport, "runtug.spawn")
	if got, ok := event.Tags["node_open_dispatches"]; ok {
		t.Fatalf("refused trace carries node_open_dispatches=%q, want none", got)
	}
}

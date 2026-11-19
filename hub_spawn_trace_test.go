package panewire

import (
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

// TestSpawnTraceChildSpansAfterPlacement holds the steps after placement (hk
// 5585): node claim, pane start, brief submit and landing proof each arrive as
// a child of the runtug.spawn transaction, named for the step and carrying the
// step's elapsed time, so a long trace shows which step held the time.
func TestSpawnTraceChildSpansAfterPlacement(t *testing.T) {
	h := asyncSpawnHub(t)
	transport := &spawnSentryTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{Dsn: "https://public@example.invalid/1", EnableTracing: true, TracesSampleRate: 1, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	h.spawnSentry = client
	req := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now().Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	task, err = h.inbox.store.bindSpawn(t.Context(), req.RequestID, task.Attempt, hubSpawnRequest{RequestID: t14RequestID(984), Machine: "machine-a", Args: []string{"--job", "test-job"}, WaitSeconds: 300}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	task = sentSpawnFixture(t, h.inbox.store, task)
	// A landed result: no error event, so the transaction is the only traffic.
	stageMS := map[string]int64{"claim": 5, "placement": 20, "checkout": 30, "launch": 40, "submit": 60, "landing": 70}
	result := hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Profile: "model-a", Role: "implementer", StageMS: stageMS}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, result, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.recordSpawnTrace(task, result)

	transport.mu.Lock()
	defer transport.mu.Unlock()
	var tx *sentry.Event
	for _, e := range transport.events {
		if e.Type == "transaction" {
			tx = e
		}
	}
	if tx == nil || tx.Transaction != "runtug.spawn" {
		t.Fatalf("transaction=%+v events=%d", tx, len(transport.events))
	}
	root, ok := tx.Contexts["trace"]["span_id"].(sentry.SpanID)
	if !ok {
		t.Fatalf("transaction has no span id: %+v", tx.Contexts["trace"])
	}
	spans := map[string]*sentry.Span{}
	for _, span := range tx.Spans {
		spans[span.Op] = span
	}
	for _, step := range []string{"claim", "placement", "checkout", "launch", "submit", "landing"} {
		span := spans[step]
		if span == nil {
			t.Fatalf("no %q child span; spans=%v", step, spans)
		}
		if span.ParentSpanID != root {
			t.Fatalf("%q parent=%v, want the transaction %v", step, span.ParentSpanID, root)
		}
		if got, want := span.EndTime.Sub(span.StartTime), time.Duration(stageMS[step])*time.Millisecond; got != want {
			t.Fatalf("%q lasted %v, want %v", step, got, want)
		}
	}
	if len(tx.Spans) != len(stageMS) {
		t.Fatalf("child spans=%d, want %d", len(tx.Spans), len(stageMS))
	}
}

// The strict hub check takes submit only from a hub that lists its feature;
// any other hub gets the old landing figure, submit plus landing proof, and the
// journaled result keeps its own map (hk 5585).
func TestSpawnStageMSForHubFoldsSubmitForOlderHub(t *testing.T) {
	result := hubSpawnResult{StageMS: map[string]int64{"launch": 4, "submit": 7, "landing": 3}}
	kept := spawnStageMSForHub(result, true)
	if kept.StageMS["submit"] != 7 || kept.StageMS["landing"] != 3 {
		t.Fatalf("new hub got %v, want submit kept", kept.StageMS)
	}
	folded := spawnStageMSForHub(result, false)
	if _, ok := folded.StageMS["submit"]; ok || folded.StageMS["landing"] != 10 || folded.StageMS["launch"] != 4 {
		t.Fatalf("older hub got %v, want submit folded into landing", folded.StageMS)
	}
	if !validSpawnStages(folded.StageMS) {
		t.Fatalf("folded map is not one the hub accepts: %v", folded.StageMS)
	}
	if result.StageMS["submit"] != 7 || result.StageMS["landing"] != 3 {
		t.Fatalf("fold changed the journaled map: %v", result.StageMS)
	}
	if got := spawnStageMSForHub(hubSpawnResult{StageMS: map[string]int64{"landing": 3}}, false); got.StageMS["landing"] != 3 {
		t.Fatalf("a result without submit changed: %v", got.StageMS)
	}
}

// A hub that accepts submit must list hubFeatureSpawnStageSubmit in its
// hub.features frame, or nodes fold the key for it.
func TestHubFeaturesAdvertiseSpawnStageSubmit(t *testing.T) {
	for _, feature := range hubFeaturesFrame().Features {
		if feature == hubFeatureSpawnStageSubmit {
			return
		}
	}
	t.Fatalf("hub.features lacks %q", hubFeatureSpawnStageSubmit)
}

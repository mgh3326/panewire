package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/mgh3326/panewire/internal/fleet"
)

type spawnJobsFixture struct{ record map[string]any }

func (spawnJobsFixture) Claim(context.Context, string, string, string) error { return nil }
func (spawnJobsFixture) Put(context.Context, string, map[string]any) error   { return nil }
func (f spawnJobsFixture) Get(context.Context, string) (map[string]any, error) {
	if f.record != nil {
		return f.record, nil
	}
	return nil, fleet.ErrJobNotFound
}
func asyncSpawnHub(t *testing.T) *HubServer {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: spawnJobsFixture{}, Out: out, Err: errout}, nil
	}
	return h
}
func TestAsyncSpawnAcceptedOnlyAfterDurableReservation(t *testing.T) {
	h := asyncSpawnHub(t)
	req := durableSpawnRequest()
	raw, _ := json.Marshal(req)
	submit := func(body []byte, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/v1/spawns", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, r)
		return w
	}
	for range 2 {
		w := submit(raw, t14OperatorToken)
		if w.Code != http.StatusAccepted || !strings.Contains(w.Body.String(), "durable_hub_reservation") {
			t.Fatalf("accept=%d %s", w.Code, w.Body)
		}
	}
	task, err := h.inbox.store.spawnTask(t.Context(), req.RequestID)
	if err != nil || task.Status != "accepted" {
		t.Fatalf("ACK without durable ownership: %+v %v", task, err)
	}
	changed := req
	changed.Brief = "conflicting request"
	other, _ := json.Marshal(changed)
	if w := submit(other, t14OperatorToken); w.Code != 409 {
		t.Fatalf("payload conflict=%d", w.Code)
	}
	if w := submit(raw, "node-token"); w.Code != 401 {
		t.Fatalf("node accepted=%d", w.Code)
	}
	h.inbox.store.abort()
	if w := submit(raw, t14OperatorToken); w.Code != 503 {
		t.Fatalf("broken durable store ACKed=%d", w.Code)
	}
}
func TestAsyncSpawnWithoutDurableStoreRefuses(t *testing.T) {
	h, _ := t14Hub(t)
	raw, _ := json.Marshal(durableSpawnRequest())
	r := httptest.NewRequest("POST", "/v1/spawns", bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
	w := httptest.NewRecorder()
	h.Handler().ServeHTTP(w, r)
	if w.Code != 503 {
		t.Fatalf("accept without durable store=%d", w.Code)
	}
}
func TestSpawnOutboxFailureRollsBackResult(t *testing.T) {
	ctx := t.Context()
	s := openSpawnStore(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	req := durableSpawnRequest()
	task, _, err := s.claimSpawn(ctx, req, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(`CREATE TRIGGER fail_notification BEFORE INSERT ON spawn_notifications BEGIN SELECT RAISE(ABORT,'injected lost event'); END`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.completeSpawn(ctx, task, hubSpawnOutcome{RC: 0, Stage: "landing"}, time.Now()); err == nil {
		t.Fatal("injected outbox failure succeeded")
	}
	saved, err := s.spawnTask(ctx, req.RequestID)
	if err != nil || saved.Status != "accepted" {
		t.Fatalf("result committed without notification: %+v %v", saved, err)
	}
	if _, err = s.db.Exec(`DROP TRIGGER fail_notification`); err != nil {
		t.Fatal(err)
	}
	if changed, err := s.completeSpawn(ctx, task, hubSpawnOutcome{RC: 0, Stage: "landing"}, time.Now()); err != nil || !changed {
		t.Fatalf("replayed durable result: %v %v", changed, err)
	}
}
func TestSpawnCleanedRetryPreservesKeyAndFencesOldAttempt(t *testing.T) {
	ctx := t.Context()
	s := openSpawnStore(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	req := durableSpawnRequest()
	task, _, err := s.claimSpawn(ctx, req, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.completeSpawn(ctx, task, hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", Retryable: true}, time.Now()); err != nil {
		t.Fatal(err)
	}
	old := task
	task, err = s.spawnTask(ctx, req.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := s.resumeCleanedSpawn(ctx, task, time.Now())
	if err != nil || retry.Attempt != 2 || retry.Request.RequestID != req.RequestID {
		t.Fatalf("retry %+v %v", retry, err)
	}
	if changed, err := s.completeSpawn(ctx, old, hubSpawnOutcome{RC: 0, Stage: "landing"}, time.Now()); err != nil || changed {
		t.Fatalf("stale attempt replaced new owner: %v %v", changed, err)
	}
	if _, err = s.bindSpawn(ctx, req.RequestID, old.Attempt, hubSpawnRequest{RequestID: t14RequestID(999), Machine: "machine-b"}); !errors.Is(err, errSpawnClaimed) {
		t.Fatalf("stale dispatch accepted: %v", err)
	}
}

type spawnSentryTransport struct {
	mu      sync.Mutex
	events  []*sentry.Event
	flushes []time.Duration
	onFlush func()
}

func (*spawnSentryTransport) Configure(sentry.ClientOptions) {}
func (t *spawnSentryTransport) SendEvent(e *sentry.Event) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, e)
}
func (t *spawnSentryTransport) Flush(timeout time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.flushes = append(t.flushes, timeout)
	if t.onFlush != nil {
		t.onFlush()
	}
	return true
}
func (*spawnSentryTransport) Close() {}
func TestSpawnSentryOnlySpawnAlwaysSampledAndNormalized(t *testing.T) {
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
	task, err = h.inbox.store.bindSpawn(t.Context(), req.RequestID, task.Attempt, hubSpawnRequest{RequestID: t14RequestID(983), Machine: "machine-a", Args: []string{"--job", "test-job"}, WaitSeconds: 300})
	if err != nil {
		t.Fatal(err)
	}
	task = sentSpawnFixture(t, h.inbox.store, task)
	// A reconciling outcome sends no Sentry traffic (hk 3676): only the timeout's
	// transaction and error event remain below.
	result := hubSpawnOutcome{RC: 75, Stage: "landing", Reason: "unknown", Host: "machine-a", Profile: "model-a", Role: "implementer", StageMS: map[string]int64{"claim": 5, "placement": 20, "checkout": 30, "launch": 40, "landing": 50}}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, result, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.recordSpawnTrace(task, result)
	h.recordSpawnTrace(task, result)
	result.RC = 124
	result.Reason = "timeout"
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, result, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.recordSpawnTrace(task, result)
	r := httptest.NewRequest("GET", "/healthz", nil)
	h.Handler().ServeHTTP(httptest.NewRecorder(), r)
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.events) != 2 {
		t.Fatalf("transactions/errors/other traffic=%d", len(transport.events))
	}
	var tx, failure *sentry.Event
	for _, e := range transport.events {
		if e.Type == "transaction" {
			tx = e
		} else {
			failure = e
		}
	}
	if tx == nil || tx.Transaction != "runtug.spawn" || len(tx.Spans) != 5 || failure == nil || strings.Join(failure.Fingerprint, ":") != "spawn:landing:timeout" {
		t.Fatalf("tx=%+v failure=%+v", tx, failure)
	}
	raw, _ := json.Marshal(transport.events)
	if bytes.Contains(raw, []byte(req.Brief)) || bytes.Contains(raw, []byte("/work/brief.md")) {
		t.Fatal("telemetry exposed brief/path")
	}
	if got := spawnTraceReason("checkout_refused:dynamic-path"); got != "checkout_refused" {
		t.Fatal(got)
	}
}

func TestSpawnSentryKeepsRequiredTagsBeforePlacement(t *testing.T) {
	h := asyncSpawnHub(t)
	transport := &spawnSentryTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{Dsn: "https://public@example.invalid/1", EnableTracing: true, TracesSampleRate: 1, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	h.spawnSentry = client
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h.finishSpawnOutcome(task, hubSpawnOutcome{RC: 78, Stage: "placement", Reason: "configuration", Retryable: true})
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.events) != 2 {
		t.Fatal(len(transport.events))
	}
	for _, event := range transport.events {
		for _, tag := range []string{"job", "host", "profile", "role", "rc"} {
			if event.Tags[tag] == "" {
				t.Fatalf("missing %s: %+v", tag, event.Tags)
			}
		}
		if event.Tags["host"] != "unassigned" || event.Tags["profile"] != "auto" {
			t.Fatalf("invented placement: %+v", event.Tags)
		}
	}
}

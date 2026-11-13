package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/getsentry/sentry-go"
	"github.com/mgh3326/panewire/internal/fleet"
)

const (
	spawnReasonForeground = "agent not in foreground; brief withheld"
	spawnReasonFix        = "Inspect the failed pane with panewire find JOB."
)

// failSpawnThroughNode runs one durable spawn to a fake node that answers with
// the wire's bare spawn_failed, and returns the operation's request id.
func failSpawnThroughNode(t *testing.T, h *HubServer, server *httptest.Server, refusal string, rc int, retryable bool) string {
	t.Helper()
	node := t14Node(t, h, server, true)
	req := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	dispatch := hubSpawnRequest{RequestID: t14RequestID(741), Machine: "machine-a", CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Args: []string{"--job", "test-job"}, WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli()}
	if task, err = h.inbox.store.bindSpawn(t.Context(), req.RequestID, task.Attempt, dispatch); err != nil {
		t.Fatal(err)
	}
	transport := directSpawnTransport{h: h, task: task}
	done := make(chan error, 1)
	go func() { _, err := transport.dispatch(t.Context(), dispatch); done <- err }()
	sent := t14ReadNodeSpawn(t, node)
	tail := "stderr: " + refusal + "\nFix: " + spawnReasonFix
	if err = wsjson.Write(t.Context(), node, map[string]any{"type": "job.spawn.result", "request_id": sent.RequestID, "rc": rc, "job_id": "", "pane": "", "stdout_tail": tail, "error": "spawn_failed", "stage": "launch", "retryable": retryable}); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	return req.RequestID
}

func spawnOperationStatus(t *testing.T, server *httptest.Server, requestID string) (status, reason string) {
	t.Helper()
	code, _, raw := t14HTTP(t, http.MethodGet, server.URL+"/v1/spawns/"+requestID, t14OperatorToken, nil)
	var body struct {
		Status string `json:"status"`
		Result struct {
			Reason string `json:"reason"`
		} `json:"result"`
	}
	if code != http.StatusOK || json.Unmarshal(raw, &body) != nil {
		t.Fatalf("status API: %d %s", code, raw)
	}
	return body.Status, body.Result.Reason
}

func TestSpawnReasonCodeReachesStatusAPIAndLane(t *testing.T) {
	const reason = "spawn_failed:foreground_missing: " + spawnReasonForeground
	for _, test := range []struct {
		retryable bool
		status    string
	}{{false, "failed"}, {true, "retryable"}} {
		t.Run(test.status, func(t *testing.T) {
			h, _, destination := spawnNotifyHub(t, "")
			server := httptest.NewServer(h.Handler())
			defer server.Close()
			requestID := failSpawnThroughNode(t, h, server, spawnReasonForeground, 70, test.retryable)
			status, got := spawnOperationStatus(t, server, requestID)
			if status != test.status || got != reason {
				t.Fatalf("status=%q reason=%q", status, got)
			}
			h.flushSpawnNotifications(t.Context())
			injected := spawnFailedInjections(destination)
			replay := "no"
			if test.retryable {
				replay = "yes"
			}
			want := "[panewire] SPAWN_FAILED test-job request=" + requestID + " host=machine-a profile=none rc=70 stage=launch replay=" + replay + " reason=" + reason + " fix=" + spawnReasonFix
			if len(injected) != 1 || !strings.HasSuffix(injected[0].Text, " :: "+want) {
				t.Fatalf("lane text: %+v, want %q", injected, want)
			}
		})
	}
}

func TestSpawnReasonUnclassifiedKeepsTheBareForm(t *testing.T) {
	h, _, _ := spawnNotifyHub(t, "")
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	requestID := failSpawnThroughNode(t, h, server, "a refusal nobody has written yet", 70, false)
	if status, reason := spawnOperationStatus(t, server, requestID); status != "failed" || reason != "spawn_failed: a refusal nobody has written yet" {
		t.Fatalf("status=%q reason=%q", status, reason)
	}
}

// hk 3301: a brief upload refused by hk's secret guard reaches the
// SPAWN_FAILED reason with the guard's pattern name, never the matched text.
func TestSpawnReasonNamesHKGuardPattern(t *testing.T) {
	const tail = "stdout\nstderr: panewire: brief upload to briefs/job-a.md failed: handoffkeep: secret_like_content:private_key\nFix: hk's secret guard refused the write as secret-like content and named the pattern in the reason; remove the secret-shaped text from the brief or task field and retry."
	result := hubSpawnResult{RC: 81, Error: "spawn_failed", StdoutTail: tail}
	reason := spawnOutcomeReason(spawnResultReason(result), spawnResultRefusal(result.StdoutTail))
	if !strings.HasPrefix(reason, "spawn_failed:hk_guard_refused: ") || !strings.Contains(reason, "secret_like_content:private_key") {
		t.Fatalf("reason=%q", reason)
	}
	notice := spawnNotificationText("job-a", "req-1", hubSpawnOutcome{RC: 81, Stage: "launch", Reason: reason, Fix: spawnResultFix(result.StdoutTail)})
	if !strings.Contains(notice, "reason=spawn_failed:hk_guard_refused") || !strings.Contains(notice, "secret_like_content:private_key") {
		t.Fatalf("notice=%q", notice)
	}
}

// Every status branch reads a bare code, so the family lands where bare
// spawn_failed always did, and the bare codes keep their own branches.
func TestSpawnReasonFamilyDecidesNoStatus(t *testing.T) {
	for _, retryable := range []bool{false, true} {
		bare := spawnOutcomeStatus(hubSpawnOutcome{RC: 70, Reason: "spawn_failed", Retryable: retryable})
		for _, reason := range []string{"spawn_failed: text", "spawn_failed:foreground_missing", "spawn_failed:foreground_missing: text"} {
			if got := spawnOutcomeStatus(hubSpawnOutcome{RC: 70, Reason: reason, Retryable: retryable}); got != bare {
				t.Errorf("%q retryable=%v: status %q, bare gives %q", reason, retryable, got, bare)
			}
		}
		for _, code := range []string{"timeout", "disconnected", "unknown"} {
			if got := spawnOutcomeReason(code, "late text"); got != code {
				t.Errorf("%s kept text: %q", code, got)
			}
		}
	}
}

func spawnReasonSentryClient(t *testing.T, transport *spawnSentryTransport) *sentry.Client {
	t.Helper()
	options := spawnSentryOptions("https://public@example.invalid/1")
	options.Transport = transport
	client, err := sentry.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func spawnReasonErrorEvent(t *testing.T, transport *spawnSentryTransport) (*sentry.Event, []byte) {
	t.Helper()
	transport.mu.Lock()
	defer transport.mu.Unlock()
	var failure *sentry.Event
	for _, event := range transport.events {
		if event.Type != "transaction" {
			if failure != nil {
				t.Fatalf("more than one error event: %+v", transport.events)
			}
			failure = event
		}
	}
	if failure == nil {
		t.Fatalf("no error event: %+v", transport.events)
	}
	raw, err := json.Marshal(transport.events)
	if err != nil {
		t.Fatal(err)
	}
	return failure, raw
}

func TestSpawnReasonCodeInSentryEvent(t *testing.T) {
	h, _, _ := spawnNotifyHub(t, "")
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	transport := &spawnSentryTransport{}
	h.spawnSentry = spawnReasonSentryClient(t, transport)
	requestID := failSpawnThroughNode(t, h, server, spawnReasonForeground, 70, false)
	failure, raw := spawnReasonErrorEvent(t, transport)
	if failure.Message != "SPAWN_FAILED launch/spawn_failed:foreground_missing" {
		t.Fatalf("message=%q", failure.Message)
	}
	if got := strings.Join(failure.Fingerprint, "|"); got != "spawn|launch|spawn_failed:foreground_missing" {
		t.Fatalf("fingerprint=%q", got)
	}
	if failure.Tags["reason"] != "spawn_failed:foreground_missing" || failure.Tags["request"] != requestID || failure.Tags["attempt"] != strconv.Itoa(1) || failure.Tags["stage"] != "launch" {
		t.Fatalf("tags=%+v", failure.Tags)
	}
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	for _, event := range transport.events {
		if event.Environment != "fleet" || event.Release != "panewire@"+fleet.BuildVersion || event.ServerName != "panewire-hub" || event.ServerName == hostname {
			t.Fatalf("client options: env=%q release=%q server=%q", event.Environment, event.Release, event.ServerName)
		}
		if !event.User.IsEmpty() || event.Request != nil || len(event.Breadcrumbs) != 0 {
			t.Fatalf("identifying fields survived: %+v", event)
		}
	}
	if bytes.Contains(raw, []byte("not in foreground")) || bytes.Contains(raw, []byte("withheld")) {
		t.Fatalf("refusal text reached Sentry: %s", raw)
	}
}

func TestSpawnReasonSentryUnclassifiedStaysBoundedAndOmitsText(t *testing.T) {
	h, _, _ := spawnNotifyHub(t, "")
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	transport := &spawnSentryTransport{}
	h.spawnSentry = spawnReasonSentryClient(t, transport)
	failSpawnThroughNode(t, h, server, "refused at /private/secret-checkout/path", 70, false)
	failure, raw := spawnReasonErrorEvent(t, transport)
	if failure.Tags["reason"] != "spawn_failed" || failure.Message != "SPAWN_FAILED launch/spawn_failed" {
		t.Fatalf("message=%q tags=%+v", failure.Message, failure.Tags)
	}
	if bytes.Contains(raw, []byte("secret-checkout")) {
		t.Fatalf("refusal text reached Sentry: %s", raw)
	}
}

func TestSpawnReasonSentryOptionsScrubEveryEventType(t *testing.T) {
	transport := &spawnSentryTransport{}
	client := spawnReasonSentryClient(t, transport)
	for _, kind := range []string{"", "transaction"} {
		client.CaptureEvent(&sentry.Event{
			Type:        kind,
			Message:     "tainted",
			User:        sentry.User{ID: "someone", IPAddress: "192.0.2.1"},
			Request:     &sentry.Request{URL: "https://example.invalid/private"},
			Breadcrumbs: []*sentry.Breadcrumb{{Message: "crumb"}},
		}, nil, sentry.NewScope())
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.events) != 2 {
		t.Fatalf("events=%d", len(transport.events))
	}
	for _, event := range transport.events {
		if !event.User.IsEmpty() || event.Request != nil || len(event.Breadcrumbs) != 0 {
			t.Fatalf("type %q kept identifying fields: %+v", event.Type, event)
		}
	}
}

func TestSpawnTraceCloseFlushesOnceAfterWorkersStop(t *testing.T) {
	h := asyncSpawnHub(t)
	var stopped atomic.Bool
	var stoppedAtFlush atomic.Bool
	transport := &spawnSentryTransport{onFlush: func() { stoppedAtFlush.Store(stopped.Load()) }}
	h.spawnSentry = spawnReasonSentryClient(t, transport)
	h.spawnWorkers.Add(1)
	go func() {
		defer h.spawnWorkers.Done()
		<-h.closeCtx.Done()
		stopped.Store(true)
	}()
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.flushes) != 1 || transport.flushes[0] <= 0 || transport.flushes[0] > 2*time.Second {
		t.Fatalf("flushes=%v", transport.flushes)
	}
	if !stoppedAtFlush.Load() {
		t.Fatal("flushed before the spawn workers stopped")
	}
}

func TestSpawnTraceCloseWithoutClientBuildsNone(t *testing.T) {
	h := asyncSpawnHub(t)
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	h.recordSpawnTrace(hubSpawnTask{}, hubSpawnOutcome{RC: 70})
	if h.spawnSentry != nil {
		t.Fatal("a trace after Close built a client nothing will flush")
	}
}

func runFailingHubSpawn(t *testing.T, requestID, stderr string, rc int) hubSpawnResult {
	t.Helper()
	message := hubOutboundMessage{RequestID: requestID, CWDKey: "repo-a", BriefInline: "brief", WaitSeconds: 5, Args: []string{"--job", "job-coded"}}
	return (&HubClient{spawnConfigPath: t14SpawnConfig(t, t.TempDir()), spawnEngine: func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
		output := &hubSpawnOutput{}
		_, _ = output.stderr.Write([]byte(stderr))
		return output, fleet.Execution{RC: rc, Stage: "launch"}
	}}).runHubSpawn(t.Context(), message)
}

func TestSpawnReasonCodeInNodeFleetEvent(t *testing.T) {
	for _, test := range []struct {
		name, stderr string
		rc           int
		reason       string
	}{
		{"classified by text", spawnReasonForeground, 70, "foreground_missing"},
		{"classified by status", "QUOTA_UNKNOWN: credit pool status unavailable", 89, "quota_unknown"},
		{"unclassified", "a refusal nobody has written yet", 70, "spawn_failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("XDG_STATE_HOME", t.TempDir())
			requestID := t14RequestID(792)
			result := runFailingHubSpawn(t, requestID, test.stderr, test.rc)
			// The wire stays bare: an older hub rejects any other error value.
			if result.Error != "spawn_failed" {
				t.Fatalf("wire error=%q", result.Error)
			}
			if _, ok := parseHubSpawnResult(hubSpawnResultWire(t, result)); !ok {
				t.Fatal("the hub must still accept the result")
			}
			raw, events := readFleetEventsFile(t)
			if len(events) != 1 {
				t.Fatalf("want one event: %s", raw)
			}
			event := events[0]
			if event["kind"] != "node_spawn" || event["outcome"] != "failed" || event["reason"] != test.reason || event["request_id"] != requestID {
				t.Fatalf("event=%+v", event)
			}
			if !strings.HasPrefix(event["detail"], "spawn_failed rc=") {
				t.Fatalf("detail=%q", event["detail"])
			}
		})
	}
}

func TestSpawnReasonFleetEventOmitsAnEmptyRequestID(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	if !emitFleetEvent(time.Unix(0, 0), fleetEvent{Kind: "node_spawn", Job: "job-a", Outcome: "failed", Reason: "spawn_failed"}) {
		t.Fatal("append failed")
	}
	raw, events := readFleetEventsFile(t)
	if len(events) != 1 || strings.Contains(raw, "request_id") {
		t.Fatalf("events: %s", raw)
	}
}

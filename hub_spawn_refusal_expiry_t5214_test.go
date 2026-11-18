package panewire

import (
	"context"
	"io"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A host-local refusal rules its host out for the launch backoff, not for the
// request's life (hk 5214). These tests drive the transport the way a placement
// pass does: a candidate the exclusion still rules out returns without binding
// or touching the node, and one it no longer rules out is bound and dispatched.

// expiringRefusalHub opens a hub on the inbox at path with a 10-minute launch
// backoff in its placement policy, so a refusal expires inside a request's
// one-hour deadline. Its spawns run against a job store that never has a job, so
// a dispatched request is only what the node answers.
func expiringRefusalHub(t *testing.T, path string) *HubServer {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	policy := backoffPolicyFile(t, `{"local_machine":"machine-a","spill_targets":[],"max_active_jobs":5,"launch_backoff_minutes":10}`)
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: path, PlacementPolicyPath: policy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: spawnJobsFixture{}, Out: out, Err: errout}, nil
	}
	return h
}

// expiringRefusalRequest is an auto placement with the longest deadline a
// submission may carry (3600 s), so it is still open after two backoffs.
func expiringRefusalRequest() fleet.Submission {
	request := autoSpawnRequest()
	request.DeadlineSeconds = 3600
	return request
}

func checkoutDirtyAnswer() map[string]any {
	return map[string]any{"rc": 81, "job_id": "", "pane": "", "stdout_tail": "", "error": "checkout_refused:dirty", "stage": "checkout", "retryable": true}
}

func landedAnswer() map[string]any {
	return map[string]any{"rc": 0, "job_id": "job-landed", "pane": "pane-landed", "stdout_tail": "ok"}
}

// placeOnNode runs one placement candidate against machine-a: the transport binds
// the request and dispatches it, and the node reads the spawn and answers with
// answer. It fails the test unless the node was the one asked, and returns the
// placement's reply and error.
func placeOnNode(t *testing.T, transport *directSpawnTransport, node *websocket.Conn, request fleet.Submission, id int, answer map[string]any) (fleet.HubSpawnReply, error) {
	t.Helper()
	type placed struct {
		reply fleet.HubSpawnReply
		err   error
	}
	done := make(chan placed, 1)
	go func() {
		reply, err := dispatchPlacedSpawn(boundedContext(t), transport, refusalNodeRequest(request, id, "machine-a"))
		done <- placed{reply, err}
	}()
	sent := t14ReadNodeSpawn(t, node)
	if sent.RequestID != t14RequestID(id) {
		t.Fatalf("node was sent request %s, want %s", sent.RequestID, t14RequestID(id))
	}
	answer["type"], answer["request_id"] = "job.spawn.result", sent.RequestID
	if err := wsjson.Write(context.Background(), node, answer); err != nil {
		t.Fatal(err)
	}
	got := <-done
	return got.reply, got.err
}

// placementSkipsHost asks the transport to place request on machine-a. An
// exclusion that still holds answers NextHost without a bind, so the node sees
// nothing; any other reply fails the test.
func placementSkipsHost(t *testing.T, transport *directSpawnTransport, request fleet.Submission, id int) {
	t.Helper()
	reply, err := transport.Spawn(boundedContext(t), refusalNodeRequest(request, id, "machine-a"))
	if err != nil || !reply.NextHost {
		t.Fatalf("host was not skipped inside its refusal window: reply=%+v err=%v", reply, err)
	}
}

func refusalExclusions(t *testing.T, h *HubServer, requestID string) []hubSpawnExclusion {
	t.Helper()
	task, err := h.inbox.store.spawnTask(context.Background(), requestID)
	if err != nil {
		t.Fatal(err)
	}
	return task.Exclusions
}

func TestHostRefusalExpiresAndHostIsTriedAgain_t5214(t *testing.T) {
	h := expiringRefusalHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	clock := time.Now().UTC()
	h.now = func() time.Time { return clock }
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	node := t14Node(t, h, server, true)
	request := expiringRefusalRequest()
	task, _, err := h.inbox.store.claimSpawn(context.Background(), request, "test-job", "parent", clock)
	if err != nil {
		t.Fatal(err)
	}
	transport := &directSpawnTransport{h: h, task: task}
	backoff := h.hostRefusalBackoff()

	// machine-a is the only host and refuses a dirty checkout: the request waits
	// and the refusal rules the host out for the launch backoff.
	if reply, err := placeOnNode(t, transport, node, request, 940, checkoutDirtyAnswer()); err != nil || !reply.NextHost {
		t.Fatalf("refusal did not hand the request to the next candidate: %+v", reply)
	}
	first := refusalExclusions(t, h, request.RequestID)
	if len(first) != 1 || first[0].ExpiresMS != clock.Add(backoff).UnixMilli() {
		t.Fatalf("refusal exclusion %+v, want one expiring at backoff %s", first, backoff)
	}

	// Inside the backoff the host is still skipped, and the node is not asked.
	clock = clock.Add(backoff - time.Minute)
	placementSkipsHost(t, transport, request, 941)

	// Past the backoff the next check asks machine-a again. It refuses once more,
	// and that refusal re-arms the host for another backoff.
	clock = clock.Add(time.Minute + time.Second)
	if reply, err := placeOnNode(t, transport, node, request, 942, checkoutDirtyAnswer()); err != nil || !reply.NextHost {
		t.Fatalf("repeat refusal did not hand the request on: %+v", reply)
	}
	rearmed := refusalExclusions(t, h, request.RequestID)
	if len(rearmed) != 2 || rearmed[1].ExpiresMS != clock.Add(backoff).UnixMilli() {
		t.Fatalf("repeat refusal did not re-arm the host: %+v", rearmed)
	}

	// The second refusal holds until its own expiry. Then the host is asked and
	// its clean checkout is not refused again: the answer is not handed on, and
	// no refusal is recorded for it. (Landing is verified by the node's receipt
	// and pane, which this fixture does not stage, so the placement may end
	// pending here.)
	clock = time.UnixMilli(rearmed[1].ExpiresMS - 1)
	placementSkipsHost(t, transport, request, 943)
	clock = time.UnixMilli(rearmed[1].ExpiresMS)
	if reply, _ := placeOnNode(t, transport, node, request, 944, landedAnswer()); reply.NextHost {
		t.Fatalf("clean checkout was handed on: %+v", reply)
	}
	if exclusions := refusalExclusions(t, h, request.RequestID); len(exclusions) != 2 {
		t.Fatalf("clean checkout recorded a refusal: %+v", exclusions)
	}
}

func TestHostRefusalExpiryHoldsAcrossRestart_t5214(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	clock := time.Now().UTC()
	first := expiringRefusalHub(t, path)
	first.now = func() time.Time { return clock }
	server := httptest.NewServer(first.Handler())
	t.Cleanup(server.Close)
	node := t14Node(t, first, server, true)
	request := expiringRefusalRequest()
	task, _, err := first.inbox.store.claimSpawn(context.Background(), request, "test-job", "parent", clock)
	if err != nil {
		t.Fatal(err)
	}
	transport := &directSpawnTransport{h: first, task: task}
	if _, err = placeOnNode(t, transport, node, request, 950, checkoutDirtyAnswer()); err != nil {
		t.Fatal(err)
	}
	backoff := first.hostRefusalBackoff()
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}

	// The restarted hub reads the exclusion and its expiry from the store.
	second := expiringRefusalHub(t, path)
	second.now = func() time.Time { return clock }
	server = httptest.NewServer(second.Handler())
	t.Cleanup(server.Close)
	node = t14Node(t, second, server, true)
	task, err = second.inbox.store.spawnTask(context.Background(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	transport = &directSpawnTransport{h: second, task: task}
	clock = clock.Add(backoff - time.Minute)
	placementSkipsHost(t, transport, request, 951)

	clock = clock.Add(time.Minute + time.Second)
	if reply, _ := placeOnNode(t, transport, node, request, 952, landedAnswer()); reply.NextHost {
		t.Fatalf("restarted hub did not ask the host after the stored expiry: %+v", reply)
	}
}

func TestNonHostLocalRefusalStaysExcludedForLife_t5214(t *testing.T) {
	h := expiringRefusalHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	clock := time.Now().UTC()
	h.now = func() time.Time { return clock }
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	node := t14Node(t, h, server, true)
	request := expiringRefusalRequest()
	task, _, err := h.inbox.store.claimSpawn(context.Background(), request, "test-job", "parent", clock)
	if err != nil {
		t.Fatal(err)
	}
	transport := &directSpawnTransport{h: h, task: task}

	// A not-started answer moves the request on too, but it is not a host-local
	// refusal: the host stays ruled out for the request's life.
	notStarted := map[string]any{"rc": 124, "job_id": "", "pane": "", "stdout_tail": "", "error": "deadline_not_started", "stage": "claim", "retryable": true}
	if reply, err := placeOnNode(t, transport, node, request, 960, notStarted); err != nil || !reply.NextHost {
		t.Fatalf("not-started answer did not hand the request on: %+v", reply)
	}
	exclusions := refusalExclusions(t, h, request.RequestID)
	if len(exclusions) != 1 || exclusions[0].ExpiresMS != 0 {
		t.Fatalf("non-host-local refusal expires: %+v", exclusions)
	}
	clock = clock.Add(3 * h.hostRefusalBackoff())
	placementSkipsHost(t, transport, request, 961)
}

func TestLiveExclusionAtExpiry_t5214(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	for _, test := range []struct {
		name      string
		expiresMS int64
		live      bool
	}{
		{"never expires", 0, true},
		{"before expiry", now.Add(time.Millisecond).UnixMilli(), true},
		{"at expiry", now.UnixMilli(), false},
		{"after expiry", now.Add(-time.Second).UnixMilli(), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := (hubSpawnExclusion{Machine: "machine-a", ExpiresMS: test.expiresMS}).liveAt(now); got != test.live {
				t.Fatalf("liveAt=%v, want %v", got, test.live)
			}
		})
	}
}

func TestPlacementTraceDropsExpiredRefusalAndShowsRemaining_t5214(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	previous := fleet.PlacementTrace{Source: "auto", Candidates: []fleet.PlacementCandidateRecord{
		{Host: "desktop", Outcome: "skipped", Reason: "hub_checkout_refused:dirty"},
		{Host: "laptop", Outcome: "skipped", Reason: "hub_checkout_refused:dirty_retry_in_10s"},
		{Host: "mac", Outcome: "busy", Reason: "pool_cap"},
	}}
	exclusions := []hubSpawnExclusion{
		{Machine: "desktop", Reason: "checkout_refused:dirty", ExpiresMS: now.Add(540 * time.Second).UnixMilli()},
		{Machine: "laptop", Reason: "checkout_refused:dirty", ExpiresMS: now.Add(-time.Second).UnixMilli()},
		{Machine: "server", Reason: "deadline_not_started"},
	}
	trace := continueSpawnPlacementTrace(previous, fleet.PlacementTrace{}, exclusions, now)
	rows := map[string]fleet.PlacementCandidateRecord{}
	for _, candidate := range trace.Candidates {
		rows[candidate.Host] = candidate
	}
	if _, listed := rows["laptop"]; listed {
		t.Fatalf("expired refusal is still listed as skipped: %+v", rows["laptop"])
	}
	if got := rows["desktop"]; got.Outcome != "skipped" || got.Reason != "hub_checkout_refused:dirty_retry_in_540s" {
		t.Fatalf("live refusal row=%+v, want its remaining time", got)
	}
	if got := rows["server"]; got.Outcome != "skipped" || got.Reason != "hub_deadline_not_started" {
		t.Fatalf("life-long refusal row=%+v", got)
	}
	if got := rows["mac"]; got.Outcome != "busy" || got.Reason != "pool_cap" {
		t.Fatalf("engine's own candidate row was touched: %+v", got)
	}
}

// A row this pass evaluated itself is not a stale carry-over: an expired refusal
// does not remove a fresh hub-transport skip of the same host (mergewake finding,
// hk 5214).
func TestPlacementTraceKeepsFreshSkipOfExpiredRefusalHost_t5214(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	previous := fleet.PlacementTrace{Source: "auto", Candidates: []fleet.PlacementCandidateRecord{
		{Host: "laptop", Outcome: "skipped", Reason: "hub_checkout_refused:dirty"},
	}}
	current := fleet.PlacementTrace{Candidates: []fleet.PlacementCandidateRecord{
		{Host: "laptop", Outcome: "skipped", Reason: "hub_unavailable"},
	}}
	exclusions := []hubSpawnExclusion{
		{Machine: "laptop", Reason: "checkout_refused:dirty", ExpiresMS: now.Add(-time.Second).UnixMilli()},
	}
	trace := continueSpawnPlacementTrace(previous, current, exclusions, now)
	for _, candidate := range trace.Candidates {
		if candidate.Host == "laptop" {
			if candidate.Outcome != "skipped" || candidate.Reason != "hub_unavailable" {
				t.Fatalf("fresh skip of the host was replaced: %+v", candidate)
			}
			return
		}
	}
	t.Fatal("fresh skip of the host was dropped from the trace")
}

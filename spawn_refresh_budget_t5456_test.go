package panewire

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 5456: admission for a shared quota refresh lives in one place. A demand
// that could not finish inside the caller's own budget is refused, a live
// compatible demand is joined, and another account on the same machine and
// pool is a different attempt.
func TestQuotaRefreshAdmissionUsesTheCallersBudget_t5456(t *testing.T) {
	retryHK(t)
	hub := timeoutRefreshHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), &controlPlaneLogCapture{})
	key := "codex:0000000000000001"
	now := time.Now()
	// A one-second measurement with four seconds left cannot be waited for.
	if _, admission := hub.scheduleQuotaRefresh("machine-a", "codex", key, time.Second, now.Add(4*time.Second)); admission != fleet.QuotaRefreshRefused {
		t.Fatal("admitted a demand that cannot finish inside the caller's budget")
	}
	// Nor one whose window alone outlives the budget.
	if _, admission := hub.scheduleQuotaRefresh("machine-a", "codex", key, time.Minute, now.Add(30*time.Second)); admission != fleet.QuotaRefreshRefused {
		t.Fatal("admitted a measurement longer than the budget")
	}
	id, admission := hub.scheduleQuotaRefresh("machine-a", "codex", key, time.Minute, now.Add(2*time.Minute))
	if admission != fleet.QuotaRefreshAdmitted || id == "" {
		t.Fatalf("admission=%v id=%q, want an attempt with an ID", admission, id)
	}
	// The same demand joins that live attempt.
	if again, admission := hub.scheduleQuotaRefresh("machine-a", "codex", key, time.Minute, now.Add(2*time.Minute)); admission != fleet.QuotaRefreshAdmitted || again != id {
		t.Fatalf("join = %q/%v, want the live attempt %q", again, admission, id)
	}
	// Another account on the same machine and pool is a different attempt.
	if other, admission := hub.scheduleQuotaRefresh("machine-a", "codex", "codex:0000000000000002", time.Minute, now.Add(2*time.Minute)); admission != fleet.QuotaRefreshAdmitted || other == id {
		t.Fatalf("another account = %q/%v, want a separate attempt", other, admission)
	}
}

// A demand inside the post-completion cooldown is refused without an attempt:
// nothing is queued for the caller to wait on.
func TestQuotaRefreshCooldownRefusesBeforeAnAttempt_t5456(t *testing.T) {
	retryHK(t)
	hub := timeoutRefreshHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), &controlPlaneLogCapture{})
	key := "codex:0000000000000001"
	now := time.Now()
	id, admission := hub.scheduleQuotaRefresh("machine-a", "codex", key, time.Minute, now.Add(time.Hour))
	if admission != fleet.QuotaRefreshAdmitted {
		t.Fatal("the first demand was not admitted")
	}
	demand := hubQuotaRefreshKey{machine: "machine-a", pool: "codex", accountKey: key}
	hub.quotaRefresh.mu.Lock()
	delete(hub.quotaRefresh.keys, demand) // as a completed attempt leaves it
	hub.quotaRefresh.completed[demand] = now.Add(hubQuotaRefreshRetryDelay)
	hub.quotaRefresh.mu.Unlock()
	if _, admission := hub.scheduleQuotaRefresh("machine-a", "codex", key, time.Minute, now.Add(time.Hour)); admission != fleet.QuotaRefreshRefused {
		t.Fatalf("a demand inside the cooldown after a success = %v, want refused", admission)
	}
	hub.quotaRefresh.mu.Lock()
	hub.quotaRefresh.completed[demand] = now.Add(-time.Second)
	hub.quotaRefresh.mu.Unlock()
	if next, admission := hub.scheduleQuotaRefresh("machine-a", "codex", key, time.Minute, now.Add(time.Hour)); admission != fleet.QuotaRefreshAdmitted || next == id {
		t.Fatalf("after the cooldown = %q/%v, want a new attempt", next, admission)
	}
}

// A request that reaches its limit without a candidate says what the last
// attempt tried and why each pool was dropped, and spawn status prints it
// (hk 5456 wanted 3 and 4).
func TestSpawnGivenUpNamesTriedCandidates_t5456(t *testing.T) {
	retryHK(t)
	hub := timeoutRefreshHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), &controlPlaneLogCapture{})
	request := durableSpawnRequest()
	task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// The attempt waited on one pool's refresh, which finished without making
	// the reading usable.
	key := "codex:0000000000000001"
	if err := hub.inbox.store.noteSpawnRefreshWaits(t.Context(), request.RequestID, task.Attempt, []hubSpawnRefreshWait{{Machine: "machine-a", Pool: "codex", AccountKey: key, Attempt: "qr-1", At: time.Now()}}); err != nil {
		t.Fatal(err)
	}
	if err := hub.inbox.store.recordQuotaRefreshResult(t.Context(), "qr-1", "machine-a", "codex", key, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	expired, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	expired.Deadline = time.Now().Add(-time.Second)
	expired.Expires = time.Now().Add(-time.Second)
	hub.executeSpawnTask(t.Context(), expired)
	final, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != "given_up" {
		t.Fatalf("status = %s, want given_up", final.Status)
	}
	if len(final.Outcome.Tried) != 1 || final.Outcome.Tried[0].Pool != "codex" || final.Outcome.Tried[0].Host != "machine-a" {
		t.Fatalf("tried = %+v, want the codex refresh this attempt waited on", final.Outcome.Tried)
	}
	for _, want := range []string{"no candidate could take it", "tried: codex/machine-a", "refresh finished without a usable reading"} {
		if !strings.Contains(final.Outcome.Reason, want) {
			t.Fatalf("given up reason %q does not name %q", final.Outcome.Reason, want)
		}
	}
}

// A request given up before any attempt was placed says that too, instead of
// an empty list.
func TestSpawnGivenUpWithoutCandidatesSaysSo_t5456(t *testing.T) {
	task := hubSpawnTask{Attempt: 2, Expires: time.Now().Add(-time.Minute)}
	outcome := spawnGivenUpOutcome(task, "machine-a", nil)
	if !strings.Contains(outcome.Reason, "never found a candidate") {
		t.Fatalf("reason %q does not say no candidate was found", outcome.Reason)
	}
	if len(outcome.Tried) != 0 {
		t.Fatalf("tried = %+v, want none", outcome.Tried)
	}
}

// hk 5456 review: a wait is settled only by the attempt it names. Another
// attempt's completion for the same machine and pool — an earlier or a later
// one — says nothing about it, and a wait recorded before attempts had IDs is
// uncorrelated.
func TestSpawnRefreshSettledUsesTheAttemptTheWaitNames_t5456(t *testing.T) {
	retryHK(t)
	hub := timeoutRefreshHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), &controlPlaneLogCapture{})
	key := "codex:0000000000000001"
	now := time.Now()
	task := hubSpawnTask{RefreshWaits: []hubSpawnRefreshWait{{Machine: "machine-a", Pool: "codex", AccountKey: key, Attempt: "qr-7", At: now}}}
	record := func(id, outcome string) {
		t.Helper()
		if err := hub.inbox.store.recordQuotaRefreshResult(t.Context(), id, "machine-a", "codex", key, outcome, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	// Another attempt's successful completion does not settle this wait.
	record("qr-6", "")
	if reason, settled := hub.spawnRefreshSettled(task, "machine-a", "codex", key); settled {
		t.Fatalf("another attempt's completion settled this wait: %q", reason)
	}
	// This wait's own attempt finished without a usable reading.
	record("qr-7", "")
	reason, settled := hub.spawnRefreshSettled(task, "machine-a", "codex", key)
	if !settled || !strings.Contains(reason, "without a usable reading") {
		t.Fatalf("settled=%v reason=%q, want a finished attempt with no usable reading", settled, reason)
	}
	// Another account on the same machine and pool is not this demand.
	if reason, settled := hub.spawnRefreshSettled(task, "machine-a", "codex", "codex:0000000000000002"); settled {
		t.Fatalf("another account settled this wait: %q", reason)
	}
	// A node that measured another account cannot supply this one: settled,
	// and the host is named.
	record("qr-7", "account_mismatch")
	reason, settled = hub.spawnRefreshSettled(task, "machine-a", "codex", key)
	if !settled || !strings.Contains(reason, "machine-a") || !strings.Contains(reason, "another account") {
		t.Fatalf("settled=%v reason=%q, want the account mismatch named", settled, reason)
	}
	// A transient failure is retried, not settled.
	record("qr-7", "node_timeout")
	if reason, settled := hub.spawnRefreshSettled(task, "machine-a", "codex", key); settled {
		t.Fatalf("a timeout settled the wait: %q", reason)
	}
	// A wait recorded before attempts had IDs is uncorrelated: it is never
	// settled, however many rows the machine and pool have.
	legacy := hubSpawnTask{RefreshWaits: []hubSpawnRefreshWait{{Machine: "machine-a", Pool: "codex", AccountKey: key}}}
	if err := hub.inbox.store.recordQuotaRefreshAttempt(t.Context(), "machine-a", "codex", "", time.Now()); err != nil {
		t.Fatal(err)
	}
	if reason, settled := hub.spawnRefreshSettled(legacy, "machine-a", "codex", key); settled {
		t.Fatalf("an uncorrelated wait settled: %q", reason)
	}
}

// t5456Node connects a node that advertises exactly the given capabilities.
func t5456Node(t *testing.T, hub *HubServer, server *httptest.Server, machine string, capabilities []string) *websocket.Conn {
	t.Helper()
	headers := http.Header{}
	headers.Set(hubMachineIDHeader, machine)
	headers.Set(hubAuthorizationHeader, "Bearer node-token")
	connection, _, err := websocket.Dial(t.Context(), t14WSURL(server.URL, "/v1/agent"), &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.CloseNow() })
	if err := wsjson.Write(t.Context(), connection, map[string]any{"type": "hello", "machine_id": machine, "version": "t5456", "accepting": true, "capabilities": capabilities}); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, "the node to connect", func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		record := hub.nodes[machine]
		return record != nil && record.agent != nil && record.state == "connected"
	})
	return connection
}

// hk 5456 review: a node built before the account-aware refresh decodes its
// arguments strictly. The account is sent only to a connection that advertised
// support; an older node receives the pool-only request it knows.
func TestQuotaRefreshSendsTheAccountOnlyToAnAdvertisingNode_t5456(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-old": "node-token", "machine-new": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	old := t5456Node(t, hub, server, "machine-old", []string{"spawn_v2", "spawn_ops_v1"})
	newer := t5456Node(t, hub, server, "machine-new", []string{"spawn_v2", "spawn_ops_v1", hubCapabilityQuotaRefreshAccount})
	key := "codex:0000000000000001"
	deadline := time.Now().Add(time.Hour)
	if _, admission := hub.scheduleQuotaRefresh("machine-old", "codex", key, time.Minute, deadline); admission != fleet.QuotaRefreshAdmitted {
		t.Fatalf("old node admission = %v", admission)
	}
	if _, admission := hub.scheduleQuotaRefresh("machine-new", "codex", key, time.Minute, deadline); admission != fleet.QuotaRefreshAdmitted {
		t.Fatalf("new node admission = %v", admission)
	}
	if command := nodeCommandRead(t, old); string(command["args"]) != `{"pool":"codex"}` {
		t.Fatalf("an older node received %s, want the pool-only request", command["args"])
	}
	if command := nodeCommandRead(t, newer); string(command["args"]) != `{"account_key":"`+key+`","pool":"codex"}` {
		t.Fatalf("an advertising node received %s, want the account-named request", command["args"])
	}
}

// hk 5456 review (outside-diff): a request that parks and later reaches its
// limit names what the parked pass tried. The pass's answer is durable with the
// park, so a restart and the placement passes in between do not lose it.
func TestSpawnGivenUpAfterAParkNamesWhatTheParkedPassTried_t5456(t *testing.T) {
	retryHK(t)
	hub := timeoutRefreshHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), &controlPlaneLogCapture{})
	request := durableSpawnRequest()
	task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	key := "codex:0000000000000001"
	tried := []hubSpawnTried{{Host: "oci", Pool: "codex", AccountKey: key, Reason: "quota exhausted window=7d cutoff=99"}}
	if _, err := hub.parkSpawnTask(t.Context(), task, spawnParkQuota, "placement refused", tried, hub.spawnWakeSnapshot()); err != nil {
		t.Fatal(err)
	}
	parked, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if len(parked.Tried) != 1 || parked.Tried[0].Host != "oci" || parked.Tried[0].AccountKey != key {
		t.Fatalf("parked tried = %+v, want the pass's candidates persisted", parked.Tried)
	}
	// A placement pass that changes nothing else must not lose it, and the
	// expiry names it.
	parked.Deadline = time.Now().Add(-time.Second)
	parked.Expires = time.Now().Add(-time.Second)
	hub.executeSpawnTask(t.Context(), parked)
	final, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != "given_up" {
		t.Fatalf("status = %s, want given_up", final.Status)
	}
	for _, want := range []string{"no candidate could take it", "codex/oci", key, "cutoff=99"} {
		if !strings.Contains(final.Outcome.Reason, want) {
			t.Fatalf("given up reason %q does not name %q", final.Outcome.Reason, want)
		}
	}
}

// hk 5456 review: a demand admitted again after a transient failure replaces
// its earlier entry, so the request's link always names the attempt it is
// currently waiting on.
func TestSpawnRefreshWaitsReplaceARetriedDemand_t5456(t *testing.T) {
	codex := "codex:0000000000000001"
	claude := "claude:0000000000000001"
	previous := []hubSpawnRefreshWait{{Machine: "m", Pool: "codex", AccountKey: codex, Attempt: "qr-1"}}
	current := []hubSpawnRefreshWait{
		{Machine: "m", Pool: "codex", AccountKey: codex, Attempt: "qr-2"},
		{Machine: "m", Pool: "claude", AccountKey: claude, Attempt: "qr-3"},
	}
	merged := mergeSpawnRefreshWaits(previous, current)
	if len(merged) != 2 {
		t.Fatalf("merged = %+v, want one entry per demand", merged)
	}
	if merged[0].Attempt != "qr-2" || merged[1].Attempt != "qr-3" {
		t.Fatalf("merged = %+v, want the retry's attempt to replace the old one", merged)
	}
}

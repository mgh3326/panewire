package panewire

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

// acceptObserveHub is a hub with one worker lane reporting to one design lane
// and a fleet-event recorder in place of the state-directory file.
func acceptObserveHub(t *testing.T) (*HubServer, *[]fleetEvent, *spawnSentryTransport) {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	lanes := `{"lanes":{"worker-a":{"machine":"host-a","pane":"w1:p1","parent":"design-a"},"design-a":{"machine":"host-a","pane":"w1:p2"}}}`
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": "op", "host-a": "node-token"},
		ReportRelayPath: r20LanesFile(t, lanes),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	events := &[]fleetEvent{}
	hub.fleetEvents = func(_ time.Time, ev fleetEvent) bool {
		*events = append(*events, ev)
		return true
	}
	transport := &spawnSentryTransport{}
	hub.spawnSentry = spawnReasonSentryClient(t, transport)
	return hub, events, transport
}

// Acceptance (a): a working_wait_expired drop emits one relay event with the
// dropped outcome and one warning-level Sentry event with its fingerprint.
func TestRelayWorkingWaitDropEmitsFleetEventAndSentry(t *testing.T) {
	hub, events, transport := acceptObserveHub(t)
	dropped := relayDroppedPayload{JobID: "lane-event-worker-a-e17", Pane: "w1:p1", Lane: "worker-a", OriginalEventID: 17, Reason: relayDroppedWorkingExpired, Harness: "cmd"}

	hub.observeRelayDropped("host-a", dropped)
	hub.observeRelayDropped("host-a", dropped) // a repeated report is the same row outcome

	if len(*events) != 1 {
		t.Fatalf("relay events=%+v, want one", *events)
	}
	ev := (*events)[0]
	if ev.Kind != "relay" || ev.Outcome != "dropped:working_wait_expired" || ev.Job != dropped.JobID || !strings.Contains(ev.Detail, "harness=cmd") {
		t.Fatalf("relay event=%+v", ev)
	}
	failure, raw := spawnReasonErrorEvent(t, transport)
	if failure.Level != sentry.LevelWarning || strings.Join(failure.Fingerprint, "|") != "relay-dropped|working_wait_expired|cmd" {
		t.Fatalf("sentry=%+v raw=%s", failure, raw)
	}
	if failure.Tags["reason"] != "working_wait_expired" || failure.Tags["harness"] != "cmd" || failure.Tags["lane"] != "worker-a" {
		t.Fatalf("sentry tags=%+v", failure.Tags)
	}
}

// A relay delivered for a worker lane is recorded once, and a design lane's
// row is recorded too.
func TestRelayDeliveredEmitsOneFleetEvent(t *testing.T) {
	hub, events, _ := acceptObserveHub(t)
	pending := relayPending{eventID: 17, kind: "lane.event", machine: "host-a", pane: "w1:p1", event: hubJobEventPayload{JobID: "lane-event-worker-a-e17", OwnerLane: "worker-a"}}
	if !hub.markRelayEventDelivered(pending) {
		t.Fatal("delivery was not accepted")
	}
	hub.markRelayEventDelivered(pending)
	if len(*events) != 1 || (*events)[0].Outcome != "delivered" || (*events)[0].Kind != "relay" {
		t.Fatalf("relay events=%+v", *events)
	}
}

// An unconfirmed report is one row outcome, not one per retry.
func TestRelayUnconfirmedEmitsOneFleetEvent(t *testing.T) {
	hub, events, _ := acceptObserveHub(t)
	pending := relayPending{eventID: 21, kind: "lane.event", machine: "host-a", pane: "w1:p1", event: hubJobEventPayload{JobID: "lane-event-worker-a-e21", OwnerLane: "worker-a"}}
	hub.observeRelayUnconfirmed(pending, relayAckPayload{Reason: "maybe_in_pane presend:composer_unread"})
	hub.observeRelayUnconfirmed(pending, relayAckPayload{Reason: "maybe_in_pane postsend:composer_unread"})
	if len(*events) != 1 || (*events)[0].Outcome != "unconfirmed:maybe_in_pane" {
		t.Fatalf("relay events=%+v", *events)
	}
}

// Acceptance (b): the fifth held refusal on one key inside the window sends
// one Sentry event; a sixth within the hour sends none.
func TestSpawnHeldLoopReportsOncePerRoleKeyPerHour(t *testing.T) {
	hub, events, transport := acceptObserveHub(t)
	now := time.Unix(1_800_000_000, 0)
	hub.now = func() time.Time { return now }
	for range spawnHeldLoopCount {
		hub.observeSpawnHeld("test-job-r2", "42:reviewer:p1", "succeeded_pane_held", "pane_listed", "holder_job=test-job")
		now = now.Add(time.Minute)
	}
	if len(transport.events) != 1 {
		t.Fatalf("sentry events=%d, want 1", len(transport.events))
	}
	failure, raw := spawnReasonErrorEvent(t, transport)
	if strings.Join(failure.Fingerprint, "|") != "spawn-held-loop|succeeded_pane_held" || failure.Tags["role_key"] != "42:reviewer:p1" {
		t.Fatalf("sentry=%+v raw=%s", failure, raw)
	}
	hub.observeSpawnHeld("test-job-r2", "42:reviewer:p1", "succeeded_pane_held", "pane_listed", "holder_job=test-job")
	if len(transport.events) != 1 {
		t.Fatalf("a sixth hold within the hour sent another event: %d", len(transport.events))
	}
	if len(*events) != spawnHeldLoopCount+1 {
		t.Fatalf("spawn_held events=%d, want %d", len(*events), spawnHeldLoopCount+1)
	}
	if last := (*events)[len(*events)-1]; last.Kind != "spawn_held" || last.Outcome != "succeeded_pane_held" {
		t.Fatalf("spawn_held event=%+v", last)
	}
}

// Acceptance (d): two admission_refused causes carry different fingerprints.
func TestAdmissionRefusedSentryFingerprintSplitsByReason(t *testing.T) {
	hub := asyncSpawnHub(t)
	transport := &spawnSentryTransport{}
	hub.spawnSentry = spawnReasonSentryClient(t, transport)
	for i, reason := range []string{"admission_refused: missing seat policy for worker-x", "admission_refused: quota catalog unavailable"} {
		req := durableSpawnRequest()
		req.RequestID = fmt.Sprintf("d676220b-e763-4af1-aefd-00000000003%d", i)
		req.Args = append([]string(nil), req.Args...)
		job := fmt.Sprintf("admit-job-%d", i)
		for j, arg := range req.Args {
			switch arg {
			case "test-job":
				req.Args[j] = job
			case "42":
				req.Args[j] = fmt.Sprintf("5%d", i)
			}
		}
		task, _, err := hub.inbox.store.claimSpawn(t.Context(), req, job, "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		outcome := hubSpawnOutcome{RC: 70, Stage: "placement", Reason: reason, Host: "machine-a"}
		if _, err = hub.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil {
			t.Fatal(err)
		}
		hub.recordSpawnTrace(task, outcome)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	var fingerprints []string
	for _, event := range transport.events {
		if event.Type != "transaction" {
			fingerprints = append(fingerprints, strings.Join(event.Fingerprint, "|"))
		}
	}
	if len(fingerprints) != 2 || fingerprints[0] == fingerprints[1] {
		t.Fatalf("fingerprints=%v", fingerprints)
	}
	for _, want := range []string{"spawn|placement|admission_refused:seat_policy_missing", "spawn|placement|admission_refused:quota_catalog"} {
		if !containsString(fingerprints, want) {
			t.Fatalf("fingerprints=%v missing %q", fingerprints, want)
		}
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

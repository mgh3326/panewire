package panewire

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// #725: a relay inject is de-duplicated by the durable (lane, event_id)
// identity it carries. event_id is the handoffkeep row id, which the
// lane.event idempotency index pins 1:1 to the canonical producer identity
// idleWakeEventID builds — so suppressing a repeat of (lane, event_id) is
// suppressing a repeat of the same canonical event. Shadow mode is the
// default: it records every decision and changes nothing.
//
// r725Directive is r27Directive with the receiving lane explicit: the dedupe
// key scopes per lane, and two lanes legitimately receive one event each.
func r725Directive(id int64, lane, pane, text string) hubOutboundMessage {
	return hubOutboundMessage{Type: "relay.inject", Kind: "lane.event", JobID: "relay-job-" + strconv.FormatInt(id, 10), Pane: pane, Lane: lane, EventID: id, Text: text, DeliverPolicy: "idle"}
}

// r725Wake is t650Wake with the namespace explicit: a generation reset is
// exactly the same pane and seq under a new namespace, which must be a new
// identity, never a duplicate of the old generation's row.
func r725Wake(namespace, pane, label string, sequence uint64, changedAt time.Time) hubJobEventPayload {
	eventID := idleWakeEventID(namespace, pane, sequence)
	request := idleWakeRouteRequest{EventID: eventID, Pane: pane, Label: label, State: "done", ChangedAt: changedAt.UTC().Format(time.RFC3339Nano), StateChangeSeq: sequence}
	text := idleWakeRouteText(request, idleWakeOwnerResolution{Lane: t650Director})
	return hubJobEventPayload{JobID: laneEventTransportID(t650Director, eventID), Epoch: 1, OwnerLane: t650Director, EventID: eventID, Text: text}
}

// r725Node mirrors r27Node but lets a test control the inject verdict — the
// dedupe contract lives exactly at the boundary where the inject answers.
func r725Node(t *testing.T, store *Store, fake *r27FakeHerdr, inject func(context.Context, string, string) bool) (*HubClient, *[]string, chan hubClientEvent) {
	t.Helper()
	if fake == nil {
		t.Fatal("R27 isolation guard: fake herdr command seam is required")
	}
	prompts := new([]string)
	events := make(chan hubClientEvent, 32)
	client := &HubClient{outbox: store, relayCommand: fake.run, relayInject: func(ctx context.Context, pane, text string) bool {
		if !inject(ctx, pane, text) {
			return false
		}
		*prompts = append(*prompts, pane+"\x00"+text)
		return true
	}}
	client.setRelayEmitter(func(event hubClientEvent) { events <- event })
	client.relayBusyManager().restore(t.Context())
	return client, prompts, events
}

// r725AwaitClosed bounds every "did the goroutine finish" wait. A broken
// in-flight claim — one never released or never deleted — must fail as an
// assertion, not as a test-timeout hang that hides which path leaked.
func r725AwaitClosed(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not return", what)
	}
}

// r725AwaitCalls polls the fake's call log until a pane-targeted command —
// `agent get pane-old` rather than just `get` — has run, so a test can pin
// which of two racing offers reached the probe first.
func r725AwaitCalls(t *testing.T, fake *r27FakeHerdr, command, pane string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		fake.mu.Lock()
		seen := false
		for _, call := range fake.calls {
			if len(call) > 2 && call[0] == "agent" && call[1] == command && call[2] == pane {
				seen = true
				break
			}
		}
		fake.mu.Unlock()
		if seen {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("agent %s never targeted %s", command, pane)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// r725AwaitRoute polls until the lane's observed route equals the pane a
// directive just named — the deterministic point where a parked repeat is
// guaranteed to sit inside the claim wait.
func r725AwaitRoute(t *testing.T, manager *relayBusyManager, lane, pane string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if observed, known := manager.observedLanePane(lane); known && observed == pane {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("lane=%s never observed pane=%s", lane, pane)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func r725DeliveredReasons(t *testing.T, events <-chan hubClientEvent, want int) []relayAckPayload {
	t.Helper()
	var acks []relayAckPayload
	deadline := time.After(time.Second)
	for len(acks) < want {
		select {
		case event := <-events:
			if event.Kind != "relay.delivered" {
				continue
			}
			ack, valid := decodeRelayAckPayload(event.Payload)
			if !valid {
				t.Fatalf("delivered payload rejected by hub parser: %s", event.Payload)
			}
			acks = append(acks, ack)
		case <-deadline:
			t.Fatalf("delivered acks=%d want %d", len(acks), want)
		}
	}
	return acks
}

// A resend of a delivered (lane, event_id) — the everyday shape of a lost
// relay.delivered ack followed by a hub re-inject — is suppressed and
// re-acknowledged so the durable row finally closes.
func TestR725ResendAfterDeliverySuppressesOnce(t *testing.T) {
	r27GuardInbox(t)
	t.Setenv("PANEWIRE_RELAY_DEDUPE", "apply")
	fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 2)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, events := r27Node(t, store, fake)
	manager := client.relayBusyManager()
	directive := r725Directive(72501, "lane-a", "fixture-pane", "round one done")
	manager.offer(t.Context(), directive)
	manager.offer(t.Context(), directive)
	if got := *prompts; len(got) != 1 {
		t.Fatalf("prompts=%q", got)
	}
	acks := r725DeliveredReasons(t, events, 2)
	// The re-ack carries the row's own coordinates — JobID, Pane and
	// OriginalEventID are what the hub's pending-window and late-delivery
	// paths match against, so a wrong one leaves the durable row open.
	if acks[1].Reason != "dedupe_suppressed" || acks[1].OriginalEventID != 72501 || acks[1].JobID != directive.JobID || acks[1].Pane != directive.Pane {
		t.Fatalf("re-ack=%+v", acks[1])
	}
	counts := manager.RelayDedupeCounts()
	if counts.Suppressed != 1 || counts.WouldSuppress != 0 || counts.Mismatch != 0 {
		t.Fatalf("counts=%+v", counts)
	}
	record, found, err := store.RelayDeliveredByKey(t.Context(), "lane-a", 72501)
	if err != nil || !found || record.Pane != "fixture-pane" {
		t.Fatalf("delivered record=%+v found=%t err=%v", record, found, err)
	}
}

// Two copies of one inject racing offer() meet the in-flight claim: the
// loser waits for the winner's verdict, finds the delivered record, and is
// a recorded duplicate — one prompt, one suppression, one inject attempt.
func TestR725ConcurrentSameIdentityDeliversOnce(t *testing.T) {
	r27GuardInbox(t)
	t.Setenv("PANEWIRE_RELAY_DEDUPE", "apply")
	entered := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	injectCalls := 0
	fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 2)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, _ := r725Node(t, store, fake, func(context.Context, string, string) bool {
		mu.Lock()
		injectCalls++
		first := injectCalls == 1
		mu.Unlock()
		if first {
			once.Do(func() { close(entered) })
			<-gate
		}
		return true
	})
	manager := client.relayBusyManager()
	directive := r725Directive(72502, "lane-a", "fixture-pane", "concurrent")
	done := make(chan struct{})
	go func() {
		defer close(done)
		manager.offer(t.Context(), directive)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first offer never reached inject")
	}
	repeat := make(chan struct{})
	go func() {
		defer close(repeat)
		manager.offer(t.Context(), directive)
	}()
	// The repeat must still be parked while the winner is inside inject: a
	// verdict returned now would be a guess, and a wrong guess suppresses a
	// delivery whose original may still fail.
	select {
	case <-repeat:
		t.Fatal("concurrent repeat resolved before the first delivery did")
	case <-time.After(50 * time.Millisecond):
	}
	close(gate)
	r725AwaitClosed(t, done, "first offer")
	r725AwaitClosed(t, repeat, "concurrent repeat")
	if got := *prompts; len(got) != 1 {
		t.Fatalf("prompts=%q", got)
	}
	mu.Lock()
	attempts := injectCalls
	mu.Unlock()
	if attempts != 1 {
		t.Fatalf("a suppressed resend still attempted an inject: calls=%d", attempts)
	}
	if counts := manager.RelayDedupeCounts(); counts.Suppressed != 1 {
		t.Fatalf("counts=%+v", counts)
	}
}

// A concurrent repeat is not a verdict on its own — it waits for the
// in-flight delivery's outcome. If that outcome is lane_rerouted (the route
// the repeat itself just updated), the repeat is the retry that still lands
// the event on the new pane; suppressing it would lose the delivery.
func TestR725InflightRepeatReroutedPaneStillDelivers(t *testing.T) {
	r27GuardInbox(t)
	t.Setenv("PANEWIRE_RELAY_DEDUPE", "apply")
	getGate := make(chan struct{})
	fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", getGate: getGate, started: make(chan struct{}, 4)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, events := r27Node(t, store, fake)
	manager := client.relayBusyManager()
	first := make(chan struct{})
	go func() {
		defer close(first)
		manager.offer(t.Context(), r725Directive(72590, "lane-a", "pane-old", "done body"))
	}()
	r725AwaitCalls(t, fake, "get", "pane-old")
	second := make(chan struct{})
	go func() {
		defer close(second)
		manager.offer(t.Context(), r725Directive(72590, "lane-a", "pane-new", "done body"))
	}()
	r725AwaitRoute(t, manager, "lane-a", "pane-new")
	close(getGate)
	r725AwaitClosed(t, first, "first offer")
	r725AwaitClosed(t, second, "rerouted repeat")
	// The first copy dies as lane_rerouted and records nothing; the repeat
	// is the sole delivery and must land on the new pane.
	if got := *prompts; len(got) != 1 || !strings.HasPrefix(got[0], "pane-new") {
		t.Fatalf("prompts=%q", got)
	}
	if counts := manager.RelayDedupeCounts(); counts.Suppressed != 0 {
		t.Fatalf("the surviving delivery was counted as a suppression: %+v", counts)
	}
	r27Await(t, events, "relay.dropped")
}

// The mismatch clause at the in-flight window: a concurrent repeat whose
// payload — or destination — differs from the winner's waits for the
// landing, then is counted as a mismatch and delivered, never suppressed.
func TestR725InflightRepeatMismatchStillDelivers(t *testing.T) {
	r27GuardInbox(t)
	t.Setenv("PANEWIRE_RELAY_DEDUPE", "apply")
	for _, sub := range []struct {
		name      string
		pane      string
		text      string
		wantPanes []string
	}{
		{name: "changed payload", pane: "fixture-pane", text: "v2 text", wantPanes: []string{"fixture-pane", "fixture-pane"}},
		{name: "rerouted pane", pane: "pane-new", text: "done body", wantPanes: []string{"fixture-pane", "pane-new"}},
	} {
		t.Run(sub.name, func(t *testing.T) {
			entered := make(chan struct{})
			gate := make(chan struct{})
			var once sync.Once
			fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 4)}
			store := NewMemoryStore(t)
			defer store.Close()
			client, prompts, _ := r725Node(t, store, fake, func(context.Context, string, string) bool {
				once.Do(func() { close(entered) })
				<-gate
				return true
			})
			manager := client.relayBusyManager()
			first := make(chan struct{})
			go func() {
				defer close(first)
				manager.offer(t.Context(), r725Directive(72591, "lane-a", "fixture-pane", "done body"))
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("first offer never reached inject")
			}
			second := make(chan struct{})
			go func() {
				defer close(second)
				manager.offer(t.Context(), r725Directive(72591, "lane-a", sub.pane, sub.text))
			}()
			close(gate)
			r725AwaitClosed(t, first, "first offer")
			r725AwaitClosed(t, second, "mismatched repeat")
			if got := *prompts; len(got) != 2 {
				t.Fatalf("prompts=%q", got)
			} else {
				for index, pane := range sub.wantPanes {
					if !strings.HasPrefix(got[index], pane+"\x00") {
						t.Fatalf("prompts=%q", got)
					}
				}
			}
			if counts := manager.RelayDedupeCounts(); counts.Mismatch != 1 || counts.Suppressed != 0 {
				t.Fatalf("counts=%+v", counts)
			}
		})
	}
}

// A restart never re-delivers: the durable record outlives the manager, and a
// rebuilt client answers the hub's replay from the same store.
func TestR725DeliveredRecordSurvivesClientRestart(t *testing.T) {
	r27GuardInbox(t)
	t.Setenv("PANEWIRE_RELAY_DEDUPE", "apply")
	fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 2)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, _ := r27Node(t, store, fake)
	client.relayBusyManager().offer(t.Context(), r725Directive(72503, "lane-a", "fixture-pane", "first life"))
	if got := *prompts; len(got) != 1 {
		t.Fatalf("prompts=%q", got)
	}
	// New client, same durable store: the manager is memory, the record is not.
	restarted, restartedPrompts, events := r27Node(t, store, fake)
	manager := restarted.relayBusyManager()
	manager.offer(t.Context(), r725Directive(72503, "lane-a", "fixture-pane", "first life"))
	if got := *restartedPrompts; len(got) != 0 {
		t.Fatalf("restart re-prompted=%q", got)
	}
	if acks := r725DeliveredReasons(t, events, 1); acks[0].Reason != "dedupe_suppressed" {
		t.Fatalf("re-ack=%+v", acks[0])
	}
	if counts := manager.RelayDedupeCounts(); counts.Suppressed != 1 {
		t.Fatalf("counts=%+v", counts)
	}
}

// The crash window the contract names: the delivery was recorded and the
// held-row delete never ran, so both rows exist at boot. The reconcile runs
// at resume() — the first point where the emitter exists, matching the
// daemon's SetRelayOutbox → hello order. Apply retires the stale lease and
// closes the durable row; shadow counts and restores as before.
func TestR725RestoreReconcilesHeldRowAlreadyDelivered(t *testing.T) {
	r27GuardInbox(t)
	seed := func(t *testing.T, store *Store, text string) {
		t.Helper()
		item := relayHeld{Pane: "fixture-pane", Lane: "lane-a", EventID: 72504, JobID: "relay-job-72504", Text: text, HeldSince: time.Now(), DeliverPolicy: "idle", RecvSeq: 1}
		inserted, err := store.InsertRelayHeld(t.Context(), item)
		if err != nil || !inserted {
			t.Fatalf("seed held inserted=%t err=%v", inserted, err)
		}
	}
	t.Run("apply retires the stale held row", func(t *testing.T) {
		t.Setenv("PANEWIRE_RELAY_DEDUPE", "apply")
		fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 1)}
		store := NewMemoryStore(t)
		defer store.Close()
		seed(t, store, "crash window")
		if err := store.RecordRelayDelivered(t.Context(), "lane-a", 72504, "fixture-pane", relayPayloadFingerprint("crash window"), time.Now()); err != nil {
			t.Fatalf("seed delivered err=%v", err)
		}
		client, prompts, events := r27Node(t, store, fake)
		client.relayBusyManager().resume(t.Context())
		if got := *prompts; len(got) != 0 {
			t.Fatalf("restored prompt=%q", got)
		}
		if rows, err := store.RelayHeldForPane(t.Context(), "fixture-pane"); err != nil || len(rows) != 0 {
			t.Fatalf("held rows=%+v err=%v", rows, err)
		}
		if acks := r725DeliveredReasons(t, events, 1); acks[0].Reason != "dedupe_suppressed" {
			t.Fatalf("restore ack=%+v", acks[0])
		}
		manager := client.relayBusyManager()
		if counts := manager.RelayDedupeCounts(); counts.Suppressed != 1 {
			t.Fatalf("counts=%+v", counts)
		}
		// A retired row must leave memory too — a kept ghost would re-arm
		// and double-report a row the store no longer holds.
		manager.mu.Lock()
		ghosts := len(manager.held["fixture-pane"])
		manager.mu.Unlock()
		if ghosts != 0 {
			t.Fatalf("manager.held still lists %d retired rows", ghosts)
		}
	})
	t.Run("shadow restores but counts", func(t *testing.T) {
		t.Setenv("PANEWIRE_RELAY_DEDUPE", "shadow")
		fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", waitGate: make(chan struct{}), started: make(chan struct{}, 1)}
		store := NewMemoryStore(t)
		defer store.Close()
		seed(t, store, "crash window")
		if err := store.RecordRelayDelivered(t.Context(), "lane-a", 72504, "fixture-pane", relayPayloadFingerprint("crash window"), time.Now()); err != nil {
			t.Fatalf("seed delivered err=%v", err)
		}
		client, _, _ := r27Node(t, store, fake)
		manager := client.relayBusyManager()
		manager.resume(t.Context())
		if rows, err := store.RelayHeldForPane(t.Context(), "fixture-pane"); err != nil || len(rows) != 1 {
			t.Fatalf("shadow restore changed held rows=%+v err=%v", rows, err)
		}
		if counts := manager.RelayDedupeCounts(); counts.WouldSuppress != 1 || counts.Suppressed != 0 {
			t.Fatalf("counts=%+v", counts)
		}
		// A reconnect replays resume() — the reconcile is once-only, so a
		// second resume must not recount the same restored row.
		manager.resume(t.Context())
		if counts := manager.RelayDedupeCounts(); counts.WouldSuppress != 1 {
			t.Fatalf("second resume recounts: counts=%+v", counts)
		}
	})
	t.Run("off reconciles nothing", func(t *testing.T) {
		t.Setenv("PANEWIRE_RELAY_DEDUPE", "off")
		fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", waitGate: make(chan struct{}), started: make(chan struct{}, 1)}
		store := NewMemoryStore(t)
		defer store.Close()
		seed(t, store, "crash window")
		if err := store.RecordRelayDelivered(t.Context(), "lane-a", 72504, "fixture-pane", relayPayloadFingerprint("crash window"), time.Now()); err != nil {
			t.Fatalf("seed delivered err=%v", err)
		}
		client, _, _ := r27Node(t, store, fake)
		client.relayBusyManager().resume(t.Context())
		if rows, err := store.RelayHeldForPane(t.Context(), "fixture-pane"); err != nil || len(rows) != 1 {
			t.Fatalf("off-mode reconcile changed held rows=%+v err=%v", rows, err)
		}
		if counts := client.relayBusyManager().RelayDedupeCounts(); counts != (relayDedupeCounters{}) {
			t.Fatalf("off-mode reconcile counted: %+v", counts)
		}
	})
	t.Run("restored row disagreeing with its record is a kept mismatch", func(t *testing.T) {
		t.Setenv("PANEWIRE_RELAY_DEDUPE", "apply")
		fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", waitGate: make(chan struct{}), started: make(chan struct{}, 1)}
		store := NewMemoryStore(t)
		defer store.Close()
		seed(t, store, "edited held text")
		if err := store.RecordRelayDelivered(t.Context(), "lane-a", 72504, "fixture-pane", relayPayloadFingerprint("different delivered text"), time.Now()); err != nil {
			t.Fatalf("seed delivered err=%v", err)
		}
		client, _, _ := r27Node(t, store, fake)
		client.relayBusyManager().resume(t.Context())
		if rows, err := store.RelayHeldForPane(t.Context(), "fixture-pane"); err != nil || len(rows) != 1 {
			t.Fatalf("mismatched held row was dropped: rows=%+v err=%v", rows, err)
		}
		if counts := client.relayBusyManager().RelayDedupeCounts(); counts.Mismatch != 1 || counts.Suppressed != 0 {
			t.Fatalf("counts=%+v", counts)
		}
	})
}

// A new seq is a new event — done A then done B with no working between is
// two identities, and both must reach the pane. This is the anti-coalescing
// guarantee: nothing about an earlier done suppresses a later one.
func TestR725ConsecutiveDoneEventsBothDeliver(t *testing.T) {
	r27GuardInbox(t)
	t.Setenv("PANEWIRE_RELAY_DEDUPE", "apply")
	fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 4)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, _ := r27Node(t, store, fake)
	manager := client.relayBusyManager()
	manager.offer(t.Context(), r725Directive(72510, "lane-a", "fixture-pane", "done round A"))
	manager.offer(t.Context(), r725Directive(72511, "lane-a", "fixture-pane", "done round B"))
	if got := *prompts; len(got) != 2 {
		t.Fatalf("prompts=%q", got)
	}
	// A lookup key that dropped the event id would read round A's record
	// while offering round B: the texts differ, so the tell is Mismatch.
	if counts := manager.RelayDedupeCounts(); counts.Suppressed != 0 || counts.WouldSuppress != 0 || counts.Mismatch != 0 {
		t.Fatalf("counts=%+v", counts)
	}
}

// Arrival order is not identity: replayed batches can arrive out of order,
// and each distinct (lane, event_id) is delivered exactly once regardless.
func TestR725OutOfOrderDistinctIDsBothDeliver(t *testing.T) {
	r27GuardInbox(t)
	t.Setenv("PANEWIRE_RELAY_DEDUPE", "apply")
	fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 4)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, _ := r27Node(t, store, fake)
	manager := client.relayBusyManager()
	manager.offer(t.Context(), r725Directive(72521, "lane-a", "fixture-pane", "newer first"))
	manager.offer(t.Context(), r725Directive(72520, "lane-a", "fixture-pane", "older second"))
	if got := *prompts; len(got) != 2 {
		t.Fatalf("prompts=%q", got)
	}
	if counts := manager.RelayDedupeCounts(); counts.Suppressed != 0 || counts.Mismatch != 0 {
		t.Fatalf("counts=%+v", counts)
	}
}

// The mismatch clause: the same identity arriving with a different payload
// — or naming a different destination pane — is recorded and delivered,
// never silently absorbed as a duplicate.
func TestR725SameIDChangedPayloadIsMismatchNotDuplicate(t *testing.T) {
	r27GuardInbox(t)
	t.Setenv("PANEWIRE_RELAY_DEDUPE", "apply")
	fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 6)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, _ := r27Node(t, store, fake)
	manager := client.relayBusyManager()
	manager.offer(t.Context(), r725Directive(72530, "lane-a", "fixture-pane", "original text"))
	manager.offer(t.Context(), r725Directive(72530, "lane-a", "fixture-pane", "changed text"))
	manager.offer(t.Context(), r725Directive(72530, "lane-a", "fixture-pane-2", "original text"))
	if got := *prompts; len(got) != 3 || !strings.Contains(got[2], "fixture-pane-2") {
		t.Fatalf("prompts=%q", got)
	}
	if counts := manager.RelayDedupeCounts(); counts.Mismatch != 2 || counts.Suppressed != 0 {
		t.Fatalf("counts=%+v", counts)
	}
}

// The dedupe key is (lane, event_id): the same durable identity is owed to
// each receiving lane once, and suppressing lane-a's resend says nothing
// about lane-b's copy.
func TestR725DedupeKeyScopesPerReceivingLane(t *testing.T) {
	r27GuardInbox(t)
	t.Setenv("PANEWIRE_RELAY_DEDUPE", "apply")
	fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 4)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, _ := r27Node(t, store, fake)
	manager := client.relayBusyManager()
	// Identical payload on both lanes: a lookup that dropped the lane half
	// of the key would find lane-a's record a perfect match and swallow
	// lane-b's copy — the fingerprint must not get a chance to mask it.
	manager.offer(t.Context(), r725Directive(72540, "lane-a", "fixture-pane", "same body"))
	manager.offer(t.Context(), r725Directive(72540, "lane-b", "fixture-pane", "same body"))
	manager.offer(t.Context(), r725Directive(72540, "lane-a", "fixture-pane", "same body"))
	if got := *prompts; len(got) != 2 {
		t.Fatalf("prompts=%q", got)
	}
	if counts := manager.RelayDedupeCounts(); counts.Suppressed != 1 || counts.Mismatch != 0 {
		t.Fatalf("counts=%+v", counts)
	}
}

// Shadow is the rollout default: every repeat is counted and journaled, and
// delivery behaves exactly as if the feature were absent.
func TestR725ShadowCountsWithoutSuppressing(t *testing.T) {
	r27GuardInbox(t)
	t.Setenv("PANEWIRE_RELAY_DEDUPE", "shadow")
	fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 4)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, events := r27Node(t, store, fake)
	manager := client.relayBusyManager()
	directive := r725Directive(72550, "lane-a", "fixture-pane", "shadow week")
	manager.offer(t.Context(), directive)
	manager.offer(t.Context(), directive)
	if got := *prompts; len(got) != 2 {
		t.Fatalf("shadow changed delivery: prompts=%q", got)
	}
	// Both copies are acknowledged as real deliveries — no dedupe_suppressed
	// reason may leave the node in shadow mode.
	acks := r725DeliveredReasons(t, events, 2)
	for _, ack := range acks {
		if ack.Reason == "dedupe_suppressed" {
			t.Fatalf("shadow emitted a suppression ack: %+v", ack)
		}
	}
	if counts := manager.RelayDedupeCounts(); counts.WouldSuppress != 1 || counts.Suppressed != 0 {
		t.Fatalf("counts=%+v", counts)
	}
}

// The mode switch is a deliberate opt-in: anything that is not literally
// "apply" — unset, empty, or misspelled — stays in shadow and suppresses
// nothing. "off" is the only other explicit value.
func TestR725ModeNeverSilentlyApplies(t *testing.T) {
	for _, env := range []string{"apply", "off", "shadow", "", "APPLY", "apply ", "yes", "1"} {
		t.Run("env="+env, func(t *testing.T) {
			t.Setenv("PANEWIRE_RELAY_DEDUPE", env)
			mode := relayDedupeModeFromEnv()
			want := relayDedupeShadow
			switch env {
			case "apply":
				want = relayDedupeApply
			case "off":
				want = relayDedupeOff
			}
			if mode != want {
				t.Fatalf("mode=%d want %d", mode, want)
			}
			if env != "apply" && mode.applies() {
				t.Fatalf("env %q silently applied dedupe", env)
			}
		})
	}
}

// The legacy clause: an inject whose identity cannot be verified — no lane,
// or no durable row id — is always delivered, counted as unknown, and never
// suppressed under a manufactured key.
func TestR725UnverifiableIdentityAlwaysDelivers(t *testing.T) {
	r27GuardInbox(t)
	t.Setenv("PANEWIRE_RELAY_DEDUPE", "apply")
	fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 8)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, _ := r27Node(t, store, fake)
	manager := client.relayBusyManager()
	noID := r725Directive(0, "lane-a", "fixture-pane", "pre-durable sender")
	noLane := r725Directive(72560, "", "fixture-pane", "no lane on wire")
	manager.offer(t.Context(), noID)
	manager.offer(t.Context(), noID)
	manager.offer(t.Context(), noLane)
	manager.offer(t.Context(), noLane)
	if got := *prompts; len(got) != 4 {
		t.Fatalf("prompts=%q", got)
	}
	if counts := manager.RelayDedupeCounts(); counts.Unknown != 4 || counts.Suppressed != 0 {
		t.Fatalf("counts=%+v", counts)
	}
}

// The other half of the crash contract: a failed inject records nothing, so
// the held row retries — and a resend of the same identity before the retry
// lands is still recoverable through the held row, not a delivered record.
func TestR725FailedDeliveryIsRedeliveredNotRecorded(t *testing.T) {
	r27GuardInbox(t)
	t.Setenv("PANEWIRE_RELAY_DEDUPE", "apply")
	gate := make(chan struct{})
	var mu sync.Mutex
	injectCalls := 0
	fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", waitGate: gate, started: make(chan struct{}, 4)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, events := r725Node(t, store, fake, func(context.Context, string, string) bool {
		mu.Lock()
		defer mu.Unlock()
		injectCalls++
		return injectCalls > 1
	})
	manager := client.relayBusyManager()
	manager.offer(t.Context(), r725Directive(72570, "lane-a", "fixture-pane", "first inject fails"))
	if got := *prompts; len(got) != 0 {
		t.Fatalf("failed inject prompted=%q", got)
	}
	// Nothing landed, so nothing was recorded: the retry must come from the
	// held row, and a resend must still see this event as undelivered.
	if _, found, err := store.RelayDeliveredByKey(t.Context(), "lane-a", 72570); err != nil || found {
		t.Fatalf("failed delivery recorded found=%t err=%v", found, err)
	}
	r27WaitStarted(t, fake)
	close(gate)
	r27Await(t, events, "relay.delivered")
	if got := *prompts; len(got) != 1 {
		t.Fatalf("prompts=%q", got)
	}
	if _, found, err := store.RelayDeliveredByKey(t.Context(), "lane-a", 72570); err != nil || !found {
		t.Fatalf("landed delivery not recorded found=%t err=%v", found, err)
	}
}

// Hub side: a resend of a (lane, event_id) the hub already persisted is a
// duplicate — but when its body disagrees with the durable row it is a
// recorded mismatch, not a quiet one.
func TestR725HubResendWithChangedPayloadCountsMismatch(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	mismatches := r20t5Subscribe(t, hub)
	source := &hubAgent{persisted: make(chan hubRelayPersistedEvent, 4)}
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	hub.relayLaneEvent(hubJobEventPayload{JobID: "t-1", OwnerLane: "lane-a", EventID: "iw-7", Text: "round done"}, source)
	hub.relayLaneEvent(hubJobEventPayload{JobID: "t-2", OwnerLane: "lane-a", EventID: "iw-7", Text: "round done — edited"}, source)
	if fake.rowCount() != 1 || drainRelays(destination) != 1 {
		t.Fatalf("rows=%d injections=%d", fake.rowCount(), drainRelays(destination))
	}
	if hub.RelayPayloadMismatchCount() != 1 {
		t.Fatalf("mismatches=%d", hub.RelayPayloadMismatchCount())
	}
	events := mismatches("relay.mismatch")
	if len(events) != 1 || !strings.Contains(string(events[0]), `"payload_mismatch"`) || !strings.Contains(string(events[0]), `"iw-7"`) {
		t.Fatalf("mismatch broadcasts=%s", events)
	}
	// A resend whose body agrees with the durable row stays a plain duplicate.
	hub.relayLaneEvent(hubJobEventPayload{JobID: "t-3", OwnerLane: "lane-a", EventID: "iw-7", Text: "round done"}, source)
	if hub.RelayPayloadMismatchCount() != 1 {
		t.Fatalf("matching resend counted as mismatch: %d", hub.RelayPayloadMismatchCount())
	}
}

// Hub side: the durable fingerprint survives delivery and restart replay —
// the resend that needs comparing is the one that arrives afterwards.
func TestR725HubMismatchAfterDeliveryAndReplaySeed(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	fake.undelivere = []handoffkeepRelayEvent{
		{ID: 900, Kind: "lane.event", OwnerLane: "lane-a", EventID: "iw-9", Text: "durable body", JobID: laneEventTransportID("lane-a", "iw-9")},
	}
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	source := &hubAgent{persisted: make(chan hubRelayPersistedEvent, 4)}
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	// A restarted hub replays the durable row: the fingerprint is seeded from
	// the stored text before the injection claim.
	hub.replayUndeliveredLaneEvents(t.Context())
	if drainRelays(destination) != 1 {
		t.Fatal("replay did not inject the seeded row")
	}
	hub.relayLaneEvent(hubJobEventPayload{JobID: "t-9", OwnerLane: "lane-a", EventID: "iw-9", Text: "changed body"}, source)
	if hub.RelayPayloadMismatchCount() != 1 {
		t.Fatalf("mismatches=%d", hub.RelayPayloadMismatchCount())
	}
	// Delivery forgets lanePersisted; the fingerprint must not be forgotten —
	// a post-delivery resend still compares against the durable body.
	hub.markRelayEventDelivered(relayPending{machine: "host-a", pane: "w1:p1", eventID: 900, kind: "lane.event", event: hubJobEventPayload{JobID: "t-9", OwnerLane: "lane-a", EventID: "iw-9"}})
	hub.relayLaneEvent(hubJobEventPayload{JobID: "t-10", OwnerLane: "lane-a", EventID: "iw-9", Text: "changed again"}, source)
	if hub.RelayPayloadMismatchCount() != 2 {
		t.Fatalf("post-delivery mismatches=%d", hub.RelayPayloadMismatchCount())
	}
}

// Hub side, end to end through the node parser: two dones for one pane —
// consecutive seqs with no working event between them — are distinct
// canonical identities, and both are injected. A generation reset repeats
// the seq under a new namespace and is likewise new. Once acknowledged, a
// hub restart replays none of them.
func TestR725HubDistinctIdentitiesAndRestartReplay(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	workers := t650Workers(2)
	lanes := t650Lanes(workers)
	hub := r20Hub(t, lanes, client, nil)
	director, workerNode := t650Attach(hub)
	changed := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	ns := strings.Repeat("b", 32)
	var directives []hubRelayInjectEvent
	// done -> done, no working between: seq 7 then seq 8.
	directives = append(directives, t650Inject(t, hub, workerNode, director, r725Wake(ns, "w16:p1", "b1", 7, changed)))
	directives = append(directives, t650Inject(t, hub, workerNode, director, r725Wake(ns, "w16:p1", "b1", 8, changed)))
	// Pane reuse: generation reset, same seq under a new namespace.
	directives = append(directives, t650Inject(t, hub, workerNode, director, r725Wake(strings.Repeat("c", 32), "w16:p1", "b1", 7, changed)))
	// Interleaved pane: another worker's same seq is another identity.
	directives = append(directives, t650Inject(t, hub, workerNode, director, r725Wake(ns, "w16:p2", "b2", 7, changed)))
	for _, directive := range directives {
		t650Send(t, hub, "host-a", director, "relay.delivered", relayAckPayload{JobID: directive.JobID, Pane: directive.Pane, OriginalEventID: directive.EventID})
		if got := fake.deliveredToFor(directive.EventID); got != "host-a/wB:pD8" {
			t.Fatalf("row %d delivered_to=%q", directive.EventID, got)
		}
	}
	_, restartedDirector := t650Restart(t, lanes, client)
	if resent := drainRelays(restartedDirector); resent != 0 {
		t.Fatalf("hub restart re-sent %d of %d acknowledged idle-wakes, want 0", resent, len(directives))
	}
}

// "off" is the escape hatch: no gate, no suppression, every copy delivered —
// identical to the code before the feature existed.
func TestR725OffModeDeliversEveryCopy(t *testing.T) {
	r27GuardInbox(t)
	t.Setenv("PANEWIRE_RELAY_DEDUPE", "off")
	fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 4)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, _ := r27Node(t, store, fake)
	manager := client.relayBusyManager()
	directive := r725Directive(72580, "lane-a", "fixture-pane", "off mode")
	manager.offer(t.Context(), directive)
	manager.offer(t.Context(), directive)
	if got := *prompts; len(got) != 2 {
		t.Fatalf("off mode suppressed a delivery: prompts=%q", got)
	}
	if counts := manager.RelayDedupeCounts(); counts.Suppressed != 0 || counts.WouldSuppress != 0 || counts.Mismatch != 0 || counts.Unknown != 0 {
		t.Fatalf("off mode touched the dedupe counters: %+v", counts)
	}
}

// The ordering inside deliver() is the crash contract: the delivered record
// must exist before the ack leaves — the record is what a restart consults,
// so an ack first would let a crash produce an unanswerable replay.
func TestR725DeliveredAckOnlyAfterRecord(t *testing.T) {
	r27GuardInbox(t)
	t.Setenv("PANEWIRE_RELAY_DEDUPE", "apply")
	fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 2)}
	store := NewMemoryStore(t)
	defer store.Close()
	prompts := new([]string)
	events := make(chan hubClientEvent, 32)
	var ackedBeforeRecord atomic.Bool
	client := &HubClient{outbox: store, relayCommand: fake.run, relayInject: func(_ context.Context, pane, text string) bool {
		*prompts = append(*prompts, pane+"\x00"+text)
		return true
	}}
	client.setRelayEmitter(func(event hubClientEvent) {
		if event.Kind == "relay.delivered" {
			if _, found, err := store.RelayDeliveredByKey(t.Context(), "lane-a", 72581); err == nil && !found {
				ackedBeforeRecord.Store(true)
			}
		}
		events <- event
	})
	client.relayBusyManager().restore(t.Context())
	client.relayBusyManager().offer(t.Context(), r725Directive(72581, "lane-a", "fixture-pane", "ordering"))
	if got := *prompts; len(got) != 1 {
		t.Fatalf("prompts=%q", got)
	}
	r27Await(t, events, "relay.delivered")
	if ackedBeforeRecord.Load() {
		t.Fatal("relay.delivered was emitted before the delivered record existed")
	}
}

// Store invariants the dedupe gate depends on: first-writer-wins (a repeat
// delivery never rewrites the pane's original record) and pruning bounded
// by relayDeliveredMaxAge (a fresh record is never vacuumed away).
func TestR725DeliveredRecordStoreInvariants(t *testing.T) {
	store := NewMemoryStore(t)
	defer store.Close()
	now := time.Now()
	if err := store.RecordRelayDelivered(t.Context(), "lane-a", 72582, "pane-first", "sha-first", now); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordRelayDelivered(t.Context(), "lane-a", 72582, "pane-second", "sha-second", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	record, found, err := store.RelayDeliveredByKey(t.Context(), "lane-a", 72582)
	if err != nil || !found || record.Pane != "pane-first" || record.PayloadSHA != "sha-first" {
		t.Fatalf("first-writer-wins broken: record=%+v found=%t err=%v", record, found, err)
	}
	if err := store.RecordRelayDelivered(t.Context(), "lane-a", 72583, "pane-old", "sha-old", now.Add(-relayDeliveredMaxAge-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordRelayDelivered(t.Context(), "lane-a", 72584, "pane-new", "sha-new", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.RelayDeliveredByKey(t.Context(), "lane-a", 72583); err != nil || found {
		t.Fatalf("stale record survived pruning: found=%t err=%v", found, err)
	}
	for _, id := range []int64{72582, 72584} {
		if _, found, err := store.RelayDeliveredByKey(t.Context(), "lane-a", id); err != nil || !found {
			t.Fatalf("fresh record %d pruned: found=%t err=%v", id, found, err)
		}
	}
}

// Hub side, durable-duplicate corner: a restarted hub holds no in-memory
// claim, so the resend reaches the POST — handoffkeep answers 200 with the
// original row, and a changed body must surface as a counted mismatch while
// the durable text is what gets injected.
func TestR725HubPostConflictCountsMismatch(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	lanes := `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`
	hub := r20Hub(t, lanes, client, nil)
	source := &hubAgent{persisted: make(chan hubRelayPersistedEvent, 4)}
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	hub.relayLaneEvent(hubJobEventPayload{JobID: "t-1", OwnerLane: "lane-a", EventID: "iw-7", Text: "original"}, source)
	if drainRelays(destination) != 1 {
		t.Fatal("first event not injected")
	}
	restarted := r20Hub(t, lanes, client, nil)
	restarted.nodes["host-a"] = &hubNodeRecord{agent: destination}
	restarted.relayLaneEvent(hubJobEventPayload{JobID: "t-2", OwnerLane: "lane-a", EventID: "iw-7", Text: "changed"}, source)
	if restarted.RelayPayloadMismatchCount() != 1 {
		t.Fatalf("post-conflict mismatches=%d", restarted.RelayPayloadMismatchCount())
	}
	if fake.rowCount() != 1 {
		t.Fatalf("the duplicate created a second durable row: rows=%d", fake.rowCount())
	}
	select {
	case directive := <-destination.relays:
		if !strings.Contains(directive.Text, "original") || strings.Contains(directive.Text, "changed") {
			t.Fatalf("injected text=%q, want the durable row's body", directive.Text)
		}
	case <-time.After(time.Second):
		t.Fatal("the durable duplicate was not re-injected")
	}
}

// #725 round 2, the shape the wait-on-verdict rework exposed: the winning
// copy ends up HELD on the pane the lane has since left — a busy old pane,
// no inject failure needed. A parked repeat that wakes and folds into that
// stale row strands the event: the row expires as lane_rerouted on release
// and the repeat's own destination never sees a prompt. The woken repeat
// must retire the stale lease and deliver itself through the normal path.
func TestR725InflightRepeatHeldOldPaneReroutesToNew(t *testing.T) {
	r27GuardInbox(t)
	for _, mode := range []string{"apply", "shadow"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("PANEWIRE_RELAY_DEDUPE", mode)
			getGate := make(chan struct{})
			fake := &r27FakeHerdr{
				t:               t,
				getStatusByPane: map[string]string{"pane-old": "working", "pane-new": "idle"},
				waitStatus:      "idle",
				getGate:         getGate,
				waitGate:        make(chan struct{}),
				started:         make(chan struct{}, 6),
			}
			store := NewMemoryStore(t)
			defer store.Close()
			client, prompts, events := r27Node(t, store, fake)
			manager := client.relayBusyManager()
			first := make(chan struct{})
			go func() {
				defer close(first)
				manager.offer(t.Context(), r725Directive(72592, "lane-a", "pane-old", "done body"))
			}()
			// The first copy must be the one inside the pane-old probe —
			// it holds the claim by then — so the repeat provably parks.
			r725AwaitCalls(t, fake, "get", "pane-old")
			second := make(chan struct{})
			go func() {
				defer close(second)
				manager.offer(t.Context(), r725Directive(72592, "lane-a", "pane-new", "done body"))
			}()
			r725AwaitRoute(t, manager, "lane-a", "pane-new")
			close(getGate)
			r725AwaitClosed(t, first, "first offer")
			r725AwaitClosed(t, second, "rerouted repeat")
			// The winner held on the busy old pane; the repeat retires
			// that lease and lands on the pane the lane actually uses.
			if got := *prompts; len(got) != 1 || !strings.HasPrefix(got[0], "pane-new") {
				t.Fatalf("prompts=%q", got)
			}
			if counts := manager.RelayDedupeCounts(); counts.Mismatch != 1 || counts.Suppressed != 0 {
				t.Fatalf("counts=%+v", counts)
			}
			if rows, err := store.RelayHeldForPane(t.Context(), "pane-old"); err != nil || len(rows) != 0 {
				t.Fatalf("stale held rows=%+v err=%v", rows, err)
			}
			if event := r27Await(t, events, "relay.dropped"); !strings.Contains(string(event.Payload), "lane_rerouted") {
				t.Fatalf("stale row left held without a rerouted drop: %s", event.Payload)
			}
		})
	}
}

// The sequential half of the same fix: a resend that arrives while its row
// sits held on a pane the lane has left. Before, every mode folded it into
// the doomed row and the event died with the lease; now the stale lease is
// retired and the copy delivers to the current pane. "off" keeps the exact
// pre-#725 behavior — the queued row wins — so the escape hatch stays honest.
func TestR725HeldResendReroutedPaneDeliversOnNewPane(t *testing.T) {
	r27GuardInbox(t)
	for _, sub := range []struct {
		mode        string
		wantPrompts int
		wantHeld    int
	}{
		{mode: "apply", wantPrompts: 1, wantHeld: 0},
		{mode: "shadow", wantPrompts: 1, wantHeld: 0},
		{mode: "off", wantPrompts: 0, wantHeld: 1},
	} {
		t.Run(sub.mode, func(t *testing.T) {
			t.Setenv("PANEWIRE_RELAY_DEDUPE", sub.mode)
			fake := &r27FakeHerdr{
				t:               t,
				getStatusByPane: map[string]string{"pane-old": "working", "pane-new": "idle"},
				waitStatus:      "idle",
				waitGate:        make(chan struct{}),
				started:         make(chan struct{}, 4),
			}
			store := NewMemoryStore(t)
			defer store.Close()
			client, prompts, _ := r27Node(t, store, fake)
			manager := client.relayBusyManager()
			manager.offer(t.Context(), r725Directive(72593, "lane-a", "pane-old", "done body"))
			manager.offer(t.Context(), r725Directive(72593, "lane-a", "pane-new", "done body"))
			if got := *prompts; len(got) != sub.wantPrompts {
				t.Fatalf("prompts=%q", got)
			}
			if sub.wantPrompts == 1 && !strings.HasPrefix((*prompts)[0], "pane-new") {
				t.Fatalf("prompts=%q, want the re-routed pane", prompts)
			}
			if rows, err := store.RelayHeldForPane(t.Context(), "pane-old"); err != nil || len(rows) != sub.wantHeld {
				t.Fatalf("held rows=%+v err=%v", rows, err)
			}
			if sub.mode == "off" {
				return
			}
			if counts := manager.RelayDedupeCounts(); counts.Mismatch != 1 || counts.Suppressed != 0 {
				t.Fatalf("counts=%+v", counts)
			}
		})
	}
}

// A same-pane resend with a different payload is a counted mismatch, and the
// queued row wins — an operator's relay.edit is never overridden by a replay
// carrying the original text.
func TestR725HeldResendSamePaneDifferentTextKeepsQueued(t *testing.T) {
	r27GuardInbox(t)
	t.Setenv("PANEWIRE_RELAY_DEDUPE", "apply")
	fake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "idle", waitGate: make(chan struct{}), started: make(chan struct{}, 4)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, _ := r27Node(t, store, fake)
	manager := client.relayBusyManager()
	manager.offer(t.Context(), r725Directive(72594, "lane-a", "fixture-pane", "queued original"))
	manager.offer(t.Context(), r725Directive(72594, "lane-a", "fixture-pane", "replay text"))
	if got := *prompts; len(got) != 0 {
		t.Fatalf("prompts=%q", got)
	}
	rows, err := store.RelayHeldForPane(t.Context(), "fixture-pane")
	if err != nil || len(rows) != 1 || rows[0].Text != "queued original" {
		t.Fatalf("held rows=%+v err=%v", rows, err)
	}
	if counts := manager.RelayDedupeCounts(); counts.Mismatch != 1 || counts.Suppressed != 0 {
		t.Fatalf("counts=%+v", counts)
	}
}

// The reconcile belongs to resume(), never restore(): restore() runs before
// the first hello has attached the relay emitter, so a re-acknowledgement
// emitted there lands in the void and the durable row stays open — the exact
// round-1 ordering bug. restore() must only arm the pending flag.
func TestR725ReconcileRunsAtResumeNotRestore(t *testing.T) {
	r27GuardInbox(t)
	t.Setenv("PANEWIRE_RELAY_DEDUPE", "apply")
	fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", waitGate: make(chan struct{}), started: make(chan struct{}, 1)}
	store := NewMemoryStore(t)
	defer store.Close()
	item := relayHeld{Pane: "fixture-pane", Lane: "lane-a", EventID: 72595, JobID: "relay-job-72595", Text: "crash window", HeldSince: time.Now(), DeliverPolicy: "idle", RecvSeq: 1}
	if inserted, err := store.InsertRelayHeld(t.Context(), item); err != nil || !inserted {
		t.Fatalf("seed held inserted=%t err=%v", inserted, err)
	}
	if err := store.RecordRelayDelivered(t.Context(), "lane-a", 72595, "fixture-pane", relayPayloadFingerprint("crash window"), time.Now()); err != nil {
		t.Fatalf("seed delivered err=%v", err)
	}
	// A bare client — no emitter yet, matching daemon order.
	client := &HubClient{outbox: store, relayCommand: fake.run}
	manager := client.relayBusyManager()
	manager.restore(t.Context())
	if rows, err := store.RelayHeldForPane(t.Context(), "fixture-pane"); err != nil || len(rows) != 1 {
		t.Fatalf("restore() ran the reconcile itself: held rows=%+v err=%v", rows, err)
	}
	if counts := manager.RelayDedupeCounts(); counts != (relayDedupeCounters{}) {
		t.Fatalf("restore() counted a decision it must not make yet: %+v", counts)
	}
	events := make(chan hubClientEvent, 32)
	client.setRelayEmitter(func(event hubClientEvent) { events <- event })
	manager.resume(t.Context())
	if rows, err := store.RelayHeldForPane(t.Context(), "fixture-pane"); err != nil || len(rows) != 0 {
		t.Fatalf("resume() left the delivered row held: %+v err=%v", rows, err)
	}
	if acks := r725DeliveredReasons(t, events, 1); acks[0].Reason != "dedupe_suppressed" {
		t.Fatalf("resume ack=%+v", acks[0])
	}
}

// A parked repeat waits on the winner AND on its own context: a caller whose
// context dies must give up immediately — its verdict would be used by
// nobody, and lingering waiters hold the read loop open for nothing.
func TestR725InflightWaiterContextCancelReturns(t *testing.T) {
	r27GuardInbox(t)
	t.Setenv("PANEWIRE_RELAY_DEDUPE", "apply")
	entered := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	fake := &r27FakeHerdr{t: t, getStatus: "idle", waitStatus: "idle", started: make(chan struct{}, 2)}
	store := NewMemoryStore(t)
	defer store.Close()
	client, prompts, _ := r725Node(t, store, fake, func(context.Context, string, string) bool {
		once.Do(func() { close(entered) })
		<-gate
		return true
	})
	manager := client.relayBusyManager()
	first := make(chan struct{})
	go func() {
		defer close(first)
		manager.offer(t.Context(), r725Directive(72596, "lane-a", "fixture-pane", "done body"))
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first offer never reached inject")
	}
	waiterCtx, cancelWaiter := context.WithCancel(t.Context())
	second := make(chan struct{})
	go func() {
		defer close(second)
		manager.offer(waiterCtx, r725Directive(72596, "lane-a", "fixture-pane", "done body"))
	}()
	cancelWaiter()
	r725AwaitClosed(t, second, "cancelled waiter")
	close(gate)
	r725AwaitClosed(t, first, "first offer")
	if got := *prompts; len(got) != 1 {
		t.Fatalf("prompts=%q", got)
	}
	// The cancelled waiter neither claimed nor leaked: the claim map must be
	// empty once the winner finishes.
	manager.mu.Lock()
	inflight := len(manager.dedupeInFlight)
	manager.mu.Unlock()
	if inflight != 0 {
		t.Fatalf("dedupe claims leaked: %d", inflight)
	}
}

package panewire

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #507: job.lost and job.revoked join the relay kind set. These tests pin the
// widened contract end to end — file, node scan, wire, hub dispatch, route,
// handoffkeep row, acknowledgement — plus the two guards that keep the new
// kinds safe: the hub's own revocation marker must never echo back, and a
// signal without reason/owner-lane must never enter the outbox.

// t507Lanes gives the worker lane its own pane and the captain a distinct
// parent pane, so owner-route and parent-route injections are distinguishable.
const t507Lanes = `{"lanes":{` +
	`"lane-w":{"machine":"host-a","pane":"w1:p1"},` +
	`"lane-dir":{"machine":"host-a","pane":"w1:p9"},` +
	`"lane-cap":{"machine":"host-a","pane":"w1:p2","parent":"lane-dir"}}}`

// t507DrainRelays returns the queued injections instead of just counting them;
// the pane and text on the directive are the assertions these tests need.
func t507DrainRelays(agent *hubAgent) []hubRelayInjectEvent {
	var out []hubRelayInjectEvent
	for {
		select {
		case directive := <-agent.relays:
			out = append(out, directive)
		default:
			return out
		}
	}
}

// t507Deliver is r20t7Deliver with the injections returned rather than
// counted: the pane a signal lands on is the routing assertion.
func t507Deliver(t *testing.T, hub *HubServer, agent *hubAgent, node *HubClient, event hubClientEvent) ([]hubRelayInjectEvent, []hubRelayPersistedEvent) {
	t.Helper()
	wire, err := json.Marshal(hubClientWireEvent(event))
	if err != nil {
		t.Fatal(err)
	}
	unknown := hub.UnknownMessageCount()
	hub.handleAgentMessage("host-a", "remote-a", agent, wire)
	if hub.UnknownMessageCount() != unknown {
		t.Fatalf("the hub rejected a %s payload as unknown: %s", event.Kind, event.Payload)
	}
	injected := t507DrainRelays(agent)
	acknowledgements := drainPersisted(agent)
	for _, ack := range acknowledgements {
		node.recordRelayPersisted(hubOutboundMessage{Type: ack.Type, JobID: ack.JobID, Kind: ack.Kind, Epoch: ack.Epoch, ReportPath: ack.ReportPath, Reason: ack.Reason, EventID: ack.EventID, ProducerEventID: ack.ProducerEventID})
	}
	return injected, acknowledgements
}

// AC: the scanner forwards both new kinds. report_path normalization is part
// of the assertion: an empty (or "/dev/null") report is the event file itself,
// the same substitution `panewire emit` makes.
func TestT507ScannerOffersLostAndRevoked(t *testing.T) {
	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()

	// The lost record carries no agent_label of its own — the claim is the
	// node-local source for it, exactly as the real sentinel files read.
	r20t7WriteEvent(t, inbox, "t507-lost", "00001-job.claim.json",
		`{"kind":"job.claim","job_id":"t507-lost","payload":{"agent_label":"wrk-lost","owner_lane":"lane-w"},"seq":1}`)
	r20t7WriteEvent(t, inbox, "t507-lost", "00002-job.lost.json",
		`{"kind":"job.lost","job_id":"t507-lost","owner_lane":"lane-w","label":"lane-w","host":"host-a","report_path":"","epoch":1,"reason":"timeout"}`)
	// The job.revoked record is the shape `panewire job close` writes: flat
	// kind/owner_lane/reason, no report path.
	r20t7WriteEvent(t, inbox, "t507-closed", "00001-job.revoked.json",
		`{"kind":"job.revoked","job_id":"t507-closed","owner_lane":"lane-cap","closed_by":"lane-cap","outcome":"abandoned","reason":"superseded by retry","source":"panewire job close","host":"host-a","epoch":1}`)

	node := r20Node(inbox, store)
	events := node.jobCompletionEvents()
	if len(events) != 2 {
		t.Fatalf("the scan offered %d events, want 2", len(events))
	}
	byKind := map[string]hubClientEvent{}
	for _, event := range events {
		byKind[event.Kind] = event
	}
	lost, revoked := byKind["job.lost"], byKind["job.revoked"]
	if lost.Kind == "" || revoked.Kind == "" {
		t.Fatalf("kinds offered=%v", byKind)
	}

	lostEvent, ok := decodeHubJobEscalationPayload(lost.Payload)
	if !ok || lostEvent.Reason != "timeout" || lostEvent.OwnerLane != "lane-w" {
		t.Fatalf("job.lost payload=%s", lost.Payload)
	}
	if lostEvent.AgentLabel != "wrk-lost" {
		t.Fatalf("job.lost agent_label=%q, want the claim's wrk-lost", lostEvent.AgentLabel)
	}
	lostFile := filepath.Join(inbox, "jobs", "t507-lost", "events", "00002-job.lost.json")
	if lostEvent.ReportPath != lostFile {
		t.Fatalf("job.lost report_path=%q, want the event file %q", lostEvent.ReportPath, lostFile)
	}

	revokedEvent, ok := decodeHubJobEscalationPayload(revoked.Payload)
	if !ok || revokedEvent.Reason != "superseded by retry" || revokedEvent.OwnerLane != "lane-cap" {
		t.Fatalf("job.revoked payload=%s", revoked.Payload)
	}
	revokedFile := filepath.Join(inbox, "jobs", "t507-closed", "events", "00001-job.revoked.json")
	if revokedEvent.ReportPath != revokedFile {
		t.Fatalf("job.revoked report_path=%q, want the event file %q", revokedEvent.ReportPath, revokedFile)
	}
}

// AC1 end to end for job.lost: the wire message reaches the owner lane's own
// pane, the handoffkeep row is kind job.lost, and the node's outbox row is
// retired by the acknowledgement. The job record stays untouched — lost is an
// observation, not a terminal state.
func TestT507LostRelaysToOwnerPane(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, t507Lanes, client, 8)

	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	r20t7WriteEvent(t, inbox, "t507-lost-relay", "00001-job.lost.json",
		`{"kind":"job.lost","job_id":"t507-lost-relay","owner_lane":"lane-w","agent_label":"wrk-lost","label":"lane-w","host":"host-a","epoch":1,"reason":"sentinel: heartbeat silent"}`)

	node := r20Node(inbox, store)
	events := node.jobCompletionEvents()
	if len(events) != 1 || events[0].Kind != "job.lost" {
		t.Fatalf("offered=%d", len(events))
	}
	directives, acknowledgements := t507Deliver(t, hub, agent, node, events[0])
	if len(directives) != 1 || len(acknowledgements) != 1 {
		t.Fatalf("injected=%d acknowledgements=%d, want 1 and 1", len(directives), len(acknowledgements))
	}
	if acknowledgements[0].Kind != "job.lost" {
		t.Fatalf("acknowledgement kind=%q", acknowledgements[0].Kind)
	}
	if directives[0].Pane != "w1:p1" || directives[0].Kind != "job.lost" {
		t.Fatalf("job.lost must reach the owner lane's own pane w1:p1: %+v", directives[0])
	}
	if !strings.HasPrefix(directives[0].Text, "[lost] ") || !strings.Contains(directives[0].Text, "sentinel: heartbeat silent") {
		t.Fatalf("injection text=%q", directives[0].Text)
	}
	if got := fake.attemptsFor("job.lost", "t507-lost-relay", 1, events[0].relayKey.ReportPath, "sentinel: heartbeat silent"); got != 1 {
		t.Fatalf("handoffkeep attempts=%d, want 1", got)
	}
	hub.mu.Lock()
	registered := hub.jobs["t507-lost-relay"]
	hub.mu.Unlock()
	if registered != nil {
		t.Fatalf("a job.lost created or closed a job record: %+v — lost is not terminal", registered)
	}
}

// AC1 end to end for job.revoked: close is an owner→parent notification, so
// the note lands in the parent lane's pane; the job record is terminal under
// the same epoch fencing as a completion.
func TestT507RevokedRelaysToParentPaneAndClosesJob(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, t507Lanes, client, 8)

	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	r20t7WriteEvent(t, inbox, "t507-rev-relay", "00001-job.revoked.json",
		`{"kind":"job.revoked","job_id":"t507-rev-relay","owner_lane":"lane-cap","agent_label":"wrk-cap","label":"lane-cap","host":"host-a","epoch":1,"reason":"operator closed the task"}`)

	node := r20Node(inbox, store)
	events := node.jobCompletionEvents()
	if len(events) != 1 || events[0].Kind != "job.revoked" {
		t.Fatalf("offered=%d", len(events))
	}
	directives, acknowledgements := t507Deliver(t, hub, agent, node, events[0])
	if len(directives) != 1 || len(acknowledgements) != 1 || acknowledgements[0].Kind != "job.revoked" {
		t.Fatalf("injected=%d acknowledgements=%+v", len(directives), acknowledgements)
	}
	if directives[0].Pane != "w1:p9" {
		t.Fatalf("job.revoked must reach the owner lane's parent pane w1:p9, got %+v", directives[0])
	}
	if !strings.HasPrefix(directives[0].Text, "[revoked] ") || !strings.Contains(directives[0].Text, "operator closed the task") {
		t.Fatalf("injection text=%q", directives[0].Text)
	}
	if got := fake.attemptsFor("job.revoked", "t507-rev-relay", 1, events[0].relayKey.ReportPath, "operator closed the task"); got != 1 {
		t.Fatalf("handoffkeep attempts=%d, want 1", got)
	}
	// The job was never heartbeat-registered, so this is the late-register
	// path: the record exists as a receipt and is terminal, never a
	// redispatch candidate.
	hub.mu.Lock()
	registered := hub.jobs["t507-rev-relay"]
	hub.mu.Unlock()
	if registered == nil || !registered.Completed {
		t.Fatalf("a node-reported job.revoked left no terminal job record: %+v", registered)
	}
}

// The echo guard: the hub's own revocation marker carries only
// type/job_id/epoch — no reason, no owner lane — so the scanner must never
// offer it back. The panewire job close record beside it does relay.
func TestT507HubRevocationMarkerDoesNotEcho(t *testing.T) {
	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	// Byte-identical to what writeHubRevocation writes when a hub→node
	// job.revoked lands.
	r20t7WriteEvent(t, inbox, "t507-echo", "00001-job.revoked.json",
		`{"type":"job.revoked","job_id":"t507-echo","epoch":1}`)
	r20t7WriteEvent(t, inbox, "t507-close", "00001-job.revoked.json",
		`{"kind":"job.revoked","job_id":"t507-close","owner_lane":"lane-w","reason":"owner declared closed","host":"host-a","epoch":1}`)

	node := r20Node(inbox, store)
	events := node.jobCompletionEvents()
	if len(events) != 1 || events[0].Kind != "job.revoked" {
		t.Fatalf("offered=%d, want exactly the owner-declared revocation", len(events))
	}
	event, ok := decodeHubJobEscalationPayload(events[0].Payload)
	if !ok || event.JobID != "t507-close" {
		t.Fatalf("offered the hub's own marker back: %s", events[0].Payload)
	}
}

// A signal without reason or a routable owner lane must never reach the
// outbox: the hub decode rejects the first, and the second can never resolve
// a route, so it would resend forever.
func TestT507SignalMissingReasonOrLaneIsRefused(t *testing.T) {
	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	r20t7WriteEvent(t, inbox, "t507-nolane", "00001-job.lost.json",
		`{"kind":"job.lost","job_id":"t507-nolane","label":"x","host":"host-a","epoch":1,"reason":"timeout"}`)
	r20t7WriteEvent(t, inbox, "t507-noreason", "00001-job.revoked.json",
		`{"kind":"job.revoked","job_id":"t507-noreason","owner_lane":"lane-w","host":"host-a","epoch":1}`)
	if events := r20Node(inbox, store).jobCompletionEvents(); len(events) != 0 {
		t.Fatalf("the scan offered %d unroutable signals, want 0", len(events))
	}

	inbox = t.TempDir()
	cases := [][]string{
		{"--kind", "job.lost", "--job", "t507-e1", "--owner-lane", "lane-w", "--inbox-root", inbox},
		{"--kind", "job.lost", "--job", "t507-e2", "--reason", "timeout", "--inbox-root", inbox},
		{"--kind", "job.revoked", "--job", "t507-e3", "--owner-lane", "lane-w", "--inbox-root", inbox},
	}
	for index, args := range cases {
		if code := runEmitCLI(args, &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code != ExitUsage {
			t.Fatalf("case %d code=%d, want ExitUsage", index, code)
		}
	}
	if _, err := os.Stat(filepath.Join(inbox, "jobs")); err == nil {
		t.Fatal("a refused signal wrote into the inbox")
	}
}

// AC: /dev/null is the sentinel's spelling of "no report". Emit, the dedupe
// reader, and the scanner must all land on the same normalized key or the same
// event arrives twice.
func TestT507EmitDevNullReportNormalizesToEventFile(t *testing.T) {
	inbox := t.TempDir()
	args := []string{"--kind", "job.lost", "--job", "t507-devnull", "--reason", "timeout", "--owner-lane", "lane-w",
		"--label", "lane-w", "--host", "host-a", "--report", "/dev/null", "--inbox-root", inbox}
	if code := runEmitCLI(args, &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code != ExitOK {
		t.Fatalf("emit code=%d", code)
	}
	names := r20EventFiles(t, inbox, "t507-devnull")
	if len(names) != 1 {
		t.Fatalf("event files=%v", names)
	}
	raw, err := os.ReadFile(filepath.Join(inbox, "jobs", "t507-devnull", "events", names[0]))
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if json.Unmarshal(raw, &record) != nil {
		t.Fatalf("event file=%s", raw)
	}
	if path, present := record["report_path"]; present && path != "" {
		t.Fatalf("/dev/null was written verbatim: report_path=%q", path)
	}

	store := NewMemoryStore(t)
	defer store.Close()
	events := r20Node(inbox, store).jobCompletionEvents()
	if len(events) != 1 {
		t.Fatalf("the emitted signal was not scanned back: %d events", len(events))
	}
	want := filepath.Join(inbox, "jobs", "t507-devnull", "events", names[0])
	if events[0].relayKey.ReportPath != want {
		t.Fatalf("report path diverged between emit and scan: key=%q want %q", events[0].relayKey.ReportPath, want)
	}
}

// Old-hub compatibility: a hub built before the kind set widened receives a
// job.lost/job.revoked event kind it has no dispatch for. The inbound event
// falls through every kind branch — no crash, no injection, no handoffkeep
// call, no state. The fixture asserts that unchanged: an unrecognized kind is
// inert.
func TestT507OldHubShapedReceiptIsHarmless(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, t507Lanes, client, 8)

	for _, kind := range []string{"job.retired", "job.frobnicate"} {
		wire, err := json.Marshal(hubClientWireEvent(hubClientEvent{
			Kind:    kind,
			Payload: json.RawMessage(`{"job_id":"t507-oldhub","epoch":1,"owner_lane":"lane-w","reason":"x"}`),
		}))
		if err != nil {
			t.Fatal(err)
		}
		hub.handleAgentMessage("host-a", "remote-a", agent, wire)
	}
	if injected := drainRelays(agent); injected != 0 {
		t.Fatalf("an unrecognized kind injected %d times", injected)
	}
	if acknowledgements := drainPersisted(agent); len(acknowledgements) != 0 {
		t.Fatalf("an unrecognized kind was acknowledged: %+v", acknowledgements)
	}
	if posts := fake.count(http.MethodPost, "/v1/relay/events"); posts != 0 {
		t.Fatalf("an unrecognized kind reached handoffkeep: POSTs=%d", posts)
	}
	hub.mu.Lock()
	registered := hub.jobs["t507-oldhub"]
	hub.mu.Unlock()
	if registered != nil {
		t.Fatalf("an unrecognized kind created a job record: %+v", registered)
	}
}

// Old-handoffkeep compatibility: a schema that predates the CHECK migration
// rejects the new kinds. The existing contract handles that rejection —
// relay.unpersisted broadcast, no injection, and the dedupe key released so
// the node's retry is not swallowed. Deploy order is documented in
// docs/r20-relay-persistence.md; this pins the failure shape.
func TestT507OldHandoffkeepRejectsNewKindsVisibly(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	fake.status = http.StatusUnprocessableEntity // the CHECK constraint's reply
	hub, agent := r20t5Hub(t, t507Lanes, client, 8)
	events := r20t5Subscribe(t, hub)

	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	r20t7WriteEvent(t, inbox, "t507-oldkeep", "00001-job.lost.json",
		`{"kind":"job.lost","job_id":"t507-oldkeep","owner_lane":"lane-w","label":"lane-w","host":"host-a","epoch":1,"reason":"timeout"}`)
	node := r20Node(inbox, store)
	queued := node.jobCompletionEvents()
	if len(queued) != 1 {
		t.Fatalf("offered=%d", len(queued))
	}

	wire, err := json.Marshal(hubClientWireEvent(queued[0]))
	if err != nil {
		t.Fatal(err)
	}
	hub.handleAgentMessage("host-a", "remote-a", agent, wire)

	if injected := drainRelays(agent); injected != 0 {
		t.Fatalf("a rejected persist injected %d times, want 0", injected)
	}
	if acknowledgements := drainPersisted(agent); len(acknowledgements) != 0 {
		t.Fatalf("an unpersisted row was acknowledged: %+v", acknowledgements)
	}
	if unpersisted := events("relay.unpersisted"); len(unpersisted) != 1 {
		t.Fatalf("the CHECK rejection was not observable: relay.unpersisted=%d", len(unpersisted))
	}
	hub.mu.Lock()
	_, blocked := hub.relayDedupe[relayEventDedupeKey("job.lost", hubJobEventPayload{JobID: "t507-oldkeep", Epoch: 1, OwnerLane: "lane-w", Label: "lane-w", Host: "host-a", ReportPath: queued[0].relayKey.ReportPath, Reason: "timeout"})]
	hub.mu.Unlock()
	if blocked {
		t.Fatal("a CHECK rejection left the dedupe key behind: the node's retry would be swallowed")
	}
}

// Fixture AC: the real wrk sentinel file in testdata/task603 is the shape this
// change exists for — flat kind, no report, reason+owner_lane present. It must
// be offered as job.lost with the claim's agent label and the event file as
// its report path.
func TestT507Task603FixtureLostIsOffered(t *testing.T) {
	const fixtureJob = "529-deploy-view-20260923-1535"
	fixtureFile := filepath.Join("testdata", "task603", "jobs", fixtureJob, "events", "00006-job.lost.json")
	if _, err := os.Stat(fixtureFile); err != nil {
		t.Fatal(err)
	}
	var offered *hubScannedRelayEvent
	for _, event := range scanHubRelayEvents(filepath.Join("testdata", "task603")) {
		if event.Kind == "job.lost" && event.JobID == fixtureJob {
			found := event
			offered = &found
			break
		}
	}
	if offered == nil {
		t.Fatal("the real wrk job.lost file was not offered")
	}
	if offered.Reason != "timeout" || offered.OwnerLane != "b529-deploy-view" {
		t.Fatalf("offered=%+v", *offered)
	}
	if offered.AgentLabel != "b529-deploy-view" {
		t.Fatalf("agent_label=%q, want the claim's b529-deploy-view", offered.AgentLabel)
	}
	if offered.ReportPath != fixtureFile {
		t.Fatalf("report_path=%q, want the event file %q", offered.ReportPath, fixtureFile)
	}
}

// The widened set is the whole contract: wrk TERMINAL {completed, joined,
// revoked} all relay, plus the escalate signal, plus the non-terminal lost
// observation, plus lane.event. Removing either new kind turns the offer tests
// above RED — that is the required mutant property.
func TestT507RelayKindSetIsComplete(t *testing.T) {
	want := map[string]bool{"job.completed": true, "job.escalate": true, "job.joined": true, "job.lost": true, "job.revoked": true, "lane.event": true}
	if len(emitRelayKinds) != len(want) {
		t.Fatalf("emitRelayKinds=%v", emitRelayKinds)
	}
	for kind := range want {
		if !emitRelayKinds[kind] {
			t.Fatalf("emitRelayKinds lost %q", kind)
		}
	}
	// A duplicate delivery is a duplicate whichever kind carried it: the
	// outbox key keeps kind in the primary position, so a job.lost and a
	// job.revoked for one job never collapse into one row.
	lostKey := relayEventOutboxKeyFor(relayEventWireForm(hubScannedRelayEvent{Kind: "job.lost", HubActiveJob: HubActiveJob{JobID: "t507-keys", Epoch: 1, ReportPath: "e.json"}, Reason: "timeout", EventID: "00001-job.lost.json"}))
	revokedKey := relayEventOutboxKeyFor(relayEventWireForm(hubScannedRelayEvent{Kind: "job.revoked", HubActiveJob: HubActiveJob{JobID: "t507-keys", Epoch: 1, ReportPath: "e.json"}, Reason: "timeout", EventID: "00002-job.revoked.json"}))
	if lostKey == revokedKey {
		t.Fatal("a lost and a revoked for one job keyed identically")
	}
}

// The daemon gate mirrors emitJobRecord: a terminal signal without reason or
// owner lane is refused before it can enter the outbox, even when the request
// arrives through the socket rather than the CLI.
func TestT507DaemonRefusesUnderspecifiedSignal(t *testing.T) {
	inbox := t.TempDir()
	node := &HubClient{jobsInboxRoot: inbox, completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{}, assignedJobs: map[string]uint64{}, events: make(chan hubClientEvent, 4)}
	store := NewMemoryStore(t)
	defer store.Close()
	node.SetRelayOutbox(store)
	daemon := NewDaemon(Config{InboxRoot: inbox, Hub: HubDaemonConfig{Enabled: true, Client: node}})

	for _, req := range []localRequest{
		{Op: "emit", Kind: "job.lost", JobID: "t507-d1", ReportPath: "e.json", InboxRoot: inbox, Reason: "timeout"},
		{Op: "emit", Kind: "job.revoked", JobID: "t507-d2", ReportPath: "e.json", InboxRoot: inbox, OwnerLane: "lane-w"},
	} {
		if err := daemon.emitRelayEvent(req); err == nil {
			t.Fatalf("the daemon accepted %+v", req)
		}
	}
	select {
	case event := <-node.events:
		t.Fatalf("a refused signal was queued: %+v", event)
	default:
	}

	req := localRequest{Op: "emit", Kind: "job.lost", JobID: "t507-d3", ReportPath: "e.json", InboxRoot: inbox, Reason: "timeout", OwnerLane: "lane-w"}
	if err := daemon.emitRelayEvent(req); err != nil {
		t.Fatalf("a well-formed signal was refused: %v", err)
	}
	select {
	case <-node.events:
	case <-time.After(2 * time.Second):
		t.Fatal("a well-formed signal was not queued")
	}

	// A direct push spelling "no report" as /dev/null must resolve to the same
	// durable event file the scanner substitutes — one event, one outbox key.
	r20t7WriteEvent(t, inbox, "t507-d4", "00001-job.lost.json",
		`{"kind":"job.lost","job_id":"t507-d4","owner_lane":"lane-w","host":"host-a","epoch":1,"reason":"timeout"}`)
	push := localRequest{Op: "emit", Kind: "job.lost", JobID: "t507-d4", ReportPath: "/dev/null", InboxRoot: inbox, Reason: "timeout", OwnerLane: "lane-w", Host: "host-a"}
	if err := daemon.emitRelayEvent(push); err != nil {
		t.Fatalf("a /dev/null signal was refused: %v", err)
	}
	select {
	case event := <-node.events:
		want := filepath.Join(inbox, "jobs", "t507-d4", "events", "00001-job.lost.json")
		if event.relayKey.ReportPath != want {
			t.Fatalf("the push keyed the report as %q, want the event file %q — one event, two keys", event.relayKey.ReportPath, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the /dev/null signal was not queued")
	}
}

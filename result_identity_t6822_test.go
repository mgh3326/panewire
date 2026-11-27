package panewire

// hk 6822 (hk 4289 T5): a READY and its job.completed share one identifier.
// The lanes, hub and hk fixtures are the quiet design-lane ones (t3426*):
// handoffkeep-design and operator-desk are inbox-only, so nothing is typed for
// them and their completions merge; director-1 is typed and never merges
// (result_identity_typed_lane_t6822_test.go).

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/laneevent"
)

const (
	t6822Job      = "hk3404-review3-fix"
	t6822Director = "director-1"
)

func t6822Completion(lane, readyID, file string) hubJobEventPayload {
	c := t3426Completion(t6822Job, lane, "")
	c.ResultID, c.EventID = readyID, file
	// Each completion file of a job carries its own report path in these tests,
	// so hk's five-field key does not fold two of them into one row.
	c.ReportPath = "/jobs/" + t6822Job + "/" + file + ".report.md"
	c.ReportLastLine = "last line of " + file
	return c
}

// t6822CompletionKey is a completion's own item key; t6822Merged is the
// merged_completions a READY holds after merging cs.
func t6822CompletionKey(c hubJobEventPayload) string {
	return relayEventDedupeKey("job.completed", c)
}

func t6822Merged(cs ...hubJobEventPayload) []string {
	var out []string
	for _, c := range cs {
		out = append(out, laneevent.MergedCompletionKey(t6822CompletionKey(c)))
	}
	return out
}

// t6822FileReady relays a READY lane.event from host-a and returns its item.
func t6822FileReady(t *testing.T, h *HubServer, agent *hubAgent, lane, id string) laneInboxItem {
	t.Helper()
	h.relayLaneEvent(t3426Ready(lane, id), agent)
	return t6822Item(t, h, lane, relayLaneEventOutboxKey(lane, id))
}

func t6822Items(t *testing.T, h *HubServer, lane string) []laneInboxItem {
	t.Helper()
	items, err := h.inbox.store.list(lane, false)
	if err != nil {
		t.Fatal(err)
	}
	return items
}

// t6822Item is the lane's item whose event has key.
func t6822Item(t *testing.T, h *HubServer, lane, key string) laneInboxItem {
	t.Helper()
	items := t6822Items(t, h, lane)
	for _, item := range items {
		if relayEventDedupeKey(item.Kind, item.Event) == key {
			return item
		}
	}
	t.Fatalf("%s has no item for %q: %+v", lane, key, items)
	return laneInboxItem{}
}

// t6822Acks drains the persisted acknowledgements the node was sent.
func t6822Acks(a *hubAgent) []hubRelayPersistedEvent {
	var out []hubRelayPersistedEvent
	for {
		select {
		case p := <-a.persisted:
			out = append(out, p)
		default:
			return out
		}
	}
}

// t6822Typed relays a READY to a typed lane and confirms its injection, so the
// item is delivered and still unacked.
func t6822Typed(t *testing.T, h *HubServer, agent *hubAgent, id string) laneInboxItem {
	t.Helper()
	h.relayLaneEvent(t3426Ready(t6822Director, id), agent)
	injected := t3426Injected(agent)
	if len(injected) != 1 {
		t.Fatalf("the READY was not typed once: %+v", injected)
	}
	h.markRelayEventDelivered(h.acknowledgeRelayPendingForTest(t, "host-a", relayAckPayload{JobID: injected[0].JobID, Pane: injected[0].Pane}))
	t6822Acks(agent)
	item := t6822Item(t, h, t6822Director, relayLaneEventOutboxKey(t6822Director, id))
	if item.DeliveredAt == 0 || item.AckedAt != 0 || !item.MustAck {
		t.Fatalf("fixture READY=%+v, want delivered and unacked", item)
	}
	return item
}

func t6822Hub(t *testing.T) (*HubServer, *hubAgent, *fakeHandoffkeep, string) {
	t.Helper()
	fake, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	h, agent := t3426Hub(t, path, hk, t3426Records)
	return h, agent, fake, path
}

// A completion with result_id merges into the unacked READY item: no second
// item, no injection, no hk row, and the producer is answered with that item.
// Mutant: no merge (a second item).
func TestCompletionMergesIntoUnackedReady(t *testing.T) {
	h, agent, fake, _ := t6822Hub(t)
	ready := t6822FileReady(t, h, agent, t3426Design, "ready-1")
	t3426Drain(agent)
	// The receiver read the READY without acking it: it is still owed.
	if _, err := h.inbox.store.read(ready.ID, t3426Design, h.now()); err != nil {
		t.Fatal(err)
	}

	c := t6822Completion(t3426Design, "ready-1", "00001-job.completed.json")
	c.PR, c.Head = "https://github.com/example/repo/pull/12", "abc1234"
	h.relayJobCompletionFrom("host-a", c)

	items := t6822Items(t, h, t3426Design)
	if len(items) != 1 || items[0].ID != ready.ID {
		t.Fatalf("items=%+v, want only the READY item", items)
	}
	got := items[0].Event
	if !slices.Equal(got.MergedCompletions, t6822Merged(c)) || got.ReportPath != c.ReportPath || got.ReportLastLine != c.ReportLastLine || got.PR != c.PR || got.Head != c.Head {
		t.Fatalf("merged event=%+v", got)
	}
	if got.Text != ready.Event.Text || got.NotifyType != "ready" || got.EventID != "ready-1" || !items[0].MustAck || items[0].AckedAt != 0 {
		t.Fatalf("the READY lost its own fields or its must-ack state: %+v", items[0])
	}
	acks := t6822Acks(agent)
	if len(acks) != 1 || acks[0].ProducerEventID != c.EventID || acks[0].JobID != t6822Job || acks[0].EventID != ready.ID {
		t.Fatalf("persisted acks=%+v, want the completion answered with item %d", acks, ready.ID)
	}
	if injected := t3426Injected(agent); len(injected) != 0 {
		t.Fatalf("a design lane was typed: %+v", injected)
	}
	var payload string
	if err := h.inbox.store.db.QueryRow(`SELECT payload FROM lane_inbox WHERE id=?`, ready.ID).Scan(&payload); err != nil || strings.Contains(payload, `\u0000`) {
		t.Fatalf("stored payload holds a NUL escape: %s %v", payload, err)
	}
	if rows := t3426RelayRows(fake, t6822Job); rows != 0 {
		t.Fatalf("hk rows for an inbox-path merge=%d, want 0", rows)
	}
}

// Reaching a pane does not show the receiver read the READY. On an inbox-only
// lane a delivered, unacked one still takes the completion; on a typed lane,
// where the READY is delivered by being typed, the completion is typed too and
// gets its own item, as on the base (decision-2). Mutants: treat delivered_at
// as settled; merge on a typed lane.
func TestCompletionMergesWhileReadyDeliveredButUnacked(t *testing.T) {
	t.Run("an inbox-only lane", func(t *testing.T) {
		h, agent, _, _ := t6822Hub(t)
		ready := t6822FileReady(t, h, agent, t3426Design, "ready-1")
		if err := h.inbox.store.recordDelivery(relayLaneEventOutboxKey(t3426Design, "ready-1"), "host-a", "w1:design", h.now().UTC()); err != nil {
			t.Fatal(err)
		}
		c := t6822Completion(t3426Design, "ready-1", "00001-job.completed.json")
		h.relayJobCompletionFrom("host-a", c)
		items := t6822Items(t, h, t3426Design)
		if len(items) != 1 || items[0].ID != ready.ID || items[0].DeliveredAt == 0 || !slices.Equal(items[0].Event.MergedCompletions, t6822Merged(c)) {
			t.Fatalf("items=%+v, want the completion merged into the delivered READY", items)
		}
	})
	t.Run("a typed lane", func(t *testing.T) {
		h, agent, _, _ := t6822Hub(t)
		t6822Typed(t, h, agent, "ready-1")
		c := t6822Completion(t6822Director, "ready-1", "00001-job.completed.json")
		h.relayJobCompletionFrom("host-a", c)
		if injected := t3426Injected(agent); len(injected) != 1 || !strings.Contains(injected[0].Text, c.ReportPath) {
			t.Fatalf("injected=%+v, want the completion typed", injected)
		}
		ready := t6822Item(t, h, t6822Director, relayLaneEventOutboxKey(t6822Director, "ready-1"))
		own := t6822Item(t, h, t6822Director, t6822CompletionKey(c))
		if len(t6822Items(t, h, t6822Director)) != 2 || len(ready.Event.MergedCompletions) != 0 || own.Event.ResultID != "" {
			t.Fatalf("ready=%+v completion=%+v", ready, own)
		}
	})
}

// A completion that arrives after the READY was acked is news: it is delivered
// as before and the READY is untouched. Mutant: always merge.
func TestCompletionAfterReadyAckIsDelivered(t *testing.T) {
	// A READY stored before must-ack existed and already read is settled too.
	for settle, statement := range map[string]string{
		"acked":                "",
		"superseded":           `UPDATE lane_inbox SET superseded_at=1 WHERE id=?`,
		"read before must-ack": `UPDATE lane_inbox SET must_ack=0,read_at=1 WHERE id=?`,
	} {
		t.Run(settle, func(t *testing.T) {
			h, agent, _, _ := t6822Hub(t)
			ready := t6822FileReady(t, h, agent, t3426Design, "ready-1")
			if statement == "" {
				if n, err := h.inbox.store.ack(ready.ID, t3426Design, false, h.now()); err != nil || n != 1 {
					t.Fatal(n, err)
				}
			} else if _, err := h.inbox.store.db.Exec(statement, ready.ID); err != nil {
				t.Fatal(err)
			}
			c := t6822Completion(t3426Design, "ready-1", "00001-job.completed.json")
			h.relayJobCompletionFrom("host-a", c)

			own := t6822Item(t, h, t3426Design, t6822CompletionKey(c))
			after := t6822Item(t, h, t3426Design, relayLaneEventOutboxKey(t3426Design, "ready-1"))
			if own.ReadAt != 0 || len(after.Event.MergedCompletions) != 0 || after.Event.ReportPath != "" {
				t.Fatalf("completion=%+v ready=%+v", own, after)
			}
		})
	}
}

// The completion overtakes its READY: both are delivered, and a later resend of
// the completion stays its own item's duplicate.
// Mutants: drop the early completion; merge a delivered one on its resend.
func TestCompletionBeforeReadyDeliversBoth(t *testing.T) {
	h, agent, _, _ := t6822Hub(t)
	c := t6822Completion(t3426Design, "ready-1", "00001-job.completed.json")
	h.relayJobCompletionFrom("host-a", c)
	t6822FileReady(t, h, agent, t3426Design, "ready-1")
	t6822Acks(agent)
	h.relayJobCompletionFrom("host-a", c)

	items := t6822Items(t, h, t3426Design)
	if len(items) != 2 {
		t.Fatalf("items=%+v, want the completion and the READY", items)
	}
	if ready := t6822Item(t, h, t3426Design, relayLaneEventOutboxKey(t3426Design, "ready-1")); len(ready.Event.MergedCompletions) != 0 {
		t.Fatalf("the delivered completion was also merged: %+v", ready)
	}
	own := t6822Item(t, h, t3426Design, t6822CompletionKey(c))
	if acks := t6822Acks(agent); len(acks) != 1 || acks[0].EventID != own.ID {
		t.Fatalf("resend acks=%+v, want item %d", acks, own.ID)
	}
}

// A second completion with another event key appends to the set; the same one
// again changes nothing. Mutants: replace the set; drop the second.
func TestCompletionTwiceSameResultID(t *testing.T) {
	h, agent, _, _ := t6822Hub(t)
	t6822FileReady(t, h, agent, t3426Design, "ready-1")
	t3426Drain(agent)
	first := t6822Completion(t3426Design, "ready-1", "00001-job.completed.json")
	second := t6822Completion(t3426Design, "ready-1", "00002-job.completed.json")
	for _, c := range []hubJobEventPayload{first, second, second, first} {
		h.relayJobCompletionFrom("host-a", c)
	}
	items := t6822Items(t, h, t3426Design)
	want := t6822Merged(first, second)
	if len(items) != 1 || !slices.Equal(items[0].Event.MergedCompletions, want) || items[0].Event.ReportPath != second.ReportPath {
		t.Fatalf("items=%+v, want both keys once and the second report", items)
	}
	if acks := t6822Acks(agent); len(acks) != 4 {
		t.Fatalf("acks=%d, want every send answered", len(acks))
	}
}

// The set is in the item, so a restart keeps every merged key, also after the
// READY is acked. Mutant: compare only the first merged key.
func TestMergeIsIdempotentAcrossRestart(t *testing.T) {
	fake, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	h, agent := t3426Hub(t, path, hk, t3426Records)
	ready := t6822FileReady(t, h, agent, t3426Design, "ready-1")
	first := t6822Completion(t3426Design, "ready-1", "00001-job.completed.json")
	second := t6822Completion(t3426Design, "ready-1", "00002-job.completed.json")
	h.relayJobCompletionFrom("host-a", first)
	h.relayJobCompletionFrom("host-a", second)
	h.Close()

	h2, agent2 := t3426Hub(t, path, hk, t3426Records)
	h2.relayJobCompletionFrom("host-a", second)
	h2.relayJobCompletionFrom("host-a", first)
	items := t6822Items(t, h2, t3426Design)
	want := t6822Merged(first, second)
	if len(items) != 1 || !slices.Equal(items[0].Event.MergedCompletions, want) {
		t.Fatalf("after restart items=%+v", items)
	}
	if acks := t6822Acks(agent2); len(acks) != 2 || acks[0].EventID != ready.ID || acks[1].EventID != ready.ID {
		t.Fatalf("after restart acks=%+v, want item %d", acks, ready.ID)
	}
	if n, err := h2.inbox.store.ack(ready.ID, t3426Design, false, h2.now()); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	h2.relayJobCompletionFrom("host-a", second)
	if items := t6822Items(t, h2, t3426Design); len(items) != 1 {
		t.Fatalf("a resend of a merged completion became news after the ack: %+v", items)
	}
	if rows := t3426RelayRows(fake, t6822Job); rows != 0 {
		t.Fatalf("hk rows=%d, want 0", rows)
	}
}

// One job's two READYs stay two results: the completion joins only the one it
// names. Mutant: match by (lane, job).
func TestTwoReadysOneJobStayDistinct(t *testing.T) {
	h, agent, _, _ := t6822Hub(t)
	t6822FileReady(t, h, agent, t3426Design, "ready-1")
	t6822FileReady(t, h, agent, t3426Design, "ready-2")
	c := t6822Completion(t3426Design, "ready-2", "00001-job.completed.json")
	h.relayJobCompletionFrom("host-a", c)

	one := t6822Item(t, h, t3426Design, relayLaneEventOutboxKey(t3426Design, "ready-1"))
	two := t6822Item(t, h, t3426Design, relayLaneEventOutboxKey(t3426Design, "ready-2"))
	if len(t6822Items(t, h, t3426Design)) != 2 || len(one.Event.MergedCompletions) != 0 || len(two.Event.MergedCompletions) != 1 {
		t.Fatalf("ready-1=%+v ready-2=%+v", one, two)
	}
}

// Without a result_id, and with one that names no unread READY of the lane, the
// completion gets its own item as before. The payload without the field is what
// an older node sends. Mutant: match a READY by anything but the id.
func TestCompletionWithoutResultIDUnchanged(t *testing.T) {
	h, agent, _, _ := t6822Hub(t)
	t6822FileReady(t, h, agent, t3426Design, "ready-1")
	// A decision is must-ack and unread, as a READY is, and is still not one.
	decision := t3426Ready(t3426Design, "decision-1")
	decision.NotifyType, decision.Text = "decision", "ESC hk 3404 which order"
	h.relayLaneEvent(decision, agent)
	t6822FileReady(t, h, agent, "operator-desk", "ready-desk")

	// An older node's frame, through the hub's own decoder.
	old := t6822Completion(t3426Design, "", "00001-job.completed.json")
	wire := hubJobCompletionPayloadForJob(HubActiveJob{JobID: old.JobID, Epoch: 1, AgentLabel: old.AgentLabel, OwnerLane: old.OwnerLane, Label: old.Label, Host: old.Host, ReportPath: old.ReportPath, ReportLastLine: old.ReportLastLine}, old.EventID, false)
	decoded, ok := decodeHubJobCompletionPayload(wire)
	if !ok || decoded.ResultID != "" {
		t.Fatalf("decoded=%+v ok=%t", decoded, ok)
	}
	completions := []hubJobEventPayload{
		decoded,
		t6822Completion(t3426Design, "no-such-ready", "00002-job.completed.json"),
		t6822Completion(t3426Design, "decision-1", "00003-job.completed.json"),
		t6822Completion(t3426Design, "ready-desk", "00004-job.completed.json"),
	}
	for _, c := range completions {
		h.relayJobCompletionFrom("host-a", c)
		if item := t6822Item(t, h, t3426Design, t6822CompletionKey(c)); item.ReadAt != 0 || item.Class != "inbox" {
			t.Fatalf("%s stored as %+v", c.EventID, item)
		}
	}
	for _, item := range append(t6822Items(t, h, t3426Design), t6822Items(t, h, "operator-desk")...) {
		if len(item.Event.MergedCompletions) != 0 {
			t.Fatalf("an item took a completion that did not name it: %+v", item)
		}
	}
}

// With no hk at all the READY item is the merge's receipt, on a design lane and
// the desk. Mutant: require an hk row.
func TestMergeReceiptIsTheInboxItemWhenNoHKRow(t *testing.T) {
	h, agent := t3426Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), nil, t3426Records)
	for _, lane := range []string{t3426Design, "operator-desk"} {
		t.Run(lane, func(t *testing.T) {
			ready := t6822FileReady(t, h, agent, lane, "ready-1")
			t3426Drain(agent)
			c := t6822Completion(lane, "ready-1", "00001-job.completed.json")
			h.relayJobCompletionFrom("host-a", c)
			if acks := t6822Acks(agent); len(acks) != 1 || acks[0].EventID != ready.ID || acks[0].ProducerEventID != c.EventID {
				t.Fatalf("acks=%+v, want item %d", acks, ready.ID)
			}
			if items := t6822Items(t, h, lane); len(items) != 1 || len(items[0].Event.MergedCompletions) != 1 {
				t.Fatalf("items=%+v", items)
			}
		})
	}
}

// A merged completion supersedes what its own item would have: the job's older
// decision and progress notes, also those filed after the READY, and never the
// READY it joined. Mutants: no supersede; bound it by the READY's id.
func TestMergedCompletionSupersedesAsItsOwnItemWould(t *testing.T) {
	h, agent, _, _ := t6822Hub(t)
	cause := "job:" + t6822Job
	note := func(id, notifyType string) {
		e := t3426Ready(t3426Design, id)
		e.NotifyType, e.Text, e.Cause = notifyType, "hk 3404 "+id, cause
		h.relayLaneEvent(e, agent)
	}
	note("decision-before", "decision")
	ready := t3426Ready(t3426Design, "ready-1")
	ready.Cause = cause
	h.relayLaneEvent(ready, agent)
	note("progress-after", "progress")

	c := t6822Completion(t3426Design, "ready-1", "00001-job.completed.json")
	c.Cause = cause
	h.relayJobCompletionFrom("host-a", c)

	item := t6822Item(t, h, t3426Design, relayLaneEventOutboxKey(t3426Design, "ready-1"))
	if item.SupersededAt != 0 || len(item.Event.MergedCompletions) != 1 {
		t.Fatalf("ready=%+v", item)
	}
	for _, id := range []string{"decision-before", "progress-after"} {
		old := t6822Item(t, h, t3426Design, relayLaneEventOutboxKey(t3426Design, id))
		if old.SupersededAt == 0 || old.SupersededBy != item.ID {
			t.Fatalf("%s=%+v, want superseded by the READY item %d", id, old, item.ID)
		}
	}
}

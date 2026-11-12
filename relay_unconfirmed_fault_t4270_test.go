package panewire

// hk 4270 (decision 152): an unconfirmed delivery is retried where nothing
// was typed, the host fault lane hears once per (machine, reason) window and
// names every row it folded, and the worker's design lane hears only when the
// row is still unconfirmed after the unread limit. Contract:
// docs/relay-harness-delivery.md, "Escalation".

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

const t4270FaultLane = "panewire-fault-t4270"

// dropAs relays one row to the worker lane and has the node drop it with
// reason. It returns the durable row id.
func (rig *t3431Rig) dropAs(reason string) int64 {
	rig.t.Helper()
	dropped := rig.relayRow()
	dropped.Reason = reason
	t650Send(rig.t, rig.hub, "host-a", rig.dest, "relay.dropped", dropped)
	return dropped.OriginalEventID
}

// t4270Rig is the hk 3431 rig with the hk 4270 limits set to short real times.
func t4270Rig(t *testing.T, limits designQuietTimings) *t3431Rig {
	t.Helper()
	rig := t3431Start(t, t3431LiveJobWithTask())
	rig.task(t3431Task, "in_progress", "started")
	rig.job(t3431Job, false, false)
	rig.hub.quiet.timings = limits
	return rig
}

// faultNotices returns the host fault lane's hk rows, oldest first.
func (rig *t3431Rig) faultNotices() []string {
	rig.fake.mu.Lock()
	defer rig.fake.mu.Unlock()
	var texts []string
	for _, row := range rig.fake.stored {
		if row.OwnerLane == t4270FaultLane {
			texts = append(texts, row.Text)
		}
	}
	return texts
}

// Six drops of one fault on one host reach the host fault lane as one notice
// at the first drop and one summary at the window's end that names each of
// the other five rows. The worker's design lane hears nothing meanwhile.
func TestRelayHostFaultIsToldOnceAndSummaryNamesEveryRow_t4270(t *testing.T) {
	rig := t4270Rig(t, designQuietTimings{hostFaultLane: t4270FaultLane, hostFaultWindow: 150 * time.Millisecond, unconfirmedUnreadAfter: time.Hour})

	var rows []int64
	for range 6 {
		rows = append(rows, rig.dropAs(relayDroppedPaneReadFailed))
	}
	if got := rig.faultNotices(); len(got) != 1 || !strings.Contains(got[0], fmt.Sprintf("hk row %d", rows[0])) || !strings.Contains(got[0], "machine=host-a reason=pane_read_failed") {
		t.Fatalf("host fault lane notices after six drops=%q, want exactly one naming the first row", got)
	}
	eventually(t, 5*time.Second, "the window's summary", func() bool { return len(rig.faultNotices()) == 2 })
	summary := rig.faultNotices()[1]
	for _, row := range rows[1:] {
		if !strings.Contains(summary, fmt.Sprint(row)) {
			t.Fatalf("summary omits folded row %d: %q", row, summary)
		}
	}
	if !strings.Contains(summary, "5 more deliveries") {
		t.Fatalf("summary does not count the folded rows: %q", summary)
	}
	if got := rig.notices(); len(got) != 0 {
		t.Fatalf("the worker's design lane was told before the unread limit: %+v", got)
	}

	// A different fault on the same host is a new fault.
	rig.dropAs(relayDroppedMaybeInPane)
	if got := rig.faultNotices(); len(got) != 3 {
		t.Fatalf("a second fault kind sent %d notices in total, want 3: %q", len(got), got)
	}
}

// A row still unconfirmed when the unread limit ends is told to the worker's
// design lane, with the notice hk 3276 pinned; a drop is not told before.
func TestRelayDesignLaneToldOnlyWhenRowStaysUnconfirmed_t4270(t *testing.T) {
	rig := t4270Rig(t, designQuietTimings{unconfirmedUnreadAfter: 200 * time.Millisecond})

	row, _ := rig.drop()
	if got := rig.notices(); len(got) != 0 {
		t.Fatalf("the design lane was told at the drop: %+v", got)
	}
	var got []hubRelayInjectEvent
	eventually(t, 5*time.Second, "the unread notice", func() bool {
		got = append(got, rig.notices()...)
		return len(got) > 0
	})
	if len(got) != 1 || !strings.HasSuffix(got[0].Text, t3431TodayText(row, "mergewake-findings-1")) {
		t.Fatalf("design lane notices=%+v, want the hk 3276 notice once", got)
	}
}

// A row the pane confirms inside the limit tells nobody.
func TestRelayConfirmedRowIsNotToldToDesignLane_t4270(t *testing.T) {
	rig := t4270Rig(t, designQuietTimings{unconfirmedUnreadAfter: 200 * time.Millisecond})

	row, dropped := rig.drop()
	rig.hub.markRelayEventDelivered(relayPending{machine: "host-a", pane: dropped.Pane, eventID: row, kind: "lane.event", event: hubJobEventPayload{JobID: dropped.JobID, OwnerLane: t3431Worker}})
	// The window proves nothing more happens: wait out twice the limit.
	time.Sleep(450 * time.Millisecond)
	if got := rig.notices(); len(got) != 0 {
		t.Fatalf("a confirmed row was told to the design lane: %+v", got)
	}
}

// A row where nothing was typed is delivered again after the retry wait,
// labelled, through the row's one replay; the row is then closed in hk.
func TestRelayUnconfirmedRowThatTypedNothingIsRetried_t4270(t *testing.T) {
	rig := t4270Rig(t, designQuietTimings{unconfirmedRetryAfter: 100 * time.Millisecond, unconfirmedUnreadAfter: time.Hour})

	row := rig.dropAs(relayDroppedPaneReadFailed)
	var retry hubRelayInjectEvent
	eventually(t, 5*time.Second, "the retried injection", func() bool {
		select {
		case d := <-rig.dest.relays:
			if d.Lane == t3431Worker && d.EventID == row {
				retry = d
				return true
			}
		default:
		}
		return false
	})
	if !strings.HasPrefix(retry.Text, "[retry - first sent") || !strings.Contains(retry.Text, "fix the failing check") {
		t.Fatalf("retry text=%q, want the original text behind a retry label", retry.Text)
	}
	if to := rig.fake.deliveredToFor(row); !strings.Contains(to, "replay-retired") {
		t.Fatalf("row %d delivered_to=%q, want it retired by its one replay", row, to)
	}
}

// A row that may already be in the pane (maybe_in_pane) is never retyped by
// the retry, however long it waits.
func TestRelayUnconfirmedRowThatMayBeInPaneIsNotRetried_t4270(t *testing.T) {
	rig := t4270Rig(t, designQuietTimings{unconfirmedRetryAfter: 50 * time.Millisecond, unconfirmedUnreadAfter: time.Hour})

	row := rig.dropAs(relayDroppedMaybeInPane)
	// Proves nothing more happens: three retry waits with no second injection.
	time.Sleep(200 * time.Millisecond)
	select {
	case d := <-rig.dest.relays:
		if d.EventID == row {
			t.Fatalf("a maybe_in_pane row was typed again: %+v", d)
		}
	default:
	}
}

// The retry's own copy dropping too does not leave the row silent: the design
// lane is told when the unread limit ends, whichever way hk reads the row.
func TestRelayFailedRetryStillReachesDesignLane_t4270(t *testing.T) {
	rig := t4270Rig(t, designQuietTimings{unconfirmedRetryAfter: 60 * time.Millisecond, unconfirmedUnreadAfter: 500 * time.Millisecond})

	row := rig.dropAs(relayDroppedPaneReadFailed)
	var retried bool
	var notices []hubRelayInjectEvent
	eventually(t, 5*time.Second, "the retry's injection", func() bool {
		select {
		case d := <-rig.dest.relays:
			switch {
			case d.Lane == t3431Worker && d.EventID == row:
				retried = true
			case d.Lane == t3431Design:
				notices = append(notices, d)
			}
		default:
		}
		return retried
	})
	// The retried copy drops as well.
	t650Send(t, rig.hub, "host-a", rig.dest, "relay.dropped", relayDroppedPayload{JobID: laneEventTransportID(t3431Worker, "mergewake-findings-1"), Pane: "w1:pW", Lane: t3431Worker, OriginalEventID: row, Reason: relayDroppedPaneReadFailed})
	eventually(t, 5*time.Second, "the design lane's notice", func() bool {
		notices = append(notices, rig.notices()...)
		return len(notices) > 0
	})
	if len(notices) != 1 || !strings.Contains(notices[0].Text, fmt.Sprintf("event %d", row)) {
		t.Fatalf("design lane notices=%+v, want one naming row %d", notices, row)
	}
}

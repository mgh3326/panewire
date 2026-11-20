package panewire

import (
	"encoding/json"
	"strings"
	"testing"
)

// A filtered owner-notify receipt is a persisted hk row (ID set) that never
// reaches the owner's inbox. The refusal must not say the owner lane was
// notified for it; only a relay that reached the inbox counts.
func TestEscalateAuthorNotifiedIgnoresFilteredReceipt(t *testing.T) {
	filtered := relayLaneEventResult{ID: 9, Routed: true, Machine: filteredDeliveryPrefix + "owner-notify:ladder-step", AlreadyDelivered: true}
	if got := escalateAuthorNotified("parent", filtered); got != escalateAuthorNotNotified {
		t.Fatalf("a filtered receipt reached no owner inbox: notified=%q", got)
	}
	delivered := relayLaneEventResult{ID: 9, Routed: true, Machine: "machine-a"}
	if got := escalateAuthorNotified("parent", delivered); got != escalateAuthorOwnerLane {
		t.Fatalf("a delivered row reached the owner: notified=%q", got)
	}
	unrouted := relayLaneEventResult{ID: 9}
	if got := escalateAuthorNotified("parent", unrouted); got != escalateAuthorOwnerLane {
		t.Fatalf("a persisted row queued for the owner counts as notified: notified=%q", got)
	}
	if got := escalateAuthorNotified("", delivered); got != escalateAuthorNotNotified {
		t.Fatalf("an ownerless refusal filed nothing: notified=%q", got)
	}
}

// The legacy worker refusal's reason states the retired-role fact only. Whether
// the owner lane got the event is carried by notified, from the relay result.
func TestEscalateAuthorLegacyWorkerReasonDoesNotClaimNotice(t *testing.T) {
	f := newEscalateFixture(t, "legacy-reason")
	holder, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(holder.Request.Args); i++ {
		if holder.Request.Args[i] == "--role" {
			holder.Request.Args[i+1] = "worker"
		}
	}
	raw, err := json.Marshal(holder.Request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.hub.inbox.store.db.ExecContext(t.Context(), f.hub.inbox.store.q(`UPDATE spawn_operations SET payload=? WHERE request_id=?`), string(raw), holder.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	if _, err = f.hub.inbox.store.db.ExecContext(t.Context(), f.hub.inbox.store.q(`UPDATE spawn_reservations SET task_role=? WHERE request_id=?`), "42:worker", holder.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	result, _ := f.escalate(t, nil)
	if result.Code != "legacy_worker_submission" {
		t.Fatalf("result=%+v", result)
	}
	if strings.Contains(result.Reason, "notified") {
		t.Fatalf("the reason claims a notice; notified carries that: reason=%q", result.Reason)
	}
	if !strings.Contains(result.Reason, "retired") {
		t.Fatalf("the reason must keep the retired-role fact: reason=%q", result.Reason)
	}
	if result.Notified != escalateAuthorOwnerLane {
		t.Fatalf("the owner's decision event was enqueued: notified=%q", result.Notified)
	}
}

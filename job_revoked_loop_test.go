package panewire

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func revokedLoopFixture(t *testing.T) (*HubServer, *hubAgent, *HubClient, *Store, *fakeHandoffkeep, *bytes.Buffer) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "hub.sqlite3")
	hub, agent, fake := t3434Hub(t, path)
	var logs bytes.Buffer
	hub.logger = slog.New(slog.NewTextHandler(&logs, nil))
	if _, _, err := hub.inbox.store.claimSpawn(t.Context(), t4270Request(t4270Reviewer, t3426Design, "", "reviewer"), t4270Reviewer, t3426Design, time.Now()); err != nil {
		t.Fatal(err)
	}

	inbox := t.TempDir()
	r20t7WriteEvent(t, inbox, t4270Reviewer, "00002-job.revoked.json",
		`{"kind":"job.revoked","job_id":"`+t4270Reviewer+`","owner_lane":"`+t3426Design+`","reason":"reviewer-lifecycle","host":"host-a","epoch":1}`)
	store := NewMemoryStore(t)
	t.Cleanup(func() { _ = store.Close() })
	node := r20Node(inbox, store)
	now := time.Now().UTC()
	node.now = func() time.Time { return now }
	return hub, agent, node, store, fake, &logs
}

func revokedLoopSend(t *testing.T, hub *HubServer, agent *hubAgent, event hubClientEvent) []hubRelayPersistedEvent {
	t.Helper()
	wire, err := json.Marshal(hubClientWireEvent(event))
	if err != nil {
		t.Fatal(err)
	}
	hub.handleAgentMessage("host-a", "remote-a", agent, wire)
	return drainPersisted(agent)
}

func revokedLoopApplyReceipt(node *HubClient, receipt hubRelayPersistedEvent) bool {
	body, err := json.Marshal(receipt)
	if err != nil {
		return false
	}
	message, ok := parseHubOutbound(body)
	if !ok {
		return false
	}
	node.recordRelayPersisted(message)
	return true
}

func TestDesignRevocationReceiptStopsResendLoop(t *testing.T) {
	hub, agent, node, store, fake, logs := revokedLoopFixture(t)
	defer hub.Close()

	for processed := 1; processed <= 2; processed++ {
		events := node.jobCompletionEvents()
		if len(events) != 1 || events[0].Kind != "job.revoked" {
			t.Fatalf("scan %d offered %+v, want the pending job.revoked", processed, events)
		}
		node.commitRelaySent(events[0])
		acks := revokedLoopSend(t, hub, agent, events[0])
		if len(acks) != 1 || !revokedLoopApplyReceipt(node, acks[0]) {
			// An older node rejects this acknowledgement. Model that node by
			// leaving its outbox row unretired, then let the next scan resend it.
			if processed == 2 {
				break
			}
			node.now = func() time.Time { return time.Now().UTC().Add(relayOutboxBackoff) }
			continue
		}
		if processed == 1 {
			break
		}
	}

	if got := strings.Count(logs.String(), "design lane record kept in hk only"); got != 1 {
		t.Fatalf("design revocation was processed %d times, want 1; logs=%s", got, logs.String())
	}
	if events := node.jobCompletionEvents(); len(events) != 0 {
		t.Fatalf("the node still offers the revoked event after its receipt: %+v", events)
	}
	if got := fake.count("POST", "/v1/relay/events"); got != 1 {
		t.Fatalf("handoffkeep received %d revocation rows, want 1", got)
	}
	var persisted int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM relay_sent WHERE kind='job.revoked' AND persisted_at IS NOT NULL`).Scan(&persisted); err != nil || persisted != 1 {
		t.Fatalf("retired revoked outbox rows=%d err=%v, want 1", persisted, err)
	}
}

func TestDesignRevocationRetriesAfterPersistFailure(t *testing.T) {
	hub, agent, node, _, fake, logs := revokedLoopFixture(t)
	defer hub.Close()
	fake.status = 500

	first := node.jobCompletionEvents()
	if len(first) != 1 {
		t.Fatalf("first scan offered %+v, want one revoked event", first)
	}
	node.commitRelaySent(first[0])
	if acks := revokedLoopSend(t, hub, agent, first[0]); len(acks) != 0 {
		t.Fatalf("failed persistence acknowledged the event: %+v", acks)
	}
	node.now = func() time.Time { return time.Now().UTC().Add(relayOutboxBackoff) }
	fake.status = 201
	retry := node.jobCompletionEvents()
	if len(retry) != 1 {
		t.Fatalf("retry scan offered %+v, want one revoked event", retry)
	}
	node.commitRelaySent(retry[0])
	acks := revokedLoopSend(t, hub, agent, retry[0])
	if len(acks) != 1 || !revokedLoopApplyReceipt(node, acks[0]) {
		t.Fatalf("successful retry did not produce an applicable receipt: %+v", acks)
	}
	if got := node.jobCompletionEvents(); len(got) != 0 {
		t.Fatalf("event remained after successful retry: %+v", got)
	}
	if got := strings.Count(logs.String(), "design lane record kept in hk only"); got != 1 {
		t.Fatalf("successful retry logged %d filtered records, want 1; logs=%s", got, logs.String())
	}
}

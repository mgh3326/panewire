package panewire

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"
)

func TestDecisionReplyAfter(t *testing.T) {
	store := NewMemoryStore(t)
	defer store.Close()
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	query := func(want bool) {
		t.Helper()
		got, err := store.DecisionReplyAfter(ctx, "pane", "lane", base.UnixMilli())
		if err != nil || got != want {
			t.Fatalf("reply=%v err=%v want=%v", got, err, want)
		}
	}
	query(false)
	for _, d := range []Delivery{
		{DeliveryID: "old", ResolvedPaneID: "pane", CompletedAtMS: base.Add(-time.Second).UnixMilli(), SubmissionResult: "queued"},
		{DeliveryID: "failed", ResolvedPaneID: "pane", CompletedAtMS: base.Add(time.Second).UnixMilli(), SubmissionResult: "unproven"},
		{DeliveryID: "other", ResolvedPaneID: "other", CompletedAtMS: base.Add(time.Second).UnixMilli(), SubmissionResult: "queued"},
	} {
		if _, err := store.InsertDelivery(ctx, d, ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.RecordRelayDelivered(ctx, "wrong", 1, "pane", "hash", base.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	query(false)
	if _, err := store.InsertDelivery(ctx, Delivery{DeliveryID: "good", ResolvedPaneID: "pane", CompletedAtMS: base.Add(time.Second).UnixMilli(), SubmissionResult: "queued"}, ""); err != nil {
		t.Fatal(err)
	}
	query(true)
	if _, err := store.db.Exec(`DELETE FROM deliveries WHERE delivery_id = 'good'`); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordRelayDelivered(ctx, "lane", 2, "pane", "hash", base.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	query(true)
}

func TestDecisionReplyDaemonRequest(t *testing.T) {
	store := NewMemoryStore(t)
	defer store.Close()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := store.RecordRelayDelivered(context.Background(), "worker", 1, "pane", "hash", base.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	d := &Daemon{store: store}
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() { d.handle(context.Background(), server); close(done) }()
	if err := json.NewEncoder(client).Encode(localRequest{Op: "decision.reply", Target: "pane", OwnerLane: "worker", SinceMS: base.UnixMilli(), TimeoutMS: 2000}); err != nil {
		t.Fatal(err)
	}
	var response struct {
		OK     bool `json:"ok"`
		Result bool `json:"result"`
	}
	if err := json.NewDecoder(client).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if !response.OK || !response.Result {
		t.Fatalf("response=%+v", response)
	}
	client.Close()
	<-done
}

package panewire

import (
	"testing"
	"time"
)

func TestInboxEscalationDoesNotRepeatAfterQuarantine(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		now := time.UnixMilli(1700000000000)
		if _, err := s.put("repeat", "lane-a", "lane.event", "inbox", quietEvent("repeat", "READY | PR"), now, time.Second, 0); err != nil {
			t.Fatal(err)
		}
		if err := s.makeBatches(now.Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		batches, err := s.pendingBatches()
		if err != nil || len(batches) != 1 {
			t.Fatalf("first: %v %v", batches, err)
		}
		if err := s.quarantineBatch(batches[0].ID, "external scheduler owns lane digest"); err != nil {
			t.Fatal(err)
		}
		policy, err := loadDeliveryPolicy("default")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.recoverMustAct(policy, time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := s.makeBatches(now.Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		batches, err = s.pendingBatches()
		if err != nil || len(batches) != 0 {
			t.Fatalf("repeated escalation: %v %v", batches, err)
		}
	})
}

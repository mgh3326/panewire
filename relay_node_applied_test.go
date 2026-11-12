package panewire

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestNodeAppliedExpiredBatchLostAck(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	rig.herdr.getStatusByPane["w1:pA"] = "working"
	rig.herdr.waitStatus = "timeout"
	rig.herdr.waitGate = make(chan struct{})
	var ids []int64
	for i := 0; i < 3; i++ {
		rig.sendLaneEvent(fmt.Sprintf("lost-ack-%d", i), fmt.Sprintf("note %d", i))
		directive := rig.directive()
		directive.Deliver = "max_wait=1"
		rig.offer(directive)
		ids = append(ids, directive.EventID)
	}
	rig.flush()
	close(rig.herdr.waitGate)
	if !waitUntil(5*time.Second, func() bool {
		for _, id := range ids {
			if _, found, err := rig.store.RelayDeliveredByKey(context.Background(), t989Lane, id); err != nil || !found {
				return false
			}
		}
		return true
	}) {
		t.Fatal("batch was not recorded as delivered on node")
	}
	if len(*rig.prompts) != 1 || !strings.Contains((*rig.prompts)[0], "[대기 만료") {
		t.Fatalf("expected one expired batch, got %q", *rig.prompts)
	}
	// Discard the node's release and delivered events as a disconnected socket would.
	for len(rig.events) > 0 {
		<-rig.events
	}
	for _, id := range ids {
		if rig.row(id).DeliveredAt != "" {
			t.Fatalf("row %d unexpectedly acknowledged", id)
		}
	}
	rig.assertNoReplay(rig.restart(), "expired batch with lost acks")
}

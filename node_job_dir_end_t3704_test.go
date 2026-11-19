package panewire

import (
	"context"
	"testing"
	"time"
)

// hk 3704: the at-end job dir delete waits for the relay rows the job still
// owes the hub (fleet job_dir_end.go). jobRelayOwed answers from the same scan
// and key the send path uses, so the test drives the real send and persist.

func TestJobRelayOwedFollowsTheSendAndThePersist(t *testing.T) {
	ctx := context.Background()
	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	r20WriteEvent(t, inbox, "t3704-job", "00001-job.completed.json", `{"type":"job.completed","epoch":1,"owner_lane":"lane-a","label":"agent-a","host":"host-a","report_path":"a.md","report_last_line":"done"}`, time.Time{})
	node := r20Node(inbox, store)

	if owed, err := node.jobRelayOwed(ctx, "t3704-job"); err != nil || !owed {
		t.Fatalf("before the send: owed=%t err=%v, want owed", owed, err)
	}
	if events := node.jobCompletionEvents(); len(events) != 1 {
		t.Fatalf("send sent %d events, want 1", len(events))
	}
	if owed, err := node.jobRelayOwed(ctx, "t3704-job"); err != nil || !owed {
		t.Fatalf("sent but not persisted: owed=%t err=%v, want owed", owed, err)
	}

	node.recordRelayPersisted(hubOutboundMessage{Type: "relay.persisted", JobID: "t3704-job", Kind: "job.completed", Epoch: 1, ReportPath: "a.md", EventID: 5, ProducerEventID: "00001-job.completed.json"})
	if owed, err := node.jobRelayOwed(ctx, "t3704-job"); err != nil || owed {
		t.Fatalf("persisted: owed=%t err=%v, want settled", owed, err)
	}
	if owed, err := node.jobRelayOwed(ctx, "other-job"); err != nil || owed {
		t.Fatalf("another job: owed=%t err=%v, want settled", owed, err)
	}
}

// Without an outbox the node cannot prove the relay settled, so the answer is
// an error the at-end gate reads as owed.
func TestJobRelayOwedWithoutOutboxIsAnError(t *testing.T) {
	node := &HubClient{jobsInboxRoot: t.TempDir()}
	owed, err := node.jobRelayOwed(context.Background(), "t3704-job")
	if err == nil || !owed {
		t.Fatalf("no outbox: owed=%t err=%v, want owed with an error", owed, err)
	}
}

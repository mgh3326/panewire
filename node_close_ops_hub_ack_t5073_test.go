package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// ackingHubClose is a fake ledger whose acknowledgement lands in the real
// job ledger, as nodeFleetHubClose's does.
type ackingHubClose struct {
	*fakeHubClose
	jobs string
}

func (l ackingHubClose) AckIntentCancel(ctx context.Context, job, id string) error {
	return fleet.RecordHubCloseAck(ctx, l.jobs, job, id)
}

// ackOwingNode is an unknown close whose intent the rollback drain cancelled:
// its cancel event owes the hub's acknowledgement.
func ackOwingNode(t *testing.T) (*HubClient, string) {
	t.Helper()
	client, inbox := unknownCloseNode(t, false)
	client.nodeHubClose = ackingHubClose{fakeHubClose: client.nodeHubClose.(*fakeHubClose), jobs: filepath.Join(inbox, "jobs")}
	addCloseOpEvent(t, inbox, "00004-job.hub_close_cancelled.json", `{"kind":"job.hub_close_cancelled","payload":{"request_id":"unknown-op","reason":"rollback_unproven","hub_ack":true}}`)
	return client, inbox
}

func ackEvents(t *testing.T, inbox string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(inbox, "jobs", closeOpJob, "events"))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "job.hub_close_acked") {
			count++
		}
	}
	return count
}

func ackPending(t *testing.T, inbox string) []string {
	t.Helper()
	jobs, err := fleet.PendingHubCloseAckJobs(filepath.Join(inbox, "jobs"))
	if err != nil {
		t.Fatal(err)
	}
	return jobs
}

func ackArgs(id string) map[string]any {
	return map[string]any{"request_id": id, "job": closeOpJob, "ack": true}
}

// hk 5073: the hub's ack on pane.close_status is what clears the node's debt.
// A plain status read, the hub's own poll, does not.
func TestCloseStatusAckRecordsTheHubAcknowledgement_t5073(t *testing.T) {
	client, inbox := ackOwingNode(t)
	if result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, closeOpRef("unknown-op")); code != "" || result["state"] != "intent_cancelled" {
		t.Fatalf("plain status=%v code=%q", result, code)
	}
	if pending := ackPending(t, inbox); len(pending) != 1 {
		t.Fatalf("a plain status read cleared the ack debt: %v", pending)
	}
	result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, ackArgs("unknown-op"))
	if code != "" || result["state"] != "intent_cancelled" {
		t.Fatalf("ack status=%v code=%q", result, code)
	}
	if pending := ackPending(t, inbox); len(pending) != 0 {
		t.Fatalf("ack left the job pending: %v", pending)
	}
	// The hub repeats the ack every pass until it sees an answer: a replay
	// writes nothing more.
	if _, code := nodeRun(t, client, nodeCommandPaneCloseStatus, ackArgs("unknown-op")); code != "" || ackEvents(t, inbox) != 1 {
		t.Fatalf("replayed ack code=%q events=%d, want one durable ack", code, ackEvents(t, inbox))
	}
}

// An ack for a request the node did not answer intent_cancelled for records
// nothing, so a stray ack cannot clear another request's debt.
func TestCloseStatusAckOnlyCoversACancelledIntent_t5073(t *testing.T) {
	client, inbox := unknownCloseNode(t, false)
	if result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, ackArgs("unknown-op")); code != "" || result["state"] != fleet.CloseOpUnknown {
		t.Fatalf("status=%v code=%q", result, code)
	}
	if ackEvents(t, inbox) != 0 {
		t.Fatal("an ack was recorded for an intent that was never cancelled")
	}
}

// A failed ledger write is journal_failed, so the hub repeats the ack.
func TestCloseStatusAckFailureIsRetryable_t5073(t *testing.T) {
	client, inbox := ackOwingNode(t)
	client.nodeHubClose = failingAckHubClose{ackOwingNodeFake(client)}
	if result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, ackArgs("unknown-op")); code != "journal_failed" || result != nil {
		t.Fatalf("status=%v code=%q; want journal_failed", result, code)
	}
	if pending := ackPending(t, inbox); len(pending) != 1 {
		t.Fatalf("a failed ack cleared the debt: %v", pending)
	}
}

type failingAckHubClose struct{ *fakeHubClose }

func (failingAckHubClose) AckIntentCancel(context.Context, string, string) error {
	return os.ErrInvalid
}

func ackOwingNodeFake(client *HubClient) *fakeHubClose {
	return client.nodeHubClose.(ackingHubClose).fakeHubClose
}

// ack is a close_status argument only: release and close_op refuse it, and an
// ack alone is not a request.
func TestAckIsOnlyACloseStatusArgument_t5073(t *testing.T) {
	status := `{"request_id":"op-1","job":"` + closeOpJob + `","ack":true}`
	if _, valid := decodeNodeCommandArgs(nodeCommandPaneCloseStatus, json.RawMessage(status)); !valid {
		t.Fatal("close_status with ack was refused")
	}
	for name, tc := range map[string]struct{ op, raw string }{
		"release":  {nodeCommandPaneCloseRelease, status},
		"ack only": {nodeCommandPaneCloseStatus, `{"ack":true}`},
		"null ack": {nodeCommandPaneCloseStatus, `{"request_id":"op-1","job":"` + closeOpJob + `","ack":null}`},
		"string":   {nodeCommandPaneCloseStatus, `{"request_id":"op-1","job":"` + closeOpJob + `","ack":"yes"}`},
	} {
		if _, valid := decodeNodeCommandArgs(tc.op, json.RawMessage(tc.raw)); valid {
			t.Errorf("%s: %s accepted", name, tc.raw)
		}
	}
	if _, valid := decodeNodeCommandArgs(nodeCommandPaneCloseStatus, json.RawMessage(`{"request_id":"op-1","job":"`+closeOpJob+`"}`)); !valid {
		t.Fatal("the plain close_status form was refused")
	}
}

// The runbook gate: drain does not report success while a cancelled close
// still owes the hub's ack, and does once the ack is durable.
func TestCloseOpsDrainWaitsForTheHubAck_t5073(t *testing.T) {
	inbox := t.TempDir()
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	seedCloseOpJob(t, inbox)
	jobs := filepath.Join(inbox, "jobs")
	seedNodeCloseOp(t, inbox, "gone-op", fleet.CloseOpGone)
	in := fleet.HubCloseIntent{RequestID: "gone-op", Attempt: closeOpAttempt, Pane: "w1:p1", Label: "job-a", EndReason: "rollback test"}
	if err := fleet.RecordHubCloseIntent(context.Background(), jobs, closeOpJob, in); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	if rc := runNodeCloseOpsCLI([]string{"drain", "--inbox-root", inbox}, &out, &stderr, nil); rc != ExitConditionInvalid {
		t.Fatalf("first drain rc=%d output=%s error=%s", rc, out.String(), stderr.String())
	}
	app := &fleet.App{Jobs: jobs, Home: t.TempDir(), Now: time.Now}
	settlement := fleet.HubCloseSettlement{App: app, PaneListed: func(context.Context, string, string) (bool, error) { return true, nil }}
	if state, err := settlement.Settle(context.Background(), closeOpJob); err != nil || state != fleet.HubCloseCancelled {
		t.Fatalf("settlement=%v err=%v", state, err)
	}
	out.Reset()
	if rc := runNodeCloseOpsCLI([]string{"drain", "--inbox-root", inbox}, &out, &stderr, nil); rc != ExitConditionInvalid ||
		!strings.Contains(out.String(), "acknowledge") || !strings.Contains(out.String(), closeOpJob) || strings.Contains(out.String(), "no pending jobs") {
		t.Fatalf("drain before the hub ack rc=%d output=%s error=%s", rc, out.String(), stderr.String())
	}
	if err := fleet.RecordHubCloseAck(context.Background(), jobs, closeOpJob, "gone-op"); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if rc := runNodeCloseOpsCLI([]string{"drain", "--inbox-root", inbox}, &out, &stderr, nil); rc != ExitOK || !strings.Contains(out.String(), "no pending jobs") {
		t.Fatalf("drain after the hub ack rc=%d output=%s error=%s", rc, out.String(), stderr.String())
	}
}

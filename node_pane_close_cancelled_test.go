package panewire

import (
	"os"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func cancelledUnknownNode(t *testing.T, request string) (*HubClient, string) {
	t.Helper()
	client, inbox := unknownCloseNode(t, false)
	addCloseOpEvent(t, inbox, "00004-job.hub_close_cancelled.json", `{"kind":"job.hub_close_cancelled","payload":{"request_id":"`+request+`","reason":"rollback_unproven"}}`)
	return client, inbox
}

func TestCloseStatusReportsDurableIntentCancellation(t *testing.T) {
	client, inbox := cancelledUnknownNode(t, "unknown-op")
	result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, closeOpRef("unknown-op"))
	if code != "" || result["state"] != "intent_cancelled" || result["settled"] != false {
		t.Fatalf("status=%v code=%q; want intent cancellation", result, code)
	}
	root, err := os.OpenRoot(seedCloseOpJob(t, inbox))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	op, found, err := fleet.ReadCloseOp(root, "unknown-op")
	if err != nil || !found || op.State != fleet.CloseOpUnknown {
		t.Fatalf("unknown journal changed: %+v found=%v err=%v", op, found, err)
	}
}

func TestCloseStatusIntentCancellationMustMatchRequest(t *testing.T) {
	client, _ := cancelledUnknownNode(t, "other-op")
	result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, closeOpRef("unknown-op"))
	if code != "" || result["state"] != fleet.CloseOpUnknown {
		t.Fatalf("different request cancellation gave status=%v code=%q", result, code)
	}
}

func TestCloseStatusIntentCancellationRecognizesLaterSettlement(t *testing.T) {
	client, inbox := cancelledUnknownNode(t, "unknown-op")
	addCloseOpEvent(t, inbox, "00005-job.lost.json", `{"kind":"job.lost","payload":{"reason":"hub_close","final":true,"request_id":"other-op"}}`)
	result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, closeOpRef("unknown-op"))
	if code != "" || result["settled"] != true {
		t.Fatalf("later final settlement status=%v code=%q", result, code)
	}
}

func TestCloseStatusIntentCancellationRequiresSyncedEvents(t *testing.T) {
	client, inbox := cancelledUnknownNode(t, "unknown-op")
	client.nodeHubClose = &failingCloseEventSync{fakeHubClose: &fakeHubClose{watcher: fleet.HubCloseWatcherAware}}
	result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, closeOpRef("unknown-op"))
	if code != "journal_failed" || result != nil {
		t.Fatalf("failed sync gave status=%v code=%q", result, code)
	}
	client = closeOpNode(t, inbox, idlePaneHerdr())
	result, code = nodeRun(t, client, nodeCommandPaneCloseStatus, closeOpRef("unknown-op"))
	if code != "" || result["state"] != "intent_cancelled" {
		t.Fatalf("status after retry=%v code=%q", result, code)
	}
}

func TestCloseStatusIntentCancellationWaitsForJobEndLock(t *testing.T) {
	client, inbox := cancelledUnknownNode(t, "unknown-op")
	unlock, err := fleet.LockJobEnd(seedCloseOpJob(t, inbox))
	if err != nil {
		t.Fatal(err)
	}
	type answer struct {
		result any
		code   string
	}
	done := make(chan answer, 1)
	go func() {
		result, code := client.runCloseOpRef(nodeCloseOpRefArgs{RequestID: "unknown-op", Job: closeOpJob}, false)
		done <- answer{result, code}
	}()
	var got answer
	early := waitUntil(300*time.Millisecond, func() bool {
		select {
		case got = <-done:
			return true
		default:
			return false
		}
	})
	unlock()
	if !early {
		got = <-done
	}
	status, _ := got.result.(nodeCloseOpStatusResult)
	if early || got.code != "" || status.State != "intent_cancelled" {
		t.Fatalf("status escaped end lock: early=%v status=%+v code=%q", early, status, got.code)
	}
}

package panewire

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

type failingCloseEventSync struct{ *fakeHubClose }

func (*failingCloseEventSync) SyncEvents(*os.Root) error {
	return errors.New("simulated directory sync failure")
}

func unknownCloseNode(t *testing.T, reprompt bool) (*HubClient, string) {
	t.Helper()
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, idlePaneHerdr())
	seedNodeCloseOp(t, inbox, "unknown-op", fleet.CloseOpUnknown)
	addCloseOpEvent(t, inbox, "00003-job.hub_close_intent.json", `{"kind":"job.hub_close_intent","payload":{"request_id":"unknown-op","attempt":"00002-job.spawned.json","pane_id":"w1:p1","label":"job-a","end_reason":"quota_stop pool=devin-free"}}`)
	if reprompt {
		addCloseOpEvent(t, inbox, "00004-job.reprompted.json", `{"kind":"job.reprompted","payload":{"reason":"quota_stop_resumed"}}`)
	}
	return client, inbox
}

func TestCloseStatusSupersededAfterReprompt(t *testing.T) {
	client, inbox := unknownCloseNode(t, true)
	result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, closeOpRef("unknown-op"))
	if code != "" || result["state"] != "superseded" || result["attempt"] != "00004-job.reprompted.json" {
		t.Fatalf("status=%v code=%q; want the newer attempt's superseded proof", result, code)
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

func TestCloseStatusSameAttemptIsNotSuperseded(t *testing.T) {
	client, _ := unknownCloseNode(t, false)
	client.nodeHubClose = &failingCloseEventSync{fakeHubClose: &fakeHubClose{watcher: fleet.HubCloseWatcherAware}}
	result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, closeOpRef("unknown-op"))
	if code != "" || result["state"] != fleet.CloseOpUnknown {
		t.Fatalf("same attempt status=%v code=%q; want unknown", result, code)
	}
}

func TestCloseStatusSupersededRequiresSyncedEvents(t *testing.T) {
	client, inbox := unknownCloseNode(t, true)
	client.nodeHubClose = &failingCloseEventSync{fakeHubClose: &fakeHubClose{watcher: fleet.HubCloseWatcherAware}}
	result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, closeOpRef("unknown-op"))
	if code != "journal_failed" || result != nil {
		t.Fatalf("failed sync produced status=%v code=%q", result, code)
	}
	// Reconstruct the node after the failed durability step, as on restart.
	client = closeOpNode(t, inbox, idlePaneHerdr())
	result, code = nodeRun(t, client, nodeCommandPaneCloseStatus, closeOpRef("unknown-op"))
	if code != "" || result["state"] != "superseded" {
		t.Fatalf("status after retry=%v code=%q", result, code)
	}
}

func TestCloseStatusSupersededWaitsForJobEndLock(t *testing.T) {
	client, inbox := unknownCloseNode(t, true)
	unlock, err := fleet.LockJobEnd(seedCloseOpJob(t, inbox))
	if err != nil {
		t.Fatal(err)
	}
	type answer struct {
		result any
		code   string
	}
	started := make(chan struct{})
	done := make(chan answer, 1)
	go func() {
		close(started)
		result, code := client.runCloseOpRef(nodeCloseOpRefArgs{RequestID: "unknown-op", Job: closeOpJob}, false)
		done <- answer{result, code}
	}()
	<-started
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
	if early || got.code != "" || status.State != "superseded" {
		t.Fatalf("status escaped end lock: early=%v status=%+v code=%q", early, status, got.code)
	}
}

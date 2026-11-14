package panewire

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

// The login screen is seen after the node's hk job claim, so the refusal ends
// the operation on that host (hk 4614); the desk still gets its one-time notice.
func TestLoginScreenAuthUnavailableEndsOnTheHostT3891(t *testing.T) {
	nodeResult, _, _ := t3891RunHubSpawn(t, "agy-flash38-medium", true, t3891AgyLoginScreen)
	h, _, destination := spawnNotifyHubLane(t, "", "operator-desk")
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	request := autoSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	transport := &directSpawnTransport{h: h, task: task}
	type answer struct {
		reply fleet.HubSpawnReply
		err   error
	}
	first := make(chan answer, 1)
	go func() {
		reply, err := dispatchPlacedSpawn(boundedContext(t), transport, firstRunNodeRequest(request, 941, "agy-flash38-medium"))
		first <- answer{reply, err}
	}()
	sent := t14ReadNodeSpawn(t, node)
	if err := wsjson.Write(t.Context(), node, firstRunRefusal(sent.RequestID, nodeResult.StdoutTail, nodeResult.RC)); err != nil {
		t.Fatal(err)
	}
	got := <-first
	if got.err != nil || got.reply.NextHost || got.reply.RC != t3891AuthUnavailableRC {
		t.Fatalf("a login screen after the hk claim went back to placement: %+v", got)
	}
	reason := spawnResultReason(nodeResult)
	if !spawnRefusalTriesNextHost(reason) {
		t.Fatalf("reason %q is not a first-run refusal", reason)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "retryable" || saved.Node == nil || saved.Node.Machine != "machine-a" || !spawnReplayPinsHost(saved.Outcome) {
		t.Fatalf("refusal did not end the operation on its host: %+v %v", saved, err)
	}
	var notices []string
	eventually(t, 5*time.Second, "agy login notice", func() bool {
		notices = append(notices, deskNotices(t, h, destination)...)
		return len(notices) > 0
	})
	want := "[machine-a] agy needs a one-time action: log in"
	if len(notices) != 1 || notices[0] != want {
		t.Fatalf("notices=%q want exactly %q", notices, want)
	}
}

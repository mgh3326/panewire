package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// relayedUnobservedResetStop is the job.lost a node relays for a reviewer stop
// whose reset it did not observe, through the real relay path (the scan, then
// the wire payload the hub decodes).
func relayedUnobservedResetStop(t *testing.T, job string, announced time.Time) hubJobEventPayload {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "jobs", job, "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	event := map[string]any{"kind": "job.lost", "created_at": announced.Format(time.RFC3339Nano), "payload": map[string]any{"reason": "quota_stop", "pool": "devin-free", "state_change_seq": 7}}
	b, _ := json.Marshal(event)
	if err := os.WriteFile(filepath.Join(dir, "0001-job.lost.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	events := scanHubRelayEvents(root)
	if len(events) != 1 {
		t.Fatalf("events: %+v", events)
	}
	wire, ok := (&HubClient{}).relayEventForSend(events[0])
	if !ok {
		t.Fatal("stop was not queued")
	}
	decoded, valid := decodeHubJobEscalationPayload(wire.Payload)
	if !valid {
		t.Fatalf("invalid hub payload: %s", wire.Payload)
	}
	return decoded
}

// hk 5777: the node did not observe the stop's reset, so the relayed reason
// names no reset_at. The relay used to write its own default (event time + 5
// minutes) there, and the give-up then wrote that default into the immutable
// end_reason as if the provider had said so.
func TestUnobservedResetIsNotRelayedAsAReset_t5777(t *testing.T) {
	announced := time.Now().UTC().Truncate(time.Second)
	reason := relayedUnobservedResetStop(t, "mwr-t5777-p1", announced).Reason
	if strings.Contains(reason, "reset_at=") {
		t.Fatalf("reason %q names a reset the node never observed", reason)
	}
	if !strings.Contains(reason, "pool=devin-free") || !strings.Contains(reason, "seq=7") {
		t.Fatalf("reason %q lost its pool or its pane state", reason)
	}
}

// The default still applies to cooling: an announced stop with no observed reset
// cools its pool for five minutes from when it was announced, not from when the
// hub heard of it.
func TestUnobservedResetCoolsFiveMinutesFromAnnouncement_t5777(t *testing.T) {
	announced := time.Now().UTC().Truncate(time.Second)
	decoded := relayedUnobservedResetStop(t, "mwr-t5777-p2", announced)
	h := &HubServer{}
	received := announced.Add(30 * time.Second)
	h.observePoolCooling(decoded.Reason, received)
	if got := h.fleetSnapshot(received).Cooling["devin-free"]; !got.Equal(announced.Add(5 * time.Minute)) {
		t.Fatalf("cooling until %v, want five minutes from the announcement %v", got, announced)
	}
}

// The give-up of an announced reviewer stop whose reset was never observed ends
// its record with the stop's pool alone: no reset_at is written, because none
// was observed.
func TestGiveUpWritesNoResetTheNodeNeverObserved_t5777(t *testing.T) {
	const job = "mwr-t5777-p3"
	f := newQuotaReplaceFixture(t, "reviewer", job)
	f.stopPool = "devin-free"
	event := relayedUnobservedResetStop(t, job, time.Now().UTC().Truncate(time.Second))
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.hub.giveUpQuotaStop(t.Context(), f.holder, "parent", event, "not_placed")
	}()
	commands := f.serveCommands(t, 4) // probe, capture, close, mark_lost
	<-done
	want := fleet.QuotaStopEndReason("devin-free", time.Time{}, "", "")
	if reason := endReasonOf(t, commands[3]); reason != want {
		t.Fatalf("end_reason=%q, want %q", reason, want)
	}
}

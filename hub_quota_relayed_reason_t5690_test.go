package panewire

import (
	"strings"
	"testing"
	"time"
)

// hk 5690: the hub began a replacement only for the bare reason "quota_stop", but
// every relayed announcement carries the pool and the reset
// ("quota_stop pool=<p> reset_at=<t>", hub_jobs_client.go), so no announced stop
// (an implementer the quota-stop manager handed over, a credit-stopped job) was
// ever replaced. The relayed form starts the replacement: its first act is to
// look at the old pane.
func TestRelayedQuotaStopReasonStartsTheReplacement(t *testing.T) {
	for _, reason := range []string{"quota_stop", "quota_stop pool=devin-free reset_at=2026-11-19T05:59:11Z"} {
		f := newQuotaReplaceFixture(t, "reviewer", "mwr-5-p1")
		event := quotaStopEvent(f.job, "devin-free")
		event.Reason = reason
		f.hub.startQuotaReplacement("machine-a", event)
		got := make(chan string, 1)
		go func() {
			got <- strings.Trim(string(nodeCommandRead(t, f.node)["op"]), `"`)
		}()
		select {
		case op := <-got:
			if op != "agents.list" {
				t.Fatalf("reason %q: first node command %q, want the pane probe", reason, op)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("reason %q started no replacement", reason)
		}
	}
}

func TestIsQuotaStopReasonMatchesAParsedPrefix(t *testing.T) {
	for _, reason := range []string{"", "quotastop", "completed", "quota_stopped pool=x"} {
		if isQuotaStopReason(reason) {
			t.Fatalf("%q is read as a quota stop", reason)
		}
	}
	for _, reason := range []string{"quota_stop", "quota_stop pool=claude"} {
		if !isQuotaStopReason(reason) {
			t.Fatalf("%q is not read as a quota stop", reason)
		}
	}
}

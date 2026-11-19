package panewire

import (
	"context"
	"testing"
	"time"
)

// RepublishFleetQuota makes the quota loop run a round now, inside a long
// interval, and never blocks the caller (hk 5562 review).
func TestRepublishFleetQuotaRunsARoundNow_T5562(t *testing.T) {
	client := quotaClient(t)
	client.statePublish = true
	client.stateQuotaInterval = time.Hour
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.runFleetQuota(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	eventually(t, 3*time.Second, "the first round", func() bool { return client.fleetQuotaRounds.Load() == 1 })
	client.RepublishFleetQuota()
	client.RepublishFleetQuota() // a round already asked for covers this call
	eventually(t, 3*time.Second, "a round on request", func() bool { return client.fleetQuotaRounds.Load() >= 2 })
}

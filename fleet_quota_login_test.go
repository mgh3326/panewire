package panewire

import (
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

func quotaClient(t *testing.T) *HubClient {
	client := readinessClient(t, "", readinessFakes(nil))
	client.events = make(chan hubClientEvent, 8)
	client.warn = func(string) {}
	return client
}

func poolLogin(t *testing.T, client *HubClient, pool string) fleet.HarnessReadiness {
	t.Helper()
	return client.collectReadiness(t.Context()).Harnesses[pool]
}

func TestUnmeasurablePoolRefreshDoesNotInvokeCLI(t *testing.T) {
	client := quotaClient(t)
	var calls int
	warned := newWarnLog(client)
	sent := map[string]fleetQuotaSent{}
	client.refreshFleetQuota(t.Context(), "grok", sent)
	client.refreshFleetQuota(t.Context(), "grok", sent)
	if calls != 0 {
		t.Fatalf("unmeasurable pool executed an external command %d times", calls)
	}
	lines := warned.all()
	if len(lines) != 1 || !strings.Contains(lines[0], "grok") || !strings.Contains(lines[0], "unmeasurable") {
		t.Fatalf("warning lines = %q", lines)
	}
	if got := poolLogin(t, client, "grok").Login; got == fleet.ReadinessLoginMissing {
		t.Fatalf("login = %q, an unmeasurable pool must not be reported missing", got)
	}
}

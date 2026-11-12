package panewire

import (
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A host with no measurable agy account is not a login failure: its readiness
// login reads unknown, not missing, so the hub raises no login notice for it.
// The refresh still fails with the credentials kind and warns as before (hk 3783).
func TestFleetQuotaAgyWithoutAMeasurableAccountIsNotLoginMissing(t *testing.T) {
	for name, c := range map[string]struct {
		fp         string
		measurable bool
	}{
		"not configured":      {"", false},
		"not measurable here": {agyFPForTest, false},
	} {
		t.Run(name, func(t *testing.T) {
			setAgySwitch(t, true, false)
			hk := newClaudeHK(t)
			client := quotaClient(t)
			client.spawnConfigPath = claudeStore(t, hk.server.URL)
			api := newAgyAPI()
			client.nativeQuota = map[string]*nativeQuotaSource{"agy": agyTestSource(client.spawnConfigPath, agyLoginFiles(agyToken), api, c.fp, c.measurable)}
			client.nodePacelever = (&paceleverRunnerFake{pool: "agy"}).run
			warned := newWarnLog(client)
			client.nativeQuota["agy"].Warn = client.warnMessage

			client.refreshFleetQuota(t.Context(), "agy", map[string]fleetQuotaSent{})

			if got := poolLogin(t, client, "agy").Login; got != fleet.ReadinessLoginUnknown {
				t.Fatalf("login = %q, want unknown", got)
			}
			var credentials bool
			for _, line := range warned.all() {
				if strings.Contains(line, "error_kind credentials") {
					credentials = true
				}
			}
			if !credentials {
				t.Fatalf("warnings %q, want the credentials failure", warned.all())
			}
		})
	}
}

package panewire

import (
	"io"
	"net/http"
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

// agyRefusingAPI answers every request 401, so the agy measurement fails with
// auth_failed: the login really is gone on a host that may measure agy.
type agyRefusingAPI struct{}

func (agyRefusingAPI) Do(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusUnauthorized, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
}

// The guard for the rule above: with a measurable account the same login
// failure still marks the login missing (hk 3783, Do 1(e)).
func TestFleetQuotaAgyMeasurableAuthFailedIsLoginMissing(t *testing.T) {
	setAgySwitch(t, true, true)
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = claudeStore(t, hk.server.URL)
	client.nativeQuota = map[string]*nativeQuotaSource{"agy": agyTestSource(client.spawnConfigPath, agyLoginFiles(agyToken), agyRefusingAPI{}, agyFPForTest, true)}
	warned := newWarnLog(client)
	client.nativeQuota["agy"].Warn = client.warnMessage

	client.refreshFleetQuota(t.Context(), "agy", map[string]fleetQuotaSent{})

	if got := poolLogin(t, client, "agy").Login; got != fleet.ReadinessLoginMissing {
		t.Fatalf("login = %q, want missing", got)
	}
	var authFailed bool
	for _, line := range warned.all() {
		if strings.Contains(line, "error_kind auth_failed") {
			authFailed = true
		}
	}
	if !authFailed {
		t.Fatalf("warnings %q, want the auth_failed failure", warned.all())
	}
}

package panewire

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// fakeQuotaPacelever answers refresh with refresh and --json with status.
func fakeQuotaPacelever(refresh func() ([]byte, error), status string) relayCommandRunner {
	return func(_ context.Context, args ...string) ([]byte, error) {
		if len(args) == 1 && args[0] == "--json" {
			return []byte(status), nil
		}
		return refresh()
	}
}

// quotaStatus is pacelever's status with a failing devin pool of the given
// kind next to a failing claude pool. The node measures claude itself, so the
// pacelever status only ever decides a pool pacelever still measures.
func quotaStatus(kind string) string {
	return `{"providers":[{"id":"claude","status":"error","error_kind":"auth"},{"id":"devin","status":"error","error_kind":"` + kind + `"}]}`
}

// fsPaceleverFreshDevin is a fresh pacelever reading of the devin pool.
var fsPaceleverFreshDevin = strings.Replace(fsPaceleverFresh, `"id":"claude"`, `"id":"devin"`, 1)

func failedRefresh() ([]byte, error) { return nil, errors.New("exit status 2") }

// quotaClient is a readiness client whose published events land in a buffer.
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

// hk 3087: only a pacelever login error kind reads as a missing login; every
// other failed refresh leaves the login unknown.
func TestFleetQuotaRefreshLoginClassification(t *testing.T) {
	for _, test := range []struct {
		name    string
		refresh func() ([]byte, error)
		status  string
		want    string
	}{
		{"timeout", func() ([]byte, error) { return nil, context.DeadlineExceeded }, quotaStatus("auth_failed"), fleet.ReadinessLoginUnknown},
		{"runner error, network", failedRefresh, quotaStatus("network"), fleet.ReadinessLoginUnknown},
		{"excluded", failedRefresh, quotaStatus("excluded"), fleet.ReadinessLoginUnknown},
		{"auth_failed", failedRefresh, quotaStatus("auth_failed"), fleet.ReadinessLoginMissing},
		{"credentials", failedRefresh, quotaStatus("credentials"), fleet.ReadinessLoginMissing},
		{"unparsed refresh, token_expired", func() ([]byte, error) { return []byte("not json"), nil }, quotaStatus("token_expired"), fleet.ReadinessLoginMissing},
		{"unreadable status", failedRefresh, "pacelever: boom", fleet.ReadinessLoginUnknown},
		{"pool not listed", failedRefresh, `{"providers":[{"id":"claude","error_kind":"auth"}]}`, fleet.ReadinessLoginUnknown},
		{"success", func() ([]byte, error) { return []byte(fsPaceleverFreshDevin), nil }, quotaStatus("auth"), fleet.ReadinessLoginPresent},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := quotaClient(t)
			client.nodePacelever = fakeQuotaPacelever(test.refresh, test.status)
			client.refreshFleetQuota(t.Context(), "devin", map[string]fleetQuotaSent{})
			if got := poolLogin(t, client, "devin").Login; got != test.want {
				t.Fatalf("login = %q, want %q", got, test.want)
			}
		})
	}
}

// A refresh that fails with a non-login kind after a success never raises the
// "log in" one-time action, whether the last account is still fresh or not.
func TestFleetQuotaTransientFailureRaisesNoLoginAction(t *testing.T) {
	h := asyncSpawnHub(t)
	client := quotaClient(t)
	now := time.Date(2026, 11, 5, 9, 45, 0, 0, time.UTC)
	client.now = func() time.Time { return now }
	failing := false
	client.nodePacelever = fakeQuotaPacelever(func() ([]byte, error) {
		if failing {
			return failedRefresh()
		}
		return []byte(fsPaceleverFreshDevin), nil
	}, quotaStatus("network"))
	sent := map[string]fleetQuotaSent{}
	report := func() fleet.HarnessReadiness {
		client.refreshFleetQuota(t.Context(), "devin", sent)
		readiness := client.collectReadiness(t.Context())
		h.noteReadinessActions("pi", readiness, now)
		return readiness.Harnesses["devin"]
	}
	if got := report(); got.Login != fleet.ReadinessLoginPresent {
		t.Fatalf("first refresh: %+v", got)
	}
	failing = true
	for _, step := range []struct {
		after time.Duration
		want  string
	}{
		{time.Minute, fleet.ReadinessLoginPresent},
		{6 * time.Minute, fleet.ReadinessLoginUnknown},
	} {
		now = time.Date(2026, 11, 5, 9, 45, 0, 0, time.UTC).Add(step.after)
		if got := report(); got.Login != step.want {
			t.Fatalf("after %s: login = %q, want %q", step.after, got.Login, step.want)
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for key := range h.oneTimeActions {
		if key.need == oneTimeLoginMissing {
			t.Fatalf("a transient refresh failure raised a log-in action: %+v", key)
		}
	}
}

func TestFleetQuotaFailureWarningNamesPoolAndKind(t *testing.T) {
	client := quotaClient(t)
	var warnings []string
	client.warn = func(message string) { warnings = append(warnings, message) }
	client.nodePacelever = fakeQuotaPacelever(func() ([]byte, error) {
		return []byte("secret stderr text"), errors.New("exit status 4")
	}, quotaStatus("excluded"))
	sent := map[string]fleetQuotaSent{}
	client.refreshFleetQuota(t.Context(), "devin", sent)
	client.refreshFleetQuota(t.Context(), "devin", sent)
	if len(warnings) != 1 || !strings.Contains(warnings[0], "devin") || !strings.Contains(warnings[0], "excluded") || strings.Contains(warnings[0], "secret") {
		t.Fatalf("warnings = %q", warnings)
	}
}

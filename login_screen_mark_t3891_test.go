package panewire

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func setT3891SharedReading(t *testing.T, rig *loginProbeRig, measuredAt time.Time) {
	t.Helper()
	reset := rig.clock.Add(2 * time.Hour).UTC().Format(time.RFC3339)
	rig.hk.mu.Lock()
	rig.hk.latest = json.RawMessage(fmt.Sprintf(`{"kind":"auto","measured_at":%q,"buckets":[{"window":"5h","used_pct":10,"resets_at":%q,"scope":"group","model":"3p"}]}`,
		measuredAt.UTC().Format(time.RFC3339), reset))
	rig.hk.mu.Unlock()
}

func markT3891LoginScreen(t *testing.T, client *HubClient) {
	t.Helper()
	client.jobsInboxRoot = t.TempDir()
	launchWithCapture(t, client, "agy-flash38-medium", fleet.ExitSubmitUnconfirmed, agyLoginScreen)
}

func TestLoginScreenMarkMeasurableLifetimeAndOwnMeasurementT3891(t *testing.T) {
	rig := newLoginProbeRig(t, agyLoginFiles(agyToken), true, 0)
	rig.client.nativeQuota["agy"].Now = func() time.Time { return rig.clock }
	rig.client.statePublish = true
	rig.client.stateQuotaPools = []string{"agy"}
	markT3891LoginScreen(t, rig.client)
	rig.clock = rig.clock.Add(nodeLoginScreenTTL + time.Second)
	if got := rig.login(t); got != fleet.ReadinessLoginMissing {
		t.Fatalf("measurable host login after TTL = %q, want missing", got)
	}
	setT3891SharedReading(t, rig, rig.clock)
	rig.round(t) // fresh shared reading: this host did not measure
	if got := rig.login(t); got != fleet.ReadinessLoginMissing {
		t.Fatalf("fresh shared reading cleared login-screen mark: %q", got)
	}
	if got := rig.api.count(); got != 0 {
		t.Fatalf("fresh shared reading invoked this host's measurer %d times", got)
	}
	setT3891SharedReading(t, rig, rig.clock.Add(-6*time.Minute))
	rig.round(t)
	if got := rig.api.count(); got == 0 {
		t.Fatal("stale shared reading did not invoke this host's measurer")
	}
	if got := rig.login(t); got != fleet.ReadinessLoginPresent {
		t.Fatalf("own successful measurement did not clear mark: %q", got)
	}
}

func TestLoginScreenMarkMeasurableButNotPolledExpiresT3891(t *testing.T) {
	client := quotaClient(t)
	client.now = func() time.Time { return claudeTestNow }
	client.nativeQuota = map[string]*nativeQuotaSource{"agy": agyTestSource("", agyLoginFiles(agyToken), newAgyAPI(), agyFPForTest, true)}
	markT3891LoginScreen(t, client)
	client.now = func() time.Time { return claudeTestNow.Add(nodeLoginScreenTTL + time.Second) }
	if got := poolLogin(t, client, "agy").Login; got != fleet.ReadinessLoginUnknown {
		t.Fatalf("measurable but unpolled host login after TTL = %q, want unknown", got)
	}
}

func TestLoginScreenMarkUnmeasurableExpiresT3891(t *testing.T) {
	client := quotaClient(t)
	client.now = func() time.Time { return claudeTestNow }
	client.nativeQuota = map[string]*nativeQuotaSource{"agy": agyTestSource("", agyLoginFiles(""), newAgyAPI(), agyFPForTest, false)}
	markT3891LoginScreen(t, client)
	client.now = func() time.Time { return claudeTestNow.Add(nodeLoginScreenTTL + time.Second) }
	if got := poolLogin(t, client, "agy").Login; got != fleet.ReadinessLoginUnknown {
		t.Fatalf("unmeasurable host login after TTL = %q, want unknown", got)
	}
}

func TestLoginScreenMarkClearedByHubQuotaRefreshT3891(t *testing.T) {
	setAgySwitch(t, true, true)
	rig := newLoginProbeRig(t, agyLoginFiles(agyToken), true, 0)
	rig.client.nativeQuota["agy"].Now = func() time.Time { return rig.clock }
	markT3891LoginScreen(t, rig.client)
	rig.clock = rig.clock.Add(nodeLoginScreenTTL + time.Second)
	setT3891SharedReading(t, rig, rig.clock)
	if _, failure := rig.client.nodeQuotaRefresh(t.Context(), nodeQuotaRefreshArgs{Pool: "agy"}); failure != "" {
		t.Fatalf("quota.refresh failed: %q", failure)
	}
	if got := rig.api.count(); got == 0 {
		t.Fatal("quota.refresh returned the shared reading without checking this host's credential")
	}
	if got := rig.login(t); got != fleet.ReadinessLoginPresent {
		t.Fatalf("hub quota.refresh did not clear and report the measured login-screen mark: %q", got)
	}
}

func TestLoginScreenMarkClearsWhenMeasurementPublishFailsT3891(t *testing.T) {
	setAgySwitch(t, true, true)
	rig := newLoginProbeRig(t, agyLoginFiles(agyToken), true, 0)
	rig.client.nativeQuota["agy"].Now = func() time.Time { return rig.clock }
	markT3891LoginScreen(t, rig.client)
	rig.clock = rig.clock.Add(nodeLoginScreenTTL + time.Second)
	setT3891SharedReading(t, rig, rig.clock.Add(-6*time.Minute))
	rig.hk.mu.Lock()
	rig.hk.failPublish = true
	rig.hk.mu.Unlock()
	if _, failure := rig.client.nodeQuotaRefresh(t.Context(), nodeQuotaRefreshArgs{Pool: "agy"}); failure != "refresh_failed" {
		t.Fatalf("quota.refresh failure = %q, want publish failure", failure)
	}
	if got := rig.login(t); got == fleet.ReadinessLoginMissing {
		t.Fatalf("successful own measurement followed by publish failure kept login mark: %q", got)
	}
}

func TestLoginScreenMarkClearsOnLoopMeasurementWhenPublishFailsT3891(t *testing.T) {
	setAgySwitch(t, true, true)
	rig := newLoginProbeRig(t, agyLoginFiles(agyToken), true, 0)
	rig.client.nativeQuota["agy"].Now = func() time.Time { return rig.clock }
	markT3891LoginScreen(t, rig.client)
	rig.clock = rig.clock.Add(nodeLoginScreenTTL + time.Second)
	setT3891SharedReading(t, rig, rig.clock.Add(-6*time.Minute))
	rig.hk.mu.Lock()
	rig.hk.failPublish = true
	rig.hk.mu.Unlock()
	rig.round(t)
	if got := rig.api.count(); got == 0 {
		t.Fatal("quota loop did not measure this host's credential")
	}
	if got := rig.login(t); got == fleet.ReadinessLoginMissing {
		t.Fatalf("successful own measurement followed by publish failure kept login mark: %q", got)
	}
}

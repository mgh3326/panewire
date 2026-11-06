package fleet

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

// The 2026-11-07 00:12 dry-run (hk:doc evidence/2026-11-07/selection-worker-0012):
// mac-dev held a claude account and had cmd_dsflash41 = true; the commandcode
// account was known to the fleet, but only another host announced it. The
// chosen host's tried list had no cmd-dsflash41 (hk 3320). Once mac-dev
// announces the account too (the node now does, whatever its measurement
// returns), the rung is tried on the chosen host.
func commandcodePresenceDryRun(t *testing.T, macDevAnnounces bool) []string {
	t.Helper()
	rows := []CatalogRow{
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "claude-sonnet-5-5"},
		{Profile: "cmd-dsflash41", Effort: "high", Grade: "A+", Pool: "commandcode", Gate: "default", ModelID: "deepseek/deepseek-v4.1-flash"},
	}
	a, s, q := selFixture(t, "C", rows, nil, nil)
	now := a.Now()
	// The shared readings of the evidence: commandcode's 30d window 10.3%
	// used with 496 h to run (native only: pacelever cannot read 30d), the
	// claude account's 7d window nearly spent.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/quota/accounts/"), "/latest")
		bucket := map[string]any{"window": "7d", "used_pct": 92, "resets_at": now.Add(24 * time.Hour).UTC().Format(time.RFC3339), "scope": "account"}
		if strings.HasPrefix(key, "commandcode:") {
			bucket = map[string]any{"window": "30d", "used_pct": 10.28, "resets_at": now.Add(496 * time.Hour).UTC().Format(time.RFC3339), "scope": "account"}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"observation": map[string]any{"kind": "auto", "measured_at": now.Add(-time.Minute), "buckets": []map[string]any{bucket}}})
	}))
	t.Cleanup(server.Close)
	a.Source = hubObservedSource{a.Source.(*fakePlacementSource), &hkQuota{base: server.URL, token: "t", now: a.Now, freshTTL: a.sharedFreshTTL}}
	src := a.Source.(hubObservedSource).fakePlacementSource
	src.hosts = []HostView{{Name: "mac-dev", Connected: true}, {Name: "desktop", Connected: true}}
	const claudeKey, commandcodeKey = "claude:f0245b3a03eeff12", "commandcode:747042b3498054b9"
	src.presence = []PresenceRecord{
		{HostID: "mac-dev", AccountKey: claudeKey, LastSeen: a.Now()},
		{HostID: "desktop", AccountKey: commandcodeKey, LastSeen: a.Now()},
	}
	if macDevAnnounces {
		src.presence = append(src.presence, PresenceRecord{HostID: "mac-dev", AccountKey: commandcodeKey, LastSeen: a.Now()})
	}
	a.Config.Hosts = map[string]map[string]any{"mac-dev": {"cmd_dsflash41": true}, "desktop": {"cmd_dsflash41": true}}
	a.Config.HostOrder = []string{"mac-dev", "desktop"}
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "cmd-dsflash41": "commandcode"})
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	var tried []string
	for _, h := range sel.record()["hosts"].([]any) {
		row := h.(map[string]any)
		if row["outcome"] != "chosen" {
			continue
		}
		for _, x := range row["tried"].([]any) {
			tried = append(tried, x.(map[string]any)["profile"].(string))
		}
	}
	t.Logf("announces=%v tried=%v", macDevAnnounces, tried)
	return tried
}

func TestSelectDryRunTriesCommandcodeOnTheChosenHostOnceItAnnounces(t *testing.T) {
	if tried := commandcodePresenceDryRun(t, false); len(tried) == 0 || slices.Contains(tried, "cmd-dsflash41") {
		t.Fatalf("without presence from the chosen host the rung is not tried there (the 00:12 shape): %v", tried)
	}
	if tried := commandcodePresenceDryRun(t, true); !slices.Contains(tried, "cmd-dsflash41") {
		t.Fatalf("cmd-dsflash41 must be tried on the chosen host: %v", tried)
	}
}

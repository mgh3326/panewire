package panewire

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const fsConfirmedKey = "codex:fc823b1db7f49cd4"

// fsConfirmedNode is a node whose spawn.json names a handoffkeep env pointing
// at a test server answering the latest shared observation of one key.
func fsConfirmedNode(t *testing.T, measured time.Time, confirmed *time.Time, status int) *HubClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/quota/accounts/"+fsConfirmedKey+"/latest" || r.Header.Get("Authorization") != "Bearer test-token" {
			http.NotFound(w, r)
			return
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		obs := map[string]any{"kind": "auto", "measured_at": measured}
		if confirmed != nil {
			obs["confirmed_at"] = *confirmed
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"observation": obs})
	}))
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	envPath := filepath.Join(dir, "hk.env")
	if err := os.WriteFile(envPath, []byte("HANDOFFKEEP_URL="+srv.URL+"\nHANDOFFKEEP_TOKEN=test-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	spawnPath := filepath.Join(dir, "spawn.json")
	spawn, _ := json.Marshal(map[string]any{"enabled": true, "workspace": "w", "herdr_session": "s", "handoffkeep_env": envPath})
	if err := os.WriteFile(spawnPath, spawn, 0o600); err != nil {
		t.Fatal(err)
	}
	return &HubClient{spawnConfigPath: spawnPath}
}

func TestFleetQuotaConfirmedAt(t *testing.T) {
	measured := time.Now().UTC().Add(-20 * time.Minute).Truncate(time.Millisecond)
	recent := time.Now().UTC().Add(-time.Minute)
	early := measured.Add(-time.Hour)
	for _, tc := range []struct {
		name      string
		obs       time.Time
		confirmed *time.Time
		status    int
		want      bool
	}{
		{name: "same reading re-confirmed", obs: measured, confirmed: &recent, status: http.StatusOK, want: true},
		{name: "store holds a newer reading", obs: measured.Add(5 * time.Minute), confirmed: &recent, status: http.StatusOK},
		{name: "no confirmed_at", obs: measured, status: http.StatusOK},
		{name: "confirmed before measured", obs: measured, confirmed: &early, status: http.StatusOK},
		{name: "store unavailable", obs: measured, confirmed: &recent, status: http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := fsConfirmedNode(t, tc.obs, tc.confirmed, tc.status)
			got := client.fleetQuotaConfirmedAt(context.Background(), fsConfirmedKey, measured)
			if tc.want != !got.IsZero() || tc.want && !got.Equal(recent) {
				t.Fatalf("confirmed_at = %v, want set=%v (%v)", got, tc.want, recent)
			}
		})
	}
	if got := (&HubClient{}).fleetQuotaConfirmedAt(context.Background(), fsConfirmedKey, measured); !got.IsZero() {
		t.Fatalf("no spawn.json still answered %v", got)
	}
}

func TestFleetStatePublishConfirmedAt(t *testing.T) {
	measured := time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)
	quota := map[string]any{"account_key": fsConfirmedKey, "pool": "codex", "buckets": []map[string]any{{"window": "7d", "used_pct": 8, "scope": "account"}}}
	encode := func(kind string, confirmed time.Time) []byte {
		body := map[string]any{"kind": kind, "key": fsConfirmedKey, "measured_at": measured, "confirmed_at": confirmed}
		if kind == fleetStateKindQuota {
			body["quota"] = quota
		}
		raw, _ := json.Marshal(body)
		return raw
	}
	publish, ok := decodeFleetStatePublish(encode(fleetStateKindQuota, measured.Add(15*time.Minute)))
	if !ok || publish.ConfirmedAt == nil || !publish.ConfirmedAt.Equal(measured.Add(15*time.Minute)) {
		t.Fatalf("quota confirmed_at refused or lost: ok=%v %+v", ok, publish)
	}
	if _, ok := decodeFleetStatePublish(encode(fleetStateKindQuota, measured.Add(-time.Minute))); ok {
		t.Fatal("confirmed_at before measured_at accepted")
	}
	// Re-confirmation is a change the hub must re-record, not a duplicate.
	a := FleetStateMessage{Kind: fleetStateKindQuota, MeasuredAt: measured}
	c := measured.Add(10 * time.Minute)
	b := a
	b.ConfirmedAt = &c
	if fleetStateBody(a) == fleetStateBody(b) {
		t.Fatal("a re-confirmed reading compares equal to the unconfirmed one")
	}
}

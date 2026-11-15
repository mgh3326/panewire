package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The fleet status snapshot is the hk 4148 contract read by the console page
// (hk 4149): one document with hosts, quota by host and account, and the plan
// per grade. These tests freeze its shape, its byte bound and its refusal to
// carry a secret, and prove a failed write is logged once and retried.
var writeFleetStatusGolden = flag.Bool("write-fleet-status-golden", false, "rewrite testdata/fleet_status_golden.json")

const (
	fleetStatusTestNow     = "2026-11-15T12:00:00Z"
	fleetStatusTestAccount = "claude:0123456789abcdef"
)

// fleetStatusTestHub builds a hub with two known hosts, one lane-capped route
// file and one stored quota reading, all at a fixed clock.
func fleetStatusTestHub(t *testing.T, now time.Time, logs io.Writer) *HubServer {
	t.Helper()
	// The golden band comes from the built-in pool order; a developer's policy
	// path must not change it.
	t.Setenv("PANEWIRE_POOL_POLICY", "")
	config := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(config, nil, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PANEWIRE_CONFIG", config)
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": "operator-token", "node-a": "node-a-token", "node-b": "node-b-token"},
		ReportRelayPath: r20LanesFile(t, `{"lanes":{"lane-a1":{"machine":"node-a","pane":"w1:p1"},"lane-a2":{"machine":"node-a","pane":"w1:p2"},"lane-b1":{"machine":"node-b","pane":"w2:p1"}}}`),
		LanesNodeRowCap: 4,
		Now:             func() time.Time { return now },
		Logger:          slog.New(slog.NewTextHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	hub.mu.Lock()
	hub.nodes["node-a"] = &hubNodeRecord{machineID: "node-a", state: "connected", accepting: true, remoteMeta: map[string]string{"version": "panewire-r13"}, lastPing: now}
	hub.nodes["node-b"] = &hubNodeRecord{machineID: "node-b", state: "stale", remoteMeta: map[string]string{"version": "panewire-r13"}}
	hub.mu.Unlock()
	hub.stateStore().publish(FleetStateMessage{
		Host: "node-a", Kind: fleetStateKindQuota, Key: fleetStatusTestAccount,
		MeasuredAt: now.Add(-2 * time.Minute), ReceivedAt: now.Add(-2 * time.Minute),
		Quota: &FleetStateQuota{AccountKey: fleetStatusTestAccount, Pool: "claude", Buckets: []FleetStateBucket{
			{Window: "5h", UsedPct: new(float64(41)), ResetsAt: now.Add(3 * time.Hour).Format(time.RFC3339), Scope: "account"},
			{Window: "7d", UsedPct: new(float64(12)), ResetsAt: now.Add(4 * 24 * time.Hour).Format(time.RFC3339), Scope: "account"},
		}},
	}, time.Minute)
	hub.stateStore().publish(FleetStateMessage{
		Host: "node-a", Kind: fleetStateKindPublisher, ReceivedAt: now,
		Publisher: &FleetStatePublisher{Connected: true, LastSeen: now},
	}, 0)
	hub.fleetStatusPlanFn = func(context.Context) []fleet.StatusPlan {
		return []fleet.StatusPlan{
			{Grade: "C", T: "T1", Seat: "omp-dsflash41", Host: "node-a", Pool: "commandcode", AccountKey: "commandcode:747042b3498054b9", Tier: "ranked", Outcome: "ADMITTED"},
			{Grade: "A", T: "T1", Outcome: "NO_ELIGIBLE_HOST"},
		}
	}
	return hub
}

func fleetStatusGoldenPath() string { return filepath.Join("testdata", "fleet_status_golden.json") }

func TestFleetStatusGoldenDocument(t *testing.T) {
	now, err := time.Parse(time.RFC3339, fleetStatusTestNow)
	if err != nil {
		t.Fatal(err)
	}
	hub := fleetStatusTestHub(t, now, io.Discard)
	body, err := hub.fleetStatusDocument(context.Background(), now).body()
	if err != nil {
		t.Fatal(err)
	}
	if *writeFleetStatusGolden {
		if err := os.WriteFile(fleetStatusGoldenPath(), []byte(body+"\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(fleetStatusGoldenPath())
	if err != nil {
		t.Fatalf("%v (run go test -run TestFleetStatusGoldenDocument -write-fleet-status-golden .)", err)
	}
	if got, want := body, strings.TrimSuffix(string(want), "\n"); got != want {
		t.Fatalf("fleet status document differs from golden\n--- got\n%s\n--- want\n%s", got, want)
	}
}

// The document's keys are a closed list: nothing the page would render can
// arrive under an unreviewed name, and no account value is an email or a
// credential-shaped string.
func TestFleetStatusDocumentCarriesNoSecrets(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, fleetStatusTestNow)
	hub := fleetStatusTestHub(t, now, io.Discard)
	body, err := hub.fleetStatusDocument(context.Background(), now).body()
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	want := map[string]map[string]bool{
		"":       {"v": true, "at": true, "hosts": true, "quota": true, "plan": true},
		"host":   {"host": true, "connected": true, "version": true, "accepting": true, "lanes_used": true, "lanes_cap": true},
		"quota":  {"host": true, "pool": true, "account": true, "windows": true, "measured_at": true, "age_s": true, "fresh_ttl_s": true, "fresh": true, "free_state": true},
		"window": {"window": true, "used_pct": true, "resets_at": true},
		"plan":   {"grade": true, "t": true, "seat": true, "host": true, "pool": true, "account": true, "tier": true, "outcome": true},
	}
	checkKeys(t, "", doc, want)
	for _, value := range []string{"hosts", "quota", "plan"} {
		for _, row := range doc[value].([]any) {
			checkKeys(t, strings.TrimSuffix(value, "s"), row.(map[string]any), want)
			if value == "quota" {
				checkKeys(t, "window", row.(map[string]any)["windows"].([]any)[0].(map[string]any), want)
			}
		}
	}
	email := regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`)
	if email.MatchString(body) {
		t.Fatal("fleet status document carries an email-shaped value")
	}
	for _, row := range doc["quota"].([]any) {
		account := row.(map[string]any)["account"].(string)
		if !fleetStateAccountKeyPattern.MatchString(account) {
			t.Fatalf("quota account %q is not a short fingerprint key", account)
		}
	}
}

func checkKeys(t *testing.T, label string, row map[string]any, want map[string]map[string]bool) {
	t.Helper()
	allowed, ok := want[label]
	if !ok {
		t.Fatalf("no key list for %q", label)
	}
	for key := range row {
		if !allowed[key] {
			t.Fatalf("%s row carries unreviewed key %q: %v", label, key, row)
		}
	}
}

// A fleet larger than the per-list caps, or a value grown unexpectedly, still
// yields one body under the byte cap.
func TestFleetStatusDocumentBounded(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, fleetStatusTestNow)
	hub := fleetStatusTestHub(t, now, io.Discard)
	hub.mu.Lock()
	for i := range 200 {
		host := fmt.Sprintf("node-%03d", i)
		hub.nodes[host] = &hubNodeRecord{machineID: host, state: "connected", remoteMeta: map[string]string{"version": "panewire-r13"}, lastPing: now}
	}
	hub.mu.Unlock()
	for i := range 200 {
		account := fmt.Sprintf("claude:%016x", i+1)
		hub.stateStore().publish(FleetStateMessage{
			Host: fmt.Sprintf("node-%03d", i%4), Kind: fleetStateKindQuota, Key: account,
			MeasuredAt: now, ReceivedAt: now,
			Quota: &FleetStateQuota{AccountKey: account, Pool: "claude", Buckets: []FleetStateBucket{
				{Window: "5h", UsedPct: new(float64(i % 100)), ResetsAt: now.Add(time.Hour).Format(time.RFC3339)},
			}},
		}, 0)
	}
	doc := hub.fleetStatusDocument(context.Background(), now)
	if len(doc.Hosts) > fleetStatusMaxHosts || len(doc.Quota) > fleetStatusMaxQuota {
		t.Fatalf("document caps not applied: hosts=%d quota=%d", len(doc.Hosts), len(doc.Quota))
	}
	body, err := doc.body()
	if err != nil {
		t.Fatal(err)
	}
	if len(body) > fleetStatusMaxBytes {
		t.Fatalf("body %d bytes, want <= %d", len(body), fleetStatusMaxBytes)
	}
	if !json.Valid([]byte(body)) {
		t.Fatal("bounded body is not valid JSON")
	}

	// A single page unfittable under the cap is dropped entry by entry rather
	// than written over the bound.
	huge := fleetStatusDoc{V: 1, At: now}
	for i := range fleetStatusMaxQuota {
		huge.Quota = append(huge.Quota, fleetStatusQuota{
			Host: "node-a", Pool: "claude", Account: fmt.Sprintf("claude:%016x", i+1),
			Windows: []fleetStatusWindow{{Window: "5h", UsedPct: new(float64(1)), ResetsAt: strings.Repeat("x", 4096)}},
		})
	}
	body, err = huge.body()
	if err != nil {
		t.Fatal(err)
	}
	if len(body) > fleetStatusMaxBytes {
		t.Fatalf("body %d bytes, want <= %d", len(body), fleetStatusMaxBytes)
	}
}

// A quota row is measured facts, not a selection band: one account can be free
// for its free rungs and ranked for its paid rungs at once. The free tier is
// normalized as placement judges it; the plan row carries the selector's band.
func TestFleetStatusQuotaFreeStateNormalized(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, fleetStatusTestNow)
	hub := fleetStatusTestHub(t, now, io.Discard)
	publish := func(key, state string, reset *time.Time) {
		hub.stateStore().publish(FleetStateMessage{
			Host: "node-a", Kind: fleetStateKindQuota, Key: key,
			MeasuredAt: now, ReceivedAt: now,
			Quota: &FleetStateQuota{AccountKey: key, Pool: "devin", Free: &FleetStateFree{State: state, ResetsAt: reset}, Buckets: []FleetStateBucket{
				{Window: "5h", UsedPct: new(float64(10)), ResetsAt: now.Add(3 * time.Hour).Format(time.RFC3339), Scope: "account"},
			}},
		}, 0)
	}
	future, past := now.Add(time.Hour), now.Add(-time.Hour)
	available, exhausted, expired, malformed := "devin:0000000000000002", "devin:0000000000000003", "devin:0000000000000004", "devin:0000000000000005"
	publish(available, "available", nil)
	publish(exhausted, "exhausted", &future)
	publish(expired, "exhausted", &past)
	publish(malformed, "", nil)
	want := map[string]string{available: "available", exhausted: "exhausted", expired: "available", malformed: "unknown"}
	for _, row := range hub.fleetStatusQuota(now, nil, nil) {
		if expect, ok := want[row.Account]; ok && row.FreeState != expect {
			t.Errorf("%s free_state = %q, want %q", row.Account, row.FreeState, expect)
		}
	}
}

// Fresh is the spawn gate's whole rule: the pool's configured window and a
// live publisher. A dead publisher makes the same reading stale.
func TestFleetStatusFreshUsesPublisherAndTTL(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, fleetStatusTestNow)
	hub := fleetStatusTestHub(t, now, io.Discard)
	app := &fleet.App{Config: fleet.Config{Quota: map[string]any{
		"pool_fresh_ttl_s": map[string]any{"claude": int64(700)},
	}}}
	snap, err := (fleetStatusState{hub}).Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rows := hub.fleetStatusQuota(now, app, &snap)
	if len(rows) != 1 || rows[0].FreshTTLS != 700 || !rows[0].Fresh {
		t.Fatalf("rows = %+v, want fresh, fresh_ttl_s 700, live publisher", rows)
	}
	hub.stateStore().publish(FleetStateMessage{
		Host: "node-a", Kind: fleetStateKindPublisher, ReceivedAt: now,
		Publisher: &FleetStatePublisher{Connected: false, LastSeen: now},
	}, 0)
	if snap, err = (fleetStatusState{hub}).Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rows = hub.fleetStatusQuota(now, app, &snap); len(rows) != 1 || rows[0].Fresh {
		t.Fatalf("rows = %+v, want stale with a dead publisher", rows)
	}
}

// A failed write is logged once per cycle and the next cycle writes again; the
// retry is the ticker, never a tighter loop.
func TestFleetStatusWriteFailureLoggedOnceAndRetried(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, fleetStatusTestNow)
	var logs strings.Builder
	hub := fleetStatusTestHub(t, now, &logs)
	attempts := 0
	var written []string
	hub.fleetStatusPublish = func(_ context.Context, body string) error {
		attempts++
		written = append(written, body)
		if attempts == 1 {
			return errors.New("hk unreachable")
		}
		return nil
	}
	hub.publishFleetStatus(context.Background())
	hub.publishFleetStatus(context.Background())
	if attempts != 2 {
		t.Fatalf("write attempts %d, want 2 (one per cycle)", attempts)
	}
	if len(written) != 2 || written[0] == "" || written[1] == "" {
		t.Fatalf("writes = %d, want two bodies", len(written))
	}
	if got := strings.Count(logs.String(), "fleet status snapshot not written"); got != 1 {
		t.Fatalf("failure logged %d times, want once:\n%s", got, logs.String())
	}
	if !strings.Contains(logs.String(), "hk unreachable") {
		t.Fatalf("failure log lacks the cause:\n%s", logs.String())
	}
}

package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type fakeStateReader struct {
	snap  FleetSnapshot
	err   error
	calls int
}

func (f *fakeStateReader) Snapshot(context.Context) (FleetSnapshot, error) {
	f.calls++
	return f.snap, f.err
}

// fsSnapshot builds a GET /v1/state body the way the hub encodes it.
func fsSnapshot(t *testing.T, at time.Time, states ...map[string]any) FleetSnapshot {
	t.Helper()
	data, err := json.Marshal(map[string]any{"v": 1, "epoch": "e", "seq": len(states), "at": at, "states": states})
	if err != nil {
		t.Fatal(err)
	}
	var snap FleetSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		t.Fatal(err)
	}
	return snap
}

func fsPublisher(host string, connected bool, lastSeen time.Time) map[string]any {
	return map[string]any{"host": host, "kind": "publisher", "key": "", "measured_at": lastSeen, "publisher": map[string]any{"connected": connected, "last_seen": lastSeen}}
}

func fsQuota(host, key string, measured time.Time, used float64) map[string]any {
	return map[string]any{"host": host, "kind": "quota", "key": key, "measured_at": measured, "quota": map[string]any{
		"account_key": key, "pool": "claude", "source": "usage-api",
		"buckets": []map[string]any{{"window": "5h", "used_pct": used, "resets_at": measured.Add(3 * time.Hour).Format(time.RFC3339), "scope": "account"}},
	}}
}

func fsLoad(host string, connected bool, score, ratio float64, measured time.Time) map[string]any {
	return map[string]any{"host": host, "kind": "load", "key": "", "measured_at": measured, "load": map[string]any{"connected": connected, "accepting": connected, "score": score, "load_ratio": ratio, "capacity_weight": 1, "reason": "available"}}
}

// fsMeasurements counts per-spawn quota measurements: a refresh run locally or
// over SSH on a target (pacelever refresh, or panewire quota refresh for claude).
func fsMeasurements(f *fakeRunner) int {
	n := 0
	for _, c := range f.Calls {
		args := strings.Join(c.Args, " ")
		if (c.Name == "ssh" && isQuotaRefresh(args)) || ((c.Name == "pacelever" || strings.HasSuffix(c.Name, "/pacelever")) && strings.HasPrefix(args, "refresh")) || (c.Name == "panewire" && strings.HasPrefix(args, "quota refresh")) {
			n++
		}
	}
	return n
}

type fsRecord struct {
	State struct {
		Hosts StateUse            `json:"hosts"`
		Quota map[string]StateUse `json:"quota"`
	} `json:"state"`
	Candidates []struct {
		Host   string        `json:"host"`
		Chosen bool          `json:"chosen"`
		Gate   quotaJudgment `json:"quota_gate"`
	} `json:"candidates"`
}

func fsDecodeRecord(t *testing.T, rec map[string]any) fsRecord {
	t.Helper()
	data, _ := json.Marshal(rec)
	var out fsRecord
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// fsStaleAccountFixture is the 2026-11-01 setup: the spawn targets host-b,
// whose account is measured only by host-a, and the shared quota cache holds
// a reading older than fresh_ttl_s. A per-spawn measurement on host-b
// succeeds and freshens the cache, so a fallback is visible as one call.
func fsStaleAccountFixture(t *testing.T, reader *fakeStateReader) (*App, Spawn, *fakeRunner, *fakePlacementSource) {
	t.Helper()
	const key = "claude:0123456789abcdef"
	src := &fakePlacementSource{hosts: []HostView{{Name: "host-b", Connected: true, LoadRatio: ptrF(.1)}}}
	var a *App
	a, s, f := remoteFixture(t, src, &fakeWaker{}, "2026-01-01T00:00:00Z", func(args string) Result {
		switch {
		case isQuotaRefresh(args):
			src.accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: true, MeasuredAt: a.Now()}
			return Result{}
		case strings.Contains(args, "_spawn-stdin"):
			return Result{Out: "OK pane=w:p1 job=job-a\n"}
		}
		return Result{}
	})
	a.Config.Hosts = map[string]map[string]any{"host-b": {}}
	a.Config.HostOrder = []string{"host-b"}
	a.Config.Quota = map[string]any{"fresh_ttl_s": int64(1800)}
	a.Config.Hub = map[string]any{"hub_url": "https://hub.invalid"}
	src.accounts[key] = SharedAccount{Key: key, Observed: true, MeasuredAt: a.Now().Add(-40 * time.Minute)}
	a.State = reader
	a.Source = stateSource{a: a, inner: src}
	return a, s, f, src
}

// Acceptance 2: the reading host-a published is used for a spawn on host-b,
// which shares the account, with no per-spawn measurement — the stale
// refusal of 2026-11-01 does not happen.
func TestFleetStateHubReadingAdmitsSharedAccountWithoutMeasuring(t *testing.T) {
	const key = "claude:0123456789abcdef"
	reader := &fakeStateReader{}
	a, s, f, _ := fsStaleAccountFixture(t, reader)
	now := a.Now()
	reader.snap = fsSnapshot(t, now,
		fsPublisher("host-a", true, now.Add(-5*time.Second)),
		fsPublisher("host-b", true, now.Add(-5*time.Second)),
		fsQuota("host-a", key, now.Add(-20*time.Minute), 30),
		fsLoad("host-b", true, 1.8, .1, now.Add(-10*time.Second)),
	)
	handled, rec, err := a.placeAuto(context.Background(), s, "brief")
	if err != nil || !handled || rec["chose"] != "host-b" {
		t.Fatalf("handled=%v err=%v record=%v", handled, err, rec)
	}
	if n := fsMeasurements(f); n != 0 {
		t.Fatalf("per-spawn measurement calls = %d, want 0", n)
	}
	a.attachSelection(rec, s)
	got := fsDecodeRecord(t, rec)
	if got.State.Quota[key].Source != stateSourceHub || got.State.Hosts.Source != stateSourceHub {
		t.Fatalf("record state = %+v", got.State)
	}
	for _, c := range got.Candidates {
		if c.Host == "host-b" && (c.Gate.State == nil || c.Gate.State.Source != stateSourceHub || c.Gate.Measured != "" || c.Gate.Outcome != "admitted") {
			t.Fatalf("host-b gate = %+v", c.Gate)
		}
	}
	if reader.calls != 1 {
		t.Fatalf("snapshot read %d times in one placement, want 1", reader.calls)
	}
}

// Acceptance 3: a dead publisher heartbeat falls back to direct measurement
// and records source=direct; an alive heartbeat with unchanged (older but
// in-TTL) data does not fall back.
func TestFleetStatePublisherHeartbeatDecidesFallback(t *testing.T) {
	const key = "claude:0123456789abcdef"
	for _, tc := range []struct {
		name         string
		connected    bool
		lastSeen     time.Duration
		measured     time.Duration
		err          error
		hubOff       bool
		source       string
		reason       string
		measurements int
	}{
		{name: "alive, unchanged for 25 minutes", connected: true, lastSeen: 10 * time.Second, measured: 25 * time.Minute, source: stateSourceHub},
		{name: "publisher disconnected", connected: false, lastSeen: 10 * time.Second, measured: time.Minute, source: stateSourceDirect, reason: statePublisherDead, measurements: 1},
		{name: "publisher heartbeat stale", connected: true, lastSeen: 5 * time.Minute, measured: time.Minute, source: stateSourceDirect, reason: statePublisherDead, measurements: 1},
		{name: "alive but reading past the TTL", connected: true, lastSeen: 10 * time.Second, measured: 31 * time.Minute, source: stateSourceDirect, reason: stateStale, measurements: 1},
		{name: "hub unreachable", err: &HubError{Kind: hubDown}, source: stateSourceDirect, reason: stateUnavailable, measurements: 1},
		{name: "hub state off", hubOff: true, connected: true, lastSeen: 10 * time.Second, measured: time.Minute, source: stateSourceDirect, reason: stateOff, measurements: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &fakeStateReader{err: tc.err}
			a, s, f, _ := fsStaleAccountFixture(t, reader)
			if tc.hubOff {
				a.Config.Hub["state"] = "off"
			}
			now := a.Now()
			reader.snap = fsSnapshot(t, now,
				fsPublisher("host-a", tc.connected, now.Add(-tc.lastSeen)),
				fsQuota("host-a", key, now.Add(-tc.measured), 30),
			)
			handled, rec, err := a.placeAuto(context.Background(), s, "brief")
			if err != nil || !handled || rec["chose"] != "host-b" {
				t.Fatalf("handled=%v err=%v record=%v", handled, err, rec)
			}
			if n := fsMeasurements(f); n != tc.measurements {
				t.Fatalf("measurements = %d, want %d", n, tc.measurements)
			}
			a.attachSelection(rec, s)
			got := fsDecodeRecord(t, rec)
			if use := got.State.Quota[key]; use.Source != tc.source || use.Reason != tc.reason {
				t.Fatalf("record state.quota = %+v, want source=%s reason=%s", use, tc.source, tc.reason)
			}
			for _, c := range got.Candidates {
				if c.Host != "host-b" {
					continue
				}
				if c.Gate.State == nil || c.Gate.State.Source != tc.source {
					t.Fatalf("host-b gate state = %+v", c.Gate.State)
				}
				if wantMeasured := map[bool]string{true: "ok", false: ""}[tc.measurements > 0]; c.Gate.Measured != wantMeasured {
					t.Fatalf("host-b gate measured=%q want %q", c.Gate.Measured, wantMeasured)
				}
			}
		})
	}
}

// The host list comes from the hub only while every placement host is
// connected with a live publisher; a sleeping host sends placement back to
// `panewire place`, which is what triggers the hub's wake-on-spill.
func TestFleetStateHostsFallBackWhenAHostIsDown(t *testing.T) {
	a := testApp(t)
	a.Config.Hub = map[string]any{"hub_url": "https://hub.invalid"}
	now := a.Now()
	direct := &fakePlacementSource{hosts: []HostView{{Name: "direct-host", Connected: true}}}
	reader := &fakeStateReader{}
	a.State = reader
	a.Source = stateSource{a: a, inner: direct}

	reader.snap = fsSnapshot(t, now,
		fsPublisher("host-a", true, now), fsPublisher("host-b", true, now),
		fsLoad("host-a", true, 1.2, .3, now), fsLoad("host-b", true, 2.5, 0, now),
	)
	views, err := a.Source.Hosts(context.Background(), "/repo")
	if err != nil || len(views) != 2 || views[0].Name != "host-b" || *views[0].Score != 2.5 || views[0].LoadRatio != nil || *views[1].LoadRatio != .3 {
		t.Fatalf("hub hosts = %+v err=%v", views, err)
	}
	if a.fleetLog.hosts.Source != stateSourceHub {
		t.Fatalf("hosts source = %+v", a.fleetLog.hosts)
	}

	a.fleetLog = nil
	reader.snap = fsSnapshot(t, now,
		fsPublisher("host-a", true, now), fsPublisher("host-b", false, now.Add(-time.Hour)),
		fsLoad("host-a", true, 1.2, .3, now), fsLoad("host-b", false, -1000, 0, now),
	)
	views, err = a.Source.Hosts(context.Background(), "/repo")
	if err != nil || len(views) != 1 || views[0].Name != "direct-host" {
		t.Fatalf("fallback hosts = %+v err=%v", views, err)
	}
	if use := a.fleetLog.hosts; use.Source != stateSourceDirect || use.Reason != statePublisherDead {
		t.Fatalf("fallback hosts source = %+v", use)
	}
}

func TestFleetStateSnapshotIsReusedBriefly(t *testing.T) {
	a := testApp(t)
	a.Config.Hub = map[string]any{"hub_url": "https://hub.invalid"}
	clock := a.Now()
	a.Now = func() time.Time { return clock }
	reader := &fakeStateReader{snap: fsSnapshot(t, clock)}
	a.State = reader
	a.fleetSnapshot(context.Background())
	a.fleetSnapshot(context.Background())
	clock = clock.Add(stateSnapshotMaxAge)
	a.fleetSnapshot(context.Background())
	if reader.calls != 2 {
		t.Fatalf("snapshot reads = %d, want 2", reader.calls)
	}
}

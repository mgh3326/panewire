package fleet

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// burnObs is an hk observations answer: used holds the account-scope used
// share of the named window at one-hour steps, the last point at at.
func burnObs(t *testing.T, at time.Time, window string, used ...float64) string {
	t.Helper()
	var out []map[string]any
	for i, u := range used {
		out = append(out, map[string]any{
			"id":          i + 1,
			"kind":        "auto",
			"measured_at": at.Add(time.Duration(i-len(used)+1) * time.Hour).Format(time.RFC3339),
			"buckets":     []map[string]any{{"window": window, "used_pct": u, "scope": "account"}},
		})
	}
	b, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// serveBurnObs answers every quota observation read with body.
func serveBurnObs(t *testing.T, hk *spawnHK, body string) {
	t.Helper()
	hk.Handle(func(r hkRequest) (int, string, bool) {
		if r.Method == http.MethodGet && strings.Contains(r.Path, "/observations") {
			return http.StatusOK, body, true
		}
		return 0, "", false
	})
}

// burnPresence is a presence snapshot binding one account key.
func burnPresence(key string, now time.Time) *selPresence {
	return &selPresence{now: now, presence: []PresenceRecord{{HostID: "local", AccountKey: key, LastSeen: now}}, burn: map[string]burnRates{}}
}

func TestBurnHistorySteadySeriesMeasuresTheRise(t *testing.T) {
	hk := useFakeHK(t)
	a := testApp(t)
	now := a.Now()
	serveBurnObs(t, hk, burnObs(t, now, "5h", 0, 10, 20))
	sp := burnPresence("codex:0000000000000001", now)
	sp.a = a

	r := sp.burnRates(context.Background(), "codex")
	if got, want := r.Windows["5h"], 10.0; got != want {
		t.Fatalf("measured burn = %v; want %v (series 0,10,20 one hour apart)", got, want)
	}
	if r.Reason != "" {
		t.Fatalf("reason = %q on a measured series; want none", r.Reason)
	}
	rate := r.rate(5*time.Hour, PoolPolicy{})
	if rate.source != burnSourceMeasured || rate.pctPerH != 10 || rate.reason != "" {
		t.Fatalf("rate = %+v; want the measured 10 %%/h", rate)
	}
	fit := PoolPolicy{}.headroomFor(&windowRoom{usedPct: 10, window: 5 * time.Hour}, 6*time.Hour, &rate)
	if !fit.fits || fit.burnPctPerH != 10 || fit.burnSource != burnSourceMeasured || fit.burnReason != "" || fit.burnWindow != "5h" {
		t.Fatalf("fit = %+v; want 10 %%/h measured from the 5h window and fitting", fit)
	}
}

func TestBurnHistoryResetIsNotNegativeBurn(t *testing.T) {
	hk := useFakeHK(t)
	a := testApp(t)
	now := a.Now()
	// 0 -> 10 -> 5 (the 5h window reset) -> 15: two counted hours of a 10
	// point rise each. Counting the drop would give (10-5+10)/3h = 5 %/h.
	serveBurnObs(t, hk, burnObs(t, now, "5h", 0, 10, 5, 15))
	sp := burnPresence("codex:0000000000000001", now)
	sp.a = a

	r := sp.burnRates(context.Background(), "codex")
	if got, want := r.Windows["5h"], 10.0; got != want {
		t.Fatalf("measured burn = %v; want %v: a reset is not negative burn", got, want)
	}
}

func TestBurnHistoryOnePointKeepsTheDefaultAndSaysWhy(t *testing.T) {
	hk := useFakeHK(t)
	a := testApp(t)
	now := a.Now()
	serveBurnObs(t, hk, burnObs(t, now, "5h", 42))
	sp := burnPresence("claude:0000000000000002", now)
	sp.a = a

	r := sp.burnRates(context.Background(), "claude")
	if len(r.Windows) != 0 || r.Reason != burnReasonTooFewPoints {
		t.Fatalf("rates = %+v; want the defaults with reason %q", r, burnReasonTooFewPoints)
	}
	rate := r.rate(5*time.Hour, PoolPolicy{})
	if rate.source != burnSourceDefault || rate.pctPerH != 15 || rate.reason != burnReasonTooFewPoints {
		t.Fatalf("rate = %+v; want the default 15 %%/h with reason %q", rate, burnReasonTooFewPoints)
	}
	fit := PoolPolicy{}.headroomFor(&windowRoom{usedPct: 10, window: 5 * time.Hour}, 6*time.Hour, &rate)
	if fit.burnSource != burnSourceDefault || fit.burnReason != burnReasonTooFewPoints || fit.burnPctPerH != 15 {
		t.Fatalf("fit = %+v; want the default 15 %%/h and the reason recorded", fit)
	}
}

func TestBurnHistoryUnreadableKeepsTheDefaultAndSaysWhy(t *testing.T) {
	hk := useFakeHK(t)
	a := testApp(t)
	now := a.Now()
	hk.Handle(func(r hkRequest) (int, string, bool) {
		if r.Method == http.MethodGet && strings.Contains(r.Path, "/observations") {
			return http.StatusInternalServerError, `{"error":"internal"}`, true
		}
		return 0, "", false
	})
	sp := burnPresence("claude:0000000000000002", now)
	sp.a = a

	r := sp.burnRates(context.Background(), "claude")
	if len(r.Windows) != 0 || r.Reason != burnReasonUnreadable {
		t.Fatalf("rates = %+v; want the defaults with reason %q", r, burnReasonUnreadable)
	}
	rate := r.rate(168*time.Hour, PoolPolicy{})
	if rate.source != burnSourceDefault || rate.pctPerH != 2 || rate.reason != burnReasonUnreadable {
		t.Fatalf("7d rate = %+v; want the default 2 %%/h with reason %q", rate, burnReasonUnreadable)
	}
}

func TestBurnHistoryCacheSparesTheNextSpawn(t *testing.T) {
	hk := useFakeHK(t)
	a := testApp(t)
	now := a.Now()
	serveBurnObs(t, hk, burnObs(t, now, "5h", 0, 10, 20))

	first := burnPresence("codex:0000000000000001", now)
	first.a = a
	if r := first.burnRates(context.Background(), "codex"); r.Windows["5h"] != 10 {
		t.Fatalf("first read = %+v; want a measured 10 %%/h", r)
	}
	if got := len(hkRoutes(hk, "/observations")); got != 1 {
		t.Fatalf("observation reads = %d; want 1", got)
	}
	// A later spawn inside the TTL reads the cache, not hk.
	second := burnPresence("codex:0000000000000001", now.Add(time.Minute))
	second.a = a
	if r := second.burnRates(context.Background(), "codex"); r.Windows["5h"] != 10 {
		t.Fatalf("cached read = %+v; want the measured 10 %%/h", r)
	}
	if got := len(hkRoutes(hk, "/observations")); got != 1 {
		t.Fatalf("observation reads after a second spawn = %d; want 1 (the cache answers)", got)
	}
	// Past the TTL the history is read again.
	third := burnPresence("codex:0000000000000001", now.Add(burnCacheTTL+time.Minute))
	third.a = a
	if r := third.burnRates(context.Background(), "codex"); r.Windows["5h"] != 10 {
		t.Fatalf("refreshed read = %+v; want the measured 10 %%/h", r)
	}
	if got := len(hkRoutes(hk, "/observations")); got != 2 {
		t.Fatalf("observation reads after the TTL = %d; want 2", got)
	}
}

// hkRoutes counts the fake hk's requests whose path contains part.
func hkRoutes(hk *spawnHK, part string) []string {
	var out []string
	for _, route := range hk.Routes() {
		if strings.Contains(route, part) {
			out = append(out, route)
		}
	}
	return out
}

func TestSelectRecordNamesTheBurnRateAndItsSource(t *testing.T) {
	for _, tc := range []struct {
		name   string
		series string
		source string
		reason string
		window string
		pct    float64
	}{
		{"measured", "steady", burnSourceMeasured, "", "5h", 10},
		{"one point", "single", burnSourceDefault, burnReasonTooFewPoints, "", 15},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hk := useFakeHK(t)
			rows := []CatalogRow{
				{Profile: "codex-luna", Effort: "high", Grade: "B", Pool: "codex", Gate: "default", ModelID: "codex-luna"},
			}
			a, s, q := selFixture(t, "B", rows, map[string]PoolView{
				"codex": {Status: "ok", Class: "spend", Blocking: ptrF(10), Shortest: &windowRoom{usedPct: 10, window: 5 * time.Hour}},
			}, []string{"codex"})
			q.GateDo = gatePools(map[string]string{"codex-luna": "codex"})
			now := a.Now()
			switch tc.series {
			case "steady":
				serveBurnObs(t, hk, burnObs(t, now, "5h", 0, 10, 20))
			case "single":
				serveBurnObs(t, hk, burnObs(t, now, "5h", 42))
			}
			hk.SetTask("42", `{"id":42,"title":"work (grade B, T1)","state":"ready","expected":"6h","refs":{}}`)
			sel, _ := a.selectProfile(context.Background(), s)
			if sel == nil {
				t.Fatal("no plan")
			}
			row := candOutcome(sel.record(), "codex-luna", "high")
			h, ok := row["headroom"].(map[string]any)
			if !ok {
				t.Fatalf("candidate has no headroom record: %v", row)
			}
			gotReason, _ := h["burn_reason"].(string)
			gotWindow, _ := h["burn_window"].(string)
			if h["burn_source"] != tc.source || h["burn_pct_per_h"] != tc.pct || gotReason != tc.reason || gotWindow != tc.window {
				t.Fatalf("headroom = %v; want source %q, %v %%/h, reason %q, window %q", h, tc.source, tc.pct, tc.reason, tc.window)
			}
			if tc.source == burnSourceDefault && gotReason == "" {
				t.Fatalf("headroom = %v; a default must say why", h)
			}
		})
	}
}

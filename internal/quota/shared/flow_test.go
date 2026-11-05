package shared

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

const (
	testKey   = "claude:0123456789abcdef"
	testToken = "hk-token-must-not-leak"
)

var testNow = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

// fakeHK is handoffkeep's shared quota API for one account, with every call
// recorded in order. A test steers it through the hooks.
type fakeHK struct {
	t      *testing.T
	server *httptest.Server

	mu       sync.Mutex
	calls    []string
	latest   *Observation
	holder   string
	expires  time.Time
	pub      []map[string]any
	presence []Presence
	// onLatest runs before each latest answer, with the 1-based call number.
	onLatest func(h *fakeHK, n int)
	// onLease overrides lease handling for the 1-based call number: it returns
	// the lease to answer, or ok=false for the default.
	onLease func(h *fakeHK, n int, holder string) (Lease, bool)
	// failPublish makes the observation PUT fail with 500.
	failPublish bool
	latestCalls int
	leaseCalls  int
}

func newFakeHK(t *testing.T) *fakeHK {
	h := &fakeHK{t: t}
	h.server = httptest.NewServer(http.HandlerFunc(h.serve))
	t.Cleanup(h.server.Close)
	return h
}

func (h *fakeHK) client() Client {
	return Client{BaseURL: h.server.URL, Auth: func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+testToken) }}
}

func (h *fakeHK) serve(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer "+testToken {
		h.t.Errorf("%s %s without the store token", r.Method, r.URL.Path)
	}
	h.calls = append(h.calls, r.Method+" "+r.URL.Path)
	var body map[string]json.RawMessage
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	holder := ""
	if raw, ok := body["holder"]; ok {
		_ = json.Unmarshal(raw, &holder)
	}
	reply := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	switch {
	case r.Method == "GET" && r.URL.Path == "/v1/quota/accounts/"+testKey+"/latest":
		h.latestCalls++
		if h.onLatest != nil {
			h.onLatest(h, h.latestCalls)
		}
		reply(Latest{Observation: h.latest})
	case r.Method == "POST" && r.URL.Path == "/v1/quota/accounts/"+testKey+"/lease":
		h.leaseCalls++
		if h.onLease != nil {
			if lease, ok := h.onLease(h, h.leaseCalls, holder); ok {
				reply(lease)
				return
			}
		}
		if h.holder == "" || h.holder == holder || !h.expires.After(time.Now()) {
			h.holder, h.expires = holder, time.Now().Add(120*time.Second)
			reply(Lease{Holder: holder, ExpiresAt: h.expires, Acquired: true})
			return
		}
		reply(Lease{Holder: h.holder, ExpiresAt: h.expires})
	case r.Method == "PUT" && r.URL.Path == "/v1/quota/accounts/"+testKey+"/observation":
		if h.failPublish {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(testToken))
			return
		}
		var published map[string]any
		_ = json.Unmarshal(body["observation"], &published)
		published["holder"] = holder
		h.pub = append(h.pub, published)
		var x Observation
		_ = json.Unmarshal(body["observation"], &x)
		h.latest = &x
		w.WriteHeader(204)
	case r.Method == "DELETE" && r.URL.Path == "/v1/quota/accounts/"+testKey+"/lease":
		if h.holder == holder {
			h.holder = ""
		}
		w.WriteHeader(204)
	case r.Method == "PUT" && r.URL.Path == "/v1/quota/presence":
		var p Presence
		_ = json.Unmarshal(mustMarshal(body), &p)
		h.presence = append(h.presence, p)
		w.WriteHeader(204)
	default:
		h.t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		w.WriteHeader(404)
	}
}

func mustMarshal(v any) []byte { b, _ := json.Marshal(v); return b }

func (h *fakeHK) callLog() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return strings.Join(h.calls, "\n")
}

func (h *fakeHK) count(call string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, c := range h.calls {
		if c == call {
			n++
		}
	}
	return n
}

func stored(measured time.Time, used float64) *Observation {
	reset := testNow.Add(2 * time.Hour)
	return &Observation{ID: 7, Kind: KindAuto, MeasuredAt: measured, Buckets: []Bucket{{Window: "5h", UsedPct: used, ResetsAt: &reset, Scope: "account"}}}
}

func pct(v float64) *float64 { return &v }

func goodMeasurement(calls *int) Measurer {
	return func(context.Context) (quota.Observation, error) {
		*calls++
		return quota.Observation{
			Source: quota.SourceUsageAPI, AccountFP: "0123456789abcdef",
			Buckets: []quota.Bucket{
				{Window: "5h", UsedPct: pct(42), ResetsAt: testNow.Add(3 * time.Hour).Format(time.RFC3339Nano), Scope: quota.Scope{Kind: quota.ScopeAccount}},
				{Window: "7d", UsedPct: nil, Scope: quota.Scope{Kind: quota.ScopeModel, Name: "Fable"}},
				{Window: "7d", UsedPct: pct(10), Scope: quota.Scope{Kind: quota.ScopeAccount}},
			},
		}, nil
	}
}

func newFlow() *Flow {
	return &Flow{Now: func() time.Time { return testNow }, Wait: 5 * time.Millisecond, HostID: "node-a"}
}

func mustMeasureNothing(t *testing.T) Measurer {
	return func(context.Context) (quota.Observation, error) {
		t.Error("measured although the shared reading is fresh")
		return quota.Observation{}, errors.New("unexpected")
	}
}

// A fresh stored reading is returned without measuring, taking the lease or
// publishing; it is valid as of the later of measured_at and confirmed_at.
func TestFreshLatestIsReusedWithoutMeasuring(t *testing.T) {
	for name, observation := range map[string]*Observation{
		"measured recently": stored(testNow.Add(-5*time.Minute), 30),
		"confirmed recently": func() *Observation {
			o := stored(testNow.Add(-2*time.Hour), 30)
			confirmed := testNow.Add(-time.Minute)
			o.ConfirmedAt = &confirmed
			return o
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			hk := newFakeHK(t)
			hk.latest = observation
			reading, err := newFlow().Refresh(t.Context(), hk.client(), testKey, mustMeasureNothing(t))
			if err != nil || reading.Key != testKey || reading.Source != quota.SourceHandoffkeep || len(reading.Buckets) != 1 || reading.Buckets[0].UsedPct != 30 {
				t.Fatalf("reading %+v err=%v", reading, err)
			}
			if got := hk.callLog(); strings.Contains(got, "lease") || strings.Contains(got, "observation") {
				t.Fatalf("a fresh reading took the lease or published: %s", got)
			}
		})
	}
}

// Claude readings are valid for 900 s: one second older is stale.
func TestFreshnessBoundary(t *testing.T) {
	for _, tt := range []struct {
		age  time.Duration
		want bool
	}{{899 * time.Second, true}, {900 * time.Second, true}, {901 * time.Second, false}} {
		_, ok := current(stored(testNow.Add(-tt.age), 1), testKey, testNow, DefaultFreshTTL)
		if ok != tt.want {
			t.Errorf("age %s: fresh=%v want %v", tt.age, ok, tt.want)
		}
	}
	for name, o := range map[string]*Observation{
		"manual":     func() *Observation { o := stored(testNow, 1); o.Kind = "manual"; return o }(),
		"no buckets": func() *Observation { o := stored(testNow, 1); o.Buckets = nil; return o }(),
		"bad pct":    stored(testNow, 101),
		"reset passed": func() *Observation {
			o := stored(testNow, 1)
			r := testNow.Add(-time.Second)
			o.Buckets[0].ResetsAt = &r
			return o
		}(),
		"far future stamp": stored(testNow.Add(2*time.Minute), 1),
		"bad scope":        func() *Observation { o := stored(testNow, 1); o.Buckets[0].Scope = "galaxy"; return o }(),
	} {
		if _, ok := current(o, testKey, testNow, DefaultFreshTTL); ok {
			t.Errorf("%s reading accepted", name)
		}
	}
}

// Stale → lease → measure → publish → release, in that order, with the
// observation on the wire as pacelever writes it.
func TestStaleMeasuresPublishesAndReleases(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = stored(testNow.Add(-time.Hour), 30)
	var measured int
	reading, err := newFlow().Refresh(t.Context(), hk.client(), testKey, goodMeasurement(&measured))
	if err != nil || measured != 1 {
		t.Fatalf("reading %+v err=%v measured=%d", reading, err, measured)
	}
	want := strings.Join([]string{
		"GET /v1/quota/accounts/" + testKey + "/latest",
		"POST /v1/quota/accounts/" + testKey + "/lease",
		"GET /v1/quota/accounts/" + testKey + "/latest", // re-read after the lease
		"PUT /v1/quota/accounts/" + testKey + "/observation",
		"DELETE /v1/quota/accounts/" + testKey + "/lease",
		"PUT /v1/quota/presence",
	}, "\n")
	if got := hk.callLog(); got != want {
		t.Fatalf("call order\n%s\nwant\n%s", got, want)
	}
	if len(hk.pub) != 1 {
		t.Fatalf("publishes: %v", hk.pub)
	}
	published := hk.pub[0]
	if published["kind"] != "auto" || published["measured_at"] != "2026-11-05T12:00:00Z" || published["holder"] == "" {
		t.Fatalf("published %v", published)
	}
	if _, sent := published["confirmed_at"]; sent {
		t.Fatalf("confirmed_at is the store's to set: %v", published)
	}
	buckets := published["buckets"].([]any)
	if len(buckets) != 2 {
		t.Fatalf("the unmeasured bucket must be left out: %v", buckets)
	}
	first := buckets[0].(map[string]any)
	if first["window"] != "5h" || first["used_pct"] != float64(42) || first["scope"] != "account" || first["resets_at"] != "2026-11-05T15:00:00Z" {
		t.Fatalf("bucket %v", first)
	}
	if reading.Key != testKey || reading.Source != quota.SourceHandoffkeep || len(reading.Buckets) != 2 || !reading.MeasuredAt.Equal(testNow) {
		t.Fatalf("reading %+v", reading)
	}
	if len(hk.presence) != 1 || hk.presence[0].HostID != "node-a" || hk.presence[0].AccountKey != testKey {
		t.Fatalf("presence %+v", hk.presence)
	}
}

// Presence is announced at most once per interval.
func TestPresenceAtMostEveryTenMinutes(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = stored(testNow.Add(-time.Minute), 30)
	now := testNow
	flow := &Flow{Now: func() time.Time { return now }, Wait: 5 * time.Millisecond, HostID: "node-a"}
	refresh := func() {
		if _, err := flow.Refresh(t.Context(), hk.client(), testKey, mustMeasureNothing(t)); err != nil {
			t.Fatal(err)
		}
	}
	refresh()
	now = testNow.Add(9*time.Minute + 59*time.Second)
	refresh()
	if n := hk.count("PUT /v1/quota/presence"); n != 1 {
		t.Fatalf("presence announced %d times within the interval", n)
	}
	now = testNow.Add(10 * time.Minute)
	hk.latest = stored(now.Add(-time.Minute), 30)
	refresh()
	if n := hk.count("PUT /v1/quota/presence"); n != 2 {
		t.Fatalf("presence announced %d times after the interval", n)
	}
}

func TestNoHostIDNoPresence(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = stored(testNow, 30)
	flow := newFlow()
	flow.HostID = ""
	if _, err := flow.Refresh(t.Context(), hk.client(), testKey, mustMeasureNothing(t)); err != nil || hk.count("PUT /v1/quota/presence") != 0 {
		t.Fatalf("err=%v calls=%s", err, hk.callLog())
	}
}

// Another host's live lease is waited on (bounded), and the reading it
// publishes is served without measuring here.
func TestLiveLeaseOfAnotherHolderIsWaitedOn(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = stored(testNow.Add(-time.Hour), 30)
	hk.holder, hk.expires = "other-host", time.Now().Add(30*time.Second)
	hk.onLatest = func(h *fakeHK, n int) {
		if n == 3 { // the other host publishes between polls
			h.latest = stored(testNow, 77)
		}
	}
	reading, err := newFlow().Refresh(t.Context(), hk.client(), testKey, mustMeasureNothing(t))
	if err != nil || reading.Buckets[0].UsedPct != 77 {
		t.Fatalf("reading %+v err=%v", reading, err)
	}
	if hk.count("PUT /v1/quota/accounts/"+testKey+"/observation") != 0 {
		t.Fatal("published although another host held the lease")
	}
}

// A lease that is released while we wait is taken at once.
func TestLeaseReleasedWhileWaitingIsTaken(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = stored(testNow.Add(-time.Hour), 30)
	hk.holder, hk.expires = "other-host", time.Now().Add(30*time.Second)
	hk.onLatest = func(h *fakeHK, n int) {
		if n == 2 {
			h.holder = "" // the other host failed and released
		}
	}
	var measured int
	if _, err := newFlow().Refresh(t.Context(), hk.client(), testKey, goodMeasurement(&measured)); err != nil || measured != 1 {
		t.Fatalf("err=%v measured=%d", err, measured)
	}
}

// An expired lease of a dead holder is taken over.
func TestExpiredLeaseIsTakenOver(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = stored(testNow.Add(-time.Hour), 30)
	hk.onLease = func(h *fakeHK, n int, holder string) (Lease, bool) {
		if n == 1 { // the first answer shows a dead holder's lease, already expired
			return Lease{Holder: "dead-host", ExpiresAt: time.Now().Add(-time.Second)}, true
		}
		return Lease{}, false
	}
	var measured int
	if _, err := newFlow().Refresh(t.Context(), hk.client(), testKey, goodMeasurement(&measured)); err != nil || measured != 1 {
		t.Fatalf("err=%v measured=%d", err, measured)
	}
	if hk.leaseCalls != 2 {
		t.Fatalf("lease requested %d times, want a retry that takes over", hk.leaseCalls)
	}
	if len(hk.pub) != 1 {
		t.Fatalf("publishes: %v", hk.pub)
	}
}

// A holder that never publishes does not strand the wait: it is bounded by the
// holder's lease expiry, then reported as a failed shared measurement.
func TestWaitIsBoundedByTheHoldersLease(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = stored(testNow.Add(-time.Hour), 30)
	expires := time.Now().Add(150 * time.Millisecond) // fixed: a holder does not extend a lease it never renews
	hk.onLease = func(h *fakeHK, n int, holder string) (Lease, bool) {
		return Lease{Holder: "stuck-host", ExpiresAt: expires}, true
	}
	start := time.Now()
	_, err := newFlow().Refresh(t.Context(), hk.client(), testKey, mustMeasureNothing(t))
	if quota.KindOf(err) != quota.ErrKindSharedFailed {
		t.Fatalf("err = %v, want shared_failed", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("waited %s", elapsed)
	}
}

func TestCancelledWaitIsReported(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = stored(testNow.Add(-time.Hour), 30)
	hk.onLease = func(h *fakeHK, n int, holder string) (Lease, bool) {
		return Lease{Holder: "slow-host", ExpiresAt: time.Now().Add(time.Minute)}, true
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	flow := newFlow()
	flow.Wait = time.Hour
	_, err := flow.Refresh(ctx, hk.client(), testKey, mustMeasureNothing(t))
	if quota.KindOf(err) != quota.ErrKindSharedWait {
		t.Fatalf("err = %v", err)
	}
}

// An account the lease is not for refuses to publish, and the lease is given
// back.
func TestAccountMismatchRefusesToPublish(t *testing.T) {
	for name, fp := range map[string]string{"other account": "fedcba9876543210", "no account": ""} {
		t.Run(name, func(t *testing.T) {
			hk := newFakeHK(t)
			hk.latest = stored(testNow.Add(-time.Hour), 30)
			measure := func(context.Context) (quota.Observation, error) {
				return quota.Observation{AccountFP: fp, Buckets: []quota.Bucket{{Window: "5h", UsedPct: pct(1), Scope: quota.Scope{Kind: quota.ScopeAccount}}}}, nil
			}
			_, err := newFlow().Refresh(t.Context(), hk.client(), testKey, measure)
			if quota.KindOf(err) != quota.ErrKindAccountMismatch {
				t.Fatalf("err = %v", err)
			}
			if hk.count("PUT /v1/quota/accounts/"+testKey+"/observation") != 0 {
				t.Fatal("published for a different account")
			}
			if hk.count("DELETE /v1/quota/accounts/"+testKey+"/lease") != 1 {
				t.Fatalf("the lease was not released: %s", hk.callLog())
			}
		})
	}
}

// A provider 429 backs the account off: the next refresh does not measure.
func TestRateLimitBacksOff(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = stored(testNow.Add(-time.Hour), 30)
	flow := newFlow()
	var measured int
	limited := func(context.Context) (quota.Observation, error) {
		measured++
		e := quota.Fail(quota.ErrKindRateLimited, "usage HTTP 429")
		e.RetryAfter = 120 * time.Second
		return quota.Observation{}, e
	}
	if _, err := flow.Refresh(t.Context(), hk.client(), testKey, limited); quota.KindOf(err) != quota.ErrKindRateLimited {
		t.Fatalf("err = %v", err)
	}
	if _, err := flow.Refresh(t.Context(), hk.client(), testKey, limited); quota.KindOf(err) != quota.ErrKindRateLimited || measured != 1 {
		t.Fatalf("second refresh: err=%v measured=%d", err, measured)
	}
	if hk.holder != "" {
		t.Fatal("a lease was left held")
	}
	flow.Now = func() time.Time { return testNow.Add(121 * time.Second) }
	hk.latest = stored(testNow.Add(-time.Hour), 30)
	if _, err := flow.Refresh(t.Context(), hk.client(), testKey, limited); measured != 2 || quota.KindOf(err) != quota.ErrKindRateLimited {
		t.Fatalf("after the backoff: err=%v measured=%d", err, measured)
	}
}

func TestMeasurementFailureKindsPassThroughAndReleaseTheLease(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = stored(testNow.Add(-time.Hour), 30)
	measure := func(context.Context) (quota.Observation, error) {
		return quota.Observation{}, quota.Fail(quota.ErrKindTokenExpired, "token expired")
	}
	if _, err := newFlow().Refresh(t.Context(), hk.client(), testKey, measure); quota.KindOf(err) != quota.ErrKindTokenExpired {
		t.Fatalf("err = %v", err)
	}
	if hk.holder != "" || hk.count("PUT /v1/quota/accounts/"+testKey+"/observation") != 0 {
		t.Fatalf("calls: %s", hk.callLog())
	}
	plain := func(context.Context) (quota.Observation, error) {
		return quota.Observation{}, errors.New("boom " + testToken)
	}
	if _, err := newFlow().Refresh(t.Context(), hk.client(), testKey, plain); quota.KindOf(err) != quota.ErrKindProbeFailed || strings.Contains(err.Error(), testToken) {
		t.Fatalf("err = %v", err)
	}
}

func TestPublishAndStoreFailures(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = stored(testNow.Add(-time.Hour), 30)
	hk.failPublish = true
	var measured int
	_, err := newFlow().Refresh(t.Context(), hk.client(), testKey, goodMeasurement(&measured))
	if quota.KindOf(err) != quota.ErrKindSharedPublishFailed || strings.Contains(err.Error(), testToken) {
		t.Fatalf("err = %v", err)
	}
	if hk.holder != "" {
		t.Fatal("a failed publish left the lease held")
	}

	hk.server.Close()
	_, err = newFlow().Refresh(t.Context(), hk.client(), testKey, mustMeasureNothing(t))
	if quota.KindOf(err) != quota.ErrKindSharedUnavailable || strings.Contains(err.Error(), testToken) {
		t.Fatalf("unreachable store: %v", err)
	}
}

func TestBadAccountKey(t *testing.T) {
	for _, key := range []string{"", "claude", "claude:XYZ", "Claude:0123456789abcdef", "claude:0123456789abcde"} {
		if _, err := newFlow().Refresh(t.Context(), Client{BaseURL: "http://127.0.0.1:1"}, key, mustMeasureNothing(t)); quota.KindOf(err) != quota.ErrKindAccountUnknown {
			t.Errorf("key %q: %v", key, err)
		}
	}
}

func TestConvertRefusesInvalidMeasurements(t *testing.T) {
	account := quota.Scope{Kind: quota.ScopeAccount}
	for name, o := range map[string]quota.Observation{
		"no buckets":     {},
		"all unmeasured": {Buckets: []quota.Bucket{{Window: "5h", Scope: account}}},
		"out of range":   {Buckets: []quota.Bucket{{Window: "5h", UsedPct: pct(101), Scope: account}}},
		"bad reset":      {Buckets: []quota.Bucket{{Window: "5h", UsedPct: pct(1), ResetsAt: "tomorrow", Scope: account}}},
	} {
		if _, err := Convert(o, testNow); quota.KindOf(err) != quota.ErrKindProbeFailed {
			t.Errorf("%s: %v", name, err)
		}
	}
}

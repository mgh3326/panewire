package panewire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	quotaV2Account      = "acct_7k2m9q4x"
	quotaV2OtherAccount = "acct_p3n8w5r1"
	quotaV2SlotA        = "slot-3f9a1c2e7b604d18"
	quotaV2SlotB        = "slot-8c1d5e0a9f2b7364"
)

type quotaV2Clock struct{ now time.Time }

func (clock *quotaV2Clock) Now() time.Time { return clock.now }

func newQuotaV2TestHub(t *testing.T, storePath string, clock *quotaV2Clock) *HubServer {
	t.Helper()
	tokens := map[string]string{hubOperatorMachineID: quotaOperatorToken, "node-a": quotaNodeAToken, "node-b": quotaNodeBToken}
	hub, err := NewHubServer(HubServerConfig{Tokens: tokens, Now: clock.Now, Logger: slog.Default(), QuotaV2StorePath: storePath})
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

func quotaV2Do(t *testing.T, hub *HubServer, method, target, machine, token string, body any) (int, []byte) {
	t.Helper()
	var reader *bytes.Reader
	switch value := body.(type) {
	case nil:
		reader = bytes.NewReader(nil)
	case []byte:
		reader = bytes.NewReader(value)
	default:
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(data)
	}
	request := httptest.NewRequest(method, target, reader)
	if machine != "" {
		request.Header.Set(hubMachineIDHeader, machine)
	}
	if token != "" {
		request.Header.Set(hubAuthorizationHeader, "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	hub.Handler().ServeHTTP(recorder, request)
	return recorder.Code, recorder.Body.Bytes()
}

func quotaV2Bind(t *testing.T, hub *HubServer, machine, slot, account, label string, validUntil time.Time) QuotaV2Binding {
	t.Helper()
	request := map[string]any{
		"account_ref": account, "provider": "claude", "machine_id": machine, "local_slot_ref": slot,
		"entitlement_ref": "unknown", "identity_basis": "operator_attested", "valid_until": validUntil.UTC().Format(time.RFC3339),
		"verifier_receipt": "hk:doc/fixture/enroll",
	}
	if label != "" {
		request["label"] = label
	}
	status, body := quotaV2Do(t, hub, http.MethodPut, "/v2/quota/bindings", "", quotaOperatorToken, request)
	if status != http.StatusOK {
		t.Fatalf("bind %s/%s: status=%d body=%s", machine, slot, status, body)
	}
	var binding QuotaV2Binding
	if err := json.Unmarshal(body, &binding); err != nil {
		t.Fatal(err)
	}
	return binding
}

func quotaV2Ptr[T any](value T) *T { return &value }

// quotaV2Envelope carries an account-wide 5h and 7d limit plus a model 7d
// limit: the legacy R29 key (pool, account_fp, window) collides on the two 7d
// rows, the v2 key (limit_id) does not.
func quotaV2Envelope(id, machine string, revision int64, account string, measuredAt time.Time, account5h, account7d, model7d float64) QuotaV2Observation {
	reset5h := measuredAt.Add(3 * time.Hour).UTC().Format(time.RFC3339)
	reset7d := measuredAt.Add(96 * time.Hour).UTC().Format(time.RFC3339)
	return QuotaV2Observation{
		Schema: quotaV2ObservationSchema, ObservationID: id, Provider: "claude", AccountRef: account, EntitlementRef: "unknown",
		SourceMachine: machine, SourceBindingRevision: revision, CollectorVersion: "scopefuel/0.1.0+quota-v2.1",
		MeasuredAt: measuredAt.UTC().Format(time.RFC3339), Status: "success",
		Buckets: []QuotaV2Bucket{
			{LimitID: "account:-:5h", Label: quotaV2Ptr("5h"), Scope: QuotaV2Scope{Kind: "account"}, Horizon: "now", Window: "5h", WindowInstance: reset5h, UsedPct: quotaV2Ptr(account5h), ResetAt: quotaV2Ptr(reset5h)},
			{LimitID: "account:-:7d", Label: quotaV2Ptr("7d all"), Scope: QuotaV2Scope{Kind: "account"}, Horizon: "week", Window: "7d", WindowInstance: reset7d, UsedPct: quotaV2Ptr(account7d), ResetAt: quotaV2Ptr(reset7d)},
			{LimitID: "model:opus:7d", Label: quotaV2Ptr("7d Opus"), Scope: QuotaV2Scope{Kind: "model", Ref: quotaV2Ptr("Opus")}, Horizon: "week", Window: "7d", WindowInstance: reset7d, UsedPct: quotaV2Ptr(model7d), ResetAt: quotaV2Ptr(reset7d)},
		},
	}
}

func quotaV2List(t *testing.T, hub *HubServer, machine, token, account string) (int, []QuotaV2Observation) {
	t.Helper()
	status, body := quotaV2Do(t, hub, http.MethodGet, "/v2/quota/observations?provider=claude&account_ref="+account, machine, token, nil)
	if status != http.StatusOK {
		return status, nil
	}
	var response struct {
		Schema       string               `json:"schema"`
		Observations []QuotaV2Observation `json:"observations"`
	}
	if err := json.Unmarshal(body, &response); err != nil || response.Schema != quotaV2ObservationsSchema {
		t.Fatalf("list decode: %v %s", err, body)
	}
	return status, response.Observations
}

// AC1 (#578 stage 1): one account measured from two nodes. Both nodes' values
// land under the same account_ref, every limit (account 7d and model 7d) is
// kept, measured_at is kept verbatim, and nothing leaks into the legacy
// machine-keyed quota surface.
func TestQuotaV2OneAccountTwoNodesKeepsEveryLimit(t *testing.T) {
	clock := &quotaV2Clock{now: time.Date(2026, 9, 24, 1, 0, 0, 0, time.UTC)}
	hub := newQuotaV2TestHub(t, "", clock)
	bindingA := quotaV2Bind(t, hub, "node-a", quotaV2SlotA, quotaV2Account, "회사", clock.now.Add(24*time.Hour))
	bindingB := quotaV2Bind(t, hub, "node-b", quotaV2SlotB, quotaV2Account, "개인", clock.now.Add(24*time.Hour))
	if bindingA.AccountRef != bindingB.AccountRef || bindingA.BindingRevision == bindingB.BindingRevision {
		t.Fatalf("bindings not one account with distinct revisions: %+v %+v", bindingA, bindingB)
	}

	clock.now = clock.now.Add(2 * time.Minute)
	measuredA := clock.now.Add(-30 * time.Second)
	status, body := quotaV2Do(t, hub, http.MethodPost, "/v2/quota/observations", "node-a", quotaNodeAToken, quotaV2Envelope("obs-a-1", "node-a", bindingA.BindingRevision, quotaV2Account, measuredA, 20, 40, 55))
	if status != http.StatusCreated {
		t.Fatalf("node-a post: %d %s", status, body)
	}
	clock.now = clock.now.Add(time.Minute)
	measuredB := clock.now.Add(-10 * time.Second)
	status, body = quotaV2Do(t, hub, http.MethodPost, "/v2/quota/observations", "node-b", quotaNodeBToken, quotaV2Envelope("obs-b-1", "node-b", bindingB.BindingRevision, quotaV2Account, measuredB, 22, 41, 56))
	if status != http.StatusCreated {
		t.Fatalf("node-b post: %d %s", status, body)
	}

	for _, reader := range []struct{ machine, token string }{{"node-a", quotaNodeAToken}, {"node-b", quotaNodeBToken}, {"", quotaOperatorToken}} {
		status, observations := quotaV2List(t, hub, reader.machine, reader.token, quotaV2Account)
		if status != http.StatusOK || len(observations) != 2 {
			t.Fatalf("reader %q: status=%d observations=%d", reader.machine, status, len(observations))
		}
		if observations[0].SourceMachine != "node-a" || observations[1].SourceMachine != "node-b" {
			t.Fatalf("observations not ordered by measured_at: %+v", observations)
		}
		for index, want := range []time.Time{measuredA, measuredB} {
			got := observations[index]
			if got.MeasuredAt != want.UTC().Format(time.RFC3339) || got.ReceivedAt == nil || *got.ReceivedAt == got.MeasuredAt || got.AccountRef != quotaV2Account {
				t.Fatalf("measured_at/received_at lost or merged: %+v", got)
			}
			limits := []string{}
			for _, bucket := range got.Buckets {
				limits = append(limits, bucket.LimitID+"@"+bucket.Scope.Kind+"/"+bucket.Horizon+"/"+bucket.Window)
				if bucket.WindowInstance == "" || bucket.ResetAt == nil || bucket.UsedPct == nil {
					t.Fatalf("bucket field lost: %+v", bucket)
				}
			}
			sort.Strings(limits)
			if strings.Join(limits, ",") != "account:-:5h@account/now/5h,account:-:7d@account/week/7d,model:opus:7d@model/week/7d" {
				t.Fatalf("limits lost: %v", limits)
			}
		}
	}

	// Legacy R29 regression shape (hub_quota.go duplicate key): the same two 7d
	// limits as legacy rows collide on (pool, account_fp, window) and the whole
	// legacy snapshot is rejected. That legacy behaviour is left as is; v2 is
	// the path that keeps both.
	collected := measuredA.UTC().Format(time.RFC3339)
	legacy := HubQuotaSnapshot{Status: hubQuotaStatusOK, CollectedAt: &collected, Pools: []HubQuotaPool{
		{Pool: "claude", AccountFP: "a1b2c3d4", Window: "7d", UsedPct: quotaV2Ptr(40.0), Source: "cloud"},
		{Pool: "claude", AccountFP: "a1b2c3d4", Window: "7d", UsedPct: quotaV2Ptr(55.0), Source: "cloud"},
	}}
	if legacy.valid() {
		t.Fatal("legacy R29 duplicate-key shape changed; this test pins the unchanged legacy path")
	}
	if err := validateQuotaV2Envelope(quotaV2Envelope("obs-x", "node-a", 1, quotaV2Account, measuredA, 1, 2, 3)); err != nil {
		t.Fatalf("v2 rejected account-7d + model-7d: %v", err)
	}

	// No automatic mixing: the legacy list is untouched by v2 writes.
	status, body = quotaV2Do(t, hub, http.MethodGet, "/v1/quota", "", quotaOperatorToken, nil)
	if status != http.StatusOK || !bytes.Contains(body, []byte(`"machine_id":"node-a","state":"unknown","latest":null,"last_good":null`)) {
		t.Fatalf("legacy quota surface changed by v2 writes: %d %s", status, body)
	}

	// Shared contract fixture for scopefuel's evaluator (copied verbatim into
	// scopefuel tests/fixtures/quota_v2_two_nodes.json).
	_, golden := quotaV2Do(t, hub, http.MethodGet, "/v2/quota/observations?provider=claude&account_ref="+quotaV2Account, "node-a", quotaNodeAToken, nil)
	goldenPath := filepath.Join("testdata", "quota-v2-two-nodes.json")
	if os.Getenv("PANEWIRE_UPDATE_GOLDEN") == "1" {
		if err := os.WriteFile(goldenPath, golden, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil || !bytes.Equal(want, golden) {
		t.Fatalf("v2 contract fixture drifted (err=%v):\n got=%s\nwant=%s", err, golden, want)
	}
}

// AC2 mutant target: a node writes only through its own current binding.
func TestQuotaV2NodeWritesOnlyThroughItsOwnCurrentBinding(t *testing.T) {
	clock := &quotaV2Clock{now: time.Date(2026, 9, 24, 1, 0, 0, 0, time.UTC)}
	hub := newQuotaV2TestHub(t, "", clock)
	bindingA := quotaV2Bind(t, hub, "node-a", quotaV2SlotA, quotaV2Account, "", clock.now.Add(time.Hour))
	clock.now = clock.now.Add(time.Minute)
	measured := clock.now.Add(-5 * time.Second)
	post := func(machine, token string, envelope QuotaV2Observation) int {
		status, _ := quotaV2Do(t, hub, http.MethodPost, "/v2/quota/observations", machine, token, envelope)
		return status
	}

	cases := []struct {
		name           string
		machine, token string
		envelope       QuotaV2Observation
		want           int
	}{
		{"other node claims node-a as source", "node-b", quotaNodeBToken, quotaV2Envelope("obs-1", "node-a", bindingA.BindingRevision, quotaV2Account, measured, 1, 2, 3), http.StatusForbidden},
		{"other node reuses node-a revision", "node-b", quotaNodeBToken, quotaV2Envelope("obs-2", "node-b", bindingA.BindingRevision, quotaV2Account, measured, 1, 2, 3), http.StatusForbidden},
		{"unbound account", "node-a", quotaNodeAToken, quotaV2Envelope("obs-3", "node-a", bindingA.BindingRevision, quotaV2OtherAccount, measured, 1, 2, 3), http.StatusForbidden},
		{"wrong revision", "node-a", quotaNodeAToken, quotaV2Envelope("obs-4", "node-a", bindingA.BindingRevision+1, quotaV2Account, measured, 1, 2, 3), http.StatusForbidden},
		{"measured before binding began", "node-a", quotaNodeAToken, quotaV2Envelope("obs-5", "node-a", bindingA.BindingRevision, quotaV2Account, clock.now.Add(-2*time.Minute), 1, 2, 3), http.StatusForbidden},
		{"operator token is not a node", "", quotaOperatorToken, quotaV2Envelope("obs-6", "node-a", bindingA.BindingRevision, quotaV2Account, measured, 1, 2, 3), http.StatusUnauthorized},
		{"node token with operator header", hubOperatorMachineID, quotaOperatorToken, quotaV2Envelope("obs-7", "node-a", bindingA.BindingRevision, quotaV2Account, measured, 1, 2, 3), http.StatusUnauthorized},
		{"own binding", "node-a", quotaNodeAToken, quotaV2Envelope("obs-8", "node-a", bindingA.BindingRevision, quotaV2Account, measured, 1, 2, 3), http.StatusCreated},
	}
	for _, testCase := range cases {
		if got := post(testCase.machine, testCase.token, testCase.envelope); got != testCase.want {
			t.Errorf("%s: status=%d want=%d", testCase.name, got, testCase.want)
		}
	}

	// Nodes cannot enroll; a node's binding list is its own only.
	if status, _ := quotaV2Do(t, hub, http.MethodPut, "/v2/quota/bindings", "node-a", quotaNodeAToken, map[string]any{}); status != http.StatusUnauthorized {
		t.Fatalf("node enrolled a binding: %d", status)
	}
	quotaV2Bind(t, hub, "node-b", quotaV2SlotB, quotaV2OtherAccount, "", clock.now.Add(time.Hour))
	_, body := quotaV2Do(t, hub, http.MethodGet, "/v2/quota/bindings", "node-a", quotaNodeAToken, nil)
	if bytes.Contains(body, []byte("node-b")) || !bytes.Contains(body, []byte(`"machine_id":"node-a"`)) {
		t.Fatalf("node saw another node's bindings: %s", body)
	}
	// A node reads only accounts it is bound to.
	if status, _ := quotaV2List(t, hub, "node-a", quotaNodeAToken, quotaV2OtherAccount); status != http.StatusForbidden {
		t.Fatalf("node-a read an account it is not bound to: %d", status)
	}

	// Same slot A -> B: the old revision can no longer write A, nor B.
	clock.now = clock.now.Add(time.Minute)
	rebound := quotaV2Bind(t, hub, "node-a", quotaV2SlotA, quotaV2OtherAccount, "", clock.now.Add(time.Hour))
	if rebound.BindingRevision <= bindingA.BindingRevision {
		t.Fatalf("revision not monotonic: %d -> %d", bindingA.BindingRevision, rebound.BindingRevision)
	}
	late := clock.now.Add(time.Second)
	clock.now = late.Add(time.Second)
	if got := post("node-a", quotaNodeAToken, quotaV2Envelope("obs-9", "node-a", bindingA.BindingRevision, quotaV2Account, late, 1, 2, 3)); got != http.StatusForbidden {
		t.Fatalf("stale binding revision still writes account A: %d", got)
	}
	if got := post("node-a", quotaNodeAToken, quotaV2Envelope("obs-10", "node-a", bindingA.BindingRevision, quotaV2OtherAccount, late, 1, 2, 3)); got != http.StatusForbidden {
		t.Fatalf("old revision wrote the new account: %d", got)
	}
	if got := post("node-a", quotaNodeAToken, quotaV2Envelope("obs-11", "node-a", rebound.BindingRevision, quotaV2OtherAccount, late.Add(-2*time.Minute), 1, 2, 3)); got != http.StatusForbidden {
		t.Fatalf("pre-rebind measurement accepted as the new account: %d", got)
	}
	if got := post("node-a", quotaNodeAToken, quotaV2Envelope("obs-12", "node-a", rebound.BindingRevision, quotaV2OtherAccount, late, 1, 2, 3)); got != http.StatusCreated {
		t.Fatalf("current binding rejected: %d", got)
	}
	// node-a lost its binding to A, so it can no longer read A either.
	if status, _ := quotaV2List(t, hub, "node-a", quotaNodeAToken, quotaV2Account); status != http.StatusForbidden {
		t.Fatalf("rebound node still reads the old account: %d", status)
	}
	// The earlier A observation stays A's history; it did not move to B.
	_, observations := quotaV2List(t, hub, "", quotaOperatorToken, quotaV2OtherAccount)
	for _, observation := range observations {
		if observation.ObservationID == "obs-8" {
			t.Fatal("account A observation surfaced under account B")
		}
	}

	// Expiry closes the write path.
	clock.now = clock.now.Add(2 * time.Hour)
	if got := post("node-a", quotaNodeAToken, quotaV2Envelope("obs-13", "node-a", rebound.BindingRevision, quotaV2OtherAccount, clock.now, 1, 2, 3)); got != http.StatusForbidden {
		t.Fatalf("expired binding still writes: %d", got)
	}
}

func TestQuotaV2EnvelopeValidationAndIdempotency(t *testing.T) {
	clock := &quotaV2Clock{now: time.Date(2026, 9, 24, 1, 0, 0, 0, time.UTC)}
	hub := newQuotaV2TestHub(t, "", clock)
	binding := quotaV2Bind(t, hub, "node-a", quotaV2SlotA, quotaV2Account, "", clock.now.Add(time.Hour))
	clock.now = clock.now.Add(time.Minute)
	measured := clock.now
	base := func() QuotaV2Observation {
		return quotaV2Envelope("obs-v", "node-a", binding.BindingRevision, quotaV2Account, measured, 1, 2, 3)
	}
	mutate := map[string]func(*QuotaV2Observation){
		"email account_ref":      func(o *QuotaV2Observation) { o.AccountRef = "someone@example.com" },
		"path account_ref":       func(o *QuotaV2Observation) { o.AccountRef = "/home/user/.claude" },
		"unknown status":         func(o *QuotaV2Observation) { o.Status = "ok" },
		"failure with values":    func(o *QuotaV2Observation) { o.Status = "rate_limited" },
		"success without values": func(o *QuotaV2Observation) { o.Buckets = []QuotaV2Bucket{} },
		"duplicate limit_id":     func(o *QuotaV2Observation) { o.Buckets[2].LimitID = o.Buckets[1].LimitID },
		"used_pct over 100":      func(o *QuotaV2Observation) { o.Buckets[0].UsedPct = quotaV2Ptr(100.5) },
		"account scope with ref": func(o *QuotaV2Observation) { o.Buckets[0].Scope.Ref = quotaV2Ptr("x") },
		"model scope no ref":     func(o *QuotaV2Observation) { o.Buckets[2].Scope.Ref = nil },
		"bad horizon":            func(o *QuotaV2Observation) { o.Buckets[0].Horizon = "day" },
		"client received_at":     func(o *QuotaV2Observation) { o.ReceivedAt = quotaV2Ptr(measured.Format(time.RFC3339)) },
		"future measured_at":     func(o *QuotaV2Observation) { o.MeasuredAt = clock.now.Add(time.Hour).Format(time.RFC3339) },
		"missing window_instance": func(o *QuotaV2Observation) {
			o.Buckets[0].WindowInstance = ""
		},
		"wrong schema": func(o *QuotaV2Observation) { o.Schema = "quota-observation/v1" },
	}
	for name, change := range mutate {
		envelope := base()
		change(&envelope)
		if status, body := quotaV2Do(t, hub, http.MethodPost, "/v2/quota/observations", "node-a", quotaNodeAToken, envelope); status != http.StatusBadRequest {
			t.Errorf("%s: status=%d body=%s", name, status, body)
		}
	}
	raw, _ := json.Marshal(base())
	withLabel := bytes.Replace(raw, []byte(`{"schema"`), []byte(`{"label":"회사","schema"`), 1)
	if status, _ := quotaV2Do(t, hub, http.MethodPost, "/v2/quota/observations", "node-a", quotaNodeAToken, withLabel); status != http.StatusBadRequest {
		t.Fatalf("unknown envelope field accepted: %d", status)
	}

	failure := base()
	failure.ObservationID, failure.Status, failure.Buckets, failure.ErrorRef = "obs-f", "rate_limited", []QuotaV2Bucket{}, quotaV2Ptr("http:429")
	if status, body := quotaV2Do(t, hub, http.MethodPost, "/v2/quota/observations", "node-a", quotaNodeAToken, failure); status != http.StatusCreated {
		t.Fatalf("failure observation rejected: %d %s", status, body)
	}
	if status, _ := quotaV2Do(t, hub, http.MethodPost, "/v2/quota/observations", "node-a", quotaNodeAToken, base()); status != http.StatusCreated {
		t.Fatal("first post rejected")
	}
	if status, _ := quotaV2Do(t, hub, http.MethodPost, "/v2/quota/observations", "node-a", quotaNodeAToken, base()); status != http.StatusOK {
		t.Fatal("identical re-post not idempotent")
	}
	changed := base()
	changed.Buckets[0].UsedPct = quotaV2Ptr(9.0)
	if status, _ := quotaV2Do(t, hub, http.MethodPost, "/v2/quota/observations", "node-a", quotaNodeAToken, changed); status != http.StatusConflict {
		t.Fatal("same observation_id with different content accepted")
	}
	_, observations := quotaV2List(t, hub, "node-a", quotaNodeAToken, quotaV2Account)
	if len(observations) != 2 {
		t.Fatalf("observation count = %d, want 2", len(observations))
	}
}

func TestQuotaV2StoreSurvivesRestartAndRejectsCorruption(t *testing.T) {
	clock := &quotaV2Clock{now: time.Date(2026, 9, 24, 1, 0, 0, 0, time.UTC)}
	path := filepath.Join(t.TempDir(), "quota-v2.json")
	hub := newQuotaV2TestHub(t, path, clock)
	binding := quotaV2Bind(t, hub, "node-a", quotaV2SlotA, quotaV2Account, "회사", clock.now.Add(time.Hour))
	clock.now = clock.now.Add(time.Minute)
	if status, _ := quotaV2Do(t, hub, http.MethodPost, "/v2/quota/observations", "node-a", quotaNodeAToken, quotaV2Envelope("obs-p", "node-a", binding.BindingRevision, quotaV2Account, clock.now, 1, 2, 3)); status != http.StatusCreated {
		t.Fatal("post failed")
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("store not written 0600: %v %v", err, info)
	}

	restarted := newQuotaV2TestHub(t, path, clock)
	_, observations := quotaV2List(t, restarted, "node-a", quotaNodeAToken, quotaV2Account)
	if len(observations) != 1 || observations[0].ObservationID != "obs-p" {
		t.Fatalf("observations lost across restart: %+v", observations)
	}
	// A clock that stepped back must still not reuse a revision.
	clock.now = clock.now.Add(-24 * time.Hour)
	again := quotaV2Bind(t, restarted, "node-a", quotaV2SlotA, quotaV2Account, "", clock.now.Add(time.Hour))
	if again.BindingRevision <= binding.BindingRevision {
		t.Fatalf("revision reused after restart: %d <= %d", again.BindingRevision, binding.BindingRevision)
	}

	for name, content := range map[string]string{
		"not json":       "{",
		"wrong schema":   `{"schema":"other","last_revision":0,"bindings":[],"observations":[]}`,
		"unknown field":  `{"schema":"panewire.quota-v2-store/v1","last_revision":0,"bindings":[],"observations":[],"x":1}`,
		"revision ahead": fmt.Sprintf(`{"schema":"panewire.quota-v2-store/v1","last_revision":1,"bindings":[{"account_ref":%q,"provider":"claude","machine_id":"node-a","local_slot_ref":%q,"entitlement_ref":"unknown","identity_basis":"operator_attested","binding_revision":5,"verified_at":"2026-09-24T01:00:00Z","valid_until":"2026-09-25T01:00:00Z","verifier_receipt":null,"label":null}],"observations":[]}`, quotaV2Account, quotaV2SlotA),
	} {
		corrupt := filepath.Join(t.TempDir(), "quota-v2.json")
		if err := os.WriteFile(corrupt, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		tokens := map[string]string{hubOperatorMachineID: quotaOperatorToken, "node-a": quotaNodeAToken}
		if _, err := NewHubServer(HubServerConfig{Tokens: tokens, Now: clock.Now, QuotaV2StorePath: corrupt}); err == nil {
			t.Errorf("%s: corrupt store accepted", name)
		}
	}
}

func TestQuotaV2BindingEnrollmentValidation(t *testing.T) {
	clock := &quotaV2Clock{now: time.Date(2026, 9, 24, 1, 0, 0, 0, time.UTC)}
	hub := newQuotaV2TestHub(t, "", clock)
	valid := func() map[string]any {
		return map[string]any{"account_ref": quotaV2Account, "provider": "claude", "machine_id": "node-a", "local_slot_ref": quotaV2SlotA, "entitlement_ref": "unknown", "identity_basis": "operator_attested", "valid_until": clock.now.Add(time.Hour).Format(time.RFC3339)}
	}
	for name, change := range map[string]func(map[string]any){
		"unknown machine":    func(m map[string]any) { m["machine_id"] = "node-z" },
		"operator machine":   func(m map[string]any) { m["machine_id"] = hubOperatorMachineID },
		"email account":      func(m map[string]any) { m["account_ref"] = "a@b.co" },
		"expired":            func(m map[string]any) { m["valid_until"] = clock.now.Add(-time.Minute).Format(time.RFC3339) },
		"too long":           func(m map[string]any) { m["valid_until"] = clock.now.Add(31 * 24 * time.Hour).Format(time.RFC3339) },
		"bad identity basis": func(m map[string]any) { m["identity_basis"] = "guess" },
		"client revision":    func(m map[string]any) { m["binding_revision"] = 7 },
	} {
		request := valid()
		change(request)
		if status, _ := quotaV2Do(t, hub, http.MethodPut, "/v2/quota/bindings", "", quotaOperatorToken, request); status != http.StatusBadRequest {
			t.Errorf("%s: accepted (status %d)", name, status)
		}
	}
	if status, _ := quotaV2Do(t, hub, http.MethodPut, "/v2/quota/bindings", "", quotaOperatorToken, valid()); status != http.StatusOK {
		t.Fatal("valid enrollment rejected")
	}
	if status, _ := quotaV2Do(t, hub, http.MethodDelete, "/v2/quota/bindings?machine_id=node-a&provider=claude&local_slot_ref="+quotaV2SlotA, "node-a", quotaNodeAToken, nil); status != http.StatusUnauthorized {
		t.Fatal("node removed a binding")
	}
	if status, _ := quotaV2Do(t, hub, http.MethodDelete, "/v2/quota/bindings?machine_id=node-a&provider=claude&local_slot_ref="+quotaV2SlotA, "", quotaOperatorToken, nil); status != http.StatusNoContent {
		t.Fatal("operator could not remove a binding")
	}
	if status, _ := quotaV2Do(t, hub, http.MethodDelete, "/v2/quota/bindings?machine_id=node-a&provider=claude&local_slot_ref="+quotaV2SlotA, "", quotaOperatorToken, nil); status != http.StatusNotFound {
		t.Fatal("second delete did not 404")
	}
}

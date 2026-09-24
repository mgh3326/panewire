package panewire

// Contract quota-v2.r3 (hk review/2026-09-24/578-contract-r3.1) rows shared
// with scopefuel. The testdata files are byte-identical copies of scopefuel
// tests/fixtures/quota_v2_{contract,wire,parity}_r3.json; both repositories
// pin the same digests, so an edit on one side fails that side's test.
// Expected values are read from the files, never re-typed here.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var quotaV2SharedFixtures = map[string]string{
	"quota-v2-contract-r3.json": "904f80ffebd941b87021c65a25e3bdfc99b45990f031b7c93decd4d4a7c35490",
	"quota-v2-wire-r3.json":     "efdbf028a29fa3dc64a637fbe925aac5c3c389a79a08fe9b60fe52875f1ea02c",
	"quota-v2-parity-r3.json":   "7f8fbfc0ee3a1253599819b81a2a620340a67060a56473031943c2a1c8c09a83",
}

func quotaV2Fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestQuotaV2SharedFixturesArePinned(t *testing.T) {
	for name, want := range quotaV2SharedFixtures {
		sum := sha256.Sum256(quotaV2Fixture(t, name))
		if got := hex.EncodeToString(sum[:]); got != want {
			t.Errorf("%s: sha256 %s, want %s (scopefuel copy differs)", name, got, want)
		}
	}
}

// §2 / §7.4: Go accepts exactly what scopefuel accepts, per form.
func TestQuotaV2WireAndParityRows(t *testing.T) {
	for _, name := range []string{"quota-v2-wire-r3.json", "quota-v2-parity-r3.json"} {
		var rows []struct {
			ID       string          `json:"id"`
			Form     string          `json:"form"`
			Note     string          `json:"note"`
			Envelope json.RawMessage `json:"envelope"`
			Expect   string          `json:"expect"`
		}
		if err := json.Unmarshal(quotaV2Fixture(t, name), &rows); err != nil || len(rows) == 0 {
			t.Fatalf("%s: %v", name, err)
		}
		for _, row := range rows {
			got := "accept"
			if _, err := decodeQuotaV2Observation(row.Envelope, row.Form); err != nil {
				got = "reject"
			}
			if got != row.Expect {
				t.Errorf("%s %s (%s, %s): got %s, want %s", name, row.ID, row.Form, row.Note, got, row.Expect)
			}
		}
	}
}

type quotaV2HubRow struct {
	ID      string         `json:"id"`
	Binding map[string]any `json:"binding"`
	Now     *float64       `json:"now"`
	Post    map[string]any `json:"post"`
	Config  map[string]any `json:"config"`
	Request string         `json:"request"`
	ThenGet bool           `json:"then_get"`
	Expect  any            `json:"expect"`
}

func quotaV2HasKey(m map[string]any, key string) bool {
	_, ok := m[key]
	return m != nil && ok
}

// §7: every corpus hub row (H01–H22) through the real HTTP handler.
func TestQuotaV2HubRows(t *testing.T) {
	var corpus struct {
		Hub []quotaV2HubRow `json:"hub"`
	}
	if err := json.Unmarshal(quotaV2Fixture(t, "quota-v2-contract-r3.json"), &corpus); err != nil || len(corpus.Hub) == 0 {
		t.Fatalf("corpus: %v", err)
	}
	t0 := time.Date(2026, 9, 24, 1, 0, 0, 0, time.UTC)
	at := func(s float64) time.Time { return t0.Add(time.Duration(s * float64(time.Second))) }
	num := func(m map[string]any, key string) float64 {
		value, _ := m[key].(float64)
		return value
	}
	for _, row := range corpus.Hub {
		t.Run(row.ID, func(t *testing.T) {
			got := quotaV2RunHubRow(t, row, at, num)
			want := row.Expect
			if code, ok := want.(float64); ok {
				want = int(code)
			}
			if got != want {
				t.Fatalf("got %v, want %v", got, want)
			}
		})
	}
}

func quotaV2RunHubRow(t *testing.T, row quotaV2HubRow, at func(float64) time.Time, num func(map[string]any, string) float64) any {
	t.Helper()
	if row.Config["shared_bearer"] == true || (quotaV2HasKey(row.Config, "store") && row.Config["store"] == nil) {
		tokens := map[string]string{hubOperatorMachineID: quotaOperatorToken, "node-a": quotaNodeAToken}
		store := filepath.Join(t.TempDir(), "s.json")
		if row.Config["shared_bearer"] == true {
			tokens["node-a"] = quotaOperatorToken
		} else {
			store = ""
		}
		hub, err := NewHubServer(HubServerConfig{Tokens: tokens, Now: func() time.Time { return at(600) }, QuotaV2StorePath: store})
		if err != nil {
			t.Fatal(err)
		}
		status, _ := quotaV2Do(t, hub, http.MethodGet, "/v2/quota/bindings", "", quotaOperatorToken, nil)
		return status
	}

	clock := &quotaV2Clock{now: at(num(row.Binding, "valid_from"))}
	tokens := map[string]string{hubOperatorMachineID: quotaOperatorToken, "node-a": quotaNodeAToken, "node-b": quotaNodeBToken}
	hub, err := NewHubServer(HubServerConfig{Tokens: tokens, Now: clock.Now, QuotaV2StorePath: filepath.Join(t.TempDir(), "s.json"),
		QuotaV2ClockSkew: time.Duration(num(row.Config, "delta_hub_s") * float64(time.Second))})
	if err != nil {
		t.Fatal(err)
	}
	until := at(num(row.Binding, "valid_until"))
	revision := quotaV2Bind(t, hub, "node-a", quotaV2SlotA, quotaV2Account, "", until).BindingRevision
	if row.Binding["rebind_before_post"] == true {
		clock.now = clock.now.Add(time.Second)
		quotaV2Bind(t, hub, "node-a", quotaV2SlotA, quotaV2Account, "", until)
	}
	if row.Binding["also_bind_node_b"] == true {
		quotaV2Bind(t, hub, "node-b", quotaV2SlotB, quotaV2Account, "", until)
	}
	clock.now = at(600)
	if row.Now != nil {
		clock.now = at(*row.Now)
	}

	switch {
	case strings.HasPrefix(row.Request, "GET /v2/quota/observations"):
		account := row.Request[strings.Index(row.Request, "account_ref=")+len("account_ref=") : strings.Index(row.Request, " (")]
		machine, token := "node-a", quotaNodeAToken
		if strings.HasSuffix(row.Request, "(operator)") {
			machine, token = "", quotaOperatorToken
		}
		status, _ := quotaV2Do(t, hub, http.MethodGet, "/v2/quota/observations?provider=claude&account_ref="+account, machine, token, nil)
		return status
	case strings.HasPrefix(row.Request, "GET /v2/quota/bindings"):
		status, body := quotaV2Do(t, hub, http.MethodGet, "/v2/quota/bindings", "node-a", quotaNodeAToken, nil)
		if status == http.StatusOK && bytes.Contains(body, []byte(`"machine_id":"node-a"`)) && !bytes.Contains(body, []byte(`"machine_id":"node-b"`)) {
			return "200 with node-a bindings only"
		}
		return status
	}

	source := "node-a"
	if value, ok := row.Post["source_machine"].(string); ok {
		source = value
	}
	envelope := quotaV2Envelope("obs-"+strings.ToLower(row.ID), source, revision, quotaV2Account, at(num(row.Post, "measured_at")), 10, 20, 30)
	envelope.SourceSlotRef = quotaV2SlotA
	if value, ok := row.Post["source_slot_ref"].(string); ok {
		envelope.SourceSlotRef = value
	}
	if quotaV2HasKey(row.Post, "observed_at") {
		value := at(num(row.Post, "observed_at")).UTC().Format(time.RFC3339)
		envelope.Buckets[0].ObservedAt = &value
	}
	body := quotaV2JSONWithout(t, envelope, row.Post)
	machine, token := "node-a", quotaNodeAToken
	switch row.Post["bearer"] {
	case "operator":
		machine, token = "", quotaOperatorToken
	case "none":
		token = ""
	}
	status, _ := quotaV2Do(t, hub, http.MethodPost, "/v2/quota/observations", machine, token, body)
	if row.Post["repeat_identical"] == true {
		status, _ = quotaV2Do(t, hub, http.MethodPost, "/v2/quota/observations", machine, token, body)
	}
	if row.Post["repeat_with_changed_value"] == true {
		changed := envelope
		changed.Buckets = append([]QuotaV2Bucket{}, envelope.Buckets...)
		changed.Buckets[0].UsedPct = quotaV2Ptr(11.0)
		status, _ = quotaV2Do(t, hub, http.MethodPost, "/v2/quota/observations", machine, token, changed)
	}
	if row.ThenGet {
		_, listed := quotaV2List(t, hub, "node-a", quotaNodeAToken, quotaV2Account)
		if len(listed) == 1 && listed[0].ReceivedAt != nil {
			return "received_at non-null"
		}
		return "received_at missing"
	}
	return status
}

// quotaV2JSONWithout marshals the envelope and drops the keys a row sets to
// null in "post" among the envelope fields (e.g. H08 "contract_rev": null means absent).
func quotaV2JSONWithout(t *testing.T, envelope QuotaV2Observation, post map[string]any) []byte {
	t.Helper()
	data, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(data, &object); err != nil {
		t.Fatal(err)
	}
	for key, value := range post {
		if _, envelopeField := quotaV2EnvelopeFields[key]; envelopeField && value == nil {
			delete(object, key)
		}
	}
	data, err = json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// §7.3 and §7.2 edges with Δ_hub > 0, beyond the corpus rows.
func TestQuotaV2ClockSkewWidensFutureAndNarrowsWindow(t *testing.T) {
	t0 := time.Date(2026, 9, 24, 1, 0, 0, 0, time.UTC)
	clock := &quotaV2Clock{now: t0}
	tokens := map[string]string{hubOperatorMachineID: quotaOperatorToken, "node-a": quotaNodeAToken}
	hub, err := NewHubServer(HubServerConfig{Tokens: tokens, Now: clock.Now, QuotaV2StorePath: filepath.Join(t.TempDir(), "s.json"), QuotaV2ClockSkew: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	binding := quotaV2Bind(t, hub, "node-a", quotaV2SlotA, quotaV2Account, "", t0.Add(time.Hour))
	clock.now = t0.Add(10 * time.Minute)
	for index, testCase := range []struct {
		name     string
		measured time.Time
		want     int
	}{
		{"ahead by exactly Δ_hub", clock.now.Add(10 * time.Second), http.StatusCreated},
		{"ahead by more than Δ_hub", clock.now.Add(11 * time.Second), http.StatusBadRequest},
		{"within Δ_hub of valid_from", t0.Add(9 * time.Second), http.StatusForbidden},
		{"exactly Δ_hub after valid_from", t0.Add(10 * time.Second), http.StatusCreated},
	} {
		envelope := quotaV2Envelope(fmt.Sprintf("obs-skew-%d", index), "node-a", binding.BindingRevision, quotaV2Account, testCase.measured, 1, 2, 3)
		if status, body := quotaV2Do(t, hub, http.MethodPost, "/v2/quota/observations", "node-a", quotaNodeAToken, envelope); status != testCase.want {
			t.Errorf("%s: status=%d want=%d body=%s", testCase.name, status, testCase.want, body)
		}
	}
	// §7.2 closed-open end: t + Δ_hub must stay before valid_until.
	clock.now = t0.Add(time.Hour - 5*time.Second)
	for index, testCase := range []struct {
		name     string
		measured time.Time
		want     int
	}{
		{"t + Δ_hub reaches valid_until", t0.Add(time.Hour - 10*time.Second), http.StatusForbidden},
		{"t + Δ_hub just before valid_until", t0.Add(time.Hour - 11*time.Second), http.StatusCreated},
	} {
		envelope := quotaV2Envelope(fmt.Sprintf("obs-end-%d", index), "node-a", binding.BindingRevision, quotaV2Account, testCase.measured, 1, 2, 3)
		if status, body := quotaV2Do(t, hub, http.MethodPost, "/v2/quota/observations", "node-a", quotaNodeAToken, envelope); status != testCase.want {
			t.Errorf("%s: status=%d want=%d body=%s", testCase.name, status, testCase.want, body)
		}
	}
	if _, err := NewHubServer(HubServerConfig{Tokens: tokens, Now: clock.Now, QuotaV2StorePath: filepath.Join(t.TempDir(), "s.json"), QuotaV2ClockSkew: -time.Second}); err == nil {
		t.Fatal("negative Δ_hub accepted")
	}
}

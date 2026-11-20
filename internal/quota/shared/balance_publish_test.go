package shared

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// hk 5217: a store's 400 on an observation that carries `balance` is a
// publish error like any other refusal. It is not retried without the field:
// the balance-less republish that hid such a refusal for an hour is gone.
func TestFlowBalanceRefusalIsAPublishError_t5217(t *testing.T) {
	now := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/observation") {
			raw, _ := io.ReadAll(r.Body)
			mu.Lock()
			bodies = append(bodies, string(raw))
			mu.Unlock()
			if bytes.Contains(raw, []byte(`"balance"`)) {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client := Client{BaseURL: server.URL}
	f := &Flow{Now: func() time.Time { return now }}
	balance := 0.5
	obs := Observation{Kind: KindAuto, MeasuredAt: now, Buckets: []Bucket{{Window: "5h", UsedPct: 10, Scope: "account"}}, Balance: &balance}

	err := f.publish(t.Context(), client, testKey, "holder", obs)
	var status *StatusError
	if !errors.As(err, &status) || status.Status != http.StatusBadRequest {
		t.Fatalf("publish err = %v, want the store's 400 as a publish error", err)
	}
	mu.Lock()
	got := append([]string(nil), bodies...)
	mu.Unlock()
	if len(got) != 1 || !strings.Contains(got[0], `"balance"`) {
		t.Fatalf("publish bodies = %v, want one attempt with balance and no republish", got)
	}
	if obs.Balance == nil || *obs.Balance != balance {
		t.Fatal("the local observation lost its balance")
	}

	// A balance-less observation the store accepts publishes cleanly.
	obs.Balance = nil
	if err := f.publish(t.Context(), client, testKey, "holder", obs); err != nil {
		t.Fatalf("balance-less publish: %v", err)
	}
	mu.Lock()
	got = append([]string(nil), bodies...)
	mu.Unlock()
	if len(got) != 2 || strings.Contains(got[1], `"balance"`) {
		t.Fatalf("publish bodies = %v, want the balance-less observation as the second", got)
	}
}

// The observation wire shape carries the balance.
func TestObservationBalanceRoundTrip_t5138(t *testing.T) {
	balance := 17.5
	data, err := json.Marshal(Observation{Kind: KindAuto, Balance: &balance})
	if err != nil || !strings.Contains(string(data), `"balance":17.5`) {
		t.Fatalf("encoded observation = %s, %v", data, err)
	}
	var back Observation
	if err := json.Unmarshal(data, &back); err != nil || back.Balance == nil || *back.Balance != balance {
		t.Fatalf("decoded balance = %v, %v", back.Balance, err)
	}
}

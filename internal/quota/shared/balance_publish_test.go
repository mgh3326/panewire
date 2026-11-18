package shared

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// hk 5138: an older handoffkeep rejects unknown observation fields, so a
// commandcode reading that carries `balance` fails publish. The flow must
// republish the same observation without balance, keep the local reading's
// balance, warn once, and retry the field after an hour.
func TestFlowPublishesBalanceOnlyWhenTheStoreAcceptsIt_t5138(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 40, 0, 0, time.UTC)
	var mu sync.Mutex
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/lease"):
			_, _ = io.WriteString(w, `{"holder":"h","acquired":true,"expires_at":"2099-01-01T00:00:00Z"}`)
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/observation"):
			raw, _ := io.ReadAll(r.Body)
			mu.Lock()
			bodies = append(bodies, string(raw))
			mu.Unlock()
			if bytes.Contains(raw, []byte(`"balance"`)) {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	client := Client{BaseURL: server.URL}
	f := &Flow{Now: func() time.Time { return now }}
	var warns []string
	f.Warn = func(format string, args ...any) { warns = append(warns, format) }
	balance := 0.5
	obs := Observation{Kind: KindAuto, MeasuredAt: now, Buckets: []Bucket{{Window: "5h", UsedPct: 10, Scope: "account"}}, Balance: &balance}

	if err := f.publish(t.Context(), client, testKey, "holder", obs); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := append([]string(nil), bodies...)
	mu.Unlock()
	if len(got) != 2 || !strings.Contains(got[0], `"balance"`) || strings.Contains(got[1], `"balance"`) {
		t.Fatalf("publish bodies = %v, want a balance attempt then the same observation without it", got)
	}
	if len(warns) != 1 {
		t.Fatalf("warns = %v, want exactly one", warns)
	}
	if obs.Balance == nil || *obs.Balance != balance {
		t.Fatal("the local observation lost its balance")
	}

	// Remembered for the process: the next publication omits the field
	// without another refusal, and does not warn again.
	if err := f.publish(t.Context(), client, testKey, "holder", obs); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got = append([]string(nil), bodies...)
	mu.Unlock()
	if len(got) != 3 || strings.Contains(got[2], `"balance"`) {
		t.Fatalf("remembered publish bodies = %v, want one body without balance", got)
	}
	if len(warns) != 1 {
		t.Fatalf("warns after remembered refusal = %v, want one", warns)
	}

	// Retried after an hour.
	now = now.Add(balanceRetry)
	if err := f.publish(t.Context(), client, testKey, "holder", obs); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got = append([]string(nil), bodies...)
	mu.Unlock()
	if len(got) != 5 || !strings.Contains(got[3], `"balance"`) || strings.Contains(got[4], `"balance"`) {
		t.Fatalf("retry bodies = %v, want the field tried and dropped again", got)
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

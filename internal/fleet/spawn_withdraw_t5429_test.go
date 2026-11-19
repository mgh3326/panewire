package fleet

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// withdrawFakeHub answers the features read with features and the withdrawal
// POST with answer (hk 5429).
func withdrawFakeHub(t *testing.T, features []string, status int, answer map[string]any, posted *atomic.Int32, got *map[string]string) *hubHTTP {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/spawns/features":
			json.NewEncoder(w).Encode(map[string]any{"features": features})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/spawns/d676220b-e763-4af1-aefd-000000000001/withdraw":
			posted.Add(1)
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, got)
			w.WriteHeader(status)
			json.NewEncoder(w).Encode(answer)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return &hubHTTP{base: server.URL, token: "fixture", client: server.Client()}
}

const withdrawFixtureRequest = "d676220b-e763-4af1-aefd-000000000001"

func TestSpawnWithdrawRequiresTheFeature_t5429(t *testing.T) {
	var posted atomic.Int32
	var got map[string]string
	c := withdrawFakeHub(t, []string{"spawn_expires_v1"}, http.StatusOK, nil, &posted, &got)
	_, err := c.WithdrawSpawn(t.Context(), withdrawFixtureRequest, "design-lane", "parked too long")
	if ErrorCode(err) != exitConfig || !strings.Contains(err.Error(), spawnWithdrawFeature) {
		t.Fatalf("hub without the feature: code=%d err=%v", ErrorCode(err), err)
	}
	if posted.Load() != 0 {
		t.Fatalf("withdrawal sent to a hub without the feature")
	}
}

func TestSpawnWithdrawSendsOneCancelledWithdrawal_t5429(t *testing.T) {
	var posted atomic.Int32
	var got map[string]string
	answer := map[string]any{"request_id": withdrawFixtureRequest, "job_id": "test-job", "status": "cancelled", "reason": "requester_withdrawn: parked too long"}
	c := withdrawFakeHub(t, []string{spawnWithdrawFeature}, http.StatusOK, answer, &posted, &got)
	result, err := c.WithdrawSpawn(t.Context(), withdrawFixtureRequest, "design-lane", "parked too long")
	if err != nil {
		t.Fatalf("withdraw: %v", err)
	}
	if result.Status != "cancelled" || result.Job != "test-job" || result.Request != withdrawFixtureRequest {
		t.Fatalf("withdrawal answer %+v", result)
	}
	if posted.Load() != 1 || got["requester"] != "design-lane" || got["reason"] != "parked too long" {
		t.Fatalf("posted=%d body=%v", posted.Load(), got)
	}
}

func TestSpawnWithdrawRefusalsName_t5429(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		answer map[string]any
		code   int
		text   string
	}{
		{"another lane", http.StatusForbidden, map[string]any{"error": "not_requester", "fix": "withdraw with the lane that submitted the request"}, exitUsage, "did not submit"},
		{"already ended", http.StatusConflict, map[string]any{"error": "spawn_not_pending", "status": "succeeded"}, exitStateConflict, "spawn_not_pending"},
		{"no such request", http.StatusNotFound, map[string]any{"error": "not_found"}, exitUsage, "no spawn request"},
		{"malformed", http.StatusBadRequest, map[string]any{"error": "invalid_request", "fix": "name a request id, the requesting lane and a one-line --reason"}, exitUsage, "one-line --reason"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var posted atomic.Int32
			var got map[string]string
			c := withdrawFakeHub(t, []string{spawnWithdrawFeature}, test.status, test.answer, &posted, &got)
			_, err := c.WithdrawSpawn(t.Context(), withdrawFixtureRequest, "design-lane", "parked too long")
			if ErrorCode(err) != test.code || !strings.Contains(err.Error(), test.text) {
				t.Fatalf("code=%d err=%v, want code %d naming %q", ErrorCode(err), err, test.code, test.text)
			}
		})
	}
}

func TestSpawnWithdrawReasonIsOneLine_t5429(t *testing.T) {
	for reason, want := range map[string]bool{
		"parked too long":        true,
		"":                       false,
		"two\nlines":             false,
		"tab\there":              false,
		strings.Repeat("x", 256): true,
		strings.Repeat("x", 257): false,
	} {
		if got := withdrawReasonOK(reason); got != want {
			t.Fatalf("withdrawReasonOK(%q)=%v, want %v", reason, got, want)
		}
	}
}

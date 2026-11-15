package fleet

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// hk 4691: a caller that owns the placement spool (the hub's outbox) learns why
// a record is still pending, or what hk said when it refused the record for good.

func outboxRecord() PlacementRecord {
	return PlacementRecord{JobID: "job-a", Attempt: 1, TaskID: 42, DecidedAt: "2026-01-01T00:00:00Z", Source: "auto", Ranking: "unknown", StateSource: "unknown",
		Outcome: "placed", Candidates: []PlacementCandidateRecord{}, PlacementExecution: PlacementExecution{Host: "host-b", Family: "anthropic", Role: "worker", Model: "m", PaneID: "w:1"}}
}

func outboxApp(t *testing.T, status int, body string) *App {
	t.Helper()
	a := testApp(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", hkSvcTestToken)
	return a
}

func TestEnqueuePlacementRecordReportsDeliveryOutcome_t4691(t *testing.T) {
	cases := []struct {
		name        string
		status      int
		body        string
		delivered   bool
		quarantined bool
		duplicate   bool
		reason      string
	}{
		{"posted", http.StatusOK, `{"status":"recorded"}`, true, false, false, ""},
		{"already held", http.StatusConflict, `{"error":"duplicate"}`, true, false, true, ""},
		{"refused for good", http.StatusUnprocessableEntity, `{"error":"invalid_placement","reason":"tool_version is not a release"}`, true, true, false, "hk HTTP 422 invalid_placement: tool_version is not a release"},
		{"token without the grant", http.StatusForbidden, `{"error":"forbidden","reason":"token has no host grant"}`, false, false, false, "hk HTTP 403 forbidden: token has no host grant"},
		{"outage", http.StatusServiceUnavailable, `{}`, false, false, false, "hk HTTP 503"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := outboxApp(t, tc.status, tc.body)
			got, err := a.EnqueuePlacementRecord(context.Background(), outboxRecord())
			if err != nil {
				t.Fatal(err)
			}
			if got.Delivered != tc.delivered || got.Quarantined != tc.quarantined || got.Duplicate != tc.duplicate || got.Reason != tc.reason {
				t.Fatalf("got %+v, want delivered=%v quarantined=%v duplicate=%v reason=%q", got, tc.delivered, tc.quarantined, tc.duplicate, tc.reason)
			}
			if tc.delivered == a.PlacementSpooled("job-a", 1) {
				t.Fatalf("spooled=%v after delivered=%v", a.PlacementSpooled("job-a", 1), tc.delivered)
			}
			if tc.status == http.StatusForbidden {
				// A refusal that is not permanent keeps the record for the next flush.
				if again := a.FlushPlacementRecord(context.Background(), "job-a", 1); again.Delivered || again.Reason != tc.reason {
					t.Fatalf("retry: %+v", again)
				}
			}
		})
	}
}

func TestEnqueuePlacementRecordNamesAMissingCredential_t4691(t *testing.T) {
	a := testApp(t) // testApp points the credential at an absent file
	got, err := a.EnqueuePlacementRecord(context.Background(), outboxRecord())
	if err != nil {
		t.Fatal(err)
	}
	if got.Delivered || !strings.HasPrefix(got.Reason, "hk_credential_unavailable") {
		t.Fatalf("got %+v", got)
	}
	if strings.Contains(got.Reason, hkSvcTestToken) {
		t.Fatal("the reason carries a credential")
	}
}

func TestFlushPlacementRecordNamesAnUnsafeSpool_t4691(t *testing.T) {
	a := outboxApp(t, http.StatusOK, `{}`)
	real := t.TempDir()
	link := filepath.Join(t.TempDir(), "state")
	if err := os.Symlink(real, link); err != nil {
		t.Skip("symlinks unavailable: " + err.Error())
	}
	dir := filepath.Join(real, "placements")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	// A record spooled before the state directory moved behind a symlink stays
	// put, and the flush says why instead of returning silently.
	if err := os.WriteFile(filepath.Join(dir, "job-a.1.json"), []byte(`{"record":{"job_id":"job-a","attempt":1}}`), 0600); err != nil {
		t.Fatal(err)
	}
	a.StateDir = link
	got := a.FlushPlacementRecord(context.Background(), "job-a", 1)
	if got.Delivered || got.Reason != "spool_path_refused" {
		t.Fatalf("got %+v", got)
	}
}

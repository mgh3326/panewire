package panewire

import (
	"net/http"
	"path/filepath"
	"testing"
)

// Reap (internal/fleet/hygiene.go removeLane) runs
// `lanes rm <lane> --expect-machine M --expect-pane P`. The parser refused it
// as "invalid self-check flag", so every reaped lane stayed on the hub.
func TestLanesRmAcceptsTheReapGuard(t *testing.T) {
	options, err := parseLanesCLI([]string{"rm", "job-a", "--expect-machine", "mac-dev", "--expect-pane", "wF:p13X"})
	if err != nil {
		t.Fatalf("reap's rm refused: %v", err)
	}
	if options.Lane != "job-a" || options.ExpectedMachine != "mac-dev" || options.ExpectedPane != "wF:p13X" {
		t.Fatalf("options = %+v", options)
	}
	for _, args := range [][]string{
		{"rm", "job-a", "--expect-epoch", "3"},
		{"rm", "job-a", "--lane", "job-b"},
		{"add", "job-a", "--machine", "m", "--pane", "p", "--expect-pane", "p"},
	} {
		if _, err := parseLanesCLI(args); err == nil {
			t.Fatalf("%v accepted", args)
		}
	}
}

// The hub removes a lane only while it still routes where the reaper expects;
// a lane a newer attempt re-registered is kept and answered 409.
func TestLanesDeleteHonorsExpectedRoute(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		code        int
		removed     bool
	}{
		{name: "matching route", query: "?expect_machine=machine-a&expect_pane=w1:p1", code: http.StatusOK, removed: true},
		{name: "pane re-registered", query: "?expect_machine=machine-a&expect_pane=w9:p9", code: http.StatusConflict},
		{name: "other machine", query: "?expect_machine=machine-b", code: http.StatusConflict},
		{name: "no guard", query: "", code: http.StatusOK, removed: true},
		{name: "invalid machine", query: "?expect_machine=bad%20id", code: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "lanes.json")
			lanesProjectionWrite(t, path, `{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1"}}}`)
			hub := lanesWriteHub(t, path)
			remove := lanesWriteRequest(t, hub, http.MethodDelete, "/v1/lanes/lane-a"+tc.query, "fixture-operator-token", "")
			if remove.Code != tc.code {
				t.Fatalf("status=%d body=%s, want %d", remove.Code, remove.Body.String(), tc.code)
			}
			left := lanesProjectionResponse(t, lanesProjectionGet(hub, "fixture-operator-token"))
			if gone := len(left) == 0; gone != tc.removed {
				t.Fatalf("lanes after delete = %v, want removed=%v", left, tc.removed)
			}
		})
	}
}

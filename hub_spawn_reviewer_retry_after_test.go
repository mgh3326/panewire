package panewire

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"
)

// A reviewer held only by the landing grace says when it frees, so a caller
// can retry then instead of counting the 409 as a failure (hk 3353). Every
// other hold carries no time, because nothing time-based will free it.
func TestReviewerHoldRetryAfter(t *testing.T) {
	const cwd = "/work/project.test-job"
	succeeded := hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}
	failedLaunch := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", Pane: roleHeldPane}
	cases := []struct {
		name    string
		role    string
		status  string
		outcome hubSpawnOutcome
		pane    string
		// since is how long after the holder's deadline the new submission
		// arrives; the grace is ten minutes.
		since time.Duration
		// wantRetry is retry_after_s; zero means the 409 carries none.
		wantRetry int
		// wantReleased expects the new submission to claim the slot.
		wantReleased bool
	}{
		{name: "an idle reviewer inside the grace", role: "reviewer", status: "succeeded", outcome: succeeded, pane: "idle", since: 7 * time.Minute, wantRetry: 180},
		{name: "a done reviewer inside the grace", role: "reviewer", status: "succeeded", outcome: succeeded, pane: "done", since: 9 * time.Minute, wantRetry: 60},
		{name: "a fraction of a second rounds up", role: "reviewer", status: "succeeded", outcome: succeeded, pane: "done", since: 10*time.Minute - 400*time.Millisecond, wantRetry: 1},
		{name: "a blocked failed reviewer inside the grace", role: "reviewer", status: "failed", outcome: failedLaunch, pane: "blocked", since: 9 * time.Minute, wantRetry: 60},
		{name: "past the grace releases", role: "reviewer", status: "succeeded", outcome: succeeded, pane: "done", since: 11 * time.Minute, wantReleased: true},
		{name: "a worker holder", role: "worker", status: "succeeded", outcome: succeeded, pane: "done", since: 9 * time.Minute},
		{name: "a working reviewer pane", role: "reviewer", status: "succeeded", outcome: succeeded, pane: "working", since: 9 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hub, server, held := reviewerReleaseHub(t, tc.role, tc.status, tc.outcome)
			now := held.Deadline.Add(tc.since)
			hub.now = func() time.Time { return now }
			startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(unworkedRow(tc.pane, cwd), unworkedLedger(0)))
			result := t14Result(t, t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, reviewerReleaseRequest(tc.role, true)))
			if tc.wantReleased {
				if result.status != http.StatusAccepted || result.header.Get("Retry-After") != "" {
					t.Fatalf("not released cleanly: status=%d header=%v body=%s", result.status, result.header, result.body)
				}
				return
			}
			var body map[string]any
			if err := json.Unmarshal(result.body, &body); err != nil || result.status != http.StatusConflict || body["error"] != "task_role_claimed" {
				t.Fatalf("status=%d body=%s err=%v", result.status, result.body, err)
			}
			retry, present := body["retry_after_s"]
			header := result.header.Get("Retry-After")
			if tc.wantRetry == 0 {
				if present || header != "" {
					t.Fatalf("retry_after_s=%v Retry-After=%q on a hold nothing time-based frees", retry, header)
				}
				return
			}
			// JSON numbers decode to float64; the contract is an integer.
			if retry != float64(tc.wantRetry) || header != strconv.Itoa(tc.wantRetry) {
				t.Fatalf("retry_after_s=%v Retry-After=%q, want %d", retry, header, tc.wantRetry)
			}
		})
	}
}

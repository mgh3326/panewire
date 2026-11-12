package fleet

import (
	"context"
	"strings"
	"testing"
)

func TestRestoreRejectsCompletionIDBeforeReclaim(t *testing.T) {
	a := testApp(t)
	f := ledgerRunner(t, a)
	seedJob(t, a, "j", "worker")
	seedLedger(t, a, "j")
	if err := a.releaseQuota(context.Background(), Job{ID: "j"}); err != nil {
		t.Fatal(err)
	}
	f.Do = func(c Command) Result {
		if strings.Join(c.Args, " ") == "agent list" {
			return Result{Out: `{"result":{"agents":[{"pane_id":"w:p1","tab_id":"w:t1","name":"agent-a","agent_status":"working"}]}}`}
		}
		return Result{Code: 1}
	}

	err := a.lifecycle(context.Background(), "restore", []string{"j", "--completion-id", "ignored"})
	if code(err) != exitUsage || !strings.Contains(err.Error(), "accepts no --completion-id") {
		t.Fatalf("error=%v, want restore to reject --completion-id", err)
	}
	if state, _ := ledgerState(t, a, "j"); state != "released" {
		t.Fatalf("job state=%q, want released after rejecting the flag", state)
	}
}

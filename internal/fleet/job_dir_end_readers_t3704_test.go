package fleet

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// hk 3704: once the at-end pass has deleted a job's dir, the readers that run
// over the jobs root must answer for the missing dir rather than fail. The
// reap CLI is the hygiene reader; the quota pool and the close operation have
// their own missing-dir tests (node_quota_pool_reconcile_test.go,
// node_pane_close_missing_t5296_test.go).

func TestJobDirEndReapReaderAfterTheDelete(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	r.seedJob("j2")
	if got := (&JobDirTTL{RelayOwed: relayOwes(false, nil)}).ExpireEnded(context.Background(), r.a, "j1"); got != "confirmed" {
		t.Fatalf("at-end reason = %q, want confirmed", got)
	}

	for _, args := range [][]string{
		{"--apply"},
		{"--expire-job-dirs", "--job", "j1"},
		{"--apply", "--expire-job-dirs", "--job", "j1"},
	} {
		var out bytes.Buffer
		r.a.Out = &out
		if err := r.a.hygiene(context.Background(), "reap", args); err != nil {
			t.Fatalf("reap %v after the at-end delete: %v", args, err)
		}
		if strings.Contains(out.String(), "job=j1 ") && strings.Contains(out.String(), "action=delete") {
			t.Fatalf("reap %v deleted j1 again:\n%s", args, out.String())
		}
	}
	if !r.exists("j2") {
		t.Fatal("j2 was touched by reaps that named only j1")
	}
}

package fleet

// hk 3425: worker completions are records, not design-lane notices. `done`
// keeps writing the hk record for mergewake, and a claim failure whose task
// already holds the job no longer asks anyone to link it manually.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSpawnTaskLinkNoticeOnlyWhenTaskStillNeedsIt(t *testing.T) {
	for _, tc := range []struct {
		name   string
		show   string
		notice bool
	}{
		{"refs.job_id names this job", `{"id":42,"title":"implement (grade B, T1)","state":"ready","refs":{"job_id":"job-a"}}`, false},
		{"this lane is the claimant", `{"id":42,"title":"implement (grade B, T1)","state":"ready","claimed_by":"worker-a","refs":{}}`, false},
		{"another job holds it", `{"id":42,"title":"implement (grade B, T1)","state":"ready","refs":{"job_id":"other-job"}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, f, _ := cmdFixture(t)
			s.Owner = "owner"
			base := f.Do
			shows := 0
			f.Do = func(c Command) Result {
				args := strings.Join(c.Args, " ")
				if c.Name == "handoffkeep" && strings.HasPrefix(args, "tasks show") {
					shows++
					// The first show is the precheck. The notice decision reads
					// the task again, after the claim failure.
					if shows > 1 {
						return Result{Out: tc.show}
					}
				}
				if c.Name == "handoffkeep" && strings.HasPrefix(args, "tasks claim") {
					return Result{Code: 17, Err: "conflict"}
				}
				return base(c)
			}
			if err := a.Spawn(context.Background(), s); err != nil {
				t.Fatalf("landed spawn failed: %v", err)
			}
			notices := 0
			for _, c := range f.Calls {
				args := strings.Join(c.Args, " ")
				if c.Name == "panewire" && strings.HasPrefix(args, "emit --kind lane.event") && strings.Contains(args, "--event-id task-link-failed-") {
					notices++
				}
			}
			if tc.notice && notices != 1 || !tc.notice && notices != 0 {
				t.Fatalf("TASK_LINK_FAILED notices=%d, want %v", notices, tc.notice)
			}
		})
	}
}

// `done` for a worker owned by a design lane still writes the hk record (state,
// report_doc) and emits the completion to that owner. Only delivery is
// suppressed, and that is the hub's decision.
func TestDoneToDesignLaneStillWritesRecord(t *testing.T) {
	a := testApp(t)
	f := ledgerRunner(t, a)
	h := newFakeHK(t)
	h.claimJob("j", float64(42), "test-host")
	h.records["j"]["state"] = "spawned"
	a.JobStore = h.client()
	putEvent(t, a, "j", "job.claim", map[string]any{"owner_lane": "panewire-design", "parent_lane": "parent", "task_id": "42", "t_level": "T1", "role": "worker"})
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "tab_id": "w:t1", "label": "agent-a", "profile": "cmd-dsflash41", "family": "deepseek", "keep": false})
	path := filepath.Join(a.Jobs, "j", "report.md")
	if e := os.WriteFile(path, []byte("done\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := a.lifecycle(context.Background(), "done", []string{"j", "--report", path}); e != nil {
		t.Fatal(e)
	}

	rec := h.record(t, "j")
	if str(rec, "state") != "completed" || str(rec, "report_doc") == "" {
		t.Fatalf("hk record=%v", rec)
	}
	relayed := false
	for _, c := range f.Calls {
		args := strings.Join(c.Args, " ")
		if c.Name == "panewire" && strings.HasPrefix(args, "emit --kind job.completed") {
			relayed = true
			if !strings.Contains(args, fmt.Sprintf("--owner-lane %s", "panewire-design")) {
				t.Fatalf("completion emitted to the wrong owner: %s", args)
			}
		}
	}
	if !relayed {
		t.Fatal("no job.completed relay")
	}
}

package fleet

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// hk 3426: a task claim that exits non-zero while hk already links the task
// to this very job (a design session rebound it before the spawn) is not a
// failure. Any other claim failure still notifies the owner lane.

// t3426TaskLinkSpawn runs the cmd fixture spawn with a claim that exits 1 and
// a `tasks show` that answers precheck first, then show for every later read.
func t3426TaskLinkSpawn(t *testing.T, precheck, later Result) (*App, Spawn, *fakeRunner) {
	t.Helper()
	a, s, f, _ := cmdFixture(t)
	s.Owner = "handoffkeep-design"
	base := f.Do
	shows := 0
	f.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch {
		case c.Name == "handoffkeep" && strings.HasPrefix(args, "tasks claim"):
			return Result{Code: 1, Err: "conflict: task 42 is already claimed"}
		case c.Name == "handoffkeep" && strings.HasPrefix(args, "tasks show"):
			shows++
			if shows == 1 {
				return precheck
			}
			return later
		}
		return base(c)
	}
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatalf("landed spawn failed: %v", err)
	}
	if !strings.Contains(a.Out.(*bytes.Buffer).String(), "landed=yes") {
		t.Fatal("landing receipt missing")
	}
	return a, s, f
}

func t3426TaskLinkEvents(t *testing.T, a *App, s Spawn) []map[string]any {
	t.Helper()
	events, err := a.ledger().Events(context.Background(), s.Job)
	if err != nil {
		t.Fatal(err)
	}
	var links []map[string]any
	for _, ev := range events {
		if ev.Kind == "job.task_link" {
			links = append(links, ev.Payload)
		}
	}
	return links
}

func t3426TaskLinkNotices(f *fakeRunner) []string {
	var notices []string
	for _, c := range f.Calls {
		args := strings.Join(c.Args, " ")
		if c.Name == "panewire" && strings.HasPrefix(args, "emit ") && strings.Contains(args, "task-link-failed-") {
			notices = append(notices, args)
		}
	}
	return notices
}

const t3426LinkedToJob = `{"id":42,"title":"implement (grade B, T1)","state":"in_progress","claimed_by":"handoffkeep-design","refs":{"job_id":"job-a"}}`

func TestSpawnTaskLinkAlreadyLinkedToThisJobT3426(t *testing.T) {
	a, s, f := t3426TaskLinkSpawn(t, Result{Out: t3426LinkedToJob}, Result{Out: t3426LinkedToJob})
	links := t3426TaskLinkEvents(t, a, s)
	if len(links) != 1 || links[0]["status"] != "already_linked" {
		t.Fatalf("job.task_link=%v, want one already_linked", links)
	}
	if _, ok := links[0]["reason"]; ok {
		t.Fatalf("already_linked carries a failure reason: %v", links[0])
	}
	if notices := t3426TaskLinkNotices(f); len(notices) != 0 {
		t.Fatalf("TASK_LINK_FAILED emitted for a task already linked to this job: %v", notices)
	}
	if stderr := a.Err.(*bytes.Buffer).String(); strings.Contains(stderr, "link the task manually") || strings.Contains(stderr, "task claim exited") {
		t.Fatalf("warning printed: %s", stderr)
	}
}

func TestSpawnTaskLinkClaimFailureStillNotifiesT3426(t *testing.T) {
	unlinked := `{"id":42,"title":"implement (grade B, T1)","state":"ready","refs":{}}`
	for _, tc := range []struct {
		name  string
		later Result
	}{
		{"linked to another job", Result{Out: `{"id":42,"title":"implement (grade B, T1)","state":"in_progress","claimed_by":"worker-b","refs":{"job_id":"job-b"}}`}},
		{"task read fails", Result{Code: 1, Err: "handoffkeep unavailable"}},
		{"task read unparseable", Result{Out: "not json"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, f := t3426TaskLinkSpawn(t, Result{Out: unlinked}, tc.later)
			links := t3426TaskLinkEvents(t, a, s)
			if len(links) != 1 || links[0]["status"] != "failed" || links[0]["reason"] != "task claim exited with status 1" {
				t.Fatalf("job.task_link=%v, want one failed", links)
			}
			notices := t3426TaskLinkNotices(f)
			if len(notices) != 1 {
				t.Fatalf("notices=%v, want one TASK_LINK_FAILED", notices)
			}
			for _, want := range []string{"--lane handoffkeep-design", "--type automation-failed", "TASK_LINK_FAILED job=job-a task=42 reason=task claim exited with status 1; link the task manually"} {
				if !strings.Contains(notices[0], want) {
					t.Errorf("notice missing %q: %s", want, notices[0])
				}
			}
			if !strings.Contains(a.Err.(*bytes.Buffer).String(), "task claim exited with status 1; link the task manually") {
				t.Fatal("warning missing")
			}
		})
	}
}

package fleet

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"
)

// hk 3426: a task claim that exits non-zero while hk already links the task
// to this very job (a design session rebound it before the spawn) is not a
// failure. Any other claim failure still notifies the owner lane.

// t3426Answer is one hk answer: an HTTP status and its body.
type t3426Answer struct {
	status int
	body   string
}

// t3426TaskLinkSpawn runs the cmd fixture spawn with a claim hk refuses (409)
// and a task read that answers precheck first, then later for every later
// read.
func t3426TaskLinkSpawn(t *testing.T, precheck, later t3426Answer) (*App, Spawn, *fakeRunner) {
	t.Helper()
	a, s, f, _ := cmdFixture(t)
	s.Owner = "handoffkeep-design"
	shows := 0
	useFakeHK(t).Handle(func(r hkRequest) (int, string, bool) {
		switch r.Route() {
		case "POST /v1/tasks/42/claim":
			return http.StatusConflict, `{"error":"conflict","reason":"task 42 is already claimed"}`, true
		case "GET /v1/tasks/42":
			shows++
			if shows == 1 {
				return precheck.status, precheck.body, true
			}
			return later.status, later.body, true
		}
		return 0, "", false
	})
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
	a, s, f := t3426TaskLinkSpawn(t, t3426Answer{http.StatusOK, t3426LinkedToJob}, t3426Answer{http.StatusOK, t3426LinkedToJob})
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
		later t3426Answer
	}{
		{"linked to another job", t3426Answer{http.StatusOK, `{"id":42,"title":"implement (grade B, T1)","state":"in_progress","claimed_by":"worker-b","refs":{"job_id":"job-b"}}`}},
		{"task read fails", t3426Answer{http.StatusServiceUnavailable, `{"error":"unavailable"}`}},
		{"task read unparseable", t3426Answer{http.StatusOK, "not json"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, f := t3426TaskLinkSpawn(t, t3426Answer{http.StatusOK, unlinked}, tc.later)
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

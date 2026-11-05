package fleet

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestSpawnTaskClaimAfterDelivery(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fail   bool
		status string
	}{{"failed", true, "failed"}, {"linked", false, "linked"}} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, f, submitted := cmdFixture(t)
			s.Owner = "owner"
			base := f.Do
			f.Do = func(c Command) Result {
				if c.Name == "handoffkeep" && strings.HasPrefix(strings.Join(c.Args, " "), "tasks claim") && tc.fail {
					if *submitted != 2 {
						t.Fatal("claim preceded delivery")
					}
					return Result{Code: 17, Err: "token=private-value"}
				}
				return base(c)
			}
			if err := a.Spawn(context.Background(), s); err != nil {
				t.Fatalf("landed spawn failed: %v", err)
			}
			if !strings.Contains(a.Out.(*bytes.Buffer).String(), "landed=yes") {
				t.Fatal("landing receipt missing")
			}
			events, err := a.ledger().Events(context.Background(), s.Job)
			if err != nil {
				t.Fatal(err)
			}
			links := 0
			for _, ev := range events {
				if ev.Kind == "job.task_link" {
					links++
					if ev.Payload["status"] != tc.status {
						t.Fatalf("task link: %v", ev.Payload)
					}
					if tc.fail && ev.Payload["reason"] != "task claim exited with status 17" {
						t.Fatalf("task link reason: %v", ev.Payload)
					}
				}
			}
			if links != 1 {
				t.Fatalf("task link events=%d", links)
			}
			notices := 0
			for _, c := range f.Calls {
				args := strings.Join(c.Args, " ")
				if c.Name == "herdr" && strings.HasPrefix(args, "pane close") {
					t.Fatalf("closed landed pane: %s", args)
				}
				// Only the task-link notice counts; other lane notices (such as
				// an unregistered lane, hk 2888) are not this test's subject.
				if c.Name == "panewire" && strings.HasPrefix(args, "emit --kind lane.event") && strings.Contains(args, "--event-id task-link-failed-") {
					notices++
					for _, want := range []string{"--lane owner", s.Job, s.Task, "task claim exited with status 17"} {
						if !strings.Contains(args, want) {
							t.Errorf("notice missing %q: %s", want, args)
						}
					}
					if strings.Contains(args, "private-value") {
						t.Error("notice leaked stderr")
					}
				}
			}
			if tc.fail && notices != 1 || !tc.fail && notices != 0 {
				t.Fatalf("notices=%d", notices)
			}
			if tc.fail && !strings.Contains(a.Err.(*bytes.Buffer).String(), "task claim exited with status 17") {
				t.Fatal("warning missing")
			}
		})
	}
}

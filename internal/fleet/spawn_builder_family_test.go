package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func claimCalls(f *fakeRunner) int {
	n := 0
	for _, c := range f.Calls {
		if c.Name == "handoffkeep" && strings.HasPrefix(strings.Join(c.Args, " "), "tasks claim") {
			n++
		}
	}
	return n
}

// A reviewer for a task whose author job is not bound (the author was not
// launched through panewire) names only the author's family. It spawns, the
// seat is still checked against that family, and it never claims the task.
func TestReviewerBuilderFamilyWithoutBuilderJob(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Role, s.BuilderFamily = "reviewer", "anthropic"
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	if n := claimCalls(f); n != 0 {
		t.Fatalf("an unlinked reviewer claimed the task %d times", n)
	}

	a, s, f, _ = cmdFixture(t)
	s.Role, s.BuilderFamily = "reviewer", "deepseek"
	if e := a.Spawn(context.Background(), s); code(e) != exitSeatRefused {
		t.Fatalf("a seat of the author's family must still be refused: %v", e)
	}
	if n := claimCalls(f); n != 0 {
		t.Fatalf("claims after refusal: %d", n)
	}
}

// Two review passes on one task both spawn, and the task is left as it was.
// A fixture runs one spawn, so each pass gets its own: pass 1 sees the task
// unbound, pass 2 sees it bound to another job, as it would be had pass 1
// claimed it.
func TestTwoUnlinkedReviewerSpawnsOnOneTask(t *testing.T) {
	for _, tc := range []struct{ id, refs string }{{"mwr-1-pass1", `{}`}, {"mwr-1-pass2", `{"job_id":"mwr-1-pass1"}`}} {
		a, s, f, _ := cmdFixture(t)
		original := f.Do
		f.Do = func(c Command) Result {
			if c.Name == "handoffkeep" && strings.HasPrefix(strings.Join(c.Args, " "), "tasks show") {
				return Result{Out: `{"id":42,"title":"implement (grade B, T1)","state":"ready","refs":` + tc.refs + `}`}
			}
			return original(c)
		}
		s.Role, s.BuilderFamily, s.Label, s.Job = "reviewer", "anthropic", tc.id, tc.id
		if e := a.Spawn(context.Background(), s); e != nil {
			t.Fatalf("%s: %v", tc.id, e)
		}
		if n := claimCalls(f); n != 0 {
			t.Fatalf("%s claimed the task %d times; it must stay unbound", tc.id, n)
		}
	}
}

func TestBuilderFamilyWithoutBuilderJobOnlyForReviewer(t *testing.T) {
	for _, role := range []string{"worker", "tester", "builder"} {
		a, s, f, _ := cmdFixture(t)
		s.Role, s.BuilderFamily = role, "anthropic"
		if e := a.Spawn(context.Background(), s); code(e) != exitUsage {
			t.Errorf("role %s: want exit 2, got %v", role, e)
		}
		if n := claimCalls(f); n != 0 {
			t.Errorf("role %s claimed the task", role)
		}
	}
}

func TestParseSpawnBuilderFamilyAlone(t *testing.T) {
	s, e := parseSpawn([]string{"--builder-family", "anthropic", "--task", "12", "--role", "reviewer"})
	if e != nil || s.BuilderFamily != "anthropic" || s.BuilderJob != "" {
		t.Fatalf("%+v %v", s, e)
	}
}

// Over ssh the Spawn crosses as JSON and BuilderFamily/BuilderJob are not on
// the wire; the receiving host must still see an unlinked reviewer from the
// resolved fields, and a reviewer that inherits a builder job must not.
func TestUnlinkedReviewerSurvivesTheSSHEnvelope(t *testing.T) {
	for _, tc := range []struct {
		name string
		s    Spawn
		want bool
	}{
		{"family only", Spawn{Role: "reviewer", Task: "12", BuilderFamily: "anthropic", ParentFamily: "anthropic"}, true},
		{"with builder job", Spawn{Role: "reviewer", Task: "12", BuilderFamily: "anthropic", BuilderJob: "job-b", ParentFamily: "anthropic", Inherited: "job-b"}, false},
		{"worker", Spawn{Role: "worker", Task: "12", ParentFamily: "anthropic"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, e := json.Marshal(tc.s)
			if e != nil {
				t.Fatal(e)
			}
			var got Spawn
			if e := json.Unmarshal(b, &got); e != nil {
				t.Fatal(e)
			}
			if got.BuilderFamily != "" || got.unlinkedReviewer() != tc.want {
				t.Fatalf("after the envelope: unlinked=%v want %v (%+v)", got.unlinkedReviewer(), tc.want, got)
			}
		})
	}
}

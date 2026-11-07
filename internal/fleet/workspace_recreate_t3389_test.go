package fleet

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

const noWorkspaces = `{"result":{"workspaces":[]}}`

// Herdr deletes a workspace when its last pane closes, so spawn recreates a
// label that matched none; an id cannot be recreated.
func TestResolveWorkspaceCreatesMissingLabel(t *testing.T) {
	a := testApp(t)
	lists, creates := 0, 0
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		switch strings.Join(c.Args, " ") {
		case "workspace list":
			lists++
			if lists == 1 {
				return Result{Out: noWorkspaces}
			}
			return Result{Out: `{"result":{"workspaces":[{"workspace_id":"w2","label":"workers"}]}}`}
		case "workspace create --label workers":
			creates++
			return Result{Out: `{"result":{"workspace":{"workspace_id":"w2"},"tab":{"tab_id":"w2:t1"},"root_pane":{"pane_id":"w2:p0"}}}`}
		}
		t.Fatalf("unexpected herdr call: %v", c.Args)
		return Result{}
	}}
	got, created, err := a.resolveWorkspace(context.Background(), "workers")
	if err != nil {
		t.Fatal(err)
	}
	if got != "w2" || creates != 1 {
		t.Fatalf("workspace=%q creates=%d", got, creates)
	}
	if created == nil || created.id != "w2" || created.label != "workers" || created.rootPane != "w2:p0" {
		t.Fatalf("created=%+v", created)
	}
	if out := a.Out.(*bytes.Buffer).String(); !strings.Contains(out, "workspace_created label=workers workspace=w2") {
		t.Fatalf("log=%q", out)
	}
}

func TestResolveWorkspaceUsesExistingLabel(t *testing.T) {
	a := testApp(t)
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		if strings.Join(c.Args, " ") != "workspace list" {
			t.Fatalf("unexpected herdr call: %v", c.Args)
		}
		return Result{Out: `{"result":{"workspaces":[{"workspace_id":"w1","label":"workers"}]}}`}
	}}
	got, created, err := a.resolveWorkspace(context.Background(), "workers")
	if err != nil || got != "w1" || created != nil {
		t.Fatalf("workspace=%q created=%+v err=%v", got, created, err)
	}
}

func TestResolveWorkspaceRefusesAmbiguousLabel(t *testing.T) {
	a := testApp(t)
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		if strings.Join(c.Args, " ") != "workspace list" {
			t.Fatalf("unexpected herdr call: %v", c.Args)
		}
		return Result{Out: `{"result":{"workspaces":[{"workspace_id":"w1","label":"reviews"},{"workspace_id":"w2","label":"reviews"}]}}`}
	}}
	_, _, err := a.resolveWorkspace(context.Background(), "reviews")
	if exitCode(err) != exitUsage || !strings.Contains(err.Error(), "matched 2 entries") {
		t.Fatalf("exit=%d err=%v", exitCode(err), err)
	}
}

// Two spawns can race: both see no match, both create. The re-list picks the
// oldest and closes nothing.
func TestResolveWorkspaceUsesOldestAfterCreateRace(t *testing.T) {
	a := testApp(t)
	lists := 0
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		switch strings.Join(c.Args, " ") {
		case "workspace list":
			lists++
			if lists == 1 {
				return Result{Out: noWorkspaces}
			}
			return Result{Out: `{"result":{"workspaces":[{"workspace_id":"w1","label":"workers"},{"workspace_id":"w2","label":"workers"}]}}`}
		case "workspace create --label workers":
			return Result{Out: `{"result":{"workspace":{"workspace_id":"w2"},"root_pane":{"pane_id":"w2:p0"}}}`}
		}
		t.Fatalf("unexpected herdr call: %v", c.Args)
		return Result{}
	}}
	got, created, err := a.resolveWorkspace(context.Background(), "workers")
	if err != nil {
		t.Fatal(err)
	}
	if got != "w1" {
		t.Fatalf("workspace=%q, want the oldest", got)
	}
	if created == nil || created.id != "w2" || created.rootPane != "w2:p0" {
		t.Fatalf("created=%+v, want this call's own workspace", created)
	}
	if out := a.Out.(*bytes.Buffer).String(); !strings.Contains(out, "workspace_created label=workers workspace=w1 duplicates=1") {
		t.Fatalf("log=%q", out)
	}
}

func TestResolveWorkspaceRefusesVanishedID(t *testing.T) {
	a := testApp(t)
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		if strings.Join(c.Args, " ") != "workspace list" {
			t.Fatalf("unexpected herdr call: %v", c.Args)
		}
		return Result{Out: noWorkspaces}
	}}
	_, _, err := a.resolveWorkspace(context.Background(), "w1Q")
	if exitCode(err) != exitUsage || !strings.Contains(err.Error(), `workspace = "workers"`) || !strings.Contains(err.Error(), "label") {
		t.Fatalf("exit=%d err=%v", exitCode(err), err)
	}
	if got := SpawnRefusalCode(err.Error(), exitCode(err)); got != "workspace_unresolved" {
		t.Fatalf("refusal code=%q", got)
	}
}

func TestResolveWorkspaceUsesListedID(t *testing.T) {
	a := testApp(t)
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		if strings.Join(c.Args, " ") != "workspace list" {
			t.Fatalf("unexpected herdr call: %v", c.Args)
		}
		return Result{Out: `{"result":{"workspaces":[{"workspace_id":"w1Q","label":"workers"}]}}`}
	}}
	got, created, err := a.resolveWorkspace(context.Background(), "w1Q")
	if err != nil || got != "w1Q" || created != nil {
		t.Fatalf("workspace=%q created=%+v err=%v", got, created, err)
	}
}

// A failed spawn must not delete a workspace another spawn may have adopted.
// Resolve picks the oldest match, so only a non-oldest duplicate is safe to
// close; the oldest is kept because an adopter may not have created its pane.
func TestSpawnWorkspaceCleanupOnFailure(t *testing.T) {
	for _, test := range []struct {
		name       string
		workspaces string
		closed     bool
	}{
		{"oldest kept", `[{"workspace_id":"w2","label":"workers"}]`, false},
		{"non-oldest duplicate closed", `[{"workspace_id":"w1","label":"workers"},{"workspace_id":"w2","label":"workers"}]`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, s, f, _ := cmdFixture(t)
			s.Workspace = "workers"
			store := a.JobStore.(*fakeJobStore)
			store.records[s.Job] = map[string]any{"job_id": s.Job, "task_id": "other", "host": "other", "state": "claimed"}
			previous := f.Do
			created, closed := false, false
			f.Do = func(c Command) Result {
				switch strings.Join(c.Args, " ") {
				case "workspace list":
					if !created {
						return Result{Out: noWorkspaces}
					}
					return Result{Out: `{"result":{"workspaces":` + test.workspaces + `}}`}
				case "workspace create --label workers":
					created = true
					return Result{Out: `{"result":{"workspace":{"workspace_id":"w2"},"root_pane":{"pane_id":"w2:p0"}}}`}
				case "pane close w2:p0":
					closed = true
					return Result{}
				}
				return previous(c)
			}
			if err := a.Spawn(context.Background(), s); exitCode(err) != exitDuplicate {
				t.Fatalf("spawn exit=%d err=%v", exitCode(err), err)
			}
			if !created || closed != test.closed {
				t.Fatalf("created=%t root pane closed=%t, want %t", created, closed, test.closed)
			}
		})
	}
}

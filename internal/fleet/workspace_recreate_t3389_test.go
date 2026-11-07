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
			return Result{Out: `{"result":{"workspace":{"workspace_id":"w2"},"tab":{"tab_id":"w2:t1"},"root_pane":{"pane_id":"w2:p1"}}}`}
		}
		t.Fatalf("unexpected herdr call: %v", c.Args)
		return Result{}
	}}
	got, err := a.resolveWorkspace(context.Background(), "workers")
	if err != nil {
		t.Fatal(err)
	}
	if got != "w2" || creates != 1 {
		t.Fatalf("workspace=%q creates=%d", got, creates)
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
	got, err := a.resolveWorkspace(context.Background(), "workers")
	if err != nil || got != "w1" {
		t.Fatalf("workspace=%q err=%v", got, err)
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
	_, err := a.resolveWorkspace(context.Background(), "reviews")
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
			return Result{Out: `{"result":{"workspace":{"workspace_id":"w2"}}}`}
		}
		t.Fatalf("unexpected herdr call: %v", c.Args)
		return Result{}
	}}
	got, err := a.resolveWorkspace(context.Background(), "workers")
	if err != nil {
		t.Fatal(err)
	}
	if got != "w1" {
		t.Fatalf("workspace=%q, want the oldest", got)
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
	_, err := a.resolveWorkspace(context.Background(), "w1Q")
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
	got, err := a.resolveWorkspace(context.Background(), "w1Q")
	if err != nil || got != "w1Q" {
		t.Fatalf("workspace=%q err=%v", got, err)
	}
}

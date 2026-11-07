package fleet

import (
	"bytes"
	"context"
	"fmt"
	"slices"
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
	if created == nil || created.workspace != "w2" || created.tab != "w2:t1" {
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
			return Result{Out: `{"result":{"workspace":{"workspace_id":"w2"},"tab":{"tab_id":"w2:t1"},"root_pane":{"pane_id":"w2:p0"}}}`}
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
	if created == nil || created.workspace != "w2" || created.tab != "w2:t1" {
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

// A failed spawn removes only the tab it created. A workspace another spawn
// adopted keeps that spawn's tab and live session, even when the list is
// unavailable after the create.
func TestSpawnWorkspaceCleanupOnFailure(t *testing.T) {
	for _, test := range []struct {
		name       string
		workspaces string
		listError  bool
	}{
		{"oldest", `[{"workspace_id":"w2","label":"workers"}]`, false},
		{"duplicate", `[{"workspace_id":"w1","label":"workers"},{"workspace_id":"w2","label":"workers"}]`, false},
		{"listing unavailable", "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, s, f, _ := cmdFixture(t)
			s.Workspace = "workers"
			store := a.JobStore.(*fakeJobStore)
			store.records[s.Job] = map[string]any{"job_id": s.Job, "task_id": "other", "host": "other", "state": "claimed"}
			previous := f.Do
			created, closed := false, ""
			f.Do = func(c Command) Result {
				switch strings.Join(c.Args, " ") {
				case "workspace list":
					if !created {
						return Result{Out: noWorkspaces}
					}
					if test.listError {
						return Result{Code: 1, Err: "fixture workspace list failure"}
					}
					return Result{Out: `{"result":{"workspaces":` + test.workspaces + `}}`}
				case "workspace create --label workers":
					created = true
					return Result{Out: `{"result":{"workspace":{"workspace_id":"w2"},"tab":{"tab_id":"w2:t1"},"root_pane":{"pane_id":"w2:p0"}}}`}
				}
				if strings.HasPrefix(strings.Join(c.Args, " "), "tab close") {
					closed = strings.Join(c.Args, " ")
					return Result{}
				}
				return previous(c)
			}
			if err := a.Spawn(context.Background(), s); exitCode(err) != exitDuplicate {
				t.Fatalf("spawn exit=%d err=%v", exitCode(err), err)
			}
			if !created || closed != "tab close w2:t1" {
				t.Fatalf("created=%t close=%q", created, closed)
			}
		})
	}
}

// fakeHerdr models the parts of herdr's workspace, tab and pane lifecycle spawn
// depends on (herdr v0.9.5 src/workspace.rs, src/app/api/tabs.rs): a tab close
// closes the workspace when that tab was the last, and a pane close removes its
// tab when that tab holds only that pane.
type fakeHerdr struct {
	order []string            // workspace ids, oldest first
	tabs  map[string][]string // workspace id -> tab ids in order
	panes map[string][]string // tab id -> pane ids
}

func newFakeHerdr() *fakeHerdr {
	return &fakeHerdr{tabs: map[string][]string{}, panes: map[string][]string{}}
}

func (h *fakeHerdr) add(workspace string, tabs ...string) {
	h.order = append(h.order, workspace)
	for _, tab := range tabs {
		h.tabs[workspace] = append(h.tabs[workspace], tab)
		h.panes[tab] = []string{tab + "-pane"}
	}
}

// closeTab removes a tab, and the workspace with it when it was the last tab.
func (h *fakeHerdr) closeTab(tab string) bool {
	for ws, ids := range h.tabs {
		for i, id := range ids {
			if id != tab {
				continue
			}
			h.tabs[ws] = append(append([]string{}, ids[:i]...), ids[i+1:]...)
			delete(h.panes, tab)
			if len(h.tabs[ws]) == 0 {
				delete(h.tabs, ws)
				h.order = slices.DeleteFunc(h.order, func(w string) bool { return w == ws })
			}
			return true
		}
	}
	return false
}

// closePane removes a pane; herdr removes its tab when it was that tab's only
// pane, which in turn closes the workspace when the tab was the last one.
func (h *fakeHerdr) closePane(pane string) bool {
	for tab, panes := range h.panes {
		for i, id := range panes {
			if id != pane {
				continue
			}
			h.panes[tab] = append(append([]string{}, panes[:i]...), panes[i+1:]...)
			if len(h.panes[tab]) == 0 {
				return h.closeTab(tab)
			}
			return true
		}
	}
	return false
}

func (h *fakeHerdr) closeWorkspace(workspace string) bool {
	tab, ok := h.tabs[workspace]
	if !ok {
		return false
	}
	for _, id := range tab {
		delete(h.panes, id)
	}
	delete(h.tabs, workspace)
	h.order = slices.DeleteFunc(h.order, func(w string) bool { return w == workspace })
	return true
}

func (h *fakeHerdr) list() string {
	rows := []string{}
	for _, ws := range h.order {
		rows = append(rows, fmt.Sprintf(`{"workspace_id":%q,"label":"workers"}`, ws))
	}
	return `{"result":{"workspaces":[` + strings.Join(rows, ",") + `]}}`
}

// The high finding on #306: cleanup must not delete a workspace another spawn
// adopted. This call created w2 but adopted the older w1 that another spawn
// created meanwhile, and a concurrent spawn owns tab w2:t2 in w2. Removing only
// this spawn's own tab keeps that live session.
func TestSpawnCleanupKeepsWorkspaceAnotherSpawnAdopted(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Workspace = "workers"
	store := a.JobStore.(*fakeJobStore)
	store.records[s.Job] = map[string]any{"job_id": s.Job, "task_id": "other", "host": "other", "state": "claimed"}
	previous := f.Do
	herdr := newFakeHerdr()
	created := false
	touched := []string{}
	f.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch args {
		case "workspace list":
			if !created {
				return Result{Out: noWorkspaces}
			}
			return Result{Out: herdr.list()}
		case "workspace create --label workers":
			created = true
			// Concurrently, another spawn created w1 (older) and added its tab
			// w2:t2 in the workspace this call just made.
			herdr.add("w1", "w1:t1")
			herdr.add("w2", "w2:t1", "w2:t2")
			return Result{Out: `{"result":{"workspace":{"workspace_id":"w2"},"tab":{"tab_id":"w2:t1"},"root_pane":{"pane_id":"w2:t1-pane"}}}`}
		}
		if rest, ok := strings.CutPrefix(args, "workspace close "); ok {
			touched = append(touched, args)
			if !herdr.closeWorkspace(rest) {
				return Result{Code: 1, Out: `{"error":{"code":"workspace_not_found"}}`}
			}
			return Result{}
		}
		if rest, ok := strings.CutPrefix(args, "tab close "); ok {
			touched = append(touched, args)
			if !herdr.closeTab(rest) {
				return Result{Code: 1, Out: `{"error":{"code":"tab_not_found"}}`}
			}
			return Result{}
		}
		if rest, ok := strings.CutPrefix(args, "pane close "); ok {
			touched = append(touched, args)
			if !herdr.closePane(rest) {
				return Result{Code: 1, Out: `{"error":{"code":"pane_not_found"}}`}
			}
			return Result{}
		}
		return previous(c)
	}
	if err := a.Spawn(context.Background(), s); exitCode(err) != exitDuplicate {
		t.Fatalf("spawn exit=%d err=%v", exitCode(err), err)
	}
	if got := herdr.tabs["w2"]; len(got) != 1 || got[0] != "w2:t2" {
		t.Fatalf("workspace w2 tabs=%v, want the concurrent spawn's w2:t2 kept; cleanup calls=%v", got, touched)
	}
	if got := herdr.tabs["w1"]; len(got) != 1 {
		t.Fatalf("adopted workspace w1 tabs=%v, want w1 kept; cleanup calls=%v", got, touched)
	}
}

// A created workspace nobody else adopted still disappears: its own tab was the
// last, so herdr closes the workspace with it.
func TestSpawnCleanupRemovesUnusedCreatedWorkspace(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Workspace = "workers"
	store := a.JobStore.(*fakeJobStore)
	store.records[s.Job] = map[string]any{"job_id": s.Job, "task_id": "other", "host": "other", "state": "claimed"}
	previous := f.Do
	herdr := newFakeHerdr()
	created := false
	f.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch args {
		case "workspace list":
			if !created {
				return Result{Out: noWorkspaces}
			}
			return Result{Out: herdr.list()}
		case "workspace create --label workers":
			created = true
			herdr.add("w2", "w2:t1")
			return Result{Out: `{"result":{"workspace":{"workspace_id":"w2"},"tab":{"tab_id":"w2:t1"},"root_pane":{"pane_id":"w2:t1-pane"}}}`}
		}
		if rest, ok := strings.CutPrefix(args, "tab close "); ok {
			if !herdr.closeTab(rest) {
				return Result{Code: 1, Out: `{"error":{"code":"tab_not_found"}}`}
			}
			return Result{}
		}
		if rest, ok := strings.CutPrefix(args, "workspace close "); ok {
			herdr.closeWorkspace(rest)
			return Result{}
		}
		return previous(c)
	}
	if err := a.Spawn(context.Background(), s); exitCode(err) != exitDuplicate {
		t.Fatalf("spawn exit=%d err=%v", exitCode(err), err)
	}
	if _, ok := herdr.tabs["w2"]; ok {
		t.Fatalf("unused created workspace survived: tabs=%v order=%v", herdr.tabs, herdr.order)
	}
}

// Herdr before 0.8.0 refuses to close a workspace's last tab. Closing the root
// pane is the same atomic step there: it removes this spawn's tab, and the
// workspace only when no other spawn's tab shares it.
func TestSpawnCleanupFallsBackForOldHerdrLastTab(t *testing.T) {
	for _, test := range []struct {
		name          string
		tabs          []string
		fellBack      bool
		workspaceKept bool
	}{
		{"only our tab", []string{"w2:t1"}, true, false},
		{"another spawn's tab", []string{"w2:t1", "w2:t2"}, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, s, f, _ := cmdFixture(t)
			s.Workspace = "workers"
			store := a.JobStore.(*fakeJobStore)
			store.records[s.Job] = map[string]any{"job_id": s.Job, "task_id": "other", "host": "other", "state": "claimed"}
			previous := f.Do
			herdr := newFakeHerdr()
			created := false
			fellBack := false
			f.Do = func(c Command) Result {
				args := strings.Join(c.Args, " ")
				switch args {
				case "workspace list":
					if !created {
						return Result{Out: noWorkspaces}
					}
					return Result{Out: herdr.list()}
				case "workspace create --label workers":
					created = true
					herdr.add("w2", test.tabs...)
					return Result{Out: `{"result":{"workspace":{"workspace_id":"w2"},"tab":{"tab_id":"w2:t1"},"root_pane":{"pane_id":"w2:t1-pane"}}}`}
				}
				if rest, ok := strings.CutPrefix(args, "tab close "); ok {
					if len(herdr.tabs["w2"]) <= 1 {
						return Result{Code: 1, Out: `{"error":{"code":"tab_close_failed","message":"cannot close the last tab in a workspace"}}`}
					}
					if !herdr.closeTab(rest) {
						return Result{Code: 1, Out: `{"error":{"code":"tab_not_found"}}`}
					}
					return Result{}
				}
				if rest, ok := strings.CutPrefix(args, "pane close "); ok {
					fellBack = true
					if !herdr.closePane(rest) {
						return Result{Code: 1, Out: `{"error":{"code":"pane_not_found"}}`}
					}
					return Result{}
				}
				if strings.HasPrefix(args, "workspace close ") {
					t.Fatalf("cleanup closed a whole workspace: %s", args)
				}
				return previous(c)
			}
			if err := a.Spawn(context.Background(), s); exitCode(err) != exitDuplicate {
				t.Fatalf("spawn exit=%d err=%v", exitCode(err), err)
			}
			if fellBack != test.fellBack {
				t.Fatalf("fell back=%t, want %t", fellBack, test.fellBack)
			}
			if got := herdr.tabs["w2"]; test.workspaceKept && len(got) != 1 {
				t.Fatalf("workspace w2 tabs=%v, want the concurrent spawn's tab kept", got)
			} else if !test.workspaceKept && len(got) != 0 {
				t.Fatalf("workspace w2 tabs=%v, want the unused workspace gone", got)
			}
		})
	}
}

// A spawn that adopted a workspace may find it closed by another spawn's failed
// cleanup before its tab exists. A label is recreated and the tab retried.
func TestSpawnRetriesTabCreateWhenAdoptedWorkspaceVanishes(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Workspace = "workers"
	previous := f.Do
	lists, creates, tabs := 0, 0, 0
	f.Do = func(c Command) Result {
		switch strings.Join(c.Args, " ") {
		case "workspace list":
			lists++
			switch lists {
			case 1:
				return Result{Out: `{"result":{"workspaces":[{"workspace_id":"w1","label":"workers"}]}}`}
			case 2:
				return Result{Out: noWorkspaces}
			}
			return Result{Out: `{"result":{"workspaces":[{"workspace_id":"w3","label":"workers"}]}}`}
		case "workspace create --label workers":
			creates++
			return Result{Out: `{"result":{"workspace":{"workspace_id":"w3"},"tab":{"tab_id":"w3:t1"},"root_pane":{"pane_id":"w3:p0"}}}`}
		}
		if strings.HasPrefix(strings.Join(c.Args, " "), "tab create") {
			tabs++
			if tabs == 1 {
				return Result{Code: 1, Out: `{"error":{"code":"workspace_not_found"}}`}
			}
		}
		return previous(c)
	}
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if lists != 3 || creates != 1 || tabs != 2 {
		t.Fatalf("lists=%d creates=%d tabs=%d", lists, creates, tabs)
	}
}

// An explicit workspace id cannot be recreated, so a vanished id is not retried.
func TestSpawnDoesNotRetryVanishedIDWorkspace(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Workspace = "w1Q"
	previous := f.Do
	tabs := 0
	f.Do = func(c Command) Result {
		switch strings.Join(c.Args, " ") {
		case "workspace list":
			return Result{Out: `{"result":{"workspaces":[{"workspace_id":"w1Q","label":"workers"}]}}`}
		}
		if strings.HasPrefix(strings.Join(c.Args, " "), "tab create") {
			tabs++
			return Result{Code: 1, Out: `{"error":{"code":"workspace_not_found"}}`}
		}
		return previous(c)
	}
	if err := a.Spawn(context.Background(), s); err == nil || tabs != 1 {
		t.Fatalf("err=%v tabs=%d", err, tabs)
	}
}

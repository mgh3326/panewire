package fleet

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

// The node's pane sweep, local and hub-ordered, reports a workspace label that
// more than one workspace carries, and changes nothing in herdr.
func TestPaneSweepReportsWorkspaceDuplicate_t6367(t *testing.T) {
	const duplicated = `{"result":{"workspaces":[{"workspace_id":"w1","label":"workers"},{"workspace_id":"w2","label":"reviews"},{"workspace_id":"w3","label":"workers"},{"workspace_id":"w4","label":""},{"workspace_id":"w5","label":""}]}}`
	run := map[string]func(*TaskPaneSweeper, *App) error{
		"local pass": func(s *TaskPaneSweeper, a *App) error { return s.Pass(context.Background(), a) },
		"hub ordered": func(s *TaskPaneSweeper, a *App) error {
			_, err := s.SweepEnded(context.Background(), a, time.Hour)
			return err
		},
	}
	for name, sweep := range run {
		t.Run(name, func(t *testing.T) {
			a := testApp(t)
			a.Runner = &fakeRunner{Do: func(c Command) Result {
				if strings.Join(c.Args, " ") != "workspace list" {
					t.Errorf("unexpected herdr call: %v", c.Args)
				}
				return Result{Out: duplicated}
			}}
			var reports [][]any
			sweeper := &TaskPaneSweeper{Info: func(message string, attrs ...any) {
				if slices.Contains(attrs, any(workspaceDuplicate)) {
					reports = append(reports, attrs)
				}
			}}
			// No job directory at all: the report does not wait for a pane.
			if err := sweep(sweeper, a); err != nil {
				t.Fatal(err)
			}
			want := []any{"reason", "workspace_duplicate", "label", "workers", "workspaces", "w1,w3"}
			if len(reports) != 1 || !slices.Equal(reports[0], want) {
				t.Fatalf("reports=%v, want one %v", reports, want)
			}
		})
	}
}

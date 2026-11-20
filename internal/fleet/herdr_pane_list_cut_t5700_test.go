package fleet

import (
	"strings"
	"testing"
)

// cutFleetPaneList is herdr's `pane list` answer stopped at the 8192 byte
// boundary, which does not decode (herdrpanes).
func cutFleetPaneList() string {
	full := `{"result":{"panes":[{"pane_id":"w1:p1","label":"a","cwd":"` + strings.Repeat("/w", 6000) + `"}]}}`
	return full[:8192]
}

// App.panes reads a cut pane list again once, so the census, sweep and
// close readers that call it get the complete inventory.
func TestAppPanesRetriesCutPaneListOnce(t *testing.T) {
	lists := 0
	f := &fakeRunner{Do: func(c Command) Result {
		if c.Name != "herdr" || strings.Join(c.Args, " ") != "pane list" {
			return Result{}
		}
		lists++
		if lists == 1 {
			return Result{Out: cutFleetPaneList()}
		}
		return Result{Out: `{"result":{"panes":[{"pane_id":"w1:p1","label":"a"}]}}`}
	}}
	panes, err := (&App{Runner: f}).panes(t.Context())
	if err != nil {
		t.Fatalf("panes: %v", err)
	}
	if lists != 2 || len(panes) != 1 || panes[0].Pane != "w1:p1" {
		t.Fatalf("lists=%d panes=%+v, want one retry and w1:p1", lists, panes)
	}
}

// A list cut twice is an error, never an empty inventory that would read the
// pane as absent.
func TestAppPanesCutTwiceIsAnErrorNotAnEmptyList(t *testing.T) {
	f := &fakeRunner{Do: func(Command) Result {
		return Result{Out: cutFleetPaneList()}
	}}
	panes, err := (&App{Runner: f}).panes(t.Context())
	if err == nil || panes != nil {
		t.Fatalf("panes=%+v err=%v, want an error and no inventory", panes, err)
	}
}

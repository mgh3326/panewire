package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestFailedSpawnCleanupRequiresExactIdlePane(t *testing.T) {
	base := Agent{Pane: "pane-1", Name: "job", Kind: "codex", CWD: "/work/project", Status: "idle"}
	for _, test := range []struct {
		name   string
		change func(*Agent)
		close  bool
	}{
		{"idle", func(*Agent) {}, true}, {"done", func(a *Agent) { a.Status = "done" }, true}, {"working", func(a *Agent) { a.Status = "working" }, false}, {"unknown", func(a *Agent) { a.Status = "unknown" }, false}, {"changed name", func(a *Agent) { a.Name = "other" }, false}, {"changed cwd", func(a *Agent) { a.CWD = "/work/other" }, false}, {"changed harness", func(a *Agent) { a.Kind = "claude" }, false}, {"missing cwd", func(a *Agent) { a.CWD = "" }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := testApp(t)
			agent := base
			test.change(&agent)
			closed := false
			a.Runner = &fakeRunner{Do: func(c Command) Result {
				if strings.Join(c.Args, " ") == "agent list" {
					raw, _ := json.Marshal(map[string]any{"agents": []Agent{agent}})
					return Result{Out: string(raw)}
				}
				if strings.Join(c.Args, " ") == "pane close pane-1" {
					closed = true
					return Result{}
				}
				t.Fatalf("unexpected command %+v", c)
				return Result{Code: 1}
			}}
			err := a.closeFailedSpawn(context.Background(), Spawn{Label: "job", CWD: "/work/project"}, "pane-1", "codex")
			if closed != test.close || (err == nil) != test.close {
				t.Fatalf("closed=%v err=%v", closed, err)
			}
		})
	}
}

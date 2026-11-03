package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestFailedSpawnCleanupRequiresExactIdlePane(t *testing.T) {
	seq := int64(1)
	base := Agent{Pane: "pane-1", Name: "job", Kind: "codex", CWD: "/work/project", Status: "idle", StateChangeSeq: &seq}
	for _, test := range []struct {
		name   string
		change func(*Agent)
		close  bool
	}{
		{"idle", func(*Agent) {}, true}, {"done", func(a *Agent) { a.Status = "done" }, true}, {"working", func(a *Agent) { a.Status = "working" }, false}, {"unknown", func(a *Agent) { a.Status = "unknown" }, false}, {"changed name", func(a *Agent) { a.Name = "other" }, false}, {"changed cwd", func(a *Agent) { a.CWD = "/work/other" }, false}, {"changed harness", func(a *Agent) { a.Kind = "claude" }, false}, {"missing cwd", func(a *Agent) { a.CWD = "" }, false}, {"missing sequence", func(a *Agent) { a.StateChangeSeq = nil }, false}, {"focused", func(a *Agent) { a.Focused = true }, false},
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

func TestFailedSpawnCleanupKeepsUnregisteredOrChangedPane(t *testing.T) {
	for _, change := range []string{"missing", "bounce", "working", "disappeared", "missing sequence"} {
		t.Run(change, func(t *testing.T) {
			a := testApp(t)
			seq := int64(1)
			row := Agent{Pane: "pane-1", Name: "job", Kind: "codex", CWD: "/work/project", Status: "idle", StateChangeSeq: &seq}
			reads, closes := 0, 0
			a.Runner = &fakeRunner{Do: func(c Command) Result {
				if strings.Join(c.Args, " ") == "agent list" {
					reads++
					rows := []Agent{row}
					if change == "missing" || reads > 1 && change == "disappeared" {
						rows = nil
					}
					if reads > 1 {
						switch change {
						case "bounce":
							seq = 3
						case "working":
							rows[0].Status = "working"
						case "missing sequence":
							rows[0].StateChangeSeq = nil
						}
					}
					raw, _ := json.Marshal(map[string]any{"agents": rows})
					return Result{Out: string(raw)}
				}
				if strings.Join(c.Args, " ") == "pane close pane-1" {
					closes++
				}
				return Result{}
			}}
			if err := a.closeFailedSpawn(t.Context(), Spawn{Label: "job", CWD: "/work/project"}, "pane-1", "codex"); err == nil || closes != 0 {
				t.Fatalf("unsafe close: %d %v", closes, err)
			}
		})
	}
}

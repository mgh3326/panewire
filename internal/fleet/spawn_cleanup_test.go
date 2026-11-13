package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
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

// A pane cleanup refuses to close needs a receipt, or reap tools cannot find it.
func TestFailedSpawnCleanupRefusalLeavesReceipt(t *testing.T) {
	seq := int64(1)
	for _, test := range []struct {
		name   string
		status string
		want   bool
	}{{"working", "working", true}, {"idle", "idle", false}} {
		t.Run(test.name, func(t *testing.T) {
			a := testApp(t)
			agent := Agent{Pane: "pane-1", Name: "job", Kind: "omp", CWD: "/work/project", Status: test.status, StateChangeSeq: &seq}
			a.Runner = &fakeRunner{Do: func(c Command) Result {
				if strings.Join(c.Args, " ") == "agent list" {
					raw, _ := json.Marshal(map[string]any{"agents": []Agent{agent}})
					return Result{Out: string(raw)}
				}
				return Result{}
			}}
			err := a.closeFailedSpawn(t.Context(), Spawn{Job: "job-a", Label: "job", CWD: "/work/project"}, "pane-1", "omp")
			var receipts []Event
			evs, e := a.events("job-a")
			if e != nil && !errors.Is(e, fs.ErrNotExist) {
				t.Fatal(e)
			}
			for _, ev := range evs {
				if ev.Kind == "job.cleanup_refused" {
					receipts = append(receipts, ev)
				}
			}
			if !test.want {
				if err != nil || len(receipts) != 0 {
					t.Fatalf("closed pane: err=%v receipts=%d", err, len(receipts))
				}
				return
			}
			if err == nil || len(receipts) != 1 {
				t.Fatalf("err=%v receipts=%d", err, len(receipts))
			}
			p := receipts[0].Payload
			if p["pane_id"] != "pane-1" || p["label"] != "job" || p["kind"] != "omp" || p["reason"] == "" {
				t.Fatalf("receipt %+v", p)
			}
		})
	}
}

// A failed spawn that closes its pane takes the lane row it registered with it
// (hk 2887); a pane it keeps keeps its row, because the row still routes.
func TestFailedSpawnCleanupRemovesTheRegisteredLaneOnlyWhenThePaneCloses(t *testing.T) {
	seq := int64(1)
	for _, test := range []struct {
		name       string
		status     string
		laneKnown  bool
		wantClosed bool
		wantLaneRM bool
	}{
		{"closed with lane", "idle", true, true, true},
		{"closed without lane", "idle", false, true, false},
		{"kept with lane", "working", true, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := testApp(t)
			agent := Agent{Pane: "pane-1", Name: "job", Kind: "codex", CWD: "/work/project", Status: test.status, StateChangeSeq: &seq}
			var laneRemoval string
			closed := false
			a.Runner = &fakeRunner{Do: func(c Command) Result {
				switch args := strings.Join(c.Args, " "); {
				case args == "agent list":
					raw, _ := json.Marshal(map[string]any{"agents": []Agent{agent}})
					return Result{Out: string(raw)}
				case args == "pane close pane-1":
					closed = true
				case strings.HasPrefix(args, "lanes rm "):
					laneRemoval = args
				}
				return Result{}
			}}
			s := Spawn{Job: "job-a", Label: "job", CWD: "/work/project"}
			if test.laneKnown {
				s.registeredLane = &Job{HubLane: "job", Machine: "host-a", Pane: "pane-1", Role: "implementer"}
			}
			err := a.closeFailedSpawn(t.Context(), s, "pane-1", "codex")
			if closed != test.wantClosed || (err == nil) != test.wantClosed {
				t.Fatalf("closed=%v err=%v", closed, err)
			}
			if test.wantLaneRM {
				for _, want := range []string{"lanes rm job", "--expect-machine host-a", "--expect-pane pane-1"} {
					if !strings.Contains(laneRemoval, want) {
						t.Fatalf("lane removal %q lacks %q", laneRemoval, want)
					}
				}
			} else if laneRemoval != "" {
				t.Fatalf("unexpected lane removal %q", laneRemoval)
			}
		})
	}
}

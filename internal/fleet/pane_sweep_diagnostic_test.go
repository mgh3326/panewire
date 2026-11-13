package fleet

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func diagnosticRecords(t *testing.T, a *App, pane string) []paneSweepDiagnostic {
	t.Helper()
	root := filepath.Join(a.stateDir(), paneSweepDiagnosticDir)
	var records []paneSweepDiagnostic
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var record paneSweepDiagnostic
		if err := json.Unmarshal(body, &record); err != nil {
			return err
		}
		if record.Pane == pane {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if info.Mode().Perm() != 0600 {
				t.Errorf("diagnostic mode = %o, want 600", info.Mode().Perm())
			}
			records = append(records, record)
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return records
}

func TestSweepCaptureDoesNotAttributeAReassignedPaneToAJob(t *testing.T) {
	for _, mode := range []string{"ended", "idle"} {
		for _, transition := range []string{"A-to-B-to-A", "unchanged"} {
			t.Run(mode+"/"+transition, func(t *testing.T) {
				f := newPaneSweepFixture(t)
				hk := useFakeHK(t)
				pane := f.job("r", "reviewer", "42")
				f.status[pane] = "working"
				if mode == "ended" {
					putEvent(t, f.a, "r", "job.revoked", map[string]any{})
				} else {
					f.closeJob("r", "abandoned")
					f.status[pane] = "idle"
				}
				if transition == "A-to-B-to-A" {
					f.onPaneRead(func() {
						f.names[pane] = "agent-b"
						f.names[pane] = "agent-r"
					})
				}
				base := f.runner.Do
				f.runner.Do = func(c Command) Result {
					r := base(c)
					if c.Name == "herdr" && strings.Join(c.Args, " ") == "pane read "+pane {
						r.Out = "captured screen marker"
					}
					return r
				}
				if mode == "ended" {
					f.endedSweep(61, time.Hour)
				} else {
					f.at(0)
					f.at(11)
				}
				if got := f.closed(); len(got) != 0 {
					t.Fatalf("sweep closed pane without ownership-bound authorization: %v", got)
				}
				for _, call := range f.runner.Calls {
					if call.Name == "git" && len(call.Args) > 1 && call.Args[0] == "worktree" && call.Args[1] == "remove" {
						t.Fatalf("sweep removed a worktree without closing its pane: %v", call.Args)
					}
				}
				if got := screenDocPut(t, hk, "r"); got != nil {
					t.Fatalf("sweep uploaded an unattributed screen as the job: %v", got)
				}
				if f.localScreenFile("r") {
					t.Fatal("sweep wrote a job-local screen")
				}
				records := diagnosticRecords(t, f.a, pane)
				if len(records) != 1 {
					t.Fatalf("node-local diagnostic count = %d, want 1", len(records))
				}
				record := records[0]
				if record.Ownership != "unknown" || record.Screen != "captured screen marker" || record.CaptureID == "" || record.Captured == "" {
					t.Fatalf("diagnostic record = %+v", record)
				}
				if !f.infoHas("task pane kept", "reason=ownership-unproven") {
					t.Fatalf("sweep did not report unproven ownership: %q", f.info)
				}
			})
		}
	}
}

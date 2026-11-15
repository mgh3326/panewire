package panewire

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This interface is the node's scan entry point. Keeping the driver in the
// test lets the same behavioral check run against an older node.
type stallSafetyContractDriver interface {
	initStallSafety() error
	scanStallSafety(context.Context, []HerdrAgentState, time.Time) error
}

func TestStallSafetyContract(t *testing.T) {
	for _, mode := range []string{"idle", "child", "quota"} {
		t.Run(mode, func(t *testing.T) {
			d := NewDaemon(Config{})
			driver, ok := any(d).(stallSafetyContractDriver)
			if !ok {
				t.Fatal("node does not examine inactive jobs")
			}
			rig := newRoleStopRig(t, "implementer", time.Hour, `{"tasks":[{"priority":1,"expected":"4h","refs":{"job_id":"job-active"}}]}`)
			root, err := filepath.EvalSymlinks(rig.manager.inboxRoot)
			if err != nil {
				t.Fatal(err)
			}
			rig.manager.inboxRoot, rig.q.root = root, root
			dir := filepath.Join(root, "jobs", "job-active", "events")
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				path := filepath.Join(dir, entry.Name())
				body, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var event map[string]any
				if err := json.Unmarshal(body, &event); err != nil {
					t.Fatal(err)
				}
				payload := make(map[string]any, len(event))
				for key, value := range event {
					payload[key] = value
				}
				event["kind"], event["payload"] = event["type"], payload
				body, err = json.Marshal(event)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, body, 0600); err != nil {
					t.Fatal(err)
				}
			}
			foreground := "codex"
			if mode == "child" {
				foreground = "xcodebuild"
			}
			script := "#!/bin/sh\ncase \"$1 $2\" in\n  'pane process-info') printf '%s\\n' '{\"process_info\":{\"shell_pid\":1,\"foreground_processes\":[{\"pid\":2,\"argv0\":\"" + foreground + "\"}]}}' ;;\n  *) printf '%s\\n' '{\"agent\":\"codex\"}' ;;\nesac\n"
			installFakeHerdr(t, t.TempDir(), script)
			screen := "last screen lines"
			if mode == "quota" {
				body, err := os.ReadFile("testdata/quota-stop/codex.txt")
				if err != nil {
					t.Fatal(err)
				}
				screen = string(body)
			}
			reads, notes := 0, 0
			rig.q.read = func(context.Context, string) (string, error) { reads++; return screen, nil }
			rig.q.hk.note = func(context.Context, string, string, string) error { notes++; return nil }
			rig.observe("idle", 1, rig.now)
			d.idleWake = rig.manager
			if err := driver.initStallSafety(); err != nil {
				t.Fatal(err)
			}
			agent := HerdrAgentState{PaneID: "workspace:pane-1", WorkspaceID: "workspace", Label: "worker-synthetic", Status: "idle", Harness: "codex"}
			ctx := context.Background()
			for _, at := range []time.Time{rig.now, rig.now.Add(31 * time.Minute), rig.now.Add(32 * time.Minute)} {
				if err := driver.scanStallSafety(ctx, []HerdrAgentState{agent}, at); err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "child":
				if reads != 0 || notes != 0 || len(rig.owner) != 0 {
					t.Fatalf("running child was examined: reads=%d notes=%d events=%v", reads, notes, rig.owner)
				}
			case "idle":
				if reads != 1 || notes != 1 || len(rig.owner) != 1 || rig.owner[0].NotifyType != "blocked" || !strings.Contains(rig.owner[0].Text, "last screen lines") {
					t.Fatalf("idle outcome: reads=%d notes=%d events=%v", reads, notes, rig.owner)
				}
			case "quota":
				if reads != 1 || notes != 1 {
					t.Fatalf("quota examination: reads=%d notes=%d", reads, notes)
				}
				rig.manager.Tick(ctx, rig.now.Add(33*time.Minute))
				if len(rig.announce) != 1 {
					t.Fatalf("quota stop did not reach handover: %v", rig.announce)
				}
			}
		})
	}
}

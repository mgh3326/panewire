package fleet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNonCodexShortBriefRetriesDroppedEnter(t *testing.T) {
	for _, kind := range []string{"claude", "devin", "cmd"} {
		t.Run(kind, func(t *testing.T) {
			a := testApp(t)
			f := a.Runner.(*fakeRunner)
			brief := "Read /workspace/job/brief-delivered.md."
			pasted, submitted := false, false
			pastes, enters := 0, 0
			wantEnters := 1
			if kind == "cmd" {
				wantEnters = 2 // initial Enter is dropped
			}
			f.Do = func(c Command) Result {
				args := strings.Join(c.Args, " ")
				switch {
				case strings.HasPrefix(args, "agent prompt"), strings.HasPrefix(args, "pane send-text"):
					pastes++
					pasted = true
				case strings.Contains(args, "send-keys"):
					enters++
					submitted = enters == wantEnters
				case strings.HasPrefix(args, "agent list"):
					status := "idle"
					if submitted {
						status = "working"
					}
					return Result{Out: `{"agents":[{"pane_id":"w:p1","agent_status":"` + status + `"}]}`}
				case strings.HasPrefix(args, "pane read"):
					if pasted && !submitted {
						return Result{Out: "❯ " + brief}
					}
					return Result{Out: "Working on the task"}
				}
				return Result{}
			}
			if err := a.deliver(context.Background(), kind, "w:p1", brief); err != nil {
				t.Fatalf("short brief was not rescued: %v (enters=%d)", err, enters)
			}
			if pastes != 1 || enters != wantEnters {
				t.Fatalf("want one paste and %d enters, got %d/%d", wantEnters, pastes, enters)
			}
		})
	}
}

func TestLaneCredentialWithoutLocalIdentityDefersToPanewire(t *testing.T) {
	for _, tc := range []struct {
		name, contents string
		missing        bool
		result         Result
	}{
		{name: "token only", contents: "HUB_TOKEN=fixture\n"},
		{name: "syntax owned by panewire", contents: "unparsed credential fixture", result: Result{Code: 6, Err: "panewire rejected credential syntax"}},
		{name: "unreadable file", missing: true, result: Result{Code: 6, Err: "panewire credential unavailable"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := testApp(t)
			path := filepath.Join(a.Home, "lanes.env")
			if !tc.missing {
				if err := os.WriteFile(path, []byte(tc.contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			a.Config.Hub = map[string]any{"hub_url": "https://hub.example.invalid", "lanes_token_env": path}
			called := false
			a.Runner.(*fakeRunner).Do = func(c Command) Result {
				called = c.Name == "panewire" && strings.Contains(strings.Join(c.Args, " "), "--hub-token-env "+path)
				return tc.result
			}
			_, err := a.registerLane(context.Background(), lanePutRequest{Lane: "lane-a", Parent: "parent-a", Pane: "w:p1", Role: "builder", Machine: "host-a"})
			if !called || code(err) != tc.result.Code || err != nil && !strings.Contains(err.Error(), tc.result.Err) {
				t.Fatalf("panewire did not own credential decision: called=%t err=%v", called, err)
			}
		})
	}
}

func TestAutoPlacementSkipsUnconfiguredRemoteWorkspace(t *testing.T) {
	for _, tc := range []struct {
		name, failure string
		forced        bool
		wantNext      bool
	}{
		{name: "auto skips missing workspace", failure: "panewire: WORKSPACE_UNCONFIGURED: set target workspace", wantNext: true},
		{name: "forced host still refuses", failure: "panewire: WORKSPACE_UNCONFIGURED: set target workspace", forced: true},
		{name: "other config errors still stop", failure: "panewire: invalid host policy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &fakePlacementSource{hosts: []HostView{
				{Name: "host-a", Connected: true, LoadRatio: ptrF(0.1)},
				{Name: "host-b", Connected: true, LoadRatio: ptrF(0.2)},
			}}
			var spawns []string
			a, s, f := remoteFixture(t, src, &fakeWaker{}, "2026-01-01T00:00:00Z", func(args string) Result {
				if strings.Contains(args, "_spawn-stdin") {
					spawns = append(spawns, args)
					if strings.Contains(args, "host-a") {
						return Result{Code: exitConfig, Err: tc.failure}
					}
					return Result{Out: "OK pane=w:p1 job=job-a\n"}
				}
				return Result{}
			})
			a.Config.Hosts = map[string]map[string]any{"host-a": {}, "host-b": {"workspace": "remote-workspace"}}
			a.Config.HostOrder = []string{"host-a", "host-b"}
			s.Workspace = "caller-only-workspace"
			if tc.forced {
				s.Host = "host-a"
			}
			_, rec, err := a.place(context.Background(), s, "brief")
			if !tc.wantNext {
				if code(err) != exitConfig || len(spawns) != 1 {
					t.Fatalf("configuration failure did not stop: err=%v spawns=%v", err, spawns)
				}
				return
			}
			if err != nil || rec["chose"] != "host-b" || len(spawns) != 2 {
				t.Fatalf("next host not attempted: err=%v record=%v spawns=%v", err, rec, spawns)
			}
			for _, c := range f.Calls {
				if c.Name != "ssh" || !strings.Contains(strings.Join(c.Args, " "), "_spawn-stdin") {
					continue
				}
				var envelope struct{ Spawn Spawn }
				if err := json.Unmarshal([]byte(c.Input), &envelope); err != nil {
					t.Fatal(err)
				}
				wantWorkspace := ""
				if strings.Contains(strings.Join(c.Args, " "), "host-b") {
					wantWorkspace = "remote-workspace"
				}
				if envelope.Spawn.Workspace != wantWorkspace {
					t.Fatalf("wrong target workspace: got %q, want %q", envelope.Spawn.Workspace, wantWorkspace)
				}
			}
		})
	}
}

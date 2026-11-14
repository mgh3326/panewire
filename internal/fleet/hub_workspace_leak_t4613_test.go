package fleet

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestSubmissionWorkspaceIDDroppedForAutoHost(t *testing.T) {
	for _, tc := range []struct {
		name      string
		host      string
		workspace string
		wantFlag  bool
		wantVal   string
	}{
		{
			name:      "auto host drops caller workspace id",
			host:      "auto",
			workspace: "w1V",
			wantFlag:  false,
		},
		{
			name:      "auto host keeps workspace label",
			host:      "auto",
			workspace: "workers",
			wantFlag:  true,
			wantVal:   "workers",
		},
		{
			name:      "pinned local host keeps caller workspace id",
			host:      "local",
			workspace: "w1V",
			wantFlag:  true,
			wantVal:   "w1V",
		},
		{
			name:      "pinned remote host keeps explicit workspace id",
			host:      "h1",
			workspace: "w2",
			wantFlag:  true,
			wantVal:   "w2",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, f, _, _ := hubFixture(t, gitState{pushed: true, pointsAt: []string{"refs/remotes/origin/wk/x"}}, "wk/x", hubHosts())
			a.Config.Local["machine_id"] = "mac-personal"
			if err := os.MkdirAll(s.CWD, 0700); err != nil {
				t.Fatal(err)
			}
			s.Prompt = filepath.Join(a.Home, "brief.md")
			if err := os.WriteFile(s.Prompt, []byte("brief"), 0600); err != nil {
				t.Fatal(err)
			}
			s.Label, s.T, s.Owner, s.Host, s.OverrideReason, s.Containment, s.Role = "job-a", "T1", "parent", tc.host, "fixture", "hk:12", "implementer"
			s.Workspace = tc.workspace
			base := f.Do
			f.Do = func(c Command) Result {
				if c.Name == "git" && strings.Contains(strings.Join(c.Args, " "), "symbolic-ref --short refs/remotes/origin/HEAD") {
					return Result{Out: "origin/main\n"}
				}
				return base(c)
			}

			sub, err := a.PrepareSubmission(t.Context(), s)
			if err != nil {
				t.Fatalf("PrepareSubmission failed: %v", err)
			}
			hasFlag := slices.Contains(sub.Args, "-w")
			if hasFlag != tc.wantFlag {
				t.Fatalf("submission args -w present=%v, want %v (args: %v)", hasFlag, tc.wantFlag, sub.Args)
			}
			if tc.wantFlag {
				idx := slices.Index(sub.Args, "-w")
				if idx+1 >= len(sub.Args) || sub.Args[idx+1] != tc.wantVal {
					t.Fatalf("submission args -w value=%v, want %v", sub.Args[idx+1], tc.wantVal)
				}
			}
		})
	}
}

func TestHubPlacementDropsForeignWorkspaceIDOnRemoteNode(t *testing.T) {
	for _, tc := range []struct {
		name          string
		host          string
		originMachine string
		targetMachine string
		workspace     string
		wantFlag      bool
		wantVal       string
	}{
		{
			name:          "foreign workspace id dropped on remote candidate",
			host:          "auto",
			originMachine: "mac-personal",
			targetMachine: "h1",
			workspace:     "w1V",
			wantFlag:      false,
		},
		{
			name:          "workspace label preserved on remote candidate",
			host:          "auto",
			originMachine: "mac-personal",
			targetMachine: "h1",
			workspace:     "workers",
			wantFlag:      true,
			wantVal:       "workers",
		},
		{
			name:          "workspace id preserved on same origin candidate",
			host:          "auto",
			originMachine: "h1",
			targetMachine: "h1",
			workspace:     "w1V",
			wantFlag:      true,
			wantVal:       "w1V",
		},
		{
			name:          "explicit host pin preserves workspace id",
			host:          "h1",
			originMachine: "mac-personal",
			targetMachine: "h1",
			workspace:     "w2",
			wantFlag:      true,
			wantVal:       "w2",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, f, hub, _ := hubFixture(t, gitState{pushed: true, pointsAt: []string{"refs/remotes/origin/wk/x"}}, "wk/x", hubHosts())
			a.OriginMachine = tc.originMachine
			a.Config.Local["machine_id"] = tc.originMachine
			if err := os.MkdirAll(s.CWD, 0700); err != nil {
				t.Fatal(err)
			}
			s.Prompt = filepath.Join(a.Home, "brief.md")
			if err := os.WriteFile(s.Prompt, []byte("brief"), 0600); err != nil {
				t.Fatal(err)
			}
			s.Label, s.T, s.Owner, s.Host, s.OverrideReason, s.Containment, s.Role = "job-a", "T1", "parent", tc.host, "fixture", "hk:12", "implementer"
			s.Workspace = tc.workspace
			base := f.Do
			f.Do = func(c Command) Result {
				if c.Name == "git" && strings.Contains(strings.Join(c.Args, " "), "symbolic-ref --short refs/remotes/origin/HEAD") {
					return Result{Out: "origin/main\n"}
				}
				return base(c)
			}

			// Place candidate
			targetCandidate := &placeCandidate{
				name: tc.targetMachine,
				cfg:  hubHosts()[tc.targetMachine],
			}
			repo := a.localRepo(t.Context(), s.CWD)
			res := a.spawnHub(t.Context(), s, "brief content", targetCandidate, repo, tc.host != "auto")
			if res.stop != nil {
				t.Fatalf("spawnHub returned stop error: %v", res.stop)
			}
			if len(hub.spawns) == 0 {
				t.Fatal("hub.Spawn was not called")
			}
			sentArgs := hub.spawns[0].Args
			hasFlag := slices.Contains(sentArgs, "-w")
			if hasFlag != tc.wantFlag {
				t.Fatalf("hub sent args -w present=%v, want %v (args: %v)", hasFlag, tc.wantFlag, sentArgs)
			}
			if tc.wantFlag {
				idx := slices.Index(sentArgs, "-w")
				if idx+1 >= len(sentArgs) || sentArgs[idx+1] != tc.wantVal {
					t.Fatalf("hub sent args -w value=%v, want %v", sentArgs[idx+1], tc.wantVal)
				}
			}
		})
	}
}

func TestConsultIncidentReproForeignWorkspaceDropped(t *testing.T) {
	// Reproduce consult-4607-promote request c9c7a818:
	// A submission originated from mac-personal with args containing "-w", "w1V",
	// placed on remote node mac-dev (where only "workers" exists, not "w1V").
	a, s, f, hub, _ := hubFixture(t, gitState{pushed: true, pointsAt: []string{"refs/remotes/origin/wk/x"}}, "wk/x", map[string]map[string]any{
		"mac-dev": {"via": "hub", "tags": []string{"macos"}, "roles": []string{"consult"}, "brief_doc_prefix": "briefs/"},
	})
	if err := os.MkdirAll(s.CWD, 0700); err != nil {
		t.Fatal(err)
	}
	s.Prompt = filepath.Join(a.Home, "brief.md")
	if err := os.WriteFile(s.Prompt, []byte("consult brief"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Label, s.Job, s.T, s.Owner, s.Host, s.Role = "consult-4607-promote", "consult-4607-promote", "T1", "parent", "auto", "consult"
	s.Profile = "codex-astra"
	s.Workspace = "w1V" // workspace from mac-personal
	s.OverrideReason = "fixture"
	s.Containment = "hk:4607"

	base := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "git" && strings.Contains(strings.Join(c.Args, " "), "symbolic-ref --short refs/remotes/origin/HEAD") {
			return Result{Out: "origin/main\n"}
		}
		return base(c)
	}

	sub, err := a.PrepareSubmission(t.Context(), s)
	if err != nil {
		t.Fatal(err)
	}
	// 1. PrepareSubmission on mac-personal must not leak caller workspace ID w1V when host is auto
	if slices.Contains(sub.Args, "-w") {
		t.Fatalf("sub.Args leaked caller workspace: %v", sub.Args)
	}

	// 2. Even if a submission arrives with -w w1V (e.g. from an older submitter or replay of c9c7a818),
	// the hub coordinator must drop foreign workspace ID w1V when placing on mac-dev.
	replayedSub := sub
	replayedSub.OriginMachine = "mac-personal"
	replayedSub.Args = append(replayedSub.Args, "-w", "w1V")

	// Execute on hub coordinator
	targetCandidate := &placeCandidate{
		name: "mac-dev",
		cfg:  map[string]any{"via": "hub", "tags": []string{"macos"}, "roles": []string{"consult"}, "brief_doc_prefix": "briefs/"},
	}
	a.OriginMachine = replayedSub.OriginMachine
	repo := a.localRepo(t.Context(), s.CWD)
	res := a.spawnHub(t.Context(), s, "brief", targetCandidate, repo, false)
	if res.stop != nil {
		t.Fatalf("spawnHub stopped: %v", res.stop)
	}
	if len(hub.spawns) == 0 {
		t.Fatal("hub.Spawn not called")
	}
	sentArgs := hub.spawns[0].Args
	if slices.Contains(sentArgs, "-w") {
		t.Fatalf("hub sent foreign workspace w1V to mac-dev: %v", sentArgs)
	}

	// 3. Node binding: without the foreign flag, node binds its configured "workers" workspace
	parsedNodeSpawn, err := parseSpawn(sentArgs)
	if err != nil {
		t.Fatal(err)
	}
	boundNodeSpawn := bindNodeSpawn(parsedNodeSpawn, s.CWD, s.Prompt, "workers")
	if boundNodeSpawn.Workspace != "workers" {
		t.Fatalf("node bound workspace=%q, want workers", boundNodeSpawn.Workspace)
	}

	// Negative proof: if foreign w1V had been sent to the node, it would bind w1V and fail resolution on mac-dev
	leakedSpawn, err := parseSpawn(append(sentArgs, "-w", "w1V"))
	if err != nil {
		t.Fatal(err)
	}
	leakedBound := bindNodeSpawn(leakedSpawn, s.CWD, s.Prompt, "workers")
	if leakedBound.Workspace != "w1V" {
		t.Fatalf("leaked bound workspace=%q, want w1V", leakedBound.Workspace)
	}
	// On mac-dev, only "workers" exists; resolving w1V fails with workspace_unresolved (exit 2)
	nodeApp, _, _, _ := cmdFixture(t)
	nodeApp.Runner = &fakeRunner{Do: func(c Command) Result {
		if c.Name == "herdr" && len(c.Args) >= 2 && c.Args[0] == "workspace" && c.Args[1] == "list" {
			return Result{Out: `{"result":{"workspaces":[{"workspace_id":"wDEV1","label":"workers"}]}}`}
		}
		return Result{}
	}}
	_, _, err = nodeApp.resolveWorkspace(t.Context(), leakedBound.Workspace)
	if code(err) != exitUsage || !strings.Contains(err.Error(), `workspace id "w1V" is not in herdr's list`) {
		t.Fatalf("leaked bound resolution err=%v, want workspace id not in list", err)
	}
	if got := SpawnRefusalCode(err.Error(), code(err)); got != "workspace_unresolved" {
		t.Fatalf("refusal code=%q, want workspace_unresolved", got)
	}
}

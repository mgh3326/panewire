package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestAutoRemoteWorkspaceIsHostSpecific(t *testing.T) {
	for _, workspace := range []string{"remote-workspace", ""} {
		t.Run(firstNonempty(workspace, "target-local-config"), func(t *testing.T) {
			src := &fakePlacementSource{hosts: []HostView{{Name: "host-a", Connected: true, LoadRatio: ptrF(0.1)}}}
			a, s, f := remoteFixture(t, src, &fakeWaker{}, "2026-01-01T00:00:00Z", func(string) Result {
				return Result{Out: "OK pane=w:p1\n"}
			})
			a.Config.Hosts = map[string]map[string]any{"host-a": {"workspace": workspace, "herdr_session": "fleet"}}
			a.Config.HostOrder = []string{"host-a"}
			s.Workspace = "caller-only-workspace"
			if _, _, err := a.placeAuto(context.Background(), s, "brief"); err != nil {
				t.Fatal(err)
			}
			var envelope struct{ Spawn Spawn }
			if err := json.Unmarshal([]byte(spawnInput(t, f)), &envelope); err != nil {
				t.Fatal(err)
			}
			if envelope.Spawn.Workspace != workspace {
				t.Fatalf("remote workspace=%q, want %q", envelope.Spawn.Workspace, workspace)
			}
			for _, c := range f.Calls {
				if strings.Contains(strings.Join(c.Args, " "), "_spawn-stdin") && !strings.Contains(strings.Join(c.Args, " "), "HERDR_SESSION='fleet'") {
					t.Fatalf("remote session missing: %+v", c)
				}
			}
		})
	}
}

func TestDelegatedWorkspaceUsesExecutingHostConfig(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "configured"}[configured], func(t *testing.T) {
			a, s, f, _ := cmdFixture(t)
			s.Delegated, s.Workspace = true, ""
			if configured {
				a.Config.Local["workspace"] = "target-workspace"
			}
			err := a.Spawn(context.Background(), s)
			if !configured {
				if code(err) != exitConfig || !strings.Contains(err.Error(), "WORKSPACE_UNCONFIGURED") || len(f.Calls) != 0 {
					t.Fatalf("missing target workspace: %v calls=%v", err, f.Calls)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range f.Calls {
				if strings.HasPrefix(strings.Join(c.Args, " "), "tab create") && !strings.Contains(strings.Join(c.Args, " "), "--workspace target-workspace") {
					t.Fatalf("target workspace ignored: %+v", c)
				}
			}
		})
	}
}

func TestSpawnPinsSessionAcrossHerdrOperations(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	t.Setenv("HERDR_SESSION", "fleet")
	original := f.Do
	f.Do = func(c Command) Result {
		if strings.HasPrefix(strings.Join(c.Args, " "), "tab create") {
			t.Setenv("HERDR_SESSION", "default")
		}
		return original(c)
	}
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	for _, c := range f.Calls {
		if c.Name == "herdr" && !slices.Contains(c.Env, "HERDR_SESSION=fleet") {
			t.Fatalf("herdr escaped the launch session: %+v", c)
		}
	}
}

func TestLaneRegistrationUsesSessionMachineAndCredential(t *testing.T) {
	for _, session := range []string{"default", "fleet", "worker"} {
		t.Run(session, func(t *testing.T) {
			a := testApp(t)
			t.Setenv("HERDR_SESSION", session)
			machine := "host-a-" + session
			token := filepath.Join(a.Home, session+".env")
			if err := os.WriteFile(token, []byte("HUB_MACHINE_ID="+machine+"\nHUB_TOKEN=fixture\n"), 0600); err != nil {
				t.Fatal(err)
			}
			a.Config.Hub = map[string]any{
				"hub_url": "https://hub.example.invalid", "machine_id": "host-a",
				"lanes_token_env":          "other-session.env",
				"session_machine_ids":      map[string]any{session: machine},
				"session_lanes_token_envs": map[string]any{session: token},
			}
			f := a.Runner.(*fakeRunner)
			if _, err := a.registerLane(context.Background(), lanePutRequest{Lane: "lane-a", Parent: "parent-a", Pane: "w:p1", Role: "builder", Machine: a.machine()}); err != nil {
				t.Fatal(err)
			}
			args := strings.Join(f.Calls[0].Args, " ")
			if !strings.Contains(args, "--machine "+machine) || !strings.Contains(args, "--hub-token-env "+token) {
				t.Fatalf("session route/credential mismatch: %s", args)
			}
			if str(a.Config.Hub, "lanes_token_env") != "other-session.env" {
				t.Fatal("session resolution mutated shared config")
			}
		})
	}
}

func TestLaneRegistrationRejectsCredentialForOtherSession(t *testing.T) {
	a := testApp(t)
	t.Setenv("HERDR_SESSION", "default")
	token := hubEnv(t, "HUB_MACHINE_ID=host-a\nHUB_TOKEN=fixture\n", 0600)
	a.Config.Hub = map[string]any{
		"hub_url": "https://hub.example.invalid", "lanes_token_env": token,
		"session_machine_ids": map[string]any{"default": "host-a-default", "fleet": "host-a"},
	}
	_, err := a.registerLane(context.Background(), lanePutRequest{Lane: "lane-a", Parent: "parent-a", Pane: "w:p1", Role: "builder", Machine: a.machine()})
	if code(err) != exitConfig || !strings.Contains(err.Error(), "LANE_SESSION_MACHINE_MISMATCH") {
		t.Fatalf("cross-session credential accepted: %v", err)
	}
	if len(a.Runner.(*fakeRunner).Calls) != 0 {
		t.Fatal("mismatched registration reached the hub")
	}
}

func TestLaneSessionCredentialKeepsExistingFallbackOrder(t *testing.T) {
	a := testApp(t)
	a.Config.Hub = map[string]any{"lanes_token_env": "node.env", "operator_token_env": "operator.env"}
	args := strings.Join(a.laneHubArgs("builder"), " ")
	if !strings.Contains(args, "--hub-token-env node.env") {
		t.Fatalf("dedicated lane authority changed: %s", args)
	}
	delete(a.Config.Hub, "lanes_token_env")
	if args := strings.Join(a.laneHubArgs("builder"), " "); !strings.Contains(args, "--hub-token-env operator.env") {
		t.Fatalf("builder operator fallback missing: %s", args)
	}
}

func TestLaneRegistrationWarningPreservesHubError(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	a.Config.Hub = map[string]any{"hub_url": "https://hub.example.invalid"}
	original := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "panewire" && strings.HasPrefix(strings.Join(c.Args, " "), "lanes add") {
			return Result{Code: 6, Err: "lanes rejected: credential machine does not match route"}
		}
		return original(c)
	}
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if warning := a.Err.(*bytes.Buffer).String(); !strings.Contains(warning, "credential machine does not match route") || !strings.Contains(warning, "session=") {
		t.Fatalf("registration cause lost: %s", warning)
	}
}

func deliveryApp(t *testing.T, screens []string, statuses []string) (*App, *fakeRunner) {
	t.Helper()
	a := testApp(t)
	reads, lists := 0, 0
	f := a.Runner.(*fakeRunner)
	f.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch {
		case strings.HasPrefix(args, "agent list"):
			status := statuses[min(lists, len(statuses)-1)]
			lists++
			return Result{Out: `{"agents":[{"pane_id":"w:p1","agent":"codex","agent_status":"` + status + `"}]}`}
		case strings.HasPrefix(args, "pane read"):
			screen := screens[min(reads, len(screens)-1)]
			reads++
			return Result{Out: screen}
		}
		return Result{}
	}
	return a, f
}

func TestCodexBriefRequiresSubmissionOnTargetPane(t *testing.T) {
	brief := "Read /workspace/job/brief-delivered.md (42 bytes, sha256 abcdef)."
	for _, tc := range []struct {
		name, screen, status string
		want                 bool
	}{
		{"empty idle pane", "› Ask Codex to do anything\nContext 0%", "idle", false},
		{"composer despite working status", "› " + brief + "\nContext 0%", "working", false},
		{"pasted chip despite working status", "[Pasted text #1 +2 lines]", "working", false},
		{"submitted and finished", "› " + brief + "\n\n• Read the brief.\n\n› Ask Codex to do anything", "idle", true},
		{"submitted wrapped pointer", "› " + brief[:25] + "\n  " + brief[25:] + "\n\n• Read the brief.\n\n› Ask Codex to do anything", "idle", true},
		{"working transition", "• Working (esc to interrupt)\n› Ask Codex to do anything", "working", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := deliveryApp(t, []string{"› Ask Codex to do anything", tc.screen}, []string{"idle", tc.status})
			err := a.deliver(context.Background(), "codex", "w:p1", brief)
			if tc.want && err != nil || !tc.want && code(err) != exitSubmitUnconfirmed {
				t.Fatalf("want success=%t, got %v", tc.want, err)
			}
		})
	}
}

func TestSpawnWorkspaceLongFlag(t *testing.T) {
	s, err := parseSpawn([]string{"--workspace", "local-workspace"})
	if err != nil || s.Workspace != "local-workspace" {
		t.Fatalf("workspace alias: %+v %v", s, err)
	}
}

func TestLaneRemovalUsesRecordedSessionCredential(t *testing.T) {
	a := testApp(t)
	t.Setenv("HERDR_SESSION", "default")
	a.Config.Hub = map[string]any{
		"lanes_token_env":          "default.env",
		"session_lanes_token_envs": map[string]any{"fleet": "fleet.env"},
	}
	j := Job{HubLane: "lane-a", Machine: "host-a", Pane: "w:p1", Role: "builder", Receipt: map[string]any{"herdr_session": "fleet"}}
	if err := a.removeLane(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	if args := strings.Join(a.Runner.(*fakeRunner).Calls[0].Args, " "); !strings.Contains(args, "--hub-token-env fleet.env") {
		t.Fatalf("lane cleanup used caller's session: %s", args)
	}
}

func TestCodexOldSubmittedBriefIsNotNewLandingEvidence(t *testing.T) {
	brief := "Read the job brief."
	screen := "› " + brief + "\n• Previous response.\n› Ask Codex to do anything"
	a, f := deliveryApp(t, []string{screen}, []string{"idle"})
	if err := a.deliver(context.Background(), "codex", "w:p1", brief); code(err) != exitSubmitUnconfirmed {
		t.Fatalf("old turn accepted: %v", err)
	}
	for _, c := range f.Calls {
		if strings.Contains(strings.Join(c.Args, " "), "send-keys") {
			t.Fatal("ambiguous delivery must not send an extra Enter")
		}
	}
}

func TestCodexTrustPromptFixtures(t *testing.T) {
	for _, name := range []string{"codex-trust-current.txt", "codex-trust-legacy.txt"} {
		t.Run(name, func(t *testing.T) {
			screen, err := os.ReadFile(filepath.Join("testdata", name))
			if err != nil {
				t.Fatal(err)
			}
			if !codexTrustPrompt(string(screen)) {
				t.Fatal("trust dialog missed")
			}
			a, _ := deliveryApp(t, []string{string(screen)}, []string{"idle"})
			if err := a.checkTrustPrompt(context.Background(), "codex", "w:p1", "/workspace/project"); code(err) != exitTrustPrompt || !strings.Contains(err.Error(), "WORKSPACE_TRUST_REQUIRED") {
				t.Fatalf("trust dialog not diagnosed: %v", err)
			}
		})
	}
	if codexTrustPrompt("› Ask Codex to do anything\nContext 0%") {
		t.Fatal("idle pane mistaken for trust dialog")
	}
}

func TestCodexTrustPromptStopsSpawnAndCleansResources(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Profile = "codex-sol"
	q := a.Quota.(*fakeQuota)
	q.LaunchValue = Launch{Profile: "codex-sol", ModelID: "example-model", Effort: "high", Grade: "A+", Gate: "default"}
	q.GateDo = func(QuotaRequest) (string, error) { return "pool=example profile=codex-sol\n", nil }
	screen, err := os.ReadFile(filepath.Join("testdata", "codex-trust-current.txt"))
	if err != nil {
		t.Fatal(err)
	}
	started := false
	original := f.Do
	f.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		if strings.HasPrefix(args, "agent start") {
			started = true
			return Result{Code: exitTimeout, Err: "agent startup timed out waiting for idle"}
		}
		if started && strings.HasPrefix(args, "agent list") {
			seq := int64(1)
			raw, _ := json.Marshal(map[string]any{"agents": []Agent{{Pane: "w:p1", Name: s.Label, Kind: "codex", Status: "idle", CWD: s.CWD, StateChangeSeq: &seq}}})
			return Result{Out: string(raw)}
		}
		if started && strings.HasPrefix(args, "pane read") {
			return Result{Out: string(screen)}
		}
		return original(c)
	}
	if err := a.Spawn(context.Background(), s); code(err) != exitTrustPrompt {
		t.Fatalf("trust dialog did not stop spawn: %v", err)
	}
	closed := false
	for _, c := range f.Calls {
		args := strings.Join(c.Args, " ")
		if strings.HasPrefix(args, "agent prompt") || strings.Contains(args, "send-keys") {
			t.Fatalf("trust prompt received job input: %s", args)
		}
		closed = closed || args == "pane close w:p1"
	}
	if hkClaimed(t) {
		t.Fatal("trust prompt claimed the task")
	}
	if !closed {
		t.Fatal("failed pane not closed")
	}
	if state, pools := ledgerState(t, a, s.Job); state != "released" || len(pools) != 0 {
		t.Fatalf("trust failure leaked lease: %s %v", state, pools)
	}
	if slices.Contains(eventKindsOf(t, a, s.Job), "job.spawned") || strings.Contains(a.Out.(*bytes.Buffer).String(), "landed=yes") {
		t.Fatal("trust failure reported success")
	}
}

func TestSessionLaneConfigurationRejectsMalformedMapping(t *testing.T) {
	for _, entry := range []string{
		`session_machine_ids = "host-a"`,
		`session_machine_ids = { fleet = "bad id" }`,
		`session_lanes_token_envs = { fleet = false }`,
	} {
		path := filepath.Join(t.TempDir(), "hosts.toml")
		if err := os.WriteFile(path, []byte("[hub]\n"+entry+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(path); code(err) != exitConfig {
			t.Fatalf("invalid session mapping accepted: %s (%v)", entry, err)
		}
	}
}

package fleet

import (
	"context"
	"slices"
	"strings"
	"testing"
)

// A claude seat is started under its job id, so ListAgents, /list-agents and
// `@name` address it by the id the fleet already uses (hk 4258).
func TestClaudeSpawnLaunchesUnderJobName(t *testing.T) {
	a, s, fr := claudeSpawnFixture(t)
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	argv := agentStartArgv(t, fr)
	i := slices.Index(argv, "--name")
	if i < 0 || i+1 >= len(argv) || argv[i+1] != "job-a" {
		t.Fatalf("claude argv lacks --name job-a: %v", argv)
	}
	if !slices.Contains(argv, "--model") || !slices.Contains(argv, "--dangerously-skip-permissions") {
		t.Fatalf("--name displaced the established claude flags: %v", argv)
	}
}

// Only claude takes --name: every other harness launches exactly as before.
func TestNonClaudeSpawnArgvCarriesNoSessionName(t *testing.T) {
	a, s, fr := ompSpawnFixture(t)
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	argv := agentStartArgv(t, fr)
	if slices.Contains(argv, "--name") || slices.Contains(argv, "-n") {
		t.Fatalf("omp argv gained a session name: %v", argv)
	}
	for _, kind := range []string{"codex", "devin", "omp", "cmd", "kiro", "opencode", "agy"} {
		before := []string{"--model", "m"}
		if got := withSessionName(kind, slices.Clone(before), "job-a"); !slices.Equal(got, before) {
			t.Fatalf("%s argv changed: %v", kind, got)
		}
	}
	if got := withSessionName("claude", []string{"--model", "m"}, ""); !slices.Equal(got, []string{"--model", "m"}) {
		t.Fatalf("a nameless launch must add no flag: %v", got)
	}
}

// With --name, Claude Code titles the composer's top edge ("──── job-a ─"),
// which the pure-rule check in claudeRule does not accept. The landing proof
// reads the composer from the prompt row down to the bottom rule, so a titled
// top edge must change neither verdict.
func TestClaudeLandingProofWithTitledComposer(t *testing.T) {
	titled := "─────────────────────────────────────────── job-a ─"
	empty := claudeBanner() + titled + "\n❯ \n" + claudeTestRule + "\n  ⏵⏵ bypass permissions on (shift+tab to cycle)\n"
	typed := claudeBanner() + titled + "\n❯ Your brief is the file /work/inbox/jobs/job/brief-delivered.md (9945 bytes, sha256 a3f67ab7fbdd). Read the\n  whole file now, then follow it exactly; it is your only instruction.\n" + claudeTestRule + "\n  ⏵⏵ bypass permissions on (shift+tab to cycle)\n"
	submitted := claudeBanner() + "❯ Your brief is the file /work/inbox/jobs/job/brief-delivered.md (9945 bytes, sha256 a3f67ab7fbdd). Read the whole\n  file now, then follow it exactly; it is your only instruction.\n· Cultivating…\n" + titled + "\n❯\n" + claudeTestRule + "\n  ⏵⏵ bypass permissions on (shift+tab to cycle)\n"
	if briefInComposer("claude", empty, claudeTestBrief) || submittedBrief("claude", empty, claudeTestBrief) {
		t.Fatal("an empty titled composer read as holding or having submitted the brief")
	}
	if !briefInComposer("claude", typed, claudeTestBrief) || submittedBrief("claude", typed, claudeTestBrief) {
		t.Fatal("a brief typed under a titled composer was not seen as residue")
	}
	if briefInComposer("claude", submitted, claudeTestBrief) || !submittedBrief("claude", submitted, claudeTestBrief) {
		t.Fatal("a submitted brief above a titled composer was not seen as submitted")
	}
	a, d := claudeDeliveryApp(t, func(d *claudeDelivery) string {
		if d.pastes == 0 {
			return empty
		}
		return submitted
	}, idleStatus)
	if err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief); err != nil {
		t.Fatalf("titled composer broke the landing proof: %v", err)
	}
	if d.pastes != 1 || d.returns != 0 {
		t.Fatalf("pastes=%d returns=%d", d.pastes, d.returns)
	}
}

// agentStartArgv is the argv herdr was asked to start: what follows "--".
func agentStartArgv(t *testing.T, fr *fakeRunner) []string {
	t.Helper()
	for _, c := range fr.Calls {
		if c.Name == "herdr" && strings.HasPrefix(strings.Join(c.Args, " "), "agent start") {
			if i := slices.Index(c.Args, "--"); i >= 0 {
				return c.Args[i+1:]
			}
		}
	}
	t.Fatal("no herdr agent start call")
	return nil
}

// claudeSpawnFixture is ompSpawnFixture with a claude seat whose pane shows a
// Claude Code composer.
func claudeSpawnFixture(t *testing.T) (*App, Spawn, *fakeRunner) {
	t.Helper()
	a, s, fr := ompSpawnFixture(t)
	s.Profile = "opus"
	a.Quota = &fakeQuota{LaunchValue: Launch{Profile: "opus", ModelID: "claude-opus-5-5", Effort: "high", Grade: "S+", Pool: "claude", Gate: "default"}, GateDo: gatePools(map[string]string{"opus": "claude"})}
	base := fr.Do
	started, submitted := false, false
	fr.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch {
		case strings.HasPrefix(args, "agent start"):
			started = true
		case strings.HasPrefix(args, "agent prompt"):
			submitted = true
		case started && strings.HasPrefix(args, "pane process-info"):
			return Result{Out: `{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":2,"argv0":"claude"}]}}`}
		case started && (strings.HasPrefix(args, "pane read") || strings.HasPrefix(args, "agent read")):
			if submitted {
				return Result{Out: claudeScreenSubmitted()}
			}
			return Result{Out: claudeScreenEmpty()}
		case strings.HasPrefix(args, "agent list"):
			status := "idle"
			if submitted {
				status = "working"
			}
			return Result{Out: `{"agents":[{"pane_id":"w:p1","tab_id":"w:t1","name":"worker-a","agent":"claude","agent_status":"` + status + `"}]}`}
		}
		return base(c)
	}
	return a, s, fr
}

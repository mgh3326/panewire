package fleet

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
)

// A dry-run that reaches a hub host builds the same node args a real submit
// does. A flag the node's own argument rule refuses must be refused here,
// naming the flag, instead of printing a plan for a spawn the node would drop.
func TestHubSpawnDryRunRefusesArgTheNodeWouldDrop(t *testing.T) {
	fixture := func(t *testing.T) (*App, Spawn, *fakeHub) {
		a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
		s.DryRun, s.Profile, s.Host, s.OverrideReason, s.Effort, s.T, s.Label = true, "sonnet", "h1", "fixture", "high", "T1", "job-a"
		s.Prompt = writeBrief(t, a)
		a.Quota = &fakeQuota{LaunchValue: Launch{Profile: "sonnet", Effort: "high", Grade: "A", ModelID: "sonnet-m", Gate: "default", Pool: "claude"}, GateDo: gatePools(map[string]string{"sonnet": "claude"})}
		return a, s, hub
	}

	a, s, hub := fixture(t)
	s.OperatorRequest = "hk:doc/x#y"
	err := a.Spawn(context.Background(), s)
	if err == nil || exitCode(err) != exitUsage || !strings.Contains(err.Error(), "--operator-request") || !strings.Contains(err.Error(), "[A-Za-z0-9._:/@-]") {
		t.Fatalf("dry-run accepted an arg the node would drop: %v", err)
	}
	if len(hub.spawns) != 0 {
		t.Fatalf("dry-run reached the hub: %v", hub.spawns)
	}

	a, s, _ = fixture(t)
	s.OperatorRequest = "hk:doc/x/y"
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatalf("the same request with a valid ref was refused: %v", err)
	}
	if out := a.Out.(interface{ String() string }).String(); !strings.Contains(out, "dry-run host=h1") {
		t.Fatalf("valid dry-run did not reach h1: %q", out)
	}
}

func TestHubSpawnDryRunRequiresEscalationOptIn(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	setHubAgyPresence(a)
	s.DryRun, s.Profile, s.Host, s.OverrideReason, s.T, s.Label = true, "agy-sonnet46", "h1", "fixture", "T1", "job-a"
	s.Containment = "hk:4174"
	s.Prompt = writeBrief(t, a)
	a.Quota = &fakeQuota{LaunchValue: Launch{Profile: "agy-sonnet46", ModelID: "claude-sonnet-4-6", Grade: "B", Gate: escalationGate, Pool: "agy"}, GateDo: gatePools(map[string]string{"agy-sonnet46": "agy"})}

	err := a.Spawn(context.Background(), s)
	if err == nil || exitCode(err) != exitSeatRefused || !strings.Contains(err.Error(), "agy-sonnet46 is a escalation rung; pass --allow-escalation") {
		t.Fatalf("hub dry-run without escalation opt-in must match node refusal: %v", err)
	}
	if len(hub.spawns) != 0 {
		t.Fatalf("dry-run reached the hub: %v", hub.spawns)
	}

	a, s, _, hub, _ = hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	setHubAgyPresence(a)
	s.DryRun, s.Profile, s.Host, s.OverrideReason, s.T, s.Label, s.AllowEscalation = true, "agy-sonnet46", "h1", "fixture", "T1", "job-a", true
	s.Containment = "hk:4174"
	s.Prompt = writeBrief(t, a)
	a.Quota = &fakeQuota{LaunchValue: Launch{Profile: "agy-sonnet46", ModelID: "claude-sonnet-4-6", Grade: "B", Gate: escalationGate, Pool: "agy"}, GateDo: gatePools(map[string]string{"agy-sonnet46": "agy"})}
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatalf("hub dry-run with escalation opt-in was refused: %v", err)
	}
	if out := a.Out.(interface{ String() string }).String(); !strings.Contains(out, "dry-run host=h1") {
		t.Fatalf("admitted hub dry-run did not print its preview: %q", out)
	}
	if len(hub.spawns) != 0 {
		t.Fatalf("dry-run reached the hub: %v", hub.spawns)
	}
}

func setHubAgyPresence(a *App) {
	source := a.Source.(*fakePlacementSource)
	key := "agy:0123456789abcdef"
	source.accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: true, MeasuredAt: a.Now()}
	base := source.presenceDo
	source.presenceDo = func() []PresenceRecord {
		records := base()
		for _, host := range []string{"h1", "h2"} {
			records = append(records, PresenceRecord{HostID: host, AccountKey: key, LastSeen: a.Now()})
		}
		return records
	}
}

// A local spawn never becomes a hub job.spawn, so the hub wire rule does not
// apply to it: a value local admission accepts stays accepted.
func TestHubSpawnLocalDryRunKeepsItsOwnArgs(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	s.DryRun, s.Profile, s.Host, s.OverrideReason, s.Effort, s.T, s.Label, s.Workspace = true, "sonnet", "local", "fixture", "high", "T1", "job-a", "workers"
	s.Prompt = writeBrief(t, a)
	if err := os.MkdirAll(s.CWD, 0o700); err != nil {
		t.Fatal(err)
	}
	s.OperatorRequest = "hk:doc/x#y"
	a.Quota = &fakeQuota{LaunchValue: Launch{Profile: "sonnet", Effort: "high", Grade: "A", ModelID: "sonnet-m", Gate: "default", Pool: "claude"}, GateDo: gatePools(map[string]string{"sonnet": "claude"})}
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatalf("a local dry-run lost an input local admission accepts: %v", err)
	}
	if out := a.Out.(interface{ String() string }).String(); !strings.Contains(out, "dry-run host=") {
		t.Fatalf("local dry-run did not print: %q", out)
	}
	if len(hub.spawns) != 0 {
		t.Fatalf("local dry-run reached the hub: %v", hub.spawns)
	}
}

// The hub sends a valid spawn's flags unchanged, in the same order: the node
// accepts this exact list, and nothing about a valid request moved.
func TestHubArgsValidSpawnUnchanged(t *testing.T) {
	s := Spawn{Keep: true,
		OperatorRequest: "hk:doc/decision/2026-11-11/model-routing-escalation/A17-6", RequestedBy: "desk", Purpose: "operator-request",
		Profile: "sonnet", Workspace: "workers", Label: "job-a", T: "T2", Job: "job-a", Owner: "desk", Effort: "high",
		Role: "implementer", Lane: "job-a", Parent: "desk", Task: "3965", ReportRelative: "job-a/report.md", ExecutionLane: "live",
		Urgency: "normal", AuthorFamily: "anthropic", BuilderJob: "job-b", Grade: "B", RoutingRole: "implementer", ReviewPass: "2",
		OverrideReason: "hk 3965", Containment: "hk:3965"}
	want := []string{
		"--keep",
		"--operator-request", "hk:doc/decision/2026-11-11/model-routing-escalation/A17-6",
		"--requested-by", "desk",
		"--purpose", "operator-request",
		"-m", "sonnet",
		"-w", "workers",
		"-l", "job-a",
		"--t", "T2",
		"--job", "job-a",
		"--owner", "desk",
		"--effort", "high",
		"--role", "implementer",
		"--lane", "job-a",
		"--parent", "desk",
		"--task", "3965",
		"--report-relative", "job-a/report.md",
		"-L", "live",
		"--urgency", "normal",
		"--author-family", "anthropic",
		"--builder-job", "job-b",
		"--grade", "B",
		"--review-pass", "2",
		"--override-reason", "hk 3965",
		"--containment", "hk:3965",
	}
	args, err := hubArgs("/jobs", s)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("valid spawn args changed:\n got %v\nwant %v", args, want)
	}
}

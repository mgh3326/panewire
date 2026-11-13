package fleet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ladderRows is one family's catalog: sonnet climbs to xhigh (an escalation
// rung), opus stops at the claude cap (high) and fable is the next model.
func ladderRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "sonnet-m"},
		{Profile: "sonnet", Effort: "xhigh", Grade: "A+", Pool: "claude", Gate: "escalation", ModelID: "sonnet-m"},
		{Profile: "sonnet", Effort: "max", Grade: "A+", Pool: "claude", Gate: "escalation", ModelID: "sonnet-m"},
		{Profile: "opus", Effort: "high", Grade: "S+", Pool: "claude", Gate: "default", ModelID: "opus-m"},
		{Profile: "opus", Effort: "xhigh", Grade: "S+", Pool: "claude", Gate: "escalation", ModelID: "opus-m"},
		{Profile: "fable", Effort: "medium", Grade: "S+", Pool: "claude", Gate: "default", ModelID: "fable-m"},
		{Profile: "fable", Effort: "high", Grade: "S+", Pool: "claude", Gate: "default", ModelID: "fable-m"},
	}
}

// The first step is the same model one effort up, even onto an escalation
// rung, as long as the rung covers the task grade.
func TestLadderStepSameModelNextEffort(t *testing.T) {
	a, _, _ := selFixture(t, "A", ladderRows(), map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)}}, []string{"claude"})
	step, ok, err := a.NextLadderRung(context.Background(), LadderPick{Profile: "sonnet", Effort: "high", Pool: "claude", Family: "anthropic"}, "A", "T1")
	if err != nil || !ok {
		t.Fatalf("step=%v ok=%v err=%v", step, ok, err)
	}
	if step.Kind != ladderSameModelNextEffort || step.Pick.Profile != "sonnet" || step.Pick.Effort != "xhigh" {
		t.Fatalf("want sonnet@xhigh by %s, got %+v", ladderSameModelNextEffort, step)
	}
}

// At the family's cap the next model enters at its entry effort; a rung above
// the cap is never a step, even when the catalog has one.
func TestLadderStepStopsAtFamilyCap(t *testing.T) {
	a, _, _ := selFixture(t, "A", ladderRows(), map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)}}, []string{"claude"})
	step, ok, err := a.NextLadderRung(context.Background(), LadderPick{Profile: "opus", Effort: "high", Pool: "claude", Family: "anthropic"}, "A", "T1")
	if err != nil || !ok {
		t.Fatalf("step=%v ok=%v err=%v", step, ok, err)
	}
	if step.Kind != ladderNextModelAtEntry || step.Pick.Profile != "fable" || step.Pick.Effort != "medium" {
		t.Fatalf("the claude cap is high: want fable@medium by %s, got %+v", ladderNextModelAtEntry, step)
	}
}

// A step never goes to a rung graded below the task.
func TestLadderStepSkipsRungsBelowTheTaskGrade(t *testing.T) {
	a, _, _ := selFixture(t, "S", ladderRows(), map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)}}, []string{"claude"})
	step, ok, err := a.NextLadderRung(context.Background(), LadderPick{Profile: "sonnet", Effort: "high", Pool: "claude", Family: "anthropic"}, "S", "T1")
	if err != nil || !ok {
		t.Fatalf("step=%v ok=%v err=%v", step, ok, err)
	}
	// sonnet@xhigh and sonnet@max are A+, below the S task; fable@medium is S+.
	if step.Kind != ladderNextModelAtEntry || step.Pick.Profile != "fable" || step.Pick.Effort != "medium" {
		t.Fatalf("want fable@medium, got %+v", step)
	}
}

// A job that already runs above its family's cap (an operator-pinned rung)
// has no same-model step: the ladder goes on to the next model rather than
// slicing past the cap.
func TestLadderStepAboveTheCapGoesToTheNextModel(t *testing.T) {
	a, _, _ := selFixture(t, "A", ladderRows(), map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)}}, []string{"claude"})
	step, ok, err := a.NextLadderRung(context.Background(), LadderPick{Profile: "opus", Effort: "xhigh", Pool: "claude", Family: "anthropic"}, "A", "T1")
	if err != nil || !ok {
		t.Fatalf("step=%v ok=%v err=%v", step, ok, err)
	}
	if step.Kind != ladderNextModelAtEntry || step.Pick.Profile != "fable" || step.Pick.Effort != "medium" {
		t.Fatalf("opus@xhigh is above the claude cap (high): want fable@medium by %s, got %+v", ladderNextModelAtEntry, step)
	}
}

// The top of the last family with no other family's seat is the policy's
// "no step left".
func TestLadderStepNoRungAtTheTop(t *testing.T) {
	a, _, _ := selFixture(t, "A", ladderRows(), map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)}}, []string{"claude"})
	step, ok, err := a.NextLadderRung(context.Background(), LadderPick{Profile: "fable", Effort: "high", Pool: "claude", Family: "anthropic"}, "A", "T1")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("the top of the only family has no step: %+v", step)
	}
}

// A consult-only rung is never a ladder step: it is the wrong seat to run an
// author on.
func TestLadderStepSkipsConsultOnlyRungs(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "sonnet-m"},
		{Profile: "sonnet", Effort: "xhigh", Grade: "A+", Pool: "claude", Gate: "consult_only", ModelID: "sonnet-m"},
	}
	if _, ok := ladderNextEffort(rows, LadderPick{Profile: "sonnet", Effort: "high", Pool: "claude"}, LadderFamily{Models: []string{"sonnet"}, Efforts: []string{"low", "medium", "high", "xhigh"}, LadderCap: "xhigh"}, "A"); ok {
		t.Fatal("a consult_only rung must not be a ladder step")
	}
}

// The embedded block is the policy's: every family names its models and the
// claude cap stays high for opus.
func TestLadderFamiliesPolicyBlock(t *testing.T) {
	families, err := LoadFamilies()
	if err != nil {
		t.Fatal(err)
	}
	claude := families.Families["claude"]
	if claude.LadderCap != "high" || claude.CapByModel["sonnet"] != "max" || claude.Entry != "medium" || !contains(claude.Models, "opus") {
		t.Fatalf("claude family: %+v", claude)
	}
	if families.Ladder.TriggerReviewedHeads != 3 {
		t.Fatalf("trigger: %+v", families.Ladder)
	}
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

// fakeEscalateHub answers the one route escalate-author uses.
type fakeEscalateHub struct {
	*fakeHub
	request EscalateAuthorRequest
	result  EscalateAuthorResult
	err     error
	calls   int
}

func (h *fakeEscalateHub) EscalateAuthor(_ context.Context, request EscalateAuthorRequest) (EscalateAuthorResult, error) {
	h.calls++
	h.request = request
	return h.result, h.err
}

func escalateCLIFixture(t *testing.T, fromProfile, fromEffort string, withOtherFamily bool) (*App, *fakeEscalateHub) {
	t.Helper()
	rows := ladderRows()
	if withOtherFamily {
		rows = append(rows,
			CatalogRow{Profile: "codex-sol", Effort: "high", Grade: "A+", Pool: "codex", Gate: "default", ModelID: "sol-m"},
		)
	}
	a, _, q := selFixture(t, "A", rows, map[string]PoolView{
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)},
		"codex":  {Status: "ok", Class: "spend", Blocking: ptrF(10)},
	}, []string{"claude", "codex"})
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "opus": "claude", "fable": "claude", "codex-sol": "codex"})
	serveRouting(t, routingDoc(routingBlockV1))
	a.JobStore.(*fakeJobStore).records["job-a"] = map[string]any{
		"job_id": "job-a", "task_id": "42", "state": "spawned", "profile": fromProfile, "effort": fromEffort,
		"pool": "claude", "family": "anthropic", "model": "sonnet-m", "owner_lane": "parent",
	}
	events := []string{}
	hub := &fakeEscalateHub{fakeHub: &fakeHub{t: t, events: &events}}
	a.Hub = hub
	return a, hub
}

func writeFindings(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "findings.json")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// The CLI resolves the next rung and the consult seat, and sends both with the
// replaced job's rung so the hub can refuse a stale request.
func TestEscalateAuthorCLIResolvesRungAndConsult(t *testing.T) {
	a, hub := escalateCLIFixture(t, "sonnet", "high", true)
	hub.result = EscalateAuthorResult{Job: "job-a-l1", Rung: "sonnet@xhigh"}
	findings := writeFindings(t, `[{"ref":"r1","severity":"high","path":"x.go","line":3,"title":"t","body":"b"}]`)
	if err := a.escalateAuthorCmd(context.Background(), []string{"--task", "42", "--job", "job-a", "--pr", "mgh3326/panewire#77", "--round", "5", "--findings-file", findings}); err != nil {
		t.Fatal(err)
	}
	request := hub.request
	if request.Task != "42" || request.Job != "job-a" || request.PR != "mgh3326/panewire#77" || request.Round != 5 || request.Grade != "A" {
		t.Fatalf("request: %+v", request)
	}
	if request.FromProfile != "sonnet" || request.FromEffort != "high" {
		t.Fatalf("from: %+v", request)
	}
	if request.Profile != "sonnet" || request.Effort != "xhigh" || request.Step != ladderSameModelNextEffort {
		t.Fatalf("rung: %+v", request)
	}
	if !strings.HasPrefix(request.ConsultSeat, "codex-sol@") {
		t.Fatalf("the consult seat must be another family's, got %q", request.ConsultSeat)
	}
	var findingsSent []map[string]any
	if json.Unmarshal([]byte(request.Findings), &findingsSent) != nil || len(findingsSent) != 1 || findingsSent[0]["ref"] != "r1" {
		t.Fatalf("findings: %q", request.Findings)
	}
	if got := a.Out.(interface{ String() string }).String(); !strings.Contains(got, "ESCALATED job=job-a-l1 rung=sonnet@xhigh") {
		t.Fatalf("output: %q", got)
	}
}

// A hub refusal is rc 2: mergewake logs it and the PR goes on to its next
// round.
func TestEscalateAuthorCLIRefusalIsExitTwo(t *testing.T) {
	a, hub := escalateCLIFixture(t, "sonnet", "high", true)
	hub.result = EscalateAuthorResult{Code: "quota_refused", Reason: "every eligible account is measured exhausted"}
	err := a.escalateAuthorCmd(context.Background(), []string{"--task", "42", "--job", "job-a", "--pr", "mgh3326/panewire#77", "--round", "5", "--findings-file", writeFindings(t, `[]`)})
	if exitCode(err) != exitUsage || !strings.Contains(err.Error(), "quota_refused") {
		t.Fatalf("want rc 2 quota_refused, got %v (rc %d)", err, exitCode(err))
	}
}

// No rung left is still sent to the hub, which owns the design-lane record:
// the CLI asks for no rung and the hub answers no_rung.
func TestEscalateAuthorCLINoRungIsSentForTheRecord(t *testing.T) {
	a, hub := escalateCLIFixture(t, "fable", "high", false)
	hub.result = EscalateAuthorResult{Code: "no_rung", Reason: "the policy has no step left for this seat"}
	err := a.escalateAuthorCmd(context.Background(), []string{"--task", "42", "--job", "job-a", "--pr", "mgh3326/panewire#77", "--round", "5", "--findings-file", writeFindings(t, `[]`)})
	if exitCode(err) != exitUsage || !strings.Contains(err.Error(), "no_rung") {
		t.Fatalf("want rc 2 no_rung, got %v (rc %d)", err, exitCode(err))
	}
	if hub.calls != 1 || hub.request.Profile != "" || hub.request.Step != "" {
		t.Fatalf("the no-rung request must still reach the hub: %+v", hub.request)
	}
}

// A findings file that is not a JSON array refuses before anything is sent.
func TestEscalateAuthorCLIRejectsMalformedFindings(t *testing.T) {
	a, hub := escalateCLIFixture(t, "sonnet", "high", true)
	err := a.escalateAuthorCmd(context.Background(), []string{"--task", "42", "--job", "job-a", "--pr", "mgh3326/panewire#77", "--round", "5", "--findings-file", writeFindings(t, `{"not":"an array"}`)})
	if exitCode(err) != exitUsage || !strings.Contains(err.Error(), "JSON array") {
		t.Fatalf("want a usage refusal, got %v", err)
	}
	if hub.calls != 0 {
		t.Fatal("nothing must reach the hub")
	}
}

// A job whose record is not live cannot be escalated.
func TestEscalateAuthorCLIRejectsADeadJob(t *testing.T) {
	a, hub := escalateCLIFixture(t, "sonnet", "high", true)
	a.JobStore.(*fakeJobStore).records["job-a"]["state"] = "completed"
	err := a.escalateAuthorCmd(context.Background(), []string{"--task", "42", "--job", "job-a", "--pr", "mgh3326/panewire#77", "--round", "5", "--findings-file", writeFindings(t, `[]`)})
	if exitCode(err) != exitUsage || !strings.Contains(err.Error(), "not live") {
		t.Fatalf("want a usage refusal, got %v", err)
	}
	if hub.calls != 0 {
		t.Fatal("nothing must reach the hub")
	}
}

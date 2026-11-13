package fleet

import (
	"context"
	"strings"
	"testing"
	"time"
)

// hk 3518 (panewire#307 medium follow-up): a hub submission that carried no
// e6_arm stays unmarked — the hub process environment never fills it, and the
// same holds one hop later on the node. Only a local panewire spawn resolves
// PANEWIRE_E6_ARM/SCOPEFUEL_E6_ARM.

// e6HubEnvApp is the admission fixture of TestHubAdmissionUsesSubmittedE6Arm:
// the unrated omp-dsflash41 rung admits only with the exact marker.
func e6HubEnvApp(t *testing.T) (*App, *e6Quota) {
	t.Helper()
	a := testApp(t)
	a.Config.Local = map[string]any{"omp_commandcode": true}
	q := &e6Quota{
		fakeQuota: &fakeQuota{
			LaunchValue: Launch{Profile: "omp-dsflash41", ModelID: "provider/example-model", Effort: "high", Grade: "A+", Gate: "default", Pool: "example"},
			GateDo:      func(QuotaRequest) (string, error) { return "pool=example profile=omp-dsflash41\n", nil },
		},
		require: "omp-dsflash41@high",
	}
	a.Quota = q
	return a, q
}

func setE6HubEnv(t *testing.T) {
	t.Helper()
	t.Setenv("PANEWIRE_E6_ARM", "omp-dsflash41@high")
	t.Setenv("SCOPEFUEL_E6_ARM", "omp-dsflash41@high")
	t.Setenv("PACELEVER_E6_ARM", "omp-dsflash41@high")
}

// TestHubSubmissionE6ArmIgnoresHubEnv is the finding's reproduction: the hub
// process env names the marker the submission never carried, so admission
// must refuse the unrated rung instead of admitting it marked.
func TestHubSubmissionE6ArmIgnoresHubEnv(t *testing.T) {
	setE6HubEnv(t)
	parsed, err := ValidateSubmission(e6AdmissionSubmission(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	a, q := e6HubEnvApp(t)
	seat := a.Policy.Profiles["omp-dsflash41"]
	if _, _, err := a.admitLaunch(t.Context(), parsed, seat, "high", "high", "B", "", false); err == nil {
		t.Fatal("hub env marker admitted a submission that carried none")
	}
	if len(q.launched) == 0 || q.launched[0].E6Arm != "" {
		t.Fatalf("catalog step saw e6_arm=%q, want the submission's empty marker", q.launched[0].E6Arm)
	}
}

// TestNodeSpawnE6ArmIgnoresNodeEnv is the same rule one hop later: the node
// daemon's env must not mark a hub-delivered spawn whose argv carried no
// --e6-arm.
func TestNodeSpawnE6ArmIgnoresNodeEnv(t *testing.T) {
	setE6HubEnv(t)
	s := bindNodeSpawn(Spawn{Profile: "omp-dsflash41", Effort: "high", Role: "implementer", T: "T1", Task: "42", Owner: "parent"}, "/work/project", "brief", "workspace")
	a, q := e6HubEnvApp(t)
	seat := a.Policy.Profiles["omp-dsflash41"]
	if _, _, err := a.admitLaunch(t.Context(), s, seat, "high", "high", "B", "", false); err == nil {
		t.Fatal("node env marker admitted a spawn whose argv carried none")
	}
	if len(q.launched) == 0 || q.launched[0].E6Arm != "" {
		t.Fatalf("catalog step saw e6_arm=%q, want the argv's empty marker", q.launched[0].E6Arm)
	}
}

// TestLocalSpawnE6ArmReadsEnv keeps the initiator side: a spawn that is not
// hub- or node-delivered still resolves the marker from its environment.
func TestLocalSpawnE6ArmReadsEnv(t *testing.T) {
	setE6HubEnv(t)
	a, q := e6HubEnvApp(t)
	seat := a.Policy.Profiles["omp-dsflash41"]
	s := Spawn{CWD: "/work/project", Profile: "omp-dsflash41", Prompt: "/work/brief.md", Job: "job-a", Label: "job-a", T: "T1", Task: "42", Role: "implementer", Owner: "parent", Host: "auto", Effort: "high", Containment: "hk:12"}
	if _, _, err := a.admitLaunch(t.Context(), s, seat, "high", "high", "B", "", false); err != nil {
		t.Fatalf("local spawn lost the env marker: %v", err)
	}
	if len(q.launched) == 0 || q.launched[0].E6Arm != "omp-dsflash41@high" {
		t.Fatalf("catalog step saw e6_arm=%q, want the env marker", q.launched[0].E6Arm)
	}
}

// TestHubQuotaE6ArmIgnoresProcessEnv covers the native catalog and gate: the
// QuotaRequest a locked spawn builds carries E6ArmLocked, so neither
// launchRequest nor requestVerdict falls back to the process environment.
func TestHubQuotaE6ArmIgnoresProcessEnv(t *testing.T) {
	setE6HubEnv(t)
	isolateConfigHome(t)
	t.Setenv("PACELEVER_CONFIG", "")
	rows := []CatalogRow{{Profile: "omp-dsflash41", Effort: "high", Grade: "unrated", Pool: "commandcode", Gate: "default", ModelID: "commandcode/deepseek/deepseek-v4.1-flash"}}
	a, _, _ := launchApp(t, "native", rows, nil)
	p, err := LoadPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	a.Policy = p
	parsed, err := ValidateSubmission(e6AdmissionSubmission(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	qr, ok := a.quotaRequest(parsed)
	if !ok || qr.E6Arm != "" {
		t.Fatalf("hub quota request e6_arm=%q ok=%v, want the submission's empty marker", qr.E6Arm, ok)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if _, err := a.Quota.Launch(ctx, qr); err == nil || !strings.Contains(err.Error(), "e6_arm_required") {
		t.Fatalf("native launch admitted the unrated rung on the process env marker: %v", err)
	}
	if got := a.nativeGate(ctx, qr, true); got.Code == 0 || !strings.Contains(got.Out, "e6_arm_required") {
		t.Fatalf("native gate admitted the unrated rung on the process env marker: code=%d out=%s", got.Code, got.Out)
	}
}

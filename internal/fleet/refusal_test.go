package fleet

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
)

func TestActionableSpawnRefusals(t *testing.T) {
	for _, test := range []struct {
		message, fix string
		status       int
	}{
		{"agent not in foreground; brief withheld", "harness binary", exitConfig},
		{"workspace workers not found", "herdr workspace list", exitConfig},
		{"workspace trust: malformed workspace trust store", "Repair", exitConfig},
		{"workspace trust: refusing broad workspace trust", "specific worktree", exitConfig},
		{"workspace trust: permission denied", "trust store", exitConfig},
		{"brief is empty", "-p", exitUsage},
		{"active duplicate job job-a", "panewire find", exitDuplicate},
		{"task scale does not match --t", "--t", exitSeatRefused},
		{"initial submission unconfirmed", "saved", exitSubmitUnconfirmed},
		{"account unavailable", "Sign in", exitAuthUnavailable},
		{"role_restricted: removed astra seat alias", "Pick a seat the refusal's rule admits", exitSeatRefused},
	} {
		err := fail(test.status, "%s", test.message)
		if code(err) != test.status || !strings.Contains(spawnRefusalFix(err), test.fix) {
			t.Fatalf("%s: code=%d fix=%s", test.message, code(err), spawnRefusalFix(err))
		}
	}
	err := trustPromptError("/work/project")
	if code(err) != exitTrustPrompt || !strings.Contains(spawnRefusalFix(err), "executing CODEX_HOME") {
		t.Fatalf("real Codex trust refusal: code=%d fix=%s", code(err), spawnRefusalFix(err))
	}
	// Same status as the trust prompt, but its own fix: the host owner reviews hooks.
	err = hooksReviewError()
	if code(err) != exitTrustPrompt || !strings.Contains(spawnRefusalFix(err), "reviews the hooks") || strings.Contains(spawnRefusalFix(err), "worktree") {
		t.Fatalf("Codex hooks review refusal: code=%d fix=%s", code(err), spawnRefusalFix(err))
	}
}

// The observed causes (hk:doc research/2026-11-04/fleet-run-observability), each
// with the text its fail site writes and the status it exits with.
var spawnRefusalCauses = []struct {
	text string
	rc   int
	code string
}{
	{"agent not in foreground; brief withheld", exitConfig, "foreground_missing"},
	{`workspace "reviews" matched 2 entries; run herdr workspace list on the executing host and pass -w WORKSPACE_ID`, exitUsage, "herdr_workspace_ambiguous"},
	{"report must be under the job or parent job directory", exitUsage, "report_path"},
	{"QUOTA_UNKNOWN: credit pool status unavailable", exitQuotaUnknown, "quota_unknown"},
	{"AUTH_UNAVAILABLE: target no longer holds the selected account", exitAuthUnavailable, "auth_unavailable"},
	{"seat does not allow role worker", exitSeatRefused, "seat_refused"},
	{"role_restricted: removed astra seat alias", exitSeatRefused, "role_restricted"},
	{"BRIEF_SUBMISSION_UNCONFIRMED: session=s pane=p; no working transition or submitted brief turn", exitSubmitUnconfirmed, "submit_unconfirmed"},
	{"agent must be idle before initial brief", exitSubmitUnconfirmed, "submit_unconfirmed"},
	{"WORKSPACE_TRUST_REQUIRED: Codex directory trust prompt for /w; brief withheld; approve only this worktree in the executing CODEX_HOME", exitTrustPrompt, "trust_prompt"},
	{"HOOKS_REVIEW_REQUIRED: Codex is waiting at its Hooks need review screen; brief withheld", exitTrustPrompt, "hooks_review"},
	{"job id job-a already claimed", exitDuplicate, "duplicate"},
	{"task has another claimant without a job binding", exitUsage, "task_claimed"},
	{"task bound to another job", exitUsage, "task_bound"},
	{"task unavailable: done", exitUsage, "task_unavailable"},
	{"task does not exist", exitUsage, "task_not_found"},
	{"symlink path refused: /jobs-root", exitUsage, "symlink_path"},
	{"cwd is not a directory", exitUsage, "cwd_not_directory"},
	{"OMP_SETUP_WIZARD: omp is in its setup wizard despite OMP_SKIP_SETUP; seat withheld", exitConfig, "omp_setup_wizard"},
	// hk 3301: the brief upload's hk guard refusal names the pattern, and the
	// broad "brief" row must not claim it.
	{"brief upload to briefs/job-a.md failed: handoffkeep: secret_like_content:private_key", exitPlacement, "hk_guard_refused"},
	{"handoffkeep: secret_like_content", exitUsage, "hk_guard_refused"},
}

func TestSpawnRefusalCodeNamesEachObservedCause(t *testing.T) {
	for _, test := range spawnRefusalCauses {
		if got := SpawnRefusalCode(test.text, test.rc); got != test.code {
			t.Errorf("%q rc=%d: code %q, want %q", test.text, test.rc, got, test.code)
		}
	}
}

func TestSpawnRefusalCodeUnclassifiedIsEmpty(t *testing.T) {
	for _, test := range []struct {
		text string
		rc   int
	}{{"some refusal nobody has written yet", exitConfig}, {"some refusal nobody has written yet", exitFailure}, {"", exitFailure}, {"", 0}} {
		if got := SpawnRefusalCode(test.text, test.rc); got != "" {
			t.Errorf("%q rc=%d: code %q, want none", test.text, test.rc, got)
		}
	}
}

func TestSpawnRefusalCodeFromTheRealFailSites(t *testing.T) {
	a, _, f, _ := cmdFixture(t)
	f.Do = func(Command) Result {
		return Result{Out: `{"result":{"workspaces":[{"workspace_id":"wA","label":"reviews"},{"workspace_id":"wB","label":"reviews"}]}}`}
	}
	_, _, ambiguous := a.resolveWorkspace(context.Background(), "reviews")
	for _, test := range []struct {
		err  error
		code string
	}{{ambiguous, "herdr_workspace_ambiguous"}, {trustPromptError("/work/project"), "trust_prompt"}, {hooksReviewError(), "hooks_review"}} {
		if got := SpawnRefusalCode(test.err.Error(), code(test.err)); got != test.code {
			t.Errorf("%v rc=%d: code %q, want %q", test.err, code(test.err), got, test.code)
		}
	}
}

func TestSpawnRefusalTextWinsOverExitStatusInTableOrder(t *testing.T) {
	for _, test := range []struct {
		text string
		rc   int
		code string
	}{
		{"task scale does not match --t", exitSeatRefused, "task_scale"},
		{"requires PANEWIRE_E6_ARM=x@y", exitSeatRefused, "e6_arm_required"},
		{"workspace trust: malformed workspace trust store", exitConfig, "trust_store_malformed"},
		{"workspace trust: permission denied", exitConfig, "trust_store"},
		{"symlink path refused: /srv/workspace/brief", exitUsage, "symlink_path"},
		{"unrecognised", exitSeatRefused, "seat_refused"},
	} {
		if got := SpawnRefusalCode(test.text, test.rc); got != test.code {
			t.Errorf("%q rc=%d: code %q, want %q", test.text, test.rc, got, test.code)
		}
	}
}

func TestSpawnRefusalCodesAreAClosedSet(t *testing.T) {
	valid := regexp.MustCompile(`^[a-z0-9_]+$`)
	for _, row := range spawnRefusals {
		if !valid.MatchString(row.code) || len(row.code) > 40 || row.hint == "" {
			t.Errorf("row %+v breaks the code contract", row)
		}
		if row.match == "" && row.rc == 0 {
			t.Errorf("row %+v would match every text", row)
		}
		if !IsSpawnRefusalCode(row.code) {
			t.Errorf("%q is not in its own set", row.code)
		}
	}
	for _, code := range []string{"", "spawn_failed", "made_up", "foreground_missing: text"} {
		if IsSpawnRefusalCode(code) {
			t.Errorf("%q must not be in the set", code)
		}
	}
}

// Text and status reach the code and the hint through the same row.
func TestSpawnRefusalHintComesFromTheCodeRow(t *testing.T) {
	for _, test := range spawnRefusalCauses {
		row, ok := findSpawnRefusal(test.text, test.rc)
		if fix := spawnRefusalFix(fail(test.rc, "%s", test.text)); !ok || row.code != test.code || fix != row.hint {
			t.Errorf("%q rc=%d: row %+v, fix %q", test.text, test.rc, row, fix)
		}
	}
}

// These inputs are where the order decides, so each keeps its hint exactly.
func TestSpawnRefusalPrecedenceKeepsEveryHint(t *testing.T) {
	const (
		trustPrompt  = "Approve only this worktree in the executing CODEX_HOME, then repeat the same command; the brief was withheld until trust is confirmed."
		brief        = "Pass -p with a readable nonempty brief file; for presets, save it under jobs/JOB/brief.md or override -p."
		submit       = "Read the saved jobs/JOB/spawn-failure-*.txt capture and fix the harness submission failure; retry the same command after verified cleanup."
		submitWindow = "Read the saved jobs/JOB/spawn-failure-*.txt capture. If the brief sat in the composer on a loaded host, raise local.brief_submit_timeout_ms there; retry the same command after verified cleanup."
		generic      = "Run panewire preflight --json and repair the failed dependency or configuration named above before repeating the command."
	)
	for _, test := range []struct {
		err error
		fix string
	}{
		{fail(exitTrustPrompt, "agent not in foreground; brief withheld"), trustPrompt},
		{fail(exitSubmitUnconfirmed, "BRIEF_SUBMISSION_UNCONFIRMED: session=s pane=p; no working transition or submitted brief turn"), submitWindow},
		{fail(exitSubmitUnconfirmed, "agent must be idle before initial brief"), brief},
		{fail(exitSubmitUnconfirmed, "initial submission unconfirmed"), submit},
		{fail(exitConfig, "nothing the table names"), generic},
		{errors.New("an untyped failure"), generic},
	} {
		if got := spawnRefusalFix(test.err); got != test.fix {
			t.Errorf("%v: fix %q, want %q", test.err, got, test.fix)
		}
	}
}

// WaitSpawn quotes the stored reason in the error the submitter's Fix line reads,
// so a code in that text must not select a different row.
func TestSpawnRefusalCodeInWaitSpawnTextKeepsItsHint(t *testing.T) {
	for _, test := range spawnRefusalCauses {
		quoted := fail(test.rc, "spawn refused: spawn_failed:%s: %s (operation op)", test.code, test.text)
		if got, want := spawnRefusalFix(quoted), spawnRefusalFix(fail(test.rc, "%s", test.text)); got != want {
			t.Errorf("%s: fix %q, want %q", test.code, got, want)
		}
	}
	for _, row := range spawnRefusals {
		if row.rc == 0 {
			continue
		}
		bare, _ := findSpawnRefusal("", row.rc)
		quoted, _ := findSpawnRefusal("spawn refused: spawn_failed:"+row.code+" (operation op)", row.rc)
		if bare.hint != quoted.hint {
			t.Errorf("%s: quoted code selected %q", row.code, quoted.code)
		}
	}
}

// Refusal advice names the panewire command where one exists: `panewire quota
// refresh POOL` measures every pool (running pacelever itself for a pool
// panewire does not measure yet), and the catalog is panewire's.
func TestSpawnRefusalAdviceNamesPanewire(t *testing.T) {
	for _, row := range spawnRefusals {
		if strings.Contains(row.hint, "pacelever refresh") {
			t.Errorf("%s hint names pacelever refresh: %s", row.code, row.hint)
		}
	}
	for _, tc := range []struct {
		err  error
		want string
	}{
		{fail(3, "quota catalog: e6_arm_required: unplaced profile requires exact measurement marker"), "panewire catalog list"},
		{fail(exitQuotaUnknown, "QUOTA_UNKNOWN: claude"), "panewire quota refresh POOL"},
		{fail(exitAuthUnavailable, "AUTH_UNAVAILABLE: claude"), "panewire quota refresh POOL"},
		{fail(exitRoleWait, "ROLE_WAIT"), "panewire quota refresh POOL"},
	} {
		if fix := spawnRefusalFix(tc.err); !strings.Contains(fix, tc.want) {
			t.Errorf("%v: fix %q lacks %q", tc.err, fix, tc.want)
		}
	}
}

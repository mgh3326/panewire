package panewire

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// mergewakeReviewerLabel is a 31-character mergewake reviewer label, the length
// that made every quota replacement of a reviewer fail at herdr (hk 5961).
const mergewakeReviewerLabel = "mwr-2520-f2220b9-p12-1763700000"

func TestSuccessorNameFitsHerdrAgentName(t *testing.T) {
	if len(mergewakeReviewerLabel) != 31 {
		t.Fatalf("fixture is %d characters, want 31", len(mergewakeReviewerLabel))
	}
	q1, ok := successorName(mergewakeReviewerLabel, "q", 1)
	if !ok || len(q1) > successorNameMax || !fleet.ValidAgentName(q1) || !fleet.ValidJobID(q1) {
		t.Fatalf("q1 successor %q (ok=%v, %d chars) is not a valid agent name of at most %d", q1, ok, len(q1), successorNameMax)
	}
	if !strings.HasSuffix(q1, "-q1") {
		t.Fatalf("q1 successor %q lost its replacement marker", q1)
	}
	q2, ok := successorName(q1, "q", 2)
	if !ok || len(q2) > successorNameMax || !fleet.ValidAgentName(q2) {
		t.Fatalf("q2 successor %q of %q (ok=%v) does not fit", q2, q1, ok)
	}
	if again, _ := successorName(mergewakeReviewerLabel, "q", 1); again != q1 {
		t.Fatalf("successor name is not deterministic: %q then %q", q1, again)
	}
	other, _ := successorName(mergewakeReviewerLabel+"x", "q", 1)
	if other == q1 {
		t.Fatalf("two different olds share successor %q", q1)
	}
	if l1, _ := successorName(mergewakeReviewerLabel, "l", 1); l1 == q1 {
		t.Fatalf("ladder and quota successors of one label collide at %q", q1)
	}
}

func TestSuccessorLabelRefusesNamesOutsideHerdrCharset(t *testing.T) {
	// A job id may keep a dot or an uppercase letter (pw.job_2 is a valid job),
	// but herdr takes only a lowercase agent name for the -l label.
	if job, _, ok := quotaReplacementJob("pw.job_2"); !ok || job != "pw.job_2-q1" {
		t.Fatalf("job pw.job_2 -> %q ok=%v, want pw.job_2-q1", job, ok)
	}
	for _, label := range []string{"Mwr-1-p1", "1mwr-p1", "mwr.p1"} {
		old := fleet.Submission{Args: []string{"--job", label, "-l", label}}
		if _, err := quotaReplacementRequest(old, label, label+"-q1", "claude", "claude", "reviewer"); err == nil {
			t.Fatalf("a successor with label %q was built", label)
		}
	}
}

func TestQuotaReplaceSuccessorLabelFitsHerdr(t *testing.T) {
	job, n, ok := quotaReplacementJob(mergewakeReviewerLabel)
	if !ok || n != 1 || len(job) > successorNameMax {
		t.Fatalf("q1 job %q n=%d ok=%v", job, n, ok)
	}
	old := fleet.Submission{Args: []string{"--job", mergewakeReviewerLabel, "-l", mergewakeReviewerLabel, "--report-relative", mergewakeReviewerLabel + "/report.md"}}
	next, err := quotaReplacementRequest(old, mergewakeReviewerLabel, job, "claude", "claude", "reviewer")
	if err != nil {
		t.Fatalf("quotaReplacementRequest: %v", err)
	}
	if got := hubSpawnArg(next.Args, "--job"); got != job {
		t.Fatalf("--job %q, want %q", got, job)
	}
	if got := hubSpawnArg(next.Args, "-l"); got != job || len(got) > successorNameMax || !fleet.ValidAgentName(got) {
		t.Fatalf("-l %q, want the job name %q (at most %d chars)", got, job, successorNameMax)
	}
	if got := hubSpawnArg(next.Args, "--report-relative"); got != job+"/report.md" {
		t.Fatalf("--report-relative %q", got)
	}
	// The second replacement is built from the first one's request, the way
	// the hub builds it on the replacement's own quota stop.
	job2, n2, ok := quotaReplacementJob(job)
	if !ok || n2 != 2 || len(job2) > successorNameMax {
		t.Fatalf("q2 job %q n=%d ok=%v", job2, n2, ok)
	}
	next2, err := quotaReplacementRequest(next, job, job2, "claude", "claude", "reviewer")
	if err != nil {
		t.Fatalf("second quotaReplacementRequest: %v", err)
	}
	if got := hubSpawnArg(next2.Args, "-l"); got != job2 || !fleet.ValidAgentName(got) {
		t.Fatalf("second successor -l %q, want %q", got, job2)
	}
	if _, _, ok := quotaReplacementJob(job2); ok {
		t.Fatalf("a third quota replacement of %q was allowed", job2)
	}
}

func TestQuotaReplaceRequestRefusesAMismatchedJob(t *testing.T) {
	old := fleet.Submission{Args: []string{"--job", "mwr-9-p1", "-l", "mwr-9-p1"}}
	if _, err := quotaReplacementRequest(old, "mwr-9-p1", "mwr-9-p1-q2", "claude", "claude", "reviewer"); err == nil {
		t.Fatal("a successor that is not the next quota replacement was built")
	}
}

func TestEscalateAuthorSuccessorLabelFitsHerdr(t *testing.T) {
	job, step, ok := escalateAuthorJob(mergewakeReviewerLabel)
	if !ok || step != 1 || len(job) > successorNameMax || !fleet.ValidJobID(job) {
		t.Fatalf("ladder job %q step=%d ok=%v", job, step, ok)
	}
	args, err := escalateAuthorArgs([]string{"--job", mergewakeReviewerLabel, "-l", mergewakeReviewerLabel}, hubEscalateAuthorRequest{Job: mergewakeReviewerLabel, Profile: "claude", Round: 1}, job)
	if err != nil {
		t.Fatalf("escalateAuthorArgs: %v", err)
	}
	if got := hubSpawnArg(args, "-l"); got != job || !fleet.ValidAgentName(got) {
		t.Fatalf("ladder -l %q, want %q", got, job)
	}
}

// A ladder step keeps its marker when the name is shortened: a quota
// replacement of a ladder successor is still one ladder step, and the ladder
// successor of it is refused.
func TestEscalateAuthorShortenedLadderKeepsItsStep(t *testing.T) {
	ladder, _, ok := escalateAuthorJob(mergewakeReviewerLabel)
	if !ok {
		t.Fatalf("no ladder successor for %q", mergewakeReviewerLabel)
	}
	if _, _, again := escalateAuthorJob(ladder); again {
		t.Fatalf("a second ladder step of %q was allowed", ladder)
	}
	quota, _, ok := quotaReplacementJob(ladder)
	if !ok {
		t.Fatalf("no quota replacement of %q", ladder)
	}
	if !escalateAuthorChain.MatchString(quota) {
		t.Fatalf("quota replacement %q of ladder successor %q lost its ladder marker", quota, ladder)
	}
	if _, _, ok := escalateAuthorJob(quota); ok {
		t.Fatalf("a ladder step of %q was allowed after the quota replacement", quota)
	}
}

func TestHandoverSuccessorUsesTheSharedLabelHelper(t *testing.T) {
	old := fleet.Submission{Args: []string{"--job", mergewakeReviewerLabel, "-l", mergewakeReviewerLabel, "--report-relative", mergewakeReviewerLabel + "/report.md"}, Repository: fleet.Repository{Name: "panewire", Checkout: &fleet.HubCheckout{Origin: "https://example.test/panewire.git"}}}
	job, _, ok := quotaReplacementJob(mergewakeReviewerLabel)
	if !ok {
		t.Fatalf("no q1 job for %q", mergewakeReviewerLabel)
	}
	read := nodeHandoverCheckResult{Branch: "wk/fixture", Pushed: strings.Repeat("a", 40)}
	next, err := handoverSuccessorRequest(old, mergewakeReviewerLabel, job, "claude", "machine-b", poolExhaustion{Pool: "claude"}, read, nil)
	if err != nil {
		t.Fatalf("handoverSuccessorRequest: %v", err)
	}
	if got := hubSpawnArg(next.Args, "-l"); got != job || !fleet.ValidAgentName(got) {
		t.Fatalf("hand-over -l %q, want the shared helper's %q", got, job)
	}
	if got := hubSpawnArg(next.Args, "--parent-job"); got != mergewakeReviewerLabel {
		t.Fatalf("hand-over --parent-job %q", got)
	}
	if !slices.Contains(next.Args, "--job") {
		t.Fatalf("hand-over args carry no --job: %v", next.Args)
	}
}

func TestNodeSpawnRefusesOverlongAgentNameBeforePane(t *testing.T) {
	root := t.TempDir()
	var engineCalls int
	client := &HubClient{spawnConfigPath: t14SpawnConfig(t, root), spawnEngine: func(_ context.Context, _, _, _, _, _ string, _ time.Time, _ []string) (*hubSpawnOutput, fleet.Execution) {
		engineCalls++
		return t14EngineOK()
	}}
	// The engine is the step that opens the pane; a refused name never reaches it.
	long := mergewakeReviewerLabel + "-q1-q2"
	result := client.runHubSpawn(t.Context(), hubOutboundMessage{RequestID: t14RequestID(5961), CWDKey: "repo-a", Args: []string{"--job", long, "-l", long}, BriefInline: "brief"})
	if result.RC == 0 || result.Error != "label_invalid" || result.Retryable {
		t.Fatalf("result=%+v, want a non-retryable label_invalid refusal", result)
	}
	if engineCalls != 0 {
		t.Fatalf("the engine ran %d time(s) for an invalid agent name", engineCalls)
	}
	if !strings.Contains(result.StdoutTail, "herdr agent name") {
		t.Fatalf("refusal does not name the rule: %q", result.StdoutTail)
	}
}

// The hand-over end reason names the successor job. A long old job used to
// push that name past the end reason limit (end_reason_too_long); its successor
// now fits herdr's name limit, so the reason fits whatever the old id's length.
func TestHandoverEndReasonFitsForLongOldJob(t *testing.T) {
	old := strings.Repeat("e", 100)
	newJob, _, ok := quotaReplacementJob(old)
	if !ok {
		t.Fatalf("no successor for a 100-character job")
	}
	reason := handoverEndReason(map[string]any{"pool": "claude"}, poolExhaustion{Pool: "claude"}, newJob)
	if !validNodeEndReason(reason) {
		t.Fatalf("end reason %q (%d characters) is refused", reason, len(reason))
	}
}

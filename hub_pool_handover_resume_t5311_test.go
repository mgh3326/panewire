package panewire

import (
	"testing"
	"time"
)

// The resume limit counts the node's own resumes (hk 5311, review
// 4610713976): a stop after a node-side resume hands over, and a node record
// that cannot be read hands over rather than waiting again.

func TestPoolHandoverNodeResumeIsTheWaitTaken(t *testing.T) {
	t.Run("no resume on record waits", func(t *testing.T) {
		const job = "resume-control-job"
		f := handoverFixture(t, job, 3, 3*time.Hour)
		result := sweepHandoverWithResumes(t, f, window5h(), nil, map[string]any{"found": false, "quota_resumes": 0})
		if result.action != poolJobNotified || result.why != "reset_within_expected" {
			t.Fatalf("sweep=%+v, want the wait", result)
		}
	})
	t.Run("one node resume hands over", func(t *testing.T) {
		const job = "resume-once-job"
		f := handoverFixture(t, job, 3, 3*time.Hour)
		result := sweepHandoverWithResumes(t, f, window5h(), handoverReadReply(job), map[string]any{"found": false, "quota_resumes": 1})
		if result.action != poolJobQueued || result.why != "resume_limit" {
			t.Fatalf("sweep=%+v, want a hand-over after the node's resume", result)
		}
	})
}

func TestPoolHandoverUnreadableResumeRecordHandsOver(t *testing.T) {
	for name, resumes := range map[string]map[string]any{
		"node error":                  {"error": "result_failed"},
		"node predates quota_resumes": {"found": true, "findings_error": "missing"},
	} {
		t.Run(name, func(t *testing.T) {
			const job = "resume-unreadable-job"
			f := handoverFixture(t, job, 3, 3*time.Hour)
			result := sweepHandoverWithResumes(t, f, window5h(), handoverReadReply(job), resumes)
			if result.action != poolJobQueued || result.why != "resume_limit" {
				t.Fatalf("sweep=%+v, want a hand-over, not a wait", result)
			}
		})
	}
}

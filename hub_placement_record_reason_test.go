package panewire

import (
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hkPlacementReasonPrintable copies hk's placement record reason rule: printable
// text of at most 256 bytes. hk refuses any other reason with 400
// invalid_context ("reason: not printable text of at most 256 bytes"). The root
// test copies the predicate rather than importing hk.
func hkPlacementReasonPrintable(s string) bool {
	if len(s) > 256 || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// A spawn outcome's reason can be a whole engine message: newlines, a control
// byte, and text well past 256 bytes. The multi-byte rune below starts at byte
// 255, so a naive byte cut at 256 would split it.
func longSpawnFailureReason() string {
	head := "spawn_failed: panewire: warning: pane cleanup failed;\nreinspect\twith panewire find before retry\x00 "
	return head + strings.Repeat("x", 256-len(head)-1) + "—" + strings.Repeat("y", 1024)
}

func TestHubPlacementRecordReasonIsBoundedForHK(t *testing.T) {
	reason := longSpawnFailureReason()
	if len(reason) < 1024 {
		t.Fatalf("fixture reason is %d bytes, want at least 1024", len(reason))
	}

	t.Run("built record", func(t *testing.T) {
		task := hubSpawnTask{Job: "job-5666", Attempt: 1}
		task.Outcome = hubSpawnOutcome{RC: 1, Stage: "spawn", NodeResult: true, Host: "ncp", Reason: reason}
		tr := fleet.PlacementTrace{
			DecidedAt:   "2026-11-20T00:00:00Z",
			Source:      "auto",
			Ranking:     "score",
			StateSource: "prometheus",
			Chosen:      "ncp",
			Candidates: []fleet.PlacementCandidateRecord{{
				Host:    "mbp",
				Outcome: "skipped",
				Reason:  reason,
			}},
		}
		rec := hubPlacementRecord(task, tr, 1)
		if rec.Outcome != "failed" && rec.Outcome != "refused" {
			t.Fatalf("outcome = %q, want the spawn verdict", rec.Outcome)
		}
		if !hkPlacementReasonPrintable(rec.Reason) {
			t.Fatalf("record reason is not hk-printable (%d bytes): %q", len(rec.Reason), rec.Reason)
		}
		if !strings.HasPrefix(rec.Reason, "spawn_failed: panewire:") {
			t.Fatalf("record reason lost its leading code: %q", rec.Reason)
		}
		for _, c := range rec.Candidates {
			if !hkPlacementReasonPrintable(c.Reason) {
				t.Fatalf("candidate %s reason is not hk-printable (%d bytes): %q", c.Host, len(c.Reason), c.Reason)
			}
		}
		if got := tr.Candidates[0].Reason; got != reason {
			t.Fatalf("trace candidate reason was rewritten: the bound must not mutate the caller's trace")
		}
	})

	t.Run("posted as built", func(t *testing.T) {
		task := hubSpawnTask{Job: "job-5666", Attempt: 1}
		tr := fleet.PlacementTrace{Record: &fleet.PlacementRecord{
			Outcome:    "failed",
			Reason:     reason,
			Candidates: []fleet.PlacementCandidateRecord{{Host: "ncp", Outcome: "chosen", Reason: reason}},
		}}
		rec := hubPlacementRecord(task, tr, 1)
		if !hkPlacementReasonPrintable(rec.Reason) {
			t.Fatalf("finished record reason is not hk-printable (%d bytes): %q", len(rec.Reason), rec.Reason)
		}
		if !hkPlacementReasonPrintable(rec.Candidates[0].Reason) {
			t.Fatalf("finished record candidate reason is not hk-printable: %q", rec.Candidates[0].Reason)
		}
	})
}

package panewire_test

import (
	"testing"

	panewire "github.com/mgh3326/panewire"
)

// #547 AC5: the deliveries row records which read source and which
// classifySubmission rule judged the submission.
func TestTask547PromptRecordsSubmissionEvidence(t *testing.T) {
	devinQueued := "○ R2-MARKER\n── 2 queued ── ↑ edit · ↵ send now\n────\n❭ Press Enter to send queued messages now\n────\n"
	devinSubmitted := "❭ R2-MARKER\n⠋ Thinking 2s\n────\n❭ Guide Devin while it works\n────\n"
	cases := []struct {
		name, harness, screen, submission, evidence string
	}{
		{"devin queued only", "devin", devinQueued, "queued", "recent_unwrapped:devin_queue_banner"},
		{"devin submitted", "devin", devinSubmitted, "marker_observed", "recent_unwrapped:marker_echo"},
		{"claude queued", "claude", "Press up to edit queued messages\n", "queued", "recent_unwrapped:queued_banner"},
		{"codex echo", "codex", "› R2-MARKER\n\n• Working (1s)\n" + promptCodexComposer, "marker_observed", "recent_unwrapped:marker_echo"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newHerdrFixture(t, promptFixtureSchema(false))
			defer fixture.Close()
			configurePromptFixture(fixture, tc.harness, tc.screen)
			d, db := startPromptDaemon(t, fixture)
			defer d.Stop()
			if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t)}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitOK {
				t.Fatalf("exit=%d want %d", got, panewire.ExitOK)
			}
			delivery, ok, err := db.LatestDelivery(t.Context())
			if err != nil || !ok {
				t.Fatalf("no delivery row: ok=%t err=%v", ok, err)
			}
			if delivery.SubmissionResult != tc.submission || delivery.SubmissionEvidence != tc.evidence {
				t.Fatalf("submission=%q evidence=%q, want %q %q", delivery.SubmissionResult, delivery.SubmissionEvidence, tc.submission, tc.evidence)
			}
		})
	}
}

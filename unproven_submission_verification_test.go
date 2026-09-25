package panewire

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// R1: classifySubmission has exactly four possible outputs. This table pins
// an expectation for every one of them so the enumeration stays closed.
func TestClassifySubmissionAllFourValues(t *testing.T) {
	cases := []struct {
		name    string
		harness string
		screen  string
		marker  string
		want    string
	}{
		{
			name:    "paste chip anywhere is composer_residue regardless of harness",
			harness: "",
			screen:  "prompt\n[Pasted text #1]\n",
			marker:  "",
			want:    "composer_residue",
		},
		{
			name:    "claude echo between the last two dividers is composer_residue",
			harness: "claude",
			screen:  "───────\nhello world\n───────\n",
			marker:  "hello world",
			want:    "composer_residue",
		},
		{
			name:    "claude queued banner is queued",
			harness: "claude",
			screen:  "Press up to edit queued messages",
			marker:  "",
			want:    "queued",
		},
		{
			name:    "codex queued banner is queued",
			harness: "codex",
			screen:  "Press up to edit queued messages",
			marker:  "",
			want:    "queued",
		},
		{
			name:    "claude screen carrying the marker is marker_observed",
			harness: "claude",
			screen:  "prefix hello world suffix",
			marker:  "hello world",
			want:    "marker_observed",
		},
		{
			name:    "codex screen carrying the marker is marker_observed",
			harness: "codex",
			screen:  "hello world",
			marker:  "hello world",
			want:    "marker_observed",
		},
		{
			name:    "claude screen missing the marker is unproven",
			harness: "claude",
			screen:  "nothing relevant here",
			marker:  "hello world",
			want:    "unproven",
		},
		{
			name:    "marker present but harness is neither claude nor codex is unproven",
			harness: "some-other-harness",
			screen:  "hello world",
			marker:  "hello world",
			want:    "unproven",
		},
		{
			name:    "empty screen and marker is unproven",
			harness: "claude",
			screen:  "",
			marker:  "",
			want:    "unproven",
		},
		// devin fixtures below are literal screen strings captured live on
		// 2026-09-16 from an idle devin CLI pane (w1M:p5, v3000.10.27,
		// cwd=brewdial) via `herdr agent read --source visible`, plus the
		// operator's own capture from hk:doc
		// brief/2026-09-16/devin-submission-evidence. None are synthesized.
		{
			name:    "devin queued banner (operator capture) is queued",
			harness: "devin",
			screen:  "── 1 queued ──────────────────────────────────────── ↑ edit · ↵ send now ──\n○ hi\n─────────────────────\n❭ Press Enter to send queued messages now\n─────────────────────\nSWE-2 High",
			marker:  "",
			want:    "queued",
		},
		{
			name:    "devin marker echoed above the composer (post-submit, idle placeholder returned) is marker_observed",
			harness: "devin",
			screen:  " Just let me know.\n\n─────────────────────\n❭ hi\n─────────────────────\n─────────────────────\n❭ Ask Devin to build features, fix bugs, or work on your code\n─────────────────────\nSWE-2 High",
			marker:  "hi",
			want:    "marker_observed",
		},
		{
			name:    "devin marker still between the last two dividers (submit-to-redraw transient, live-measured) is composer_residue",
			harness: "devin",
			screen:  " Just let me know.\n\n─────────────────────\n❭ observe test marker 1789535304\n─────────────────────\nSWE-2 High",
			marker:  "observe test marker 1789",
			want:    "composer_residue",
		},
		{
			name:    "devin marker moved above the last two dividers once working starts (live-measured) is marker_observed",
			harness: "devin",
			screen:  "❭ observe test marker 1789535304\n\n⠇⠀ Thinking · 0s (esc twice to interrupt)\n─────────────────────\n❭ Guide Devin while it works\n─────────────────────\nSWE-2 High",
			marker:  "observe test marker 1789",
			want:    "marker_observed",
		},
		{
			name:    "devin marker absent entirely, idle placeholder present, is unproven",
			harness: "devin",
			screen:  "─────────────────────\n❭ Ask Devin to build features, fix bugs, or work on your code\n─────────────────────\nSWE-2 High",
			marker:  "observe test marker 1789",
			want:    "unproven",
		},
		{
			// Live-captured 2026-09-16: submitting a second prompt while
			// devin is still working on the first queues it, and the
			// queued item's own text is echoed in the queue banner
			// itself ("○ <text>") in the same screen the marker check
			// would also match against -- so queued must be decided
			// before marker_observed or this would wrongly report
			// marker_observed for a message that has not been sent yet.
			name:    "devin queued banner containing the marker itself still classifies queued, not marker_observed",
			harness: "devin",
			screen:  "❭ queuecheckA 1789535839\n\n⠀⣠ Thinking · 1s (esc twice to interrupt)\n── 1 queued ──────────────────────────────────────── ↑ edit · ↵ send now ──\n○ queuecheckB 1789535839\n─────────────────────\n❭ Press Enter to send queued messages now\n─────────────────────\nSWE-2 High",
			marker:  "queuecheckB 1789535839",
			want:    "queued",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifySubmission(tc.harness, tc.screen, tc.marker); got != tc.want {
				t.Fatalf("classifySubmission(%q, %q, %q) = %q, want %q", tc.harness, tc.screen, tc.marker, got, tc.want)
			}
		})
	}
}

// unprovenFakeHerdr writes a herdr shim on PATH whose `agent read` output cycles
// through reads (one entry per call, the last entry repeats after that) and
// whose `agent send-keys` calls are recorded but otherwise inert. Reads go
// through printf %b so a multi-line screen keeps its line breaks. It never
// touches the workstation's real herdr binary.
func unprovenFakeHerdr(t *testing.T, log string, reads []string) string {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "herdr")
	count := filepath.Join(dir, "count")
	var caseLines strings.Builder
	for i, r := range reads {
		fmt.Fprintf(&caseLines, "      %d) printf '%%b' %q ;;\n", i+1, r)
	}
	if len(reads) > 0 {
		fmt.Fprintf(&caseLines, "      *) printf '%%b' %q ;;\n", reads[len(reads)-1])
	}
	script := "#!/bin/sh\n" +
		fmt.Sprintf("echo \"$2\" >> %q\n", log) +
		"case \"$2\" in\n" +
		"  read)\n" +
		fmt.Sprintf("    n=$(cat %q 2>/dev/null || echo 0)\n", count) +
		"    n=$((n+1))\n" +
		fmt.Sprintf("    echo $n > %q\n", count) +
		"    case $n in\n" +
		caseLines.String() +
		"    esac\n" +
		"    ;;\n" +
		"  send-keys) : ;;\n" +
		"esac\n"
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

// unprovenClaudeChip is claude's live composer holding only a paste chip (#626:
// the one return keypress is sent only when the located composer holds this
// inject's own text, so a bare chip line with no composer around it no longer
// earns one).
const unprovenClaudeChip = "───────\n❯ [Pasted text #1 +3 lines]\n───────\n  ⏵⏵ bypass permissions on (shift+tab to cycle)"

// unprovenClaudeEmpty is claude's empty composer, the pre-paste read that
// proves a chip seen afterwards is the inject's own.
const unprovenClaudeEmpty = "───────\n❯\n───────\n  ⏵⏵ bypass permissions on (shift+tab to cycle)"

// unprovenClaudeQueued is claude's queue banner above an empty composer.
const unprovenClaudeQueued = "Press up to edit queued messages\n───────\n❯\n───────\n  ⏵⏵ bypass permissions on (shift+tab to cycle)"

// unprovenDevinQueued is the live devin queue capture from
// TestClassifySubmissionAllFourValues; unprovenDevinResidue is devin's
// composer still holding this inject's text; unprovenDevinQueuedDraft is the
// queue banner over a composer holding someone else's draft.
const (
	unprovenDevinQueued      = "── 1 queued ──────────────────────────────────────── ↑ edit · ↵ send now ──\n○ hi\n─────────────────────\n❭ Press Enter to send queued messages now\n─────────────────────\nSWE-2 High"
	unprovenDevinResidue     = "─────────────────────\n❭ one line\n─────────────────────\nSWE-2 High"
	unprovenDevinQueuedDraft = "── 1 queued ──────────────────────────────────────── ↑ edit · ↵ send now ──\n○ hi\n─────────────────────\n❭ 운영자 확인 완료: PASS 로 전환해줘\n─────────────────────\nSWE-2 High"
)

func unprovenSetupHerdr(t *testing.T, reads []string) (log string, calls func() []string) {
	t.Helper()
	dir := t.TempDir()
	log = filepath.Join(dir, "herdr.log")
	binDir := unprovenFakeHerdr(t, log, reads)
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return log, func() []string {
		b, err := os.ReadFile(log)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			t.Fatal(err)
		}
		return strings.Fields(string(b))
	}
}

// R1 defect ②: a directly unproven submission (no composer residue, no
// queued banner, no marker) must not be reported delivered.
func TestRelayInjectVerifySubmissionUnprovenDirectIsNotDelivered(t *testing.T) {
	_, calls := unprovenSetupHerdr(t, []string{"nothing useful on screen"})
	if relayInjectVerifySubmission(context.Background(), "test-pane", "claude", "one line") {
		t.Fatal("unproven submission (no return pressed) reported delivered")
	}
	// #683: the two reads are the visible screen and the recent-unwrapped
	// transcript.
	if got := calls(); strings.Join(got, ",") != "read,read" {
		t.Fatalf("unexpected herdr calls: %q", got)
	}
}

// R1 defect ①: this is the previously untested path. composer_residue is
// seen first (the return-once contract fires), and the re-read after that
// return keypress classifies as unproven rather than marker_observed or
// composer_residue/queued. That must still not be reported delivered.
func TestRelayInjectVerifySubmissionComposerResidueThenUnprovenIsNotDelivered(t *testing.T) {
	_, calls := unprovenSetupHerdr(t, []string{unprovenClaudeChip, "still nothing useful"})
	if relayInjectVerify(context.Background(), "test-pane", "claude", "one line", unprovenClaudeEmpty).Outcome == relayInjectDelivered {
		t.Fatal("unproven submission after one return keypress reported delivered")
	}
	if got := calls(); strings.Join(got, ",") != "read,read,send-keys,read,read" {
		t.Fatalf("unexpected herdr calls: %q", got)
	}
}

// Only a proven nonce echo reports delivered, on the direct path.
func TestRelayInjectVerifySubmissionMarkerObservedDirectIsDelivered(t *testing.T) {
	text := task687Text(9001, "one line")
	_, calls := unprovenSetupHerdr(t, []string{"earlier output\n❯ " + text})
	if !relayInjectVerifySubmission(context.Background(), "test-pane", "claude", text) {
		t.Fatal("nonce echo submission (no return pressed) reported unconfirmed")
	}
	if got := calls(); strings.Join(got, ",") != "read,read" {
		t.Fatalf("unexpected herdr calls: %q", got)
	}
}

// Regression guard for the return-once contract: composer_residue on the
// first read, followed by a proven nonce echo re-read, must still
// report delivered after the fix.
func TestRelayInjectVerifySubmissionComposerResidueThenMarkerObservedIsDelivered(t *testing.T) {
	text := task687Text(9002, "one line")
	_, calls := unprovenSetupHerdr(t, []string{unprovenClaudeChip, "earlier output\n❯ " + text})
	if relayInjectVerify(context.Background(), "test-pane", "claude", text, unprovenClaudeEmpty).Outcome != relayInjectDelivered {
		t.Fatal("nonce echo submission after one return keypress reported unconfirmed")
	}
	if got := calls(); strings.Join(got, ",") != "read,read,send-keys,read,read" {
		t.Fatalf("unexpected herdr calls: %q", got)
	}
}

// The composer_residue/queued arm's own behavior (press return exactly once,
// then treat a still-residue/queued re-read as unconfirmed) must be
// unchanged by the R1 fix.
func TestRelayInjectVerifySubmissionComposerResidueArmUnchanged(t *testing.T) {
	_, calls := unprovenSetupHerdr(t, []string{unprovenClaudeChip, unprovenClaudeChip})
	if relayInjectVerify(context.Background(), "test-pane", "claude", "one line", unprovenClaudeEmpty).Outcome == relayInjectDelivered {
		t.Fatal("still-residue submission reported delivered")
	}
	if got := calls(); strings.Join(got, ",") != "read,read,send-keys,read,read" {
		t.Fatalf("unexpected herdr calls: %q", got)
	}
}

// #683: the queued arm changed polarity -- the banner is accepted as landed
// (the harness's own queue submits it when the turn ends), and it earns no
// return keypress at all. #687: the queued row's nonce must be on the
// transcript for the banner to count.
func TestRelayInjectVerifySubmissionQueuedArmUnchanged(t *testing.T) {
	text := task687Text(9003, "one line")
	queued := "❯ " + text + "\n" + unprovenClaudeQueued
	_, calls := unprovenSetupHerdr(t, []string{queued, queued})
	result := relayInjectVerify(context.Background(), "test-pane", "claude", text, "")
	if result.Outcome != relayInjectQueued {
		t.Fatalf("queued submission: result=%+v, want queued (landed)", result)
	}
	if got := calls(); strings.Join(got, ",") != "read,read" {
		t.Fatalf("unexpected herdr calls: %q", got)
	}
}

// #626: codex draws no composer divider and its placeholder is plain text,
// so no read proves its composer holds only this inject's text. Its chip
// arm withholds the return instead; its queue banner is landed (#683).
func TestRelayInjectVerifySubmissionCodexWithholdsReturn(t *testing.T) {
	for _, screen := range []string{"[Pasted text #1]\n› Ask Codex to do anything\n"} {
		_, calls := unprovenSetupHerdr(t, []string{screen})
		result := relayInjectVerify(context.Background(), "test-pane", "codex", "one line", "")
		if result.Outcome != relayInjectMaybeInPane || result.Evidence != "return_withheld:composer_unlocated" {
			t.Fatalf("screen %q: result=%+v, want maybe-in-pane return_withheld:composer_unlocated", screen, result)
		}
		if got := calls(); strings.Join(got, ",") != "read,read" {
			t.Fatalf("screen %q: unexpected herdr calls: %q", screen, got)
		}
	}
	// codex's own queue evidence -- the documented banner and the
	// "submitted after next tool call" note -- counts as landed, without a
	// return keypress. #687: the nonce must be on the transcript too.
	text := task687Text(9004, "one line")
	for _, banner := range []string{"Press up to edit queued messages", "• Messages to be submitted after next tool call"} {
		_, calls := unprovenSetupHerdr(t, []string{banner + "\n\n❯ " + text + "\n› Ask Codex to do anything\n"})
		result := relayInjectVerify(context.Background(), "test-pane", "codex", text, "")
		if result.Outcome != relayInjectQueued || result.Evidence != "queued_banner" {
			t.Fatalf("banner %q: result=%+v, want queued", banner, result)
		}
		if got := calls(); strings.Join(got, ",") != "read,read" {
			t.Fatalf("banner %q: unexpected herdr calls: %q", banner, got)
		}
	}
}

// TestRelayInjectVerifySubmissionHarnessEvidenceMatrix is the closed
// enumeration the 2026-09-16 rework requires: every harness family (claude,
// codex, devin -- added this rework, no longer the no-evidence carve-out --
// and an unrecognized/empty harness string, which keeps the carve-out)
// crossed with every classifySubmission value it can actually reach, pinning
// both the delivered/unconfirmed result and whether that result would drive
// a hub retry (busy_relay.go's retryOrDrop fires on false;
// defaultHubRelayInject sends the prompt before verifying, so a retry
// re-injects into a still-live pane -- see hub_client.go's
// harnessHasSubmissionEvidence for why that distinction exists).
//
// Reachability by harness (classifySubmission's own gating):
//   - claude:  composer_residue (chip or divider-echo), queued, marker_observed, unproven
//   - codex:   composer_residue (chip only), queued, marker_observed, unproven
//   - devin:   composer_residue (chip, or divider-echo via the same
//     claudeComposerContains position check -- devin shares claude's fixed
//     two-divider composer layout), queued ("send now"), marker_observed,
//     unproven
//   - other:   composer_residue (chip only), unproven -- queued and
//     marker_observed stay unreachable (harness-gated); this is the
//     remaining carve-out (e.g. grok, see harnessHasSubmissionEvidence)
//
// #683 changed the verdicts this matrix pins: a queue banner is landed
// (relayInjectQueued), and anything the reads cannot prove is
// relayInjectMaybeInPane -- busy_relay.go never re-injects it. Only a send
// herdr rejected is retryable now (see relayInjectForHarness).
func TestRelayInjectVerifySubmissionHarnessEvidenceMatrix(t *testing.T) {
	// #687: proof is the injected text's own nonce, so every landed screen
	// shows the nonce-bearing relay text; banner-only screens are unproven.
	text := task687Text(9100, "one line")
	cases := []struct {
		name    string
		harness string
		reads   []string // visible and recent-unwrapped alternate per call
		want    relayInjectOutcome
	}{
		{"claude marker_observed direct", "claude", []string{"earlier output\n❯ " + text}, relayInjectDelivered},
		{"claude unproven direct, composer seen empty", "claude", []string{"nothing relevant\n" + unprovenClaudeEmpty}, relayInjectMaybeInPane},
		{"claude unproven direct, no composer on screen", "claude", []string{"nothing relevant"}, relayInjectMaybeInPane},
		{"claude queued is landed without a return", "claude", []string{"❯ " + text + "\n" + unprovenClaudeQueued}, relayInjectQueued},
		{"claude composer_residue then unproven after return", "claude", []string{unprovenClaudeChip, "", "nothing relevant\n" + unprovenClaudeEmpty}, relayInjectMaybeInPane},
		{"claude composer_residue then no composer after return", "claude", []string{unprovenClaudeChip, "", "nothing relevant"}, relayInjectMaybeInPane},

		{"codex marker_observed direct", "codex", []string{text}, relayInjectDelivered},
		{"codex unproven direct", "codex", []string{"nothing relevant"}, relayInjectMaybeInPane},
		{"codex queued is landed without a return", "codex", []string{"Press up to edit queued messages\n❯ " + text}, relayInjectQueued},
		// #626: codex's return is withheld (no located composer); the result
		// is may-be-in-pane, which busy_relay.go never re-injects.
		{"codex composer_residue, return withheld", "codex", []string{"[Pasted text #1]"}, relayInjectMaybeInPane},

		// devin: added in the 2026-09-16 rework. Since #547 the hub relay
		// sends devin through devinRelayInject, which decides retry vs
		// may-be-in-pane itself; these rows pin only this function's result.
		{"devin marker_observed direct", "devin", []string{"earlier output\n❭ " + text}, relayInjectDelivered},
		{"devin unproven direct", "devin", []string{"nothing relevant"}, relayInjectMaybeInPane},
		{"devin queued is landed without a return", "devin", []string{strings.Replace(unprovenDevinQueued, "○ hi", "○ "+text, 1)}, relayInjectQueued},
		{"devin composer_residue then unproven after return", "devin", []string{unprovenDevinResidue, "", "nothing relevant"}, relayInjectMaybeInPane},
		// The queue evidence outranks the foreign draft in the composer: the
		// banner means this inject's send was accepted into the queue, and
		// the queued row carries this inject's nonce.
		{"devin queued over a foreign draft is landed", "devin", []string{strings.Replace(unprovenDevinQueuedDraft, "○ hi", "○ "+text, 1)}, relayInjectQueued},

		// An unrecognized/empty harness string keeps the no-evidence
		// carve-out: unproven reports delivered.
		{"unrecognized harness unproven direct", "some-future-harness", []string{"nothing relevant"}, relayInjectDelivered},
		{"empty harness unproven direct", "", []string{"nothing relevant"}, relayInjectDelivered},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			unprovenSetupHerdr(t, tc.reads)
			before := unprovenClaudeEmpty
			if tc.harness == "devin" {
				before = "─────────────────────\n❭ Ask Devin to build features, fix bugs, or work on your code\n─────────────────────\nSWE-2 High"
			}
			result := relayInjectVerify(context.Background(), "test-pane", tc.harness, text, before)
			if result.Outcome != tc.want {
				t.Fatalf("harness=%q reads=%v: result=%+v, want outcome %d", tc.harness, tc.reads, result, tc.want)
			}
		})
	}
}

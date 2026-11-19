package panewire

// hk 5413 (mergewake 734, relay_harness_adapter.go): a relay-length directive
// wraps omp's composer onto continuation rows, each indented by the wrap
// indent, and only the first row carries the corner glyph. These tests run on
// the long captures in testdata/relay_omp (long-*), typed and then submitted.
//
// The capture typed the nonce "[r5413L1]" with an uppercase L, which the relay's
// nonce pattern (lowercase only) does not accept. ompLongFixture maps it to the
// relay-valid "[r5413l1]" so the echo proof is tested with a real nonce; the
// wrap shape, which is what these tests are about, is unchanged.

import (
	"strings"
	"testing"
)

// ompLongText is the directive with a relay-valid nonce.
const ompLongText = "[r5413l1] [mergewake] findings for the worker: fix the failing check in relay_harness_adapter.go by reading the composer row as the last non-blank row, and add a test that a long directive of several hundred characters, with its nonce at the head, is located and submitted exactly once; the text must not be clipped at pane width and must not be read as a fragment; reply with Fixed in the sha when the test fails on the previous head and passes on the new one, and name the test in the reply. Also check that a wrapped composer keeps its corner glyph on the last row."

// ompLongFixture is a long capture with its nonce made relay-valid.
func ompLongFixture(t *testing.T, name string) string {
	t.Helper()
	return strings.ReplaceAll(ompFixture(t, name), "[r5413L1]", "[r5413l1]")
}

// The whole wrapped composer is located: the glyph row and its continuation
// rows read as one text. Before the fix the last row is a continuation row, so
// the composer was unlocated and presend refused every long directive.
func TestOmpWrappedComposerLocatesWholeDirective_5413(t *testing.T) {
	got, ok := relayComposer("omp", ompLongFixture(t, "long-typed-unsent-visible.txt"))
	if !ok {
		t.Fatal("a wrapped omp composer must be located")
	}
	if want := compactWhitespace(ompLongText); got != want {
		t.Fatalf("wrapped composer = %q, want the whole directive %q", got, want)
	}
}

func TestOmpWrappedPendingPasteIsResidueNotProof_5413(t *testing.T) {
	reads := ompReads(ompLongFixture(t, "long-typed-unsent-visible.txt"), ompLongFixture(t, "long-typed-unsent-unwrapped.txt"))
	result, rule, proven := relayClassifyReads("omp", reads, ompLongText, true, false)
	if result != "composer_residue" || len(proven) != 0 {
		t.Fatalf("classify = %q (%s), proven %v; want composer_residue with no proof", result, rule, proven)
	}
}

// The submitted long directive is echoed above the composer, which is empty
// after the send: the nonce at its head proves delivery.
func TestOmpWrappedSubmittedEchoIsProof_5413(t *testing.T) {
	reads := ompReads(ompLongFixture(t, "long-submitted-visible.txt"), ompLongFixture(t, "long-submitted-unwrapped.txt"))
	result, rule, proven := relayClassifyReads("omp", reads, ompLongText, true, false)
	if result != "marker_observed" || len(proven) != 1 || proven[0] != "[r5413l1]" {
		t.Fatalf("classify = %q (%s), proven %v; want marker_observed on the nonce echo", result, rule, proven)
	}
}

// A row below the glyph that is not a wrap continuation means the screen holds
// something else: the composer is unlocated, so nothing is read as its text.
func TestOmpNonContinuationRowBelowComposerIsUnlocated_5413(t *testing.T) {
	screen := ompLongFixture(t, "long-typed-unsent-visible.txt") + "\nstray row with no indent\n"
	if _, ok := relayComposer("omp", screen); ok {
		t.Fatal("a non-continuation row below the composer must leave it unlocated")
	}
	if got := promptTranscript("omp", screen); got != "" {
		t.Fatalf("unlocated composer: transcript %q, want empty", got)
	}
}

package panewire

// hk 5413 (mergewake 734): omp's composer placeholder is cut only when it is the
// whole empty composer, and a blank row inside a wrapped composer is accepted.

import (
	"strings"
	"testing"
)

// An empty composer shows the right-aligned placeholder and nothing else.
func TestOmpEmptyPlaceholderIsEmptyComposer_5413(t *testing.T) {
	if got := relayComposerState("omp", ompFixture(t, "idle-empty-visible.txt")); got != "composer_empty" {
		t.Fatalf("state = %q, want composer_empty", got)
	}
}

// A draft that starts with the placeholder text is composer text. The whole
// draft must be read, so presend refuses to type into it. Before the fix the
// placeholder cut removed the draft and the composer read empty.
func TestOmpDraftStartingWithHintIsComposerText_5413(t *testing.T) {
	draft := strings.ReplaceAll(ompFixture(t, "idle-empty-visible.txt"), ompComposerHint, ompComposerHint+" keep this draft")
	if got := relayComposerState("omp", draft); got != "composer_pending" {
		t.Fatalf("state = %q, want composer_pending for a draft", got)
	}
	got, ok := relayComposer("omp", draft)
	if !ok || !strings.HasSuffix(got, "keepthisdraft") {
		t.Fatalf("composer = %q, located %v; want the whole draft", got, ok)
	}
}

// A blank row between continuation rows is part of the wrapped composer.
func TestOmpBlankRowBetweenContinuationRowsIsLocated_5413(t *testing.T) {
	screen := "╰─ [r5413p1] first\n\n   second\n"
	got, ok := relayComposer("omp", screen)
	if !ok || got != "[r5413p1]firstsecond" {
		t.Fatalf("composer = %q, located %v; want the text across the blank row", got, ok)
	}
}

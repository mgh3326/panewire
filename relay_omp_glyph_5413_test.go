package panewire

// hk 5413 (mergewake 734, thread 4625460369): the composer glyph opens the
// composer at column 0. A wrap continuation whose text begins with the glyph is
// indented, so it must not be taken for the composer row.

import "testing"

func TestOmpContinuationStartingWithGlyphIsNotComposerRow_5413(t *testing.T) {
	screen := "╰─ [r5413p1] first\n   ╰─ second\n"
	got, ok := relayComposer("omp", screen)
	if !ok || got != "[r5413p1]first╰─second" {
		t.Fatalf("composer = %q, located %v; want the whole composer from the column-0 glyph", got, ok)
	}
	if relayComposerState("omp", screen) != "composer_pending" {
		t.Fatalf("a non-empty composer must read composer_pending, not empty")
	}
}

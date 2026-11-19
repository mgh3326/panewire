package panewire

// hk 5413: relay delivery into omp panes. The screens are captured from an
// isolated omp 18.14.2 with no model signed in (testdata/relay_omp/README.md):
// an empty composer, the composer with a marker typed and not submitted, and
// the same marker submitted. The submit is echoed above the composer, which
// empties, and herdr never shows a working turn, because omp refused the
// prompt with "No model selected".

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const ompMarker = "[r5413p1] composer probe"

func ompFixture(t *testing.T, name string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", "relay_omp", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// ompScreen is a captured omp screen with the marker replaced by typed, for
// fixtures that drive a relay whose composed text is not the marker.
func ompScreen(name, typed string) string {
	body, err := os.ReadFile(filepath.Join("testdata", "relay_omp", name))
	if err != nil {
		panic(err)
	}
	return strings.ReplaceAll(string(body), ompMarker, typed)
}

// ompReads is the pair of reads one verification takes, from captured screens.
func ompReads(visible, unwrapped string) relayPaneReads {
	return relayPaneReads{visible: visible, unwrapped: unwrapped, anyOK: true, visibleOK: true}
}

func TestOmpComposerRowLocatesEmptyAndTypedText_5413(t *testing.T) {
	if got, ok := relayComposer("omp", ompFixture(t, "idle-empty-visible.txt")); !ok || got != "" {
		t.Fatalf("idle composer = %q, located %v; want empty and located", got, ok)
	}
	want := compactWhitespace(ompMarker)
	if got, ok := relayComposer("omp", ompFixture(t, "typed-unsent-visible.txt")); !ok || got != want {
		t.Fatalf("typed composer = %q, located %v; want %q", got, ok, want)
	}
}

// The transcript stops above the composer row. A composer that is not the
// last row is unlocated, and then the transcript is empty, never the screen:
// otherwise a typed row would read as an echo.
func TestOmpTranscriptStopsAboveComposer_5413(t *testing.T) {
	typed := ompFixture(t, "typed-unsent-visible.txt")
	if got := promptTranscript("omp", typed); strings.Contains(got, "composer probe") {
		t.Fatalf("transcript holds the composer row: %q", got)
	}
	if got := promptTranscript("omp", typed+"\nstray row\n"); got != "" {
		t.Fatalf("unlocated composer: transcript %q, want empty", got)
	}
}

// A pending paste is composer residue, never proof of delivery.
func TestOmpTypedUnsentIsResidueNotProof_5413(t *testing.T) {
	reads := ompReads(ompFixture(t, "typed-unsent-visible.txt"), ompFixture(t, "typed-unsent-unwrapped.txt"))
	result, rule, proven := relayClassifyReads("omp", reads, ompMarker, true, false)
	if result != "composer_residue" || len(proven) != 0 {
		t.Fatalf("classify = %q (%s), proven %v; want composer_residue with no proof", result, rule, proven)
	}
}

// The submitted echo is proof, even though herdr never shows a working turn.
func TestOmpSubmittedEchoIsProof_5413(t *testing.T) {
	reads := ompReads(ompFixture(t, "submitted-visible.txt"), ompFixture(t, "submitted-unwrapped.txt"))
	result, rule, proven := relayClassifyReads("omp", reads, ompMarker, true, false)
	if result != "marker_observed" || !slices.Equal(proven, []string{"[r5413p1]"}) || !strings.Contains(rule, "nonce_echo") {
		t.Fatalf("classify = %q (%s), proven %v; want marker_observed on the nonce echo", result, rule, proven)
	}
}

// Mutant "accept a text fragment as proof": the text without its nonce is not
// proof, and this screen shows exactly that.
func TestOmpTextFragmentIsNotProof_5413(t *testing.T) {
	visible := strings.ReplaceAll(ompFixture(t, "submitted-visible.txt"), "[r5413p1] ", "")
	unwrapped := strings.ReplaceAll(ompFixture(t, "submitted-unwrapped.txt"), "[r5413p1] ", "")
	result, rule, proven := relayClassifyReads("omp", ompReads(visible, unwrapped), ompMarker, true, false)
	if result == "marker_observed" || len(proven) != 0 {
		t.Fatalf("classify = %q (%s), proven %v; a text fragment without the nonce is not proof", result, rule, proven)
	}
}

// A draft in the composer that is not this inject's is never typed over: the
// presend check refuses before anything is sent.
func TestOmpForeignDraftIsNotTypedOver_5413(t *testing.T) {
	typed := ompFixture(t, "typed-unsent-visible.txt")
	unwrapped := ompFixture(t, "typed-unsent-unwrapped.txt")
	_, calls := task683FakeHerdr(t, "omp", []string{typed}, []string{unwrapped})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", "[r5413p2] another row", nil)
	if result.Outcome != relayInjectMaybeInPane || result.Evidence != "presend:composer_pending" {
		t.Fatalf("result=%+v, want maybe-in-pane presend:composer_pending", result)
	}
	if got := calls(); task683Count(got, "prompt") != 0 || task683Count(got, "send-keys") != 0 {
		t.Fatalf("herdr calls = %q, want nothing typed or pressed", got)
	}
}

// A typed-but-unsubmitted marker is submitted once by the one return and then
// proven by its echo.
func TestOmpTypedUnsentSubmitsOnceAndProves_5413(t *testing.T) {
	_, calls := task683FakeHerdr(t, "omp",
		[]string{ompFixture(t, "idle-empty-visible.txt"), ompFixture(t, "typed-unsent-visible.txt"), ompFixture(t, "submitted-visible.txt")},
		[]string{ompFixture(t, "idle-empty-visible.txt"), ompFixture(t, "typed-unsent-unwrapped.txt"), ompFixture(t, "submitted-unwrapped.txt")})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", ompMarker, nil)
	if result.Outcome != relayInjectDelivered {
		t.Fatalf("result=%+v, want delivered after the one return", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 || task683Count(got, "send-keys") != 1 {
		t.Fatalf("herdr calls = %q, want one prompt and one return", got)
	}
}

// Mutant "submit twice": a composer that keeps the text after the one return
// is final. The second return is never pressed.
func TestOmpStuckComposerIsSubmittedOnce_5413(t *testing.T) {
	typed := ompFixture(t, "typed-unsent-visible.txt")
	typedUnwrapped := ompFixture(t, "typed-unsent-unwrapped.txt")
	_, calls := task683FakeHerdr(t, "omp",
		[]string{ompFixture(t, "idle-empty-visible.txt"), typed, typed},
		[]string{ompFixture(t, "idle-empty-visible.txt"), typedUnwrapped, typedUnwrapped})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", ompMarker, nil)
	if result.Outcome != relayInjectMaybeInPane || !strings.Contains(result.Evidence, "composer_divider") {
		t.Fatalf("result=%+v, want maybe-in-pane on the composer after the return", result)
	}
	if got := calls(); task683Count(got, "send-keys") != 1 || task683Count(got, "prompt") != 1 {
		t.Fatalf("herdr calls = %q, want exactly one return and no second prompt", got)
	}
}

// An absent message (empty composer, no echo) is retried once. The retry's own
// presend check meets the empty composer again, so it is never a second copy
// on top of a pending paste.
func TestOmpAbsentMessageIsRetriedOnce_5413(t *testing.T) {
	prevWindow := relayObserveWindow
	relayObserveWindow = 0
	t.Cleanup(func() { relayObserveWindow = prevWindow })
	idle := ompFixture(t, "idle-empty-visible.txt")
	_, calls := task683FakeHerdr(t, "omp", []string{idle}, []string{idle})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", ompMarker, nil)
	if result.Outcome != relayInjectMaybeInPane || result.Evidence != "unproven:composer_empty" {
		t.Fatalf("result=%+v, want maybe-in-pane unproven:composer_empty", result)
	}
	if !relayRetriesOnce(result) {
		t.Fatal("an absent omp message must be retried once")
	}
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("herdr calls = %q, want one prompt for the first attempt", got)
	}
}

func TestOmpRetryPolicyNeverRetriesWhenTextMayBeInPane_5413(t *testing.T) {
	for evidence, want := range map[string]bool{
		"unproven:composer_empty":              true,
		"unproven:composer_pending":            false,
		"unproven:composer_unlocated":          false,
		"after_return:unproven:composer_empty": false,
		"presend:composer_pending":             false,
		// A presend-prefixed post-return verdict spent the one return on this
		// text: it must not be retyped (mergewake 734, busy_relay.go:1397).
		"presend:after_return:unproven:composer_empty":     false,
		"presend:after_return:unproven:composer_pending":   false,
		"presend:after_return:unproven:composer_unlocated": false,
		"presend:unproven:read_failed":                     true,
	} {
		got := relayRetriesOnce(relayInjectResult{Outcome: relayInjectMaybeInPane, Harness: "omp", Evidence: evidence})
		if got != want {
			t.Errorf("omp evidence %q: retry=%v, want %v", evidence, got, want)
		}
	}
}

// omp is held while its pane is working: nothing shows a message typed into a
// working omp pane, so a directive waits for an idle or done pane.
func TestOmpDirectiveHeldWhileWorking_5413(t *testing.T) {
	if !relayHarnessWorkingUnprovable("omp") || !relayPaneWorkingUnprovable("working", "omp") {
		t.Fatal("omp must be workingUnprovable: a directive is held while its pane is working")
	}
	if relayPaneWorkingUnprovable("idle", "omp") {
		t.Fatal("an idle omp pane takes the directive at once")
	}
}

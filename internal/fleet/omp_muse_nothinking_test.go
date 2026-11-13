package fleet

import (
	"reflect"
	"testing"
)

// Seats whose model omp lists with "reasoning": false render no thinking level:
// the live footer of an omp-museember launch on 2026-11-14 read "⬢ Muse Ember".
// omp models --json (omp 18.14.2) lists muse-ember and inkling-2-small:free with
// reasoning false; muse-spark-1.3-contributor has thinking levels, so
// omp-musespark13c stays a thinking seat (hk 4523, hk 4588).
// ompMuseNoThinkingSeats are no-thinking seats that the omp credit seed does not
// list (they were added after it, #509), so they stay out of ompNoThinkingSeats.
var ompMuseNoThinkingSeats = map[string]struct{ model, footer string }{
	"omp-museember": {"commandcode/meta/muse-ember", "Muse Ember"},
	"omp-inkling2s": {"commandcode/thinkingmachines/inkling-2-small:free", "Inkling 2 Small"},
}

func TestOMPMuseSeatsAreNoThinking(t *testing.T) {
	a := testApp(t)
	const cwd = "/home/user/work/tree"
	for profile, want := range ompMuseNoThinkingSeats {
		seat := a.Policy.Profiles[profile]
		if !seat.NoThinking || seat.Effort != "off" {
			t.Fatalf("seat %s = %+v, want a no-thinking off seat", profile, seat)
		}
		argv, _, err := a.launchArgs(seat, Launch{ModelID: want.model, Effort: "off"})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(argv, []string{"--model", want.model}) {
			t.Fatalf("%s argv %v", profile, argv)
		}
		screen := "π > ⬢ " + want.footer + " > 🌳 project/tree ▶─0.9%──…─1M─"
		if !OMPFooterProof(screen, want.footer, "off", cwd, "/home/user", "", seat.NoThinking) {
			t.Fatalf("%s: the ⬢ footer was refused", profile)
		}
	}
}

// omp-musespark13c's model has thinking levels (omp models --json: reasoning
// true, low..xhigh), so it must stay a thinking seat proved by its effort glyph.
func TestOMPMuseSpark13ContributorIsAThinkingSeat(t *testing.T) {
	a := testApp(t)
	seat := a.Policy.Profiles["omp-musespark13c"]
	if seat.NoThinking || seat.Effort != "high" {
		t.Fatalf("omp-musespark13c = %+v, want a thinking seat with effort high", seat)
	}
}

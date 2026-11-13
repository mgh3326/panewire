package fleet

import (
	"reflect"
	"testing"
)

// The Muse seats added in #509 render no thinking level: the live footer of an
// omp-museember launch on 2026-11-14 read "⬢ Muse Ember" (no effort glyph), and
// omp-musespark13c failed the same landing proof. They are no-thinking seats,
// launched with no effort and proved by the ⬢ footer (hk 4523).
// ompMuseNoThinkingSeats are no-thinking seats that the omp credit seed does not
// list (they were added after it, #509), so they stay out of ompNoThinkingSeats.
var ompMuseNoThinkingSeats = map[string]struct{ model, footer string }{
	"omp-museember":    {"commandcode/meta/muse-ember", "Muse Ember"},
	"omp-musespark13c": {"commandcode/meta/muse-spark-1.3-contributor", "Muse Spark 1.3"},
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
		screen := "π > ⬢ " + want.footer + " > 🌳 project/tree > ⑂ wk/fixture ▶─0.9%──…─1M─"
		if !OMPFooterProof(screen, want.footer, "off", cwd, "/home/user", "", "project/tree", "wk/fixture", seat.NoThinking) {
			t.Fatalf("%s: the ⬢ footer was refused", profile)
		}
	}
}

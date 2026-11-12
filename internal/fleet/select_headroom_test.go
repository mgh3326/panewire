package fleet

import (
	"math/rand/v2"
	"testing"
	"time"
)

// hrCand is a ranked candidate whose shortest window is given directly; reset
// nil means the window's reset is unknown.
func hrCand(pool string, usedPct float64, window time.Duration, reset *float64, expected time.Duration) *selCand {
	room := &windowRoom{usedPct: usedPct, window: window, resetHours: reset}
	return &selCand{
		row:      CatalogRow{Profile: pool + "-profile", Pool: pool, Grade: "B", Effort: "high"},
		tier:     2,
		tierName: "ranked",
		headroom: PoolPolicy{}.headroomFor(room, expected),
	}
}

func TestHeadroomSixHourJobPrefersRoomOrEarlyResetOverExhaustingWindow(t *testing.T) {
	const job = 6 * time.Hour
	// 10% of a 5h window left; a 6h job at 15%/h needs 90%, and the window
	// resets after the job's first half, so it does not fit.
	exhausting := hrCand("codex", 90, 5*time.Hour, ptrF(4.5), job)
	// Window resets in 1h, inside the first half of the job: fits.
	early := hrCand("claude", 40, 5*time.Hour, ptrF(1), job)
	// 95% of the window left, enough for the whole job: fits.
	ample := hrCand("devin", 5, 5*time.Hour, nil, job)

	if exhausting.fits() {
		t.Fatal("exhausting window fits a 6h job; want it not to fit")
	}
	if !early.fits() || !ample.fits() {
		t.Fatalf("early reset fits=%v ample fits=%v; want both fit", early.fits(), ample.fits())
	}
	if compareSelCands(early, exhausting, true) >= 0 {
		t.Fatal("early-reset candidate does not rank ahead of exhausting window")
	}
	if compareSelCands(ample, exhausting, true) >= 0 {
		t.Fatal("ample-room candidate does not rank ahead of exhausting window")
	}
	cands := []*selCand{exhausting, early, ample}
	sortSelCands(cands)
	if cands[len(cands)-1] != exhausting {
		t.Fatalf("exhausting window ranked %d of %d; want last", indexOfSelCand(cands, exhausting)+1, len(cands))
	}
}

func TestHeadroomFitBoundaryIsInclusive(t *testing.T) {
	// 6h at 15%/h needs exactly 90% remaining.
	exact := hrCand("codex", 10, 5*time.Hour, nil, 6*time.Hour)
	short := hrCand("codex", 10.5, 5*time.Hour, nil, 6*time.Hour)
	if !exact.fits() {
		t.Fatal("remaining equal to burn x hours does not fit; want inclusive")
	}
	if short.fits() {
		t.Fatal("remaining below burn x hours fits; want not to fit")
	}
}

func TestHeadroomNoExpectedLeavesOrderAlone(t *testing.T) {
	exhausting := hrCand("codex", 90, 5*time.Hour, ptrF(4.5), 0)
	ample := hrCand("devin", 5, 5*time.Hour, nil, 0)
	if exhausting.headroom != nil || ample.headroom != nil {
		t.Fatal("job without expected time has a headroom fit; want the key off")
	}
	bareExhausting, bareAmple := *exhausting, *ample
	bareExhausting.headroom, bareAmple.headroom = nil, nil
	if got, want := compareSelCands(exhausting, ample, true), compareSelCands(&bareExhausting, &bareAmple, true); got != want {
		t.Fatalf("order with key off = %d; want %d as without headroom", got, want)
	}
}

func TestHeadroomNeverRefusesACandidate(t *testing.T) {
	spent := hrCand("codex", 100, 5*time.Hour, nil, 6*time.Hour)
	noWindow := &selCand{row: CatalogRow{Profile: "agy-profile", Pool: "agy", Grade: "B", Effort: "high"}, tier: 2, tierName: "ranked", headroom: PoolPolicy{}.headroomFor(nil, 6*time.Hour)}
	fresh := hrCand("claude", 0, 5*time.Hour, nil, 6*time.Hour)
	if spent.fits() || noWindow.fits() {
		t.Fatal("candidate without room or window fits; want not to fit")
	}
	cands := []*selCand{spent, noWindow, fresh}
	sortSelCands(cands)
	if len(cands) != 3 {
		t.Fatalf("sort kept %d candidates; want 3", len(cands))
	}
	if cands[0] != fresh {
		t.Fatal("fitting candidate does not rank first")
	}
}

func TestHeadroomComparatorTransitiveOverRandomSets(t *testing.T) {
	rng := rand.New(rand.NewPCG(4326, 0))
	for _, expected := range []time.Duration{0, 6 * time.Hour} {
		for round := range 200 {
			set := make([]*selCand, 0, 12)
			for range 12 {
				set = append(set, hrRandomCand(rng, expected))
			}
			for _, x := range set {
				for _, y := range set {
					xy := hrSign(compareSelCands(x, y, true))
					if xy != -hrSign(compareSelCands(y, x, true)) {
						t.Fatalf("expected=%v: comparator not antisymmetric for %s/%s", expected, x.row.Pool, y.row.Pool)
					}
					for _, z := range set {
						yz := hrSign(compareSelCands(y, z, true))
						xz := hrSign(compareSelCands(x, z, true))
						if xy <= 0 && yz <= 0 && xz > 0 {
							t.Fatalf("expected=%v round %d: x<=y<=z but x>z (%s, %s, %s)", expected, round, x.row.Pool, y.row.Pool, z.row.Pool)
						}
					}
				}
			}
		}
	}
}

func TestHeadroomBurnDefaultsByWindowLength(t *testing.T) {
	var p PoolPolicy
	if got := p.burnPctPerH(5 * time.Hour); got != 15 {
		t.Fatalf("5h burn = %v; want 15", got)
	}
	if got := p.burnPctPerH(24 * time.Hour); got != 15 {
		t.Fatalf("1d burn = %v; want 15 (windows up to one day are short)", got)
	}
	if got := p.burnPctPerH(168 * time.Hour); got != 2 {
		t.Fatalf("7d burn = %v; want 2", got)
	}
	if got := p.burnPctPerH(0); got != 15 {
		t.Fatalf("unknown-window burn = %v; want the conservative short rate 15", got)
	}
	p.Burn = &BurnPolicy{ShortPctPerH: ptrF(10)}
	if got, want := p.burnPctPerH(5*time.Hour), 10.0; got != want {
		t.Fatalf("configured short burn = %v; want %v", got, want)
	}
	if got := p.burnPctPerH(168 * time.Hour); got != 2 {
		t.Fatalf("unset long burn = %v; want default 2", got)
	}
}

func TestHeadroomExpectedParsesDurationsAndFlagsGarbage(t *testing.T) {
	cases := []struct {
		in         string
		want       time.Duration
		unreadable bool
	}{
		{"6h", 6 * time.Hour, false},
		{"1d", 24 * time.Hour, false},
		{"", 0, false},
		{"soon", 0, true},
		{"-2h", 0, true},
	}
	for _, tc := range cases {
		got, unreadable := parseExpected(tc.in)
		if got != tc.want || unreadable != tc.unreadable {
			t.Fatalf("parseExpected(%q) = (%v, %v); want (%v, %v)", tc.in, got, unreadable, tc.want, tc.unreadable)
		}
	}
}

func hrRandomCand(rng *rand.Rand, expected time.Duration) *selCand {
	pools := []string{"codex", "claude", "devin", "agy", "commandcode"}
	grades := []string{"C", "B", "A", "A+"}
	efforts := []string{"low", "medium", "high"}
	c := &selCand{
		row:          CatalogRow{Profile: "p", Pool: pools[rng.IntN(len(pools))], Grade: grades[rng.IntN(len(grades))], Effort: efforts[rng.IntN(len(efforts))]},
		behindCredit: rng.IntN(4) == 0,
	}
	switch rng.IntN(4) {
	case 0:
		c.tier, c.tierName, c.risk = 0, "waste", true
		c.urgency = ptrF(float64(rng.IntN(5)))
	case 1:
		c.tier, c.tierName = 1, "free"
	case 2:
		c.tier, c.tierName, c.creditPool = 1, creditTier, true
		c.usdUrgency = ptrF(float64(rng.IntN(3)))
	default:
		c.tier, c.tierName = 2, "ranked"
	}
	windows := []time.Duration{5 * time.Hour, 24 * time.Hour, 168 * time.Hour}
	if rng.IntN(10) > 0 {
		var reset *float64
		if rng.IntN(2) == 0 {
			reset = ptrF(float64(rng.IntN(12)) + 0.5)
		}
		room := &windowRoom{usedPct: float64(rng.IntN(101)), window: windows[rng.IntN(len(windows))], resetHours: reset}
		c.headroom = PoolPolicy{}.headroomFor(room, expected)
	} else {
		c.headroom = PoolPolicy{}.headroomFor(nil, expected)
	}
	return c
}

func hrSign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	}
	return 0
}

func indexOfSelCand(cands []*selCand, want *selCand) int {
	for i, c := range cands {
		if c == want {
			return i
		}
	}
	return -1
}

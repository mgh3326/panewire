package fleet

import (
	"context"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// creditNow is the test clock's start (testApp), the base of every credit
// expiry below.
var creditNow = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// creditFreeRows is a free SWE-2 rung, an omp credit rung and a ranked
// subscription rung, all at the same grade and effort.
func creditFreeRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "devin-swe2", Effort: "high", Grade: "A", Pool: "devin", Gate: "default", ModelID: "swe-2"},
		{Profile: "omp-opus", Effort: "high", Grade: "A", Pool: "anthropic-api", Gate: "default", ModelID: "anthropic/claude-opus-5-5"},
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "claude-sonnet-5-5"},
	}
}

func creditFreeFixture(t *testing.T, pools map[string]PoolView, present []string) (*App, Spawn, *fakeQuota) {
	t.Helper()
	a, s, q := selFixture(t, "A", creditFreeRows(), pools, present)
	serveOMPRouting(t, a)
	q.GateDo = gatePools(map[string]string{"devin-swe2": "devin", "omp-opus": "anthropic-api", "sonnet": "claude"})
	return a, s, q
}

// Free ranks before credit, and credit ranks before the ranked subscription.
func TestSelectCreditRanksBesideFree_T4006(t *testing.T) {
	pools := map[string]PoolView{
		"devin":         {Status: "ok", Class: "spend", Blocking: ptrF(4)},
		"anthropic-api": {Status: "ok", Class: "spend", Credit: pctOnly(77, 220*time.Hour)(creditNow)},
		"claude":        {Status: "ok", Class: "spend", Blocking: ptrF(10)},
	}
	a, s, _ := creditFreeFixture(t, pools, []string{"devin", "anthropic-api", "claude"})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.seatName != "devin-swe2" || sel.chosen.tierName != "free" {
		t.Fatalf("the free rung must win before credit: %s in %s", sel.chosen.seatName, sel.chosen.tierName)
	}
	seats := rankedSeats(sel)
	credit, free, ranked := seatIndex(seats, "omp-opus"), seatIndex(seats, "devin-swe2"), seatIndex(seats, "sonnet")
	if credit < 0 || free < 0 || ranked < 0 || credit < free || ranked < credit {
		t.Fatalf("ranked order %v: credit and free must precede the ranked pool", seats)
	}
	if got := candOutcome(sel.record(), "sonnet", "high")["outcome"]; got != "below_choice" {
		t.Fatalf("the ranked pool is below the choice: %v", got)
	}
}

// A free rung also wins a full tie against a credit rung.
func TestSelectCreditBeatsFreeOnFullTie_T4006(t *testing.T) {
	pools := map[string]PoolView{
		"devin":         {Status: "ok", Class: "spend", Blocking: ptrF(10)},
		"anthropic-api": {Status: "ok", Class: "spend", Blocking: ptrF(10)},
	}
	a, s, _ := creditFreeFixture(t, pools, []string{"devin", "anthropic-api"})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.seatName != "devin-swe2" || sel.chosen.tierName != "free" {
		t.Fatalf("a full tie must put the free rung first: %s in %s", sel.chosen.seatName, sel.chosen.tierName)
	}
	if got := candOutcome(sel.record(), "omp-opus", "high")["outcome"]; got != "below_choice" {
		t.Fatalf("the credit rung is the runner-up: %v", got)
	}
}

// (c) A free limit holds the free rung out of the plan, and the next
// candidate is chosen; after the reset the free rung returns.
func TestSelectFreeLimitSkipsRungUntilReset_T4006(t *testing.T) {
	reset := time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC)
	pools := map[string]PoolView{
		"devin":  {Status: "ok", Class: "spend", Blocking: ptrF(4), Free: &shared.FreeBucket{State: shared.FreeExhausted, ResetsAt: &reset}},
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)},
	}
	a, s, _ := creditFreeFixture(t, pools, []string{"devin", "claude"})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	row := candOutcome(sel.record(), "devin-swe2", "high")
	if row["outcome"] != "dropped:free_limit" || row["outcome_code"] != outcomeQuotaExhausted {
		t.Fatalf("the held free rung must be dropped as free_limit: %v", row)
	}
	if sel.chosen.seatName != "sonnet" {
		t.Fatalf("the next candidate is chosen while the limit holds: %s", sel.chosen.seatName)
	}
	// The test clock advances past the reset: the same reading is available.
	if err := a.Sleep(context.Background(), 3*time.Hour); err != nil {
		t.Fatal(err)
	}
	sel, err = a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.seatName != "devin-swe2" || sel.chosen.tierName != "free" {
		t.Fatalf("after the reset the free rung returns: %s in %s", sel.chosen.seatName, sel.chosen.tierName)
	}
}

// (d) Waste-band pools still go first, and a credit rung at risk does not
// displace a waste-risk subscription.
func TestSelectWasteBeatsCredit_T4006(t *testing.T) {
	pools := map[string]PoolView{
		"devin":         {Status: "ok", Class: "spend", Blocking: ptrF(4)},
		"anthropic-api": {Status: "ok", Class: "spend", Credit: balance(80, 100*time.Hour)(creditNow)},
		"claude":        riskPool(20, 8),
	}
	a, s, _ := creditFreeFixture(t, pools, []string{"devin", "anthropic-api", "claude"})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.seatName != "sonnet" || sel.chosen.tierName != "waste" {
		t.Fatalf("the waste-risk subscription goes first: %s in %s", sel.chosen.seatName, sel.chosen.tierName)
	}
	if got := candOutcome(sel.record(), "omp-opus", "high"); got["tier"] != "credit" || got["waste_risk"] != false {
		t.Fatalf("the credit rung keeps its tier: %v", got)
	}
}

// subscriptionPreserved is a fixed subscription-only catalog: no free or
// credit rung can move, so the waste and ranked order must be unchanged.
func subscriptionPreserved() []CatalogRow {
	return []CatalogRow{
		{Profile: "sonnet", Effort: "medium", Grade: "B", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "opus", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "opus", Effort: "max", Grade: "S+", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "codex-sol", Effort: "high", Grade: "A", Pool: "codex", Gate: "default", ModelID: "m"},
		{Profile: "codex-sol", Effort: "max", Grade: "S+", Pool: "codex", Gate: "default", ModelID: "m"},
		{Profile: "devin-ds41", Effort: "high", Grade: "A+", Pool: "devin", Gate: "default", ModelID: "m"},
		{Profile: "haiku", Effort: "low", Grade: "C", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "haiku", Effort: "high", Grade: "SS", Pool: "claude", Gate: "default", ModelID: "m"},
	}
}

// (d) Preservation: with no free or credit candidate, the fixed subscription
// catalog selects exactly as before for grades S+..C.
func TestSelectSubscriptionOrderPreserved_T4006(t *testing.T) {
	pools := map[string]PoolView{
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(20)},
		"codex":  {Status: "ok", Class: "spend", Blocking: ptrF(30)},
		"devin":  {Status: "ok", Class: "spend", Blocking: ptrF(40)},
	}
	gates := gatePools(map[string]string{"sonnet": "claude", "opus": "claude", "codex-sol": "codex", "devin-ds41": "devin", "haiku": "claude"})
	for _, tc := range []struct{ grade, want string }{
		{"S+", "opus@max"},
		{"S", "opus@high"},
		{"A+", "devin-ds41@high"},
		{"A", "sonnet@high"},
		{"B", "sonnet@medium"},
		{"C", "haiku@low"},
	} {
		t.Run(tc.grade, func(t *testing.T) {
			a, s, q := selFixture(t, tc.grade, subscriptionPreserved(), pools, []string{"claude", "codex", "devin"})
			q.GateDo = gates
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			if got := sel.chosen.row.Profile + "@" + sel.chosen.row.Effort; got != tc.want {
				t.Fatalf("grade %s chose %s, want %s", tc.grade, got, tc.want)
			}
		})
	}
}

// (e) An expired, excluded or presence-less credit pool is not chosen.
func TestSelectCreditUnusableNotChosen_T4006(t *testing.T) {
	expired := func(now time.Time) *CreditBalance {
		return &CreditBalance{LeftPct: 77, ExpiresAt: creditNow.Add(-time.Hour)}
	}
	for _, tc := range []struct {
		name    string
		pools   map[string]PoolView
		present []string
		want    string
	}{
		{"expired", map[string]PoolView{"devin": {Status: "ok", Class: "spend", Blocking: ptrF(4)}, "anthropic-api": {Status: "ok", Class: "spend", Credit: expired(creditNow)}, "claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)}}, []string{"devin", "anthropic-api", "claude"}, "dropped:credit_expired"},
		{"excluded", map[string]PoolView{"devin": {Status: "ok", Class: "spend", Blocking: ptrF(4)}, "anthropic-api": {Status: "ok", Class: "exclude"}, "claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)}}, []string{"devin", "anthropic-api", "claude"}, "dropped:pool_excluded"},
		{"no presence", map[string]PoolView{"devin": {Status: "ok", Class: "spend", Blocking: ptrF(4)}, "anthropic-api": {Status: "ok", Class: "spend"}, "claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)}}, []string{"devin", "claude"}, "dropped:no_presence"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, _ := creditFreeFixture(t, tc.pools, tc.present)
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			if got := candOutcome(sel.record(), "omp-opus", "high")["outcome"]; got != tc.want {
				t.Fatalf("credit outcome = %v, want %s", got, tc.want)
			}
			if sel.chosen.seatName == "omp-opus" {
				t.Fatal("an unusable credit pool must not be chosen")
			}
		})
	}
}

// The free band is one strict weak ordering: a dollar credit first by dollars
// at risk, then the urgency-keyed group by urgency. Mixing the USD pairwise
// rule with the nil-urgency rule produced a cycle (USD credit before percent
// credit, percent credit before a free rung by urgency, free rung before USD
// credit by paceability), so the order depended on input order.
func TestOrderSelCandsFreeBandIsTotal_T4006(t *testing.T) {
	mk := func(profile string, credit bool, usd, urgency *float64) *selCand {
		return &selCand{
			row:        CatalogRow{Profile: profile, Grade: "A", Pool: "p", Effort: "high", ModelID: "m"},
			tier:       1,
			creditPool: credit,
			usdUrgency: usd,
			urgency:    urgency,
		}
	}
	a := mk("a", true, ptrF(2), nil)  // dollar credit
	b := mk("b", true, nil, ptrF(5))  // percent-only credit
	c := mk("c", false, nil, ptrF(1)) // free rung with a paid urgency
	for _, perm := range [][3]*selCand{{a, b, c}, {a, c, b}, {b, a, c}, {b, c, a}, {c, a, b}, {c, b, a}} {
		pending := []*selCand{perm[0], perm[1], perm[2]}
		orderSelCands(pending)
		if pending[0] != a || pending[1] != b || pending[2] != c {
			t.Fatalf("permutation must sort a, b, c: %s %s %s", pending[0].row.Profile, pending[1].row.Profile, pending[2].row.Profile)
		}
	}
}

// On a USD-credit tie the paceable candidate goes first, as it did before the
// group key: a USD credit's at-risk dollars can be 0 while its pool still
// carries an urgency, and both sides must order by presence, not by profile.
func TestOrderSelCandsUSDCreditTieIsPaceableFirst_T4006(t *testing.T) {
	mk := func(profile string, urgency *float64) *selCand {
		return &selCand{
			row:        CatalogRow{Profile: profile, Grade: "A", Pool: "p", Effort: "high", ModelID: "m"},
			tier:       1,
			creditPool: true,
			usdUrgency: ptrF(0),
			urgency:    urgency,
		}
	}
	paceable, unpaceable := mk("z", ptrF(5)), mk("a", nil)
	for _, perm := range [][2]*selCand{{paceable, unpaceable}, {unpaceable, paceable}} {
		pending := []*selCand{perm[0], perm[1]}
		orderSelCands(pending)
		if pending[0] != paceable {
			t.Fatalf("the paceable USD credit must go first: %s", pending[0].row.Profile)
		}
	}
}

// A USD-credit tie with urgency and no urgency, and two urgencies, is one
// strict weak ordering: presence first, then the higher urgency. Without the
// presence rule the profile key closed a cycle (Y<X, X<Z, Z<Y).
func TestOrderSelCandsUSDTieIsTotal_T4006(t *testing.T) {
	mk := func(profile string, urgency *float64) *selCand {
		return &selCand{
			row:        CatalogRow{Profile: profile, Grade: "A", Pool: "p", Effort: "high", ModelID: "m"},
			tier:       1,
			creditPool: true,
			usdUrgency: ptrF(0),
			urgency:    urgency,
		}
	}
	x, y, z := mk("c", ptrF(5)), mk("b", nil), mk("a", ptrF(3))
	for _, perm := range [][3]*selCand{{x, y, z}, {x, z, y}, {y, x, z}, {y, z, x}, {z, x, y}, {z, y, x}} {
		pending := []*selCand{perm[0], perm[1], perm[2]}
		orderSelCands(pending)
		if pending[0] != x || pending[1] != z || pending[2] != y {
			t.Fatalf("permutation must sort x, z, y: %s %s %s", pending[0].row.Profile, pending[1].row.Profile, pending[2].row.Profile)
		}
	}
}

func seatIndex(xs []string, want string) int {
	for i, x := range xs {
		if x == want {
			return i
		}
	}
	return -1
}

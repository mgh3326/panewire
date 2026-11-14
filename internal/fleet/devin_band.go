package fleet

// Devin runs two quota bands under the one catalog pool `devin`: the paid
// window its paid rungs spend, and the free-model quota only the SWE-2 rungs
// draw on (devinFreeRungs). A stop of one band must not hold the other, so each
// band has a reserved key beside its catalog pool: devin-free for the free
// rungs (devinFreeCapKey, pool_cap.go) and devin-paid for the paid ones. A stop
// of the catalog pool devin is a paid-band stop, because the free band already
// names its own key (hk 4582).

// devinPaidKey is the reserved key naming Devin's paid band: a stop of it
// holds only the paid rungs until its reset, and --exclude-pool devin-paid
// excludes only them, so the free rungs stay usable while only the paid window
// is spent.
const devinPaidKey = "devin-paid"

// isPaidDevinRung reports whether a rung of pool draws on Devin's paid window:
// a devin rung that is not one of the SWE-2 free rungs.
func isPaidDevinRung(pool, profile string) bool {
	return pool == "devin" && !IsFreeRung(profile)
}

// BandKey names the key a quota stop of pool holds: the pool itself, except
// the catalog pool devin, whose stops are paid-band stops and hold devin-paid
// alone so the free rungs stay placeable (hk 4582).
func BandKey(pool string) string {
	if pool == "devin" {
		return devinPaidKey
	}
	return pool
}

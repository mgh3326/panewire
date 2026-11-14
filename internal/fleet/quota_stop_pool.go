package fleet

import "github.com/mgh3326/panewire/internal/quotastop"

// QuotaStopPool names the pool a stop whose text leaves it open draws on. A
// Devin request-rate-limit stop names the account limit but not the rung that
// hit it (hk 4644), so the running profile decides: a SWE-2 free rung draws on
// devin's free bucket, every other Devin rung on the paid pool. A stop whose
// screen named its pool is returned unchanged.
func QuotaStopPool(stop quotastop.Stop, profile string) quotastop.Stop {
	if !stop.PoolFromProfile {
		return stop
	}
	if IsFreeRung(profile) {
		stop.Pool = "devin-free"
	} else {
		stop.Pool = "devin"
	}
	return stop
}

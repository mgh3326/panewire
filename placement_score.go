package panewire

import "math"

// Placement score v2 (docs/placement.md, "Measured-load score"). The score
// answers "which host will lose the fewest spawns", and the evidence behind
// its shape is hk:doc research/2026-11-04/placement-score-vs-failures: on the
// busiest host CPU busy did not predict spawn failures, while load per core,
// page-ins and available memory below ~40% did.
//
//	free  = cpu * mem * io * load * jobs
//	score = free * power - bridge_cost * 100 / power * claims_in_the_bridge_window
//
// The decision (decide) adds the free floor on top: a host whose free is under
// free_floor is not chosen while another usable host is at or above it.
//
// Every constant has a default here and an override in placement.json under
// "score".

// Defaults of the score constants.
const (
	placementDefaultMemFloor   = 0.20
	placementDefaultMemSpan    = 0.20
	placementDefaultPageInHalf = 10.0
	placementDefaultSwapInHalf = 20.0
	placementDefaultLoadKnee   = 0.75
	placementDefaultLoadCap    = 4.0
	placementDefaultJobsHalf   = 8.0
	placementDefaultBridgeCost = 10.0
	placementDefaultHysteresis = 1.25
	placementDefaultFreeFloor  = 0.05
)

// PlacementScoreConfig is the optional "score" object of placement.json. A nil
// field keeps its default.
type PlacementScoreConfig struct {
	// MemFloor and MemSpan shape the mem term: 0 at MemFloor available,
	// 1 at MemFloor+MemSpan.
	MemFloor *float64 `json:"mem_floor,omitempty"`
	MemSpan  *float64 `json:"mem_span,omitempty"`
	// PageInHalfMBps is the macOS page-in rate that halves the io term;
	// LinuxSwapInHalfMBps is the Linux swap-in rate that does.
	PageInHalfMBps      *float64 `json:"pagein_half_mbps,omitempty"`
	LinuxSwapInHalfMBps *float64 `json:"linux_swap_in_half_mbps,omitempty"`
	// LoadKnee is the load1-per-core below which the load term costs nothing;
	// LoadCap is where a higher reading stops costing more.
	LoadKnee *float64 `json:"load_knee,omitempty"`
	LoadCap  *float64 `json:"load_cap,omitempty"`
	// JobsHalf is the live job count that halves the jobs term.
	JobsHalf *float64 `json:"jobs_half,omitempty"`
	// BridgeCost is what one claim inside the spawn bridge costs a power-100
	// host, in points; a host of power p pays BridgeCost*100/p.
	BridgeCost *float64 `json:"bridge_cost,omitempty"`
	// Hysteresis is the factor a challenger's score must reach over the
	// policy-order winner's to take the decision.
	Hysteresis *float64 `json:"hysteresis,omitempty"`
	// FreeFloor is the free fraction under which a host is not chosen while
	// another usable host is at or above it.
	FreeFloor *float64 `json:"free_floor,omitempty"`
	// ClaimCost and AllLoadedBelow are retired (bridge_cost and free_floor
	// replace them). They stay decodable so a stale policy file still loads,
	// and their values are ignored.
	ClaimCost      *float64 `json:"claim_cost,omitempty"`
	AllLoadedBelow *float64 `json:"all_loaded_below,omitempty"`
}

// placementScoreK is PlacementScoreConfig with every default filled in.
type placementScoreK struct {
	memFloor, memSpan, pageInHalf, swapInHalf, loadKnee, loadCap, jobsHalf, bridgeCost, hysteresis, freeFloor float64
}

func (c *PlacementScoreConfig) valid() bool {
	if c == nil {
		return true
	}
	in := func(v *float64, lo, hi float64) bool { return v == nil || (*v >= lo && *v <= hi) }
	pos := func(v *float64, hi float64) bool { return v == nil || (*v > 0 && *v <= hi) }
	// The ordering holds for the effective values: a knee set above the default
	// cap would otherwise leave the load term at 1 for every reading.
	knee, loadCap := placementDefaultLoadKnee, placementDefaultLoadCap
	if c.LoadKnee != nil {
		knee = *c.LoadKnee
	}
	if c.LoadCap != nil {
		loadCap = *c.LoadCap
	}
	return in(c.MemFloor, 0, 1) && pos(c.MemSpan, 1) && pos(c.PageInHalfMBps, 100000) && pos(c.LinuxSwapInHalfMBps, 100000) &&
		in(c.LoadKnee, 0, 100) && pos(c.LoadCap, 100) && pos(c.JobsHalf, 10000) && in(c.BridgeCost, 0, 10000) &&
		in(c.Hysteresis, 1, 100) && in(c.FreeFloor, 0, 1) &&
		knee < loadCap
}

func (p PlacementPolicy) scoreK() placementScoreK {
	k := placementScoreK{placementDefaultMemFloor, placementDefaultMemSpan, placementDefaultPageInHalf, placementDefaultSwapInHalf,
		placementDefaultLoadKnee, placementDefaultLoadCap, placementDefaultJobsHalf, placementDefaultBridgeCost, placementDefaultHysteresis, placementDefaultFreeFloor}
	c := p.Score
	if c == nil {
		return k
	}
	for _, o := range []struct {
		from *float64
		to   *float64
	}{{c.MemFloor, &k.memFloor}, {c.MemSpan, &k.memSpan}, {c.PageInHalfMBps, &k.pageInHalf}, {c.LinuxSwapInHalfMBps, &k.swapInHalf},
		{c.LoadKnee, &k.loadKnee}, {c.LoadCap, &k.loadCap}, {c.JobsHalf, &k.jobsHalf}, {c.BridgeCost, &k.bridgeCost},
		{c.Hysteresis, &k.hysteresis}, {c.FreeFloor, &k.freeFloor}} {
		if o.from != nil {
			*o.to = *o.from
		}
	}
	return k
}

// PlacementTerms are the five factors of free, each 0..1 and 1 when the host
// is not slowed by that signal. They are published per candidate (explain, the
// placement fleet event) so a later tuning can see which term moved a score.
type PlacementTerms struct {
	CPU  float64 `json:"cpu"`
	Mem  float64 `json:"mem"`
	IO   float64 `json:"io"`
	Load float64 `json:"load"`
	Jobs float64 `json:"jobs"`
}

// Free is the product of the terms.
func (t PlacementTerms) Free() float64 { return t.CPU * t.Mem * t.IO * t.Load * t.Jobs }

// placementTerms computes the terms from one host's signals. A signal the host
// does not report leaves its term at 1: it is left out, not guessed, and the
// freshness guard (load_unknown) is what catches a host gone silent.
func (k placementScoreK) terms(s PlacementSignals, liveJobs int) PlacementTerms {
	t := PlacementTerms{CPU: 1, Mem: 1, IO: 1, Load: 1}
	// CPU busy is the larger of the last-minute and five-minute readings, so a
	// job that just went heavy shows at once and a sustained one is not
	// forgotten when one minute dips.
	busy := 0.0
	if s.CPUBusy5m != nil {
		busy = *s.CPUBusy5m
	}
	if s.CPUBusy1m != nil && *s.CPUBusy1m > busy {
		busy = *s.CPUBusy1m
	}
	t.CPU = 1 - math.Min(math.Max(busy, 0), 1)
	if s.MemAvailable != nil {
		t.Mem = math.Min(math.Max((*s.MemAvailable-k.memFloor)/k.memSpan, 0), 1)
	}
	if s.SwapInMBps != nil && *s.SwapInMBps > 0 {
		t.IO = 1 / (1 + *s.SwapInMBps/s.ioHalf(k))
	}
	if s.Load1PerCore != nil {
		t.Load = 1 / (1 + math.Max(0, math.Min(math.Max(*s.Load1PerCore, 0), k.loadCap)-k.loadKnee))
	}
	t.Jobs = 1 / (1 + float64(max(liveJobs, 0))/k.jobsHalf)
	return t
}

// ioHalf is the half point of the io term for this reading: Linux counts real
// swap-ins, macOS counts page-ins (file-backed reads included), which rise
// earlier for the same pain.
func (s PlacementSignals) ioHalf(k placementScoreK) float64 {
	if s.SwapInLinux {
		return k.swapInHalf
	}
	return k.pageInHalf
}

// scoreBase is the measured-load part of a candidate's score: the free
// fraction times the host's power, so at equal load the stronger host wins and
// at equal power the less loaded one does. Each claim inside the spawn bridge
// costs bridgeCost*100/power: the same job is a larger share of a weak host,
// so a weak host is held back for longer (10 on power 100, 20 on 50, 83 on
// 12). A weak or busy host stays eligible: it is ranked lower, never filtered.
// A power that is not positive (the policy refuses one) falls back to 100.
func (k placementScoreK) scoreBase(free float64, bridgedJobs int, power float64) float64 {
	return free*power - float64(bridgedJobs)*k.bridgeCost*100/positivePower(power)
}

func positivePower(power float64) float64 {
	if power > 0 {
		return power
	}
	return defaultPlacementPower
}

// decide applies the decision rules that sit on top of the scores, to the
// usable candidates in policy order (winner is the index of the policy-order
// winner). It returns the index of the candidate that takes the decision and
// the reason:
//
//   - "below_free_floor": the policy-order winner had free under freeFloor
//     while another usable host did not; the decision moved to the first host
//     at or above the floor (hysteresis then applies among those).
//   - "all_loaded": every usable host is under freeFloor; the decision goes to
//     the highest free*power (ties: higher score, then policy order).
//   - "challenger": a rival at or above the floor scored hysteresis times the
//     winner.
//   - "": the winner keeps the decision.
//
// A host with no terms has an unknown free, not a low one: it is never under
// the floor, and a decision where no host has terms is the old score rule.
func (k placementScoreK) decide(usable []PlacementCandidate, winner int) (int, string) {
	below := func(c PlacementCandidate) bool {
		return c.Signals != nil && c.Signals.Terms != nil && c.Signals.Terms.Free() < k.freeFloor
	}
	var above []int
	for i, c := range usable {
		if !below(c) {
			above = append(above, i)
		}
	}
	if len(above) == 0 {
		pick, pickValue := 0, math.Inf(-1)
		for i, c := range usable {
			v := c.Signals.Terms.Free() * c.Signals.Power
			if v > pickValue || (v == pickValue && c.Score > usable[pick].Score) {
				pick, pickValue = i, v
			}
		}
		return pick, "all_loaded"
	}
	floorReason := ""
	if below(usable[winner]) {
		winner, floorReason = above[0], "below_free_floor"
	}
	challenger := -1
	for _, i := range above {
		if i != winner && (challenger < 0 || usable[i].Score > usable[challenger].Score) {
			challenger = i
		}
	}
	if challenger >= 0 && usable[challenger].Score > usable[winner].Score && usable[challenger].Score >= k.hysteresis*usable[winner].Score {
		return challenger, "challenger"
	}
	return winner, floorReason
}

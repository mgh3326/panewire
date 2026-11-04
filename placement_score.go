package panewire

import "math"

// Placement score v2 (docs/placement.md, "Measured-load score"). The score
// answers "which host will lose the fewest spawns", and the evidence behind
// its shape is hk:doc research/2026-11-04/placement-score-vs-failures: on the
// busiest host CPU busy did not predict spawn failures, while load per core,
// page-ins and available memory below ~40% did.
//
//	free  = cpu * mem * io * load * jobs
//	score = free * power - claim_cost * claims_in_the_bridge_window
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
	placementDefaultClaimCost  = 10.0
	placementDefaultHysteresis = 1.25
	placementDefaultAllLoaded  = 15.0
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
	// ClaimCost is what one claim inside the spawn bridge costs, in points.
	ClaimCost *float64 `json:"claim_cost,omitempty"`
	// Hysteresis is the factor a challenger's score must reach over the
	// policy-order winner's to take the decision.
	Hysteresis *float64 `json:"hysteresis,omitempty"`
	// AllLoadedBelow is the best admitted score under which every host counts
	// as loaded and the decision goes to the one with the most free memory.
	AllLoadedBelow *float64 `json:"all_loaded_below,omitempty"`
}

// placementScoreK is PlacementScoreConfig with every default filled in.
type placementScoreK struct {
	memFloor, memSpan, pageInHalf, swapInHalf, loadKnee, loadCap, jobsHalf, claimCost, hysteresis, allLoadedBelow float64
}

func (c *PlacementScoreConfig) valid() bool {
	if c == nil {
		return true
	}
	in := func(v *float64, lo, hi float64) bool { return v == nil || (*v >= lo && *v <= hi) }
	pos := func(v *float64, hi float64) bool { return v == nil || (*v > 0 && *v <= hi) }
	return in(c.MemFloor, 0, 1) && pos(c.MemSpan, 1) && pos(c.PageInHalfMBps, 100000) && pos(c.LinuxSwapInHalfMBps, 100000) &&
		in(c.LoadKnee, 0, 100) && pos(c.LoadCap, 100) && pos(c.JobsHalf, 10000) && in(c.ClaimCost, 0, 10000) &&
		in(c.Hysteresis, 1, 100) && in(c.AllLoadedBelow, 0, 10000) &&
		(c.LoadKnee == nil || c.LoadCap == nil || *c.LoadKnee < *c.LoadCap)
}

func (p PlacementPolicy) scoreK() placementScoreK {
	k := placementScoreK{placementDefaultMemFloor, placementDefaultMemSpan, placementDefaultPageInHalf, placementDefaultSwapInHalf,
		placementDefaultLoadKnee, placementDefaultLoadCap, placementDefaultJobsHalf, placementDefaultClaimCost, placementDefaultHysteresis, placementDefaultAllLoaded}
	c := p.Score
	if c == nil {
		return k
	}
	for _, o := range []struct {
		from *float64
		to   *float64
	}{{c.MemFloor, &k.memFloor}, {c.MemSpan, &k.memSpan}, {c.PageInHalfMBps, &k.pageInHalf}, {c.LinuxSwapInHalfMBps, &k.swapInHalf},
		{c.LoadKnee, &k.loadKnee}, {c.LoadCap, &k.loadCap}, {c.JobsHalf, &k.jobsHalf}, {c.ClaimCost, &k.claimCost},
		{c.Hysteresis, &k.hysteresis}, {c.AllLoadedBelow, &k.allLoadedBelow}} {
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

// placementScoreBase is the measured-load part of a candidate's score: the
// free fraction times the host's power, so at equal load the stronger host
// wins and at equal power the less loaded one does. Each claim inside the
// spawn bridge costs claimCost. A weak or busy host stays eligible: it is
// ranked lower, never filtered.
func (k placementScoreK) scoreBase(free float64, bridgedJobs int, power float64) float64 {
	return free*power - float64(bridgedJobs)*k.claimCost
}

// placementChallenger applies the decision rules that sit on top of the
// scores, to the usable candidates in policy order (winner is the index of
// the policy-order winner). It returns the index of the candidate that takes
// the decision and the reason: "" keeps the winner, "all_loaded" means no
// usable host scored allLoadedBelow and the decision went to the one with the
// most free memory, "challenger" means a rival scored hysteresis times the
// winner.
func (k placementScoreK) decide(usable []PlacementCandidate, winner int) (int, string) {
	best := math.Inf(-1)
	for _, c := range usable {
		best = math.Max(best, c.Score)
	}
	mem := func(c PlacementCandidate) float64 {
		if c.Signals != nil && c.Signals.Terms != nil {
			return c.Signals.Terms.Mem
		}
		return 1
	}
	if best < k.allLoadedBelow {
		pick := 0
		for i, c := range usable {
			if m, pm := mem(c), mem(usable[pick]); m > pm || (m == pm && c.Score > usable[pick].Score) {
				pick = i
			}
		}
		return pick, "all_loaded"
	}
	challenger := -1
	for i, c := range usable {
		if i != winner && (challenger < 0 || c.Score > usable[challenger].Score) {
			challenger = i
		}
	}
	if challenger >= 0 && usable[challenger].Score > usable[winner].Score && usable[challenger].Score >= k.hysteresis*usable[winner].Score {
		return challenger, "challenger"
	}
	return winner, ""
}

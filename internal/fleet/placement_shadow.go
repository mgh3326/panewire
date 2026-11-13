package fleet

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleetevent"
)

// Placement shadow comparison (hk 3650, docs/placement.md "Shadow comparison").
//
// When the spawn engine has ranked the hosts for a job (admitHostFirst), the
// functions below rank the same hosts again under two alternative rules and
// record both next to the live order as one fleet event, placement_shadow.
// They are pure functions of the readings the live ranking already had: no
// network call, no clock but the injected one, and nothing they compute is
// read by the live path. A panic in an arm is caught and recorded as
// shadow_error.
//
// The arms:
//
//	fixed     the live rules with the two confirmed defects fixed: one load
//	          measure for every host, and the hub's decision rules (usable
//	          verdict, hysteresis, all-loaded) applied on the fleet-state path.
//	measured  fixed plus one memory signal for admission and rank, each of cpu
//	          busy and load per core used once, and a per-work-class capability
//	          in place of power.

const (
	workLight = "light"
	workHeavy = "heavy"

	shadowArmFixed    = "fixed"
	shadowArmMeasured = "measured"

	shadowAgree       = "agree"
	shadowOrderDiffer = "order_differs"
	shadowDisagree    = "disagree"
	shadowError       = "shadow_error"

	// shadowSwapPagesPerMBps converts a Linux swap-in rate in MB/s (4 KiB
	// pages) into pages per second, the unit of the node's swap_in_per_s.
	shadowSwapPagesPerMBps = 256
)

// hubSignals is the hub's signals object with the two readings only the
// shadow comparison needs; PlacementSignals itself is unchanged, so no live
// record gains a field.
type hubSignals struct {
	PlacementSignals
	CPUBusy1m   softFloat `json:"cpu_busy_1m"`
	SwapInLinux softBool  `json:"swap_in_linux"`
}

// softFloat and softBool decode the shadow-only fields without ever failing:
// a value of another type reads as absent. The live parse of a hub reply must
// not become stricter because the shadow wants one more field.
type softFloat struct{ v *float64 }

func (f *softFloat) UnmarshalJSON(b []byte) error {
	var x float64
	if string(b) != "null" && json.Unmarshal(b, &x) == nil {
		f.v = &x
	}
	return nil
}

type softBool struct{ v *bool }

func (f *softBool) UnmarshalJSON(b []byte) error {
	var x bool
	if string(b) != "null" && json.Unmarshal(b, &x) == nil {
		f.v = &x
	}
	return nil
}

func (f softBool) true() bool { return f.v != nil && *f.v }

// HostExtra holds the hub readings only the shadow comparison reads.
type HostExtra struct {
	CPUBusy1m   *float64
	SwapInLinux bool
	// Usable is the hub's own verdict (fleet-state path only).
	Usable *bool
	// The node's memory telemetry (the /v1/placement path carries it; the
	// fleet-state load value does not).
	MemFreePct, SwapUsedMB, SwapInPerS, Pressure *float64
	// MeasuredAt is when the hub measured the host (fleet-state path only).
	MeasuredAt time.Time
}

// workClassOf is the work class of a spawn role: light for reviewer, consult
// and design-type seats, heavy for implementer and anything else. It is a
// recorded guess to be checked by measurement (hk 3650); the role the class
// came from is recorded beside it.
func workClassOf(role string) string {
	switch {
	case role == "reviewer" || role == "consult" || strings.HasPrefix(role, "design"):
		return workLight
	}
	return workHeavy
}

// shadowPolicy is the [placement.shadow] and [placement.capability.*] policy.
// Every threshold has a named key; the defaults are the live values where the
// live path has one (docs/placement.md, "Shadow comparison").
type shadowPolicy struct {
	enabled bool
	// threshold is the live placement.load_threshold, applied to load5 per
	// core; cpuBusyOver applies to a CPU-busy fraction (default: threshold).
	threshold, cpuBusyOver float64
	// hysteresis and freeFloor are the hub's decision constants.
	hysteresis, freeFloor float64
	// memRefuseBelowPct refuses a host whose available memory is below it
	// (the hub's memory_free_pct_min, 30); pressureRefuseLevel refuses a macOS
	// host at or above that pressure level (2 = critical); pressureWarnMem is
	// the mem term at warn.
	memRefuseBelowPct, pressureRefuseLevel, pressureWarnMem float64
	// The mem, swap, load and jobs terms: the hub's score constants, with the
	// swap half point in pages per second.
	memFloor, memSpan, swapInHalf, loadKnee, loadCap, jobsHalf, bridgeCost float64
	// capability maps work class -> host -> capability.
	capability map[string]map[string]float64
}

const (
	shadowDefaultHysteresis     = 1.25
	shadowDefaultFreeFloor      = 0.05
	shadowDefaultMemRefusePct   = 30.0
	shadowDefaultPressureRefuse = 2.0
	shadowDefaultPressureWarn   = 0.5
	shadowDefaultMemFloor       = 0.20
	shadowDefaultMemSpan        = 0.20
	shadowDefaultSwapInHalf     = 5000.0
	shadowDefaultLoadKnee       = 0.75
	shadowDefaultLoadCap        = 4.0
	shadowDefaultJobsHalf       = 8.0
	shadowDefaultBridgeCost     = 10.0
	shadowDefaultPower          = 100.0
)

func (a *App) shadowPolicy() shadowPolicy {
	sh, _ := a.Config.Placement["shadow"].(map[string]any)
	num := func(key string, def float64) float64 {
		if n, ok := number(sh, key); ok {
			return n
		}
		return def
	}
	threshold := a.loadThreshold()
	pol := shadowPolicy{
		enabled:             boolean(sh, "enabled", true),
		threshold:           threshold,
		cpuBusyOver:         num("cpu_busy_over", threshold),
		hysteresis:          num("hysteresis", shadowDefaultHysteresis),
		freeFloor:           num("free_floor", shadowDefaultFreeFloor),
		memRefuseBelowPct:   num("mem_refuse_below_pct", shadowDefaultMemRefusePct),
		pressureRefuseLevel: num("pressure_refuse_level", shadowDefaultPressureRefuse),
		pressureWarnMem:     num("pressure_warn_mem", shadowDefaultPressureWarn),
		memFloor:            num("mem_floor", shadowDefaultMemFloor),
		memSpan:             num("mem_span", shadowDefaultMemSpan),
		swapInHalf:          num("swap_in_half_per_s", shadowDefaultSwapInHalf),
		loadKnee:            num("load_knee", shadowDefaultLoadKnee),
		loadCap:             num("load_cap", shadowDefaultLoadCap),
		jobsHalf:            num("jobs_half", shadowDefaultJobsHalf),
		bridgeCost:          num("bridge_cost", shadowDefaultBridgeCost),
		capability:          map[string]map[string]float64{},
	}
	caps, _ := a.Config.Placement["capability"].(map[string]any)
	for _, class := range []string{workLight, workHeavy} {
		table, _ := caps[class].(map[string]any)
		for host := range table {
			if n, ok := number(table, host); ok {
				if pol.capability[class] == nil {
					pol.capability[class] = map[string]float64{}
				}
				pol.capability[class][host] = n
			}
		}
	}
	return pol
}

// validShadowPolicy checks [placement.shadow] and [placement.capability].
func validShadowPolicy(placement map[string]any) error {
	if v, ok := placement["shadow"]; ok {
		sh, isMap := v.(map[string]any)
		if !isMap {
			return fail(exitConfig, "placement.shadow must be a table")
		}
		for key, v := range sh {
			if key == "enabled" {
				if _, isBool := v.(bool); !isBool {
					return fail(exitConfig, "placement.shadow.enabled must be a boolean")
				}
				continue
			}
			n, ok := number(sh, key)
			if !ok || math.IsNaN(n) || math.IsInf(n, 0) || n < 0 {
				return fail(exitConfig, "placement.shadow.%s must be a nonnegative number", key)
			}
			switch key {
			case "cpu_busy_over", "mem_refuse_below_pct", "pressure_refuse_level", "pressure_warn_mem", "mem_floor", "load_knee", "bridge_cost":
			case "all_loaded_below", "claim_cost":
				// Retired with the live rules they mirrored (free_floor and
				// bridge_cost replace them): accepted and ignored, so an
				// existing config still loads.
			case "free_floor":
				if n > 1 {
					return fail(exitConfig, "placement.shadow.free_floor must be at most 1")
				}
			case "hysteresis":
				if n < 1 {
					return fail(exitConfig, "placement.shadow.hysteresis must be at least 1")
				}
			case "mem_span", "swap_in_half_per_s", "load_cap", "jobs_half":
				if n <= 0 {
					return fail(exitConfig, "placement.shadow.%s must be positive", key)
				}
			default:
				return fail(exitConfig, "placement.shadow.%s is not a known key", key)
			}
		}
	}
	if v, ok := placement["capability"]; ok {
		caps, isMap := v.(map[string]any)
		if !isMap {
			return fail(exitConfig, "placement.capability must map a work class (light, heavy) to a table of host capabilities")
		}
		for class, v := range caps {
			table, isMap := v.(map[string]any)
			if !isMap || class != workLight && class != workHeavy {
				return fail(exitConfig, "placement.capability.%s must be a table of host capabilities (classes: light, heavy)", class)
			}
			for host := range table {
				if n, ok := number(table, host); !ok || !validID(host) || n <= 0 || math.IsNaN(n) || math.IsInf(n, 0) {
					return fail(exitConfig, "placement.capability.%s.%s must be a positive number for a host name", class, host)
				}
			}
		}
	}
	return nil
}

// shadowReq is what the arms know of the job.
type shadowReq struct {
	role, class string
	tags        []string
}

// cpuBusy is the hub's CPU-busy fraction for the host: the load ratio the hub
// sent (a five-minute fraction), else the five-minute signal.
func cpuBusy(h selHost) *float64 {
	if h.view == nil {
		return nil
	}
	if h.view.LoadRatio != nil {
		return h.view.LoadRatio
	}
	return h.view.Signals.CPUBusy5m
}

// onlyMemoryNotAccepting reports whether the hub's reason refuses the host
// for memory pressure and for nothing else.
func onlyMemoryNotAccepting(reason string) bool {
	memory := false
	for _, tok := range strings.Split(reason, ",") {
		switch {
		case tok == "not_accepting(memory_pressure)":
			memory = true
		case strings.HasPrefix(tok, "not_accepting"):
			return false
		}
	}
	return memory
}

// hubUsable applies the hub's usable verdict (placementUsable) to a host. The
// hub's own flag is used when it sent one; otherwise the verdict is rebuilt
// from the reason the hub gave. ignoreMemory leaves out the memory admission
// floor, which the measured arm replaces. A host the hub said nothing about is
// usable: that is today's behaviour.
func hubUsable(h selHost, ignoreMemory bool) bool {
	if h.slotFull {
		return false
	}
	if h.view == nil {
		return true
	}
	return viewUsable(*h.view, ignoreMemory)
}

// viewUsable is hubUsable for the hub's reading of one host.
func viewUsable(hv HostView, ignoreMemory bool) bool {
	v := &hv
	// A host the direct hub path left out for full task slots is carried as a
	// synthetic view with SlotFull set (hub_spawn_async.go).
	if v.SlotFull {
		return false
	}
	if v.Extra.Usable != nil && !ignoreMemory {
		return *v.Extra.Usable
	}
	prometheus := v.Source == "prometheus"
	for _, tok := range strings.Split(v.Reason, ",") {
		switch {
		case tok == "active_jobs>=max", tok == "task_slots_full", tok == "disconnected":
			return false
		case tok == "not_accepting(memory_pressure)":
			if !ignoreMemory {
				return false
			}
		case strings.HasPrefix(tok, "not_accepting"):
			return false
		case tok == "load_unknown", tok == "throttled":
			if prometheus {
				return false
			}
		}
	}
	return v.Connected
}

func clamp01(x float64) float64 { return math.Min(math.Max(x, 0), 1) }

// shadowMem is the one memory signal the measured arm uses for admission and
// rank: macOS pressure level, else available memory, with the swap-in rate.
type shadowMem struct {
	src    string // pressure, available, none
	term   float64
	io     float64
	refuse bool
	swap   *float64
}

func (pol shadowPolicy) memory(h selHost) shadowMem {
	m := shadowMem{src: "none", term: 1, io: 1}
	v := h.view
	if v == nil {
		return m
	}
	x, sig := v.Extra, v.Signals
	// pressure is a level 0..2 on macOS and a PSI percent on Linux, and the
	// wire does not say which. Only a host that declares the macos capability
	// has its pressure read as a level; for any other host it is ambiguous and
	// unused, and available memory decides. swap_in_linux is absent on the
	// /v1/placement path, so it cannot tell the systems apart.
	linux := x.SwapInLinux || slices.Contains(hostCaps(h.cfg), "linux")
	macLevel := x.Pressure != nil && !linux && slices.Contains(hostCaps(h.cfg), "macos") && *x.Pressure <= 2
	m.swap = x.SwapInPerS
	if m.swap == nil && x.SwapInLinux && sig.SwapInMBPS != nil {
		pages := *sig.SwapInMBPS * shadowSwapPagesPerMBps
		m.swap = &pages
	}
	if m.swap != nil && *m.swap > 0 {
		m.io = 1 / (1 + *m.swap/pol.swapInHalf)
	}
	avail := sig.MemAvailable
	if avail == nil && linux && x.MemFreePct != nil {
		f := *x.MemFreePct / 100
		avail = &f
	}
	switch {
	case macLevel:
		m.src = "pressure"
		switch {
		case *x.Pressure >= pol.pressureRefuseLevel:
			m.term, m.refuse = 0, true
		case *x.Pressure >= 1:
			m.term = pol.pressureWarnMem
		}
	case avail != nil:
		m.src = "available"
		m.term = clamp01((*avail - pol.memFloor) / pol.memSpan)
		m.refuse = *avail*100 < pol.memRefuseBelowPct
	}
	return m
}

// capabilityOf is the host's capability for the work class: the policy entry,
// else the hub's power, else the hub default (100).
func (pol shadowPolicy) capabilityOf(class, host string, h selHost) (float64, bool) {
	if c, ok := pol.capability[class][host]; ok {
		return c, true
	}
	if h.view != nil && h.view.Signals.Power != nil && *h.view.Signals.Power > 0 {
		return *h.view.Signals.Power, false
	}
	return shadowDefaultPower, false
}

// measuredFree is the measured arm's free fraction for a host: the memory,
// swap-in, load and jobs terms (the cpu term is left out, CPU busy being the
// bucket measure). ok is false for a host that sent no score signals.
func (pol shadowPolicy) measuredFree(h selHost, m shadowMem) (free float64, ok bool) {
	v := h.view
	if v == nil || v.Signals.Terms == nil && v.Signals.Power == nil {
		return 0, false
	}
	sig := v.Signals
	load := 1.0
	if sig.Load1PerCore != nil {
		load = 1 / (1 + math.Max(0, math.Min(math.Max(*sig.Load1PerCore, 0), pol.loadCap)-pol.loadKnee))
	}
	jobs := 0
	if sig.LiveJobs != nil {
		jobs = *sig.LiveJobs
	}
	jobsTerm := 1 / (1 + float64(max(jobs, 0))/pol.jobsHalf)
	return m.term * m.io * load * jobsTerm, true
}

// measuredScore rebuilds the host's score from its signals: measuredFree times
// the capability, less the bridge claims, each costing bridge_cost*100/capability
// as the live score charges them by power. A host that sent no score signals
// keeps the score the hub sent. The hub's hold and burst bonuses are not
// replicated.
func (pol shadowPolicy) measuredScore(h selHost, req shadowReq, m shadowMem) *float64 {
	free, ok := pol.measuredFree(h, m)
	if !ok {
		return h.score
	}
	bridged := 0
	if h.view.Signals.BridgedJobs != nil {
		bridged = *h.view.Signals.BridgedJobs
	}
	capability, _ := pol.capabilityOf(req.class, h.name, h)
	if capability <= 0 {
		capability = shadowDefaultPower
	}
	score := free*capability - float64(bridged)*pol.bridgeCost*100/capability
	return &score
}

// shadowDecide marks the host the hub's decision rules would name, on the
// hosts that fit, are awake and have a score, in configuration order (the
// policy order the hub uses is local machine, then spill targets). It mirrors
// placementScoreK.decide in the root package: the free floor, the all-below
// pick by free*power, and the hysteresis margin (a negative winner needs a
// rival whose score is its own divided by hysteresis). free reports a host's
// free fraction and the power that scales it; known is false when the host has
// none, and such a host is never under the floor. A request with tags takes the
// highest score, as the hub does, so none is marked. The /v1/placement path
// already carries the hub's decision; it is kept.
func shadowDecide(hosts []selHost, req shadowReq, pol shadowPolicy, free func(selHost) (free, power float64, known bool), clear bool) {
	if clear {
		for i := range hosts {
			hosts[i].decided = false
		}
	}
	if len(req.tags) > 0 || slices.ContainsFunc(hosts, func(h selHost) bool { return h.decided }) {
		return
	}
	var cands []int
	for i, h := range hosts {
		if h.fit == "" && h.awake && h.score != nil {
			cands = append(cands, i)
		}
	}
	if len(cands) == 0 {
		return
	}
	score := func(i int) float64 { return *hosts[cands[i]].score }
	below := func(i int) bool {
		f, _, known := free(hosts[cands[i]])
		return known && f < pol.freeFloor
	}
	var above []int
	for i := range cands {
		if !below(i) {
			above = append(above, i)
		}
	}
	pick := 0
	if len(above) == 0 {
		best := math.Inf(-1)
		for i := range cands {
			f, p, _ := free(hosts[cands[i]])
			if v := f * p; v > best || (v == best && score(i) > score(pick)) {
				pick, best = i, v
			}
		}
	} else {
		if below(0) {
			pick = above[0]
		}
		challenger := -1
		for _, i := range above {
			if i != pick && (challenger < 0 || score(i) > score(challenger)) {
				challenger = i
			}
		}
		bar := score(pick) * pol.hysteresis
		if score(pick) < 0 {
			bar = score(pick) / pol.hysteresis
		}
		if challenger >= 0 && score(challenger) > score(pick) && score(challenger) >= bar {
			pick = challenger
		}
	}
	hosts[cands[pick]].decided = true
}

// shadowFixed is the live ranking with Defect A and Defect B fixed.
//
// A: one load measure for every host. The hub's CPU-busy fraction when any
// candidate has one, else load5 per core, never both: a host without the
// chosen reading has none and ranks as unknown. The local max_load_ratio guard
// stays in its own unit; it is a local crash guard, not a cross-host compare.
// B: the hub's usable verdict, and its hysteresis and all-loaded decision,
// apply on the fleet-state path as on /v1/placement.
func shadowFixed(hosts []selHost, req shadowReq, pol shadowPolicy) ([]selHost, string) {
	out := slices.Clone(hosts)
	measure := "load5_per_core"
	if slices.ContainsFunc(out, func(h selHost) bool { return h.fit == "" && cpuBusy(h) != nil }) {
		measure = "cpu_busy"
	}
	for i := range out {
		h := &out[i]
		threshold := pol.threshold
		if measure == "cpu_busy" {
			h.load, threshold = cpuBusy(*h), pol.cpuBusyOver
		} else {
			h.load = h.local5
		}
		h.over = h.load != nil && *h.load >= threshold ||
			h.local && h.guard != nil && (h.local5 == nil || *h.local5 >= *h.guard)
		// A slot-full host is unusable awake or asleep: the hub's verdict.
		if h.fit == "" && (h.slotFull || h.awake && !hubUsable(*h, false)) {
			h.fit = "unusable"
		}
	}
	// The shadow's own floor, so a free_floor set only there ranks the arm the
	// way the live engine would rank with that floor.
	for i := range out {
		h := &out[i]
		h.below, h.floorValue = false, 0
		if h.view != nil && h.view.Signals.Terms != nil {
			sig := h.view.Signals
			sig.BelowFreeFloor = sig.Terms.Free() < pol.freeFloor
			h.below, h.floorValue = floorKey(sig, h.score)
		}
	}
	shadowDecide(out, req, pol, func(h selHost) (float64, float64, bool) {
		if h.view == nil || h.view.Signals.Terms == nil {
			return 0, 0, false
		}
		t := h.view.Signals.Terms
		power := shadowDefaultPower
		if h.view.Signals.Power != nil && *h.view.Signals.Power > 0 {
			power = *h.view.Signals.Power
		}
		return t.CPU * t.Mem * t.IO * t.Load * t.Jobs, power, true
	}, false)
	return out, measure
}

// shadowMeasured is fixed plus the memory signal, the capability map and the
// rescored hosts. A host the hub dropped only for its memory floor is judged
// again by the one memory signal when that signal is known; a host that signal
// refuses, or one with no memory reading at all, stays dropped.
func shadowMeasured(hosts []selHost, req shadowReq, pol shadowPolicy) []selHost {
	out, _ := shadowFixed(hosts, req, pol)
	mems := make([]shadowMem, len(out))
	for i := range out {
		h := &out[i]
		mems[i] = pol.memory(*h)
		switch {
		case h.memExcluded:
			// The hub's refusal stands unless the one memory signal is known and
			// clears the host: with no reading there is nothing to replace it.
			if h.fit = h.fitNoMem; h.fit == "" && (mems[i].src == "none" || mems[i].refuse || h.awake && !hubUsable(*h, true)) {
				h.fit = "memory_refused"
			}
		case h.fit == "" && mems[i].refuse:
			h.fit = "memory_refused"
		}
		h.score = pol.measuredScore(*h, req, mems[i])
		h.below, h.floorValue = false, 0
		if f, ok := pol.measuredFree(*h, mems[i]); ok {
			capability, _ := pol.capabilityOf(req.class, h.name, *h)
			if capability <= 0 {
				capability = shadowDefaultPower
			}
			h.below, h.floorValue = f < pol.freeFloor, f*capability
			if h.score != nil {
				h.floorValue = *h.score
			}
		}
	}
	byName := map[string]shadowMem{}
	for i, h := range out {
		byName[h.name] = mems[i]
	}
	shadowDecide(out, req, pol, func(h selHost) (float64, float64, bool) {
		f, known := pol.measuredFree(h, byName[h.name])
		capability, _ := pol.capabilityOf(req.class, h.name, h)
		if capability <= 0 {
			capability = shadowDefaultPower
		}
		return f, capability, known
	}, true)
	return out
}

func shadowRank(order []selHost) []fleetevent.ShadowHostRank {
	out := make([]fleetevent.ShadowHostRank, 0, len(order))
	for _, h := range order {
		r := fleetevent.ShadowHostRank{Host: h.name, Bucket: hostBucket(h)}
		if h.score != nil && !math.IsNaN(*h.score) && !math.IsInf(*h.score, 0) {
			r.Score = h.score
		}
		out = append(out, r)
	}
	return out
}

func shadowCompare(live, arm []fleetevent.ShadowHostRank) string {
	same := len(live) == len(arm)
	for i := 0; same && i < len(live); i++ {
		same = live[i].Host == arm[i].Host
	}
	switch {
	case same:
		return shadowAgree
	case len(live) > 0 && len(arm) > 0 && live[0].Host == arm[0].Host:
		return shadowOrderDiffer
	}
	return shadowDisagree
}

func firstHost(order []fleetevent.ShadowHostRank) string {
	if len(order) == 0 {
		return ""
	}
	return order[0].Host
}

// runShadowArm runs one arm, turning a panic into an error string.
func (a *App) runShadowArm(name string, f func() []selHost) (order []selHost, failure string) {
	defer func() {
		if r := recover(); r != nil {
			order, failure = nil, fmt.Sprint("panic: ", r)
		}
	}()
	if a.shadowProbe != nil {
		a.shadowProbe(name)
	}
	return rankHosts(f()), ""
}

// shadowInput is the readings the arms used for one host.
func (pol shadowPolicy) shadowInput(h selHost, req shadowReq, now time.Time) fleetevent.ShadowInput {
	finite := func(p *float64) *float64 {
		if p == nil || math.IsNaN(*p) || math.IsInf(*p, 0) {
			return nil
		}
		return p
	}
	in := fleetevent.ShadowInput{Host: h.name, Local: h.local, Load5Core: finite(h.local5), CPUBusy: finite(cpuBusy(h))}
	if v := h.view; v != nil {
		sig, x := v.Signals, v.Extra
		in.Load1Core, in.MemAvail, in.MemFreePct = finite(sig.Load1PerCore), finite(sig.MemAvailable), finite(x.MemFreePct)
		in.Pressure, in.Jobs, in.Power = finite(x.Pressure), sig.LiveJobs, finite(sig.Power)
		if !x.MeasuredAt.IsZero() {
			age := math.Max(now.Sub(x.MeasuredAt).Seconds(), 0)
			in.SignalAgeS = &age
		}
	}
	m := pol.memory(h)
	in.MemorySrc, in.SwapInPerS = m.src, finite(m.swap)
	if c, ok := pol.capabilityOf(req.class, h.name, h); ok {
		in.Capability = finite(&c)
	}
	return in
}

// shadowRecord is one placement_shadow event between the two halves of
// host-first selection. The host arms and the inputs are fixed before admission
// runs. The waste_first arm (placement_shadow_waste.go) reads the quota states
// admission produces, so finishPlacementShadow writes the event after it.
type shadowRecord struct {
	job, reason, outcome, detail string
	sh                           *fleetevent.PlacementShadow
}

// recordPlacementShadow ranks the hosts under the host arms and returns the
// event to finish once admission has run. live is the order the live ranking
// produced. It never changes what the caller does: every failure is contained
// here.
func (a *App) recordPlacementShadow(s Spawn, role string, tags []string, hosts, live []selHost, now time.Time) (rec *shadowRecord) {
	defer func() { _ = recover() }()
	pol := a.shadowPolicy()
	if !pol.enabled || s.DryRun {
		return nil
	}
	req := shadowReq{role: role, class: workClassOf(role), tags: tags}
	liveRank := shadowRank(live)
	sh := &fleetevent.PlacementShadow{Role: role, WorkClass: req.class, Live: fleetevent.ShadowOrder{First: firstHost(liveRank), Order: liveRank}, Arms: map[string]fleetevent.ShadowArm{}}
	measure := ""
	arms := []struct {
		name string
		rank func() []selHost
	}{
		{shadowArmFixed, func() []selHost {
			out, m := shadowFixed(hosts, req, pol)
			measure = m
			return out
		}},
		{shadowArmMeasured, func() []selHost { return shadowMeasured(hosts, req, pol) }},
	}
	outcome, all := shadowAgree, map[string]bool{}
	for _, arm := range arms {
		order, failure := a.runShadowArm(arm.name, arm.rank)
		rank := shadowRank(order)
		x := fleetevent.ShadowArm{First: firstHost(rank), Order: rank, Outcome: shadowCompare(liveRank, rank)}
		if failure != "" {
			x.Outcome, x.Error = shadowError, failure
		}
		sh.Arms[arm.name] = x
		all[x.Outcome] = true
	}
	for _, o := range []string{shadowError, shadowDisagree, shadowOrderDiffer} {
		if all[o] {
			outcome = o
			break
		}
	}
	sh.LoadMeasure = measure
	for _, h := range hosts {
		if len(sh.Inputs) == fleetevent.MaxCandidates {
			break
		}
		if h.fit == "" || h.memExcluded {
			sh.Inputs = append(sh.Inputs, pol.shadowInput(h, req, now))
		}
	}
	if len(liveRank) == 0 && len(sh.Inputs) == 0 {
		return nil
	}
	detail := fmt.Sprintf("work_class=%s role=%s fixed=%s measured=%s", req.class, role, sh.Arms[shadowArmFixed].Outcome, sh.Arms[shadowArmMeasured].Outcome)
	return &shadowRecord{job: s.Job, reason: req.class, outcome: outcome, detail: detail, sh: sh}
}

// finishPlacementShadow adds the waste_first arm to rec and writes the event.
// It runs once admission has run. usable are the ranked hosts the walk could
// still use; livePass is the pass that chose (1, or 2 for the credit band).
// snap is the waste_first order taken during admission (snapErr its failure).
// The event outcome and detail stay those of the host arms. A nil rec (shadow
// off, dry run, or nothing to record) writes nothing.
func (a *App) finishPlacementShadow(rec *shadowRecord, snap wasteSnap, snapErr string, st *hostAdmission, p *selPlan, usable []selHost, livePass int) {
	if rec == nil {
		return
	}
	defer func() { _ = recover() }()
	rec.sh.WasteFirst = a.runWasteFirst(snap, snapErr, p, usable, st, livePass)
	a.fleetEventWriter().EmitEvent(fleetevent.Event{Kind: "placement_shadow", Job: rec.job, RequestID: a.RequestID, Outcome: rec.outcome, Reason: rec.reason, Detail: rec.detail, Shadow: rec.sh})
}

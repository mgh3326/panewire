package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)

// PlacementPolicy is deliberately small and operator-owned. It selects a
// local machine first, then considers spill targets in the listed order.
type PlacementPolicy struct {
	LocalMachine     string   `json:"local_machine"`
	SpillTargets     []string `json:"spill_targets"`
	MaxActiveJobs    int      `json:"max_active_jobs"`
	LoadRatio        float64  `json:"load_ratio"`
	WakeOnSpill      bool     `json:"wake_on_spill"`
	MemoryFreePctMin *float64 `json:"memory_free_pct_min,omitempty"`
	SwapUsedMBMax    *float64 `json:"swap_used_mb_max,omitempty"`
	// QuotaExclude and QuotaBoost are retired and never evaluated, but they
	// stay decoded so a stale policy file still loads: reload logs a warning
	// and /v1/placement fails closed for any caller that still asks for the
	// quota judgment while the inert rules remain. Remove them once every
	// desk config is clean.
	QuotaExclude []json.RawMessage                `json:"quota_exclude,omitempty"`
	QuotaBoost   []json.RawMessage                `json:"quota_boost,omitempty"`
	Machines     map[string]PlacementMachineSlots `json:"machines,omitempty"`
	// Power is an optional relative speed weight per machine (100 = the
	// strongest host). A machine without an entry weighs defaultPlacementPower.
	// See docs/placement.md for how the weights are measured.
	Power map[string]float64 `json:"power,omitempty"`
}

// PlacementMachineSlots caps how many distinct tasks may hold active jobs on
// one machine. A task is the owner_lane bundle: a builder job plus every
// tester/worker job that builder spawned with itself as owner.
type PlacementMachineSlots struct {
	TaskSlots int `json:"task_slots"`
}

const (
	defaultPlacementPower = 100.0
	maxPlacementPower     = 10000.0
	// placementSparePowerBonus is the score a fully idle machine of power 100
	// earns; it only has to break ties between otherwise equal candidates.
	placementSparePowerBonus = 5.0
)

func DefaultPlacementPolicy() PlacementPolicy {
	return PlacementPolicy{LocalMachine: "mac-work", SpillTargets: []string{"desktop"}, MaxActiveJobs: 5, LoadRatio: 0.5, MemoryFreePctMin: memoryFloat(30), SwapUsedMBMax: memoryFloat(1536)}
}

func (p PlacementPolicy) valid() bool {
	if !machineIDPattern.MatchString(p.LocalMachine) || p.LocalMachine == hubOperatorMachineID || p.MaxActiveJobs < 1 || p.MaxActiveJobs > 10000 || p.LoadRatio <= 0 || p.LoadRatio > 100 ||
		!validOptionalMemoryFloat(p.MemoryFreePctMin, 0, 100) || !validOptionalMemoryFloat(p.SwapUsedMBMax, 0, math.Inf(1)) {
		return false
	}
	seen := map[string]struct{}{p.LocalMachine: {}}
	for _, target := range p.SpillTargets {
		if !machineIDPattern.MatchString(target) || target == hubOperatorMachineID {
			return false
		}
		if _, duplicate := seen[target]; duplicate {
			return false
		}
		seen[target] = struct{}{}
	}
	for machine, slots := range p.Machines {
		if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID || slots.TaskSlots < 1 || slots.TaskSlots > 100 {
			return false
		}
	}
	for machine, power := range p.Power {
		if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID || !(power > 0 && power <= maxPlacementPower) {
			return false
		}
	}
	return true
}

// power returns the machine's relative speed weight; a missing entry is 100.
func (p PlacementPolicy) power(machine string) float64 {
	if power, exists := p.Power[machine]; exists {
		return power
	}
	return defaultPlacementPower
}

// placementScoreBase is the load-and-jobs part of a candidate's score. Each
// active job costs 10 points on a power-100 host and proportionally more on a
// weaker one; an idle machine earns a bonus in proportion to its spare power
// so stronger idle hosts win ties. A weak host stays eligible: it is ranked
// lower, never filtered. load is a busy fraction and is clamped for the bonus.
func placementScoreBase(load float64, jobs int, power float64) float64 {
	spare := power * (1 - math.Min(math.Max(load, 0), 1))
	return 100.0 - load*100 - float64(jobs)*10*defaultPlacementPower/power + placementSparePowerBonus*spare/defaultPlacementPower
}

func (p PlacementPolicy) memoryFreePctMin() float64 {
	if p.MemoryFreePctMin == nil {
		return 30
	}
	return *p.MemoryFreePctMin
}

func (p PlacementPolicy) swapUsedMBMax() float64 {
	if p.SwapUsedMBMax == nil {
		return 1536
	}
	return *p.SwapUsedMBMax
}

func (p *PlacementPolicy) applyMemoryDefaults() {
	if p.MemoryFreePctMin == nil {
		p.MemoryFreePctMin = memoryFloat(30)
	}
	if p.SwapUsedMBMax == nil {
		p.SwapUsedMBMax = memoryFloat(1536)
	}
}

func ParsePlacementPolicy(data []byte) (PlacementPolicy, error) {
	var policy PlacementPolicy
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&policy); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return PlacementPolicy{}, errors.New("placement policy is invalid")
	}
	policy.applyMemoryDefaults()
	if !policy.valid() {
		return PlacementPolicy{}, errors.New("placement policy is invalid")
	}
	return policy, nil
}

func LoadPlacementPolicy(path string) (PlacementPolicy, time.Time, error) {
	if path == "" {
		return PlacementPolicy{}, time.Time{}, errors.New("placement policy path is required")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return PlacementPolicy{}, time.Time{}, errors.New("placement policy must be a regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return PlacementPolicy{}, time.Time{}, errors.New("read placement policy")
	}
	policy, err := ParsePlacementPolicy(data)
	return policy, info.ModTime(), err
}

type PlacementCandidate struct {
	Machine    string  `json:"machine"`
	Score      float64 `json:"score"`
	LoadRatio  float64 `json:"load_ratio,omitempty"`
	Throttled  bool    `json:"throttled"`
	ActiveJobs int     `json:"active_jobs"`
	// Tasks and TaskSlots serialize only for machines the policy caps;
	// uncapped machines (and policies without machines) keep the legacy wire
	// byte-identical so older CLI decoders do not reject the response.
	Tasks         *int     `json:"tasks,omitempty"`
	TaskSlots     int      `json:"task_slots,omitempty"`
	Connected     bool     `json:"connected"`
	MetricsKnown  bool     `json:"metrics_known"`
	MemoryFreePct *float64 `json:"memory_free_pct"`
	SwapUsedMB    *float64 `json:"swap_used_mb"`
	MemoryKnown   bool     `json:"memory_known"`
	HoldsActive   bool     `json:"holds_active"`
	BurstReady    bool     `json:"burst_ready"`
	Reason        string   `json:"reason"`
	// slotFull is wire-internal: a slot-full machine is filtered out of the
	// candidates array before the response is encoded, so it never serializes.
	slotFull bool
}

// PlacementQuotaDecision survives on the wire only: a new hub emits an
// `allow` stub when a caller still asks for a pool, so a pre-change
// `panewire place` keeps accepting the response during the rolling upgrade.
type PlacementQuotaDecision struct {
	Pool         string `json:"pool"`
	AccountFP    string `json:"account_fp,omitempty"`
	Decision     string `json:"decision"`
	Reason       string `json:"reason"`
	PolicyStatus string `json:"policy_status"`
}

// Legacy selector patterns kept for the compat stub's parameter check.
var (
	hubQuotaPoolPattern      = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,63}$`)
	hubQuotaAccountFPPattern = regexp.MustCompile(`^[0-9a-f]{8}$`)
)

type PlacementResult struct {
	Decision     string                  `json:"decision"`
	Candidates   []PlacementCandidate    `json:"candidates"`
	Source       string                  `json:"source"`
	Asof         time.Time               `json:"asof"`
	PolicyStatus string                  `json:"policy_status"`
	Reason       string                  `json:"reason,omitempty"`
	Quota        *PlacementQuotaDecision `json:"quota,omitempty"`
}

// MarshalJSON keeps the internal unavailable sentinel useful to callers while
// presenting the API contract's null decision to external clients.
func (r PlacementResult) MarshalJSON() ([]byte, error) {
	type wirePlacementResult struct {
		Decision     *string                 `json:"decision"`
		Candidates   []PlacementCandidate    `json:"candidates"`
		Source       string                  `json:"source"`
		Asof         time.Time               `json:"asof"`
		PolicyStatus string                  `json:"policy_status"`
		Reason       string                  `json:"reason,omitempty"`
		Quota        *PlacementQuotaDecision `json:"quota,omitempty"`
	}
	var decision *string
	if r.Decision != "unavailable" {
		decision = &r.Decision
	}
	return json.Marshal(wirePlacementResult{Decision: decision, Candidates: r.Candidates, Source: r.Source, Asof: r.Asof, PolicyStatus: r.PolicyStatus, Reason: r.Reason, Quota: r.Quota})
}

func (r *PlacementResult) UnmarshalJSON(data []byte) error {
	type wirePlacementResult struct {
		Decision     *string              `json:"decision"`
		Candidates   []PlacementCandidate `json:"candidates"`
		Source       string               `json:"source"`
		Asof         time.Time            `json:"asof"`
		PolicyStatus string               `json:"policy_status"`
		Reason       string               `json:"reason"`
		// Quota keeps an old hub's quota object decodable during a rolling
		// upgrade; the value is ignored. Drop it once every hub runs this build.
		Quota json.RawMessage `json:"quota"`
	}
	var wire wirePlacementResult
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("placement response is invalid")
	}
	r.Candidates, r.Source, r.Asof, r.PolicyStatus, r.Reason = wire.Candidates, wire.Source, wire.Asof, wire.PolicyStatus, wire.Reason
	if wire.Decision == nil {
		r.Decision = "unavailable"
	} else {
		r.Decision = *wire.Decision
	}
	return nil
}

type placementCache struct {
	key    string
	at     time.Time
	result PlacementResult
}

// placementTaskKey groups the active jobs of one task: a builder's claim
// records owner_lane as its own lane, and the tester/worker jobs it spawns
// carry that same lane as --owner. A job without an attributable owner —
// empty, or the "default" sentinel used before --owner was required —
// cannot be grouped and conservatively counts as a task of its own.
func placementTaskKey(job HubActiveJob) string {
	if job.OwnerLane == "" || job.OwnerLane == "default" {
		return "\x00job\x00" + job.JobID
	}
	return job.OwnerLane
}

func countPlacementTasks(jobs map[string]HubActiveJob) int {
	tasks := make(map[string]struct{}, len(jobs))
	for _, job := range jobs {
		tasks[placementTaskKey(job)] = struct{}{}
	}
	return len(tasks)
}

// placementCPUBusyQuery is the busy fraction (0..1) per machine: one minus the
// idle rate over five minutes, averaged over that machine's cpus. Unlike
// node_load5 it does not count blocked threads, which on macOS can push the
// load average far above the core count on a host that is barely busy.
const placementCPUBusyQuery = `clamp(1 - avg by (machine_id) (rate(node_cpu_seconds_total{mode="idle"}[5m])), 0, 1)`

type placementMetrics struct {
	load      map[string]float64
	throttled map[string]bool
}

func (h *HubServer) reloadPlacementPolicyLocked() {
	if h.placementPolicyPath == "" {
		h.placementPolicy = DefaultPlacementPolicy()
		h.placementPolicyLoaded = true
		h.placementPolicyStatus = "default"
		return
	}
	info, err := os.Lstat(h.placementPolicyPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		h.setPlacementPolicyFailureLocked("unreadable")
		return
	}
	if info.ModTime().Equal(h.placementPolicyObservedModTime) {
		return
	}
	policy, modTime, err := LoadPlacementPolicy(h.placementPolicyPath)
	if err != nil {
		h.placementPolicyObservedModTime = info.ModTime()
		h.setPlacementPolicyFailureLocked("invalid")
		return
	}
	if len(policy.QuotaExclude)+len(policy.QuotaBoost) > 0 && h.logger != nil {
		h.logger.Warn("placement policy quota rules are ignored; remove quota_exclude/quota_boost", "quota_exclude", len(policy.QuotaExclude), "quota_boost", len(policy.QuotaBoost))
	}
	h.placementPolicy, h.placementPolicyModTime = policy, modTime
	h.placementPolicyObservedModTime = modTime
	h.placementPolicyLoaded = true
	h.placementPolicyStatus = "current"
	h.placementPolicyLastFailure = ""
	h.placementCache = placementCache{}
}

func (h *HubServer) setPlacementPolicyFailureLocked(failure string) {
	status := "invalid"
	if h.placementPolicyLoaded {
		status = "stale"
	}
	changed := h.placementPolicyStatus != status || h.placementPolicyLastFailure != failure
	h.placementPolicyStatus, h.placementPolicyLastFailure = status, failure
	if changed {
		h.placementCache = placementCache{}
		h.logger.Error("placement policy reload failed", "policy_status", status, "failure", failure)
	}
}

// handleQuotaV2Gone is the retired /v2/quota shadow store's tombstone.
func (h *HubServer) handleQuotaV2Gone(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "quota_v2_unsupported"})
}

func (h *HubServer) handlePlacement(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeOperator(r) {
		hubUnauthorized(w)
		return
	}
	class := r.URL.Query().Get("class")
	if class != "worker" && class != "verifier" {
		http.Error(w, "invalid placement class", http.StatusBadRequest)
		return
	}
	// The retired quota selector stays answerable for one release: an old
	// caller still sends it, so the response carries an allow stub.
	pool, accountFP := r.URL.Query().Get("pool"), r.URL.Query().Get("account_fp")
	if (pool == "" && accountFP != "") || (pool != "" && !hubQuotaPoolPattern.MatchString(pool)) || (accountFP != "" && !hubQuotaAccountFPPattern.MatchString(accountFP)) {
		http.Error(w, "invalid quota selector", http.StatusBadRequest)
		return
	}
	result := h.placement(r.Context(), class, r.URL.Query().Get("cwd"))
	if pool != "" {
		// An old caller validating the quota object: a policy that still
		// carries now-inert quota rules cannot honestly answer allow, so
		// the selector fails closed until the desk removes them. Plain
		// placement (no pool) is unaffected and keeps the warn log.
		quota := &PlacementQuotaDecision{Pool: pool, AccountFP: accountFP, Decision: "allow", Reason: "quota_allowed", PolicyStatus: result.PolicyStatus}
		h.mu.Lock()
		rulesPresent := len(h.placementPolicy.QuotaExclude)+len(h.placementPolicy.QuotaBoost) > 0
		h.mu.Unlock()
		if rulesPresent {
			quota.Decision, quota.Reason = "unknown", "quota_policy_invalid"
			result.Decision, result.Reason = "unavailable", "quota_policy_invalid"
		}
		result.Quota = quota
	}
	writeJSON(w, http.StatusOK, result)
}

// PlacementSlotStatus is one machine's task-slot occupancy. TaskSlots is null
// when the policy does not cap that machine.
type PlacementSlotStatus struct {
	Machine   string `json:"machine"`
	TasksUsed int    `json:"tasks_used"`
	TaskSlots *int   `json:"task_slots"`
}

type placementSlotsResult struct {
	Machines     []PlacementSlotStatus `json:"machines"`
	PolicyStatus string                `json:"policy_status"`
	Asof         time.Time             `json:"asof"`
}

// handlePlacementSlots reports per-machine task-slot usage with the same
// operator auth boundary as /v1/placement. It answers "which machine has room"
// without running the placement decision.
func (h *HubServer) handlePlacementSlots(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeOperator(r) {
		hubUnauthorized(w)
		return
	}
	now := h.now().UTC()
	h.mu.Lock()
	h.reloadPlacementPolicyLocked()
	policy := h.placementPolicy
	if !policy.valid() {
		policy = DefaultPlacementPolicy()
	}
	names := map[string]struct{}{policy.LocalMachine: {}}
	for _, machine := range policy.SpillTargets {
		names[machine] = struct{}{}
	}
	for machine := range policy.Machines {
		names[machine] = struct{}{}
	}
	for machine := range h.nodes {
		names[machine] = struct{}{}
	}
	views := make([]PlacementSlotStatus, 0, len(names))
	for machine := range names {
		tasks := 0
		if record := h.nodes[machine]; record != nil {
			tasks = countPlacementTasks(record.activeJobs)
		}
		var slots *int
		if configured, exists := policy.Machines[machine]; exists {
			value := configured.TaskSlots
			slots = &value
		}
		views = append(views, PlacementSlotStatus{Machine: machine, TasksUsed: tasks, TaskSlots: slots})
	}
	status := h.placementPolicyStatus
	h.mu.Unlock()
	sort.Slice(views, func(i, j int) bool { return views[i].Machine < views[j].Machine })
	writeJSON(w, http.StatusOK, placementSlotsResult{Machines: views, PolicyStatus: status, Asof: now})
}

func (h *HubServer) placement(ctx context.Context, class, cwd string) PlacementResult {
	now := h.now().UTC()
	key := class + "\x00" + cwd
	h.mu.Lock()
	h.reloadPlacementPolicyLocked()
	if h.placementCache.key == key && now.Sub(h.placementCache.at) < 30*time.Second {
		cached := h.placementCache.result
		cached.Candidates = append([]PlacementCandidate(nil), cached.Candidates...)
		h.mu.Unlock()
		return cached
	}
	policy := h.placementPolicy
	if !policy.valid() {
		policy = DefaultPlacementPolicy()
	}
	policyStatus := h.placementPolicyStatus
	h.mu.Unlock()

	metrics, err := h.fetchPlacementMetrics(ctx)
	source := "prometheus"
	if err != nil {
		source = "hub-only"
		metrics = placementMetrics{}
	}
	result := h.makePlacement(policy, metrics, source, now)
	result.PolicyStatus = policyStatus
	h.mu.Lock()
	h.placementCache = placementCache{key: key, at: now, result: result}
	h.mu.Unlock()
	h.logger.Info("placement decision", "machine", result.Decision, "score", placementDecisionScore(result), "reason", placementDecisionReason(result))
	if policy.WakeOnSpill && result.Decision != "unavailable" && result.Decision != policy.LocalMachine && !placementConnected(result, result.Decision) {
		go func(target string) {
			wakeCtx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			_, _ = h.requestBurst(wakeCtx, target, "placement-spill", 5*time.Minute)
		}(result.Decision)
	}
	return result
}

func placementDecisionScore(result PlacementResult) float64 {
	for _, c := range result.Candidates {
		if c.Machine == result.Decision {
			return c.Score
		}
	}
	return 0
}
func placementDecisionReason(result PlacementResult) string {
	for _, c := range result.Candidates {
		if c.Machine == result.Decision {
			return c.Reason
		}
	}
	return "no_candidate"
}
func placementConnected(result PlacementResult, machine string) bool {
	for _, c := range result.Candidates {
		if c.Machine == machine {
			return c.Connected
		}
	}
	return false
}

func (h *HubServer) makePlacement(policy PlacementPolicy, metrics placementMetrics, source string, now time.Time) PlacementResult {
	machines := append([]string{policy.LocalMachine}, policy.SpillTargets...)
	candidates := make([]PlacementCandidate, 0, len(machines))
	h.mu.Lock()
	for _, machine := range machines {
		record := h.nodes[machine]
		connected, accepting, jobs, tasks := false, false, 0, 0
		var memory *HubHostMemory
		if record != nil {
			connected, accepting, jobs, tasks = record.agent != nil && record.state == "connected", h.acceptingEffectiveLocked(machine, record.accepting), len(record.activeJobs), countPlacementTasks(record.activeJobs)
			memory = cloneHubHostMemory(record.hostMemory)
		}
		taskSlots, slotCapped := policy.machineTaskSlots(machine)
		slotFull := slotCapped && tasks >= taskSlots
		holdsActive := h.holdsActiveLocked(machine, now)
		burstReady := h.burstPolicyPath != "" && h.burstPolicy.TargetMachine == machine && h.burstState.UpCompleted
		load, metricsKnown := metrics.load[machine]
		throttled := metrics.throttled[machine]
		reasons := make([]string, 0, 5)
		if !connected {
			reasons = append(reasons, "disconnected")
		}
		if connected && !accepting {
			reasons = append(reasons, "not_accepting")
		}
		if !metricsKnown && source == "prometheus" {
			reasons = append(reasons, "load_unknown")
		}
		if metricsKnown && load >= policy.LoadRatio {
			reasons = append(reasons, fmt.Sprintf("load_ratio>=%.2f", policy.LoadRatio))
		}
		if throttled {
			reasons = append(reasons, "throttled")
		}
		if jobs >= policy.MaxActiveJobs {
			reasons = append(reasons, "active_jobs>=max")
		}
		if slotFull {
			reasons = append(reasons, "task_slots_full")
		}
		if holdsActive {
			reasons = append(reasons, "hold_active")
		}
		if burstReady {
			reasons = append(reasons, "burst_ready")
		}
		memoryKnown := memory != nil && memory.FreePct != nil && memory.SwapUsedMB != nil
		memoryPressure := memory != nil &&
			(memory.FreePct != nil && *memory.FreePct < policy.memoryFreePctMin() ||
				memory.SwapUsedMB != nil && *memory.SwapUsedMB > policy.swapUsedMBMax())
		if memoryPressure {
			reasons = append(reasons, "not_accepting(memory_pressure)")
		} else if !memoryKnown {
			reasons = append(reasons, "memory_unknown")
		}
		if len(reasons) == 0 {
			reasons = append(reasons, "available")
		}
		score := placementScoreBase(load, jobs, policy.power(machine))
		if !connected {
			score -= 1000
		}
		if !accepting {
			score -= 200
		}
		if !metricsKnown && source == "prometheus" {
			score -= 500
		}
		if throttled {
			score -= 100
		}
		if holdsActive {
			score += 5
		}
		if burstReady {
			score += 5
		}
		var freePct, swapUsedMB *float64
		if memory != nil {
			freePct, swapUsedMB = cloneMemoryFloat(memory.FreePct), cloneMemoryFloat(memory.SwapUsedMB)
		}
		var tasksField *int
		if slotCapped {
			tasksField = new(int)
			*tasksField = tasks
		}
		candidates = append(candidates, PlacementCandidate{Machine: machine, Score: score, LoadRatio: load, Throttled: throttled, ActiveJobs: jobs, Tasks: tasksField, TaskSlots: taskSlots, Connected: connected, MetricsKnown: metricsKnown || source == "hub-only", MemoryFreePct: freePct, SwapUsedMB: swapUsedMB, MemoryKnown: memoryKnown, HoldsActive: holdsActive, BurstReady: burstReady, Reason: strings.Join(reasons, ","), slotFull: slotFull})
	}
	h.mu.Unlock()
	decision := policy.LocalMachine
	if !placementUsable(candidates[0], policy, source) {
		for _, candidate := range candidates[1:] {
			if placementUsable(candidate, policy, source) {
				decision = candidate.Machine
				break
			}
		}
		// Capacity pressure retains the historical local fallback when no spill
		// is available. An explicit/off effective acceptance is different: it
		// is an operator prohibition and must never be returned as a placement.
		if decision == policy.LocalMachine && strings.Contains(candidates[0].Reason, "not_accepting") {
			decision = "unavailable"
		}
	}
	// A disconnected spill target is still the actionable answer when wake is
	// enabled; callers can observe that fact in its reason. A slot-full target
	// is not actionable: waking it cannot admit the task.
	if decision == policy.LocalMachine && !placementUsable(candidates[0], policy, source) && policy.WakeOnSpill && len(candidates) > 1 {
		for _, candidate := range candidates[1:] {
			if !candidate.slotFull {
				decision = candidate.Machine
				break
			}
		}
	}
	// Prometheus responding without a local load sample is not the same as a
	// hub-only outage: never silently turn that unknown into a local decision.
	if decision == policy.LocalMachine && source == "prometheus" && !candidates[0].MetricsKnown {
		decision = "unavailable"
	}
	// The capacity-pressure fallback above can still name the local machine
	// even though it is unusable; a configured task-slot cap is an explicit
	// exhaustion signal, so it must never survive as the decision.
	slotBlocked := placementSlotFull(candidates, decision)
	if slotBlocked {
		decision = "unavailable"
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Score > candidates[j].Score })
	reason := ""
	if decision == "unavailable" {
		reason = "unavailable"
		if slotBlocked {
			// The chosen machine hit its task cap but other candidates remain:
			// distinct from task_slots_exhausted, which means none survived.
			reason = "task_slots_full"
		}
	}
	// Only machines with a free task slot are candidates. The caller may pick
	// any eligible entry when the decision is unavailable, so a slot-full
	// machine must not remain selectable here.
	candidates = filterSlotFullCandidates(candidates)
	if len(candidates) == 0 {
		decision, reason = "unavailable", "task_slots_exhausted"
	}
	return PlacementResult{Decision: decision, Candidates: candidates, Source: source, Asof: now, Reason: reason}
}

func (p PlacementPolicy) machineTaskSlots(machine string) (int, bool) {
	slots, exists := p.Machines[machine]
	if !exists {
		return 0, false
	}
	return slots.TaskSlots, true
}

func placementSlotFull(candidates []PlacementCandidate, machine string) bool {
	for _, candidate := range candidates {
		if candidate.Machine == machine {
			return candidate.slotFull
		}
	}
	return false
}

func filterSlotFullCandidates(candidates []PlacementCandidate) []PlacementCandidate {
	kept := candidates[:0]
	for _, candidate := range candidates {
		if !candidate.slotFull {
			kept = append(kept, candidate)
		}
	}
	return kept
}

func placementUsable(candidate PlacementCandidate, policy PlacementPolicy, source string) bool {
	if candidate.slotFull || !candidate.Connected || strings.Contains(candidate.Reason, "not_accepting") || candidate.ActiveJobs >= policy.MaxActiveJobs {
		return false
	}
	if source == "prometheus" && !candidate.MetricsKnown {
		return false
	}
	if source == "prometheus" && (candidate.Throttled || candidate.LoadRatio >= policy.LoadRatio) {
		return false
	}
	return true
}

func (h *HubServer) fetchPlacementMetrics(ctx context.Context) (placementMetrics, error) {
	h.mu.Lock()
	rawURL, client, bearer, user, pass := h.prometheusURL, h.prometheusClient, h.prometheusBearer, h.prometheusBasicUser, h.prometheusBasicPass
	h.mu.Unlock()
	if rawURL == "" {
		return placementMetrics{}, errors.New("prometheus unavailable")
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	queries := []string{placementCPUBusyQuery, "min by (machine_id) (min_over_time(node_thermal_cpu_speed_limit_ratio[5m]))"}
	values := make([]map[string]float64, len(queries))
	for i, query := range queries {
		got, err := prometheusVector(ctx, client, rawURL, query, bearer, user, pass)
		if err != nil {
			return placementMetrics{}, err
		}
		values[i] = got
	}
	throttled := make(map[string]bool, len(values[1]))
	for machine, value := range values[1] {
		throttled[machine] = value < 1
	}
	return placementMetrics{load: values[0], throttled: throttled}, nil
}

func prometheusVector(ctx context.Context, client *http.Client, rawURL, query, bearer, user, pass string) (map[string]float64, error) {
	base, err := url.Parse(rawURL)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, errors.New("prometheus unavailable")
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/api/v1/query"
	q := base.Query()
	q.Set("query", query)
	base.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
	if err != nil {
		return nil, err
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	} else if user != "" {
		req.SetBasicAuth(user, pass)
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, errors.New("prometheus unavailable")
	}
	var wire struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&wire) != nil || wire.Status != "success" || wire.Data.ResultType != "vector" {
		return nil, errors.New("prometheus unavailable")
	}
	result := make(map[string]float64, len(wire.Data.Result))
	for _, sample := range wire.Data.Result {
		machine := sample.Metric["machine_id"]
		if !machineIDPattern.MatchString(machine) || len(sample.Value) != 2 {
			continue
		}
		var raw string
		if json.Unmarshal(sample.Value[1], &raw) != nil {
			continue
		}
		var value float64
		if _, err := fmt.Sscan(raw, &value); err != nil {
			continue
		}
		result[machine] = value
	}
	return result, nil
}

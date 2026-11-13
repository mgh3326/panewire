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
	"sort"
	"strings"
	"sync"
	"time"
)

// PlacementPolicy is deliberately small and operator-owned. It selects a
// local machine first, then considers spill targets in the listed order.
type PlacementPolicy struct {
	Tags          map[string][]string `json:"tags,omitempty"`
	LocalMachine  string              `json:"local_machine"`
	SpillTargets  []string            `json:"spill_targets"`
	MaxActiveJobs int                 `json:"max_active_jobs"`
	// LoadRatio is retired: placement no longer filters on load, the measured
	// load only ranks candidates (docs/placement.md). It stays decodable, and
	// optional, so an existing policy file still loads.
	LoadRatio   float64 `json:"load_ratio,omitempty"`
	WakeOnSpill bool    `json:"wake_on_spill"`
	// MemoryFreePctMin is the only admission threshold left: a crash guard
	// against starting a session on a host about to run out of memory.
	MemoryFreePctMin *float64 `json:"memory_free_pct_min,omitempty"`
	// SwapUsedMBMax, SwapInPerSMax, PSISomeAvg10Max and PressureLevelMin are
	// retired load filters. Swap-in now lowers the score instead of excluding
	// the host. They stay decodable so a stale policy file still loads.
	SwapUsedMBMax    *float64 `json:"swap_used_mb_max,omitempty"`
	SwapInPerSMax    *float64 `json:"swap_in_per_s_max,omitempty"`
	PSISomeAvg10Max  *float64 `json:"psi_some_avg10_max,omitempty"`
	PressureLevelMin *float64 `json:"pressure_level_min,omitempty"`
	// SpawnBridgeSeconds is how long a freshly started job counts against its
	// host's score: the metrics lag a spawn, so until their window covers it
	// the job itself stands in for the load it is about to add. 0 means the
	// default, placementDefaultSpawnBridge.
	SpawnBridgeSeconds int `json:"spawn_bridge_seconds,omitempty"`
	// SwapInHalfMBps is retired: the io term's half points live in Score
	// (pagein_half_mbps, linux_swap_in_half_mbps). It stays decodable so a
	// stale policy file still loads, and its value is ignored.
	SwapInHalfMBps float64 `json:"swap_in_half_mbps,omitempty"`
	// Score overrides the placement score v2 constants; every field is
	// optional and a missing one keeps the default (docs/placement.md).
	Score *PlacementScoreConfig `json:"score,omitempty"`
	// QuotaExclude and QuotaBoost are retired and never evaluated. They stay
	// decodable so a stale policy file still loads: reload logs a warning and
	// strips them, so the desk can remove the keys at leisure. Remove the
	// fields once every desk config is clean.
	QuotaExclude []json.RawMessage                `json:"quota_exclude,omitempty"`
	QuotaBoost   []json.RawMessage                `json:"quota_boost,omitempty"`
	Machines     map[string]PlacementMachineSlots `json:"machines,omitempty"`
	// Power is an optional relative speed weight per machine (100 = the
	// strongest host). A machine without an entry weighs defaultPlacementPower.
	// See docs/placement.md for how the weights are measured.
	Power map[string]float64 `json:"power,omitempty"`
	// CapacityWeight is retired: power is the only per-host multiplier. It
	// stays decodable so a stale policy file still loads; reload logs a warning
	// and strips it, and the score never reads it.
	CapacityWeight json.RawMessage `json:"capacity_weight,omitempty"`
	// Laptops lists the machines placement refuses on battery or without a
	// recent power report (host_signals.go). Set it only after those nodes
	// report: docs/placement.md, "Laptop power".
	Laptops []string `json:"laptops,omitempty"`
	// LaunchFailureThreshold consecutive launch failures of one (machine,
	// harness kind) exclude that pair for LaunchBackoffMinutes; a success
	// clears it. 0 means the defaults, 2 failures and 30 minutes
	// (hub_readiness.go).
	LaunchFailureThreshold int `json:"launch_failure_threshold,omitempty"`
	LaunchBackoffMinutes   int `json:"launch_backoff_minutes,omitempty"`
	// SpawnPlacementWorkers is how many hub spawn requests are placed at once
	// (0 means 2); it is read when the spawn coordinator starts. SpawnInFlight
	// is how many spawns each node runs at once (0 means 3), and
	// SpawnInFlightHosts overrides it per machine. hub_spawn_executor.go,
	// docs/placement.md "Spawn queue".
	SpawnPlacementWorkers int            `json:"spawn_placement_workers,omitempty"`
	SpawnInFlight         int            `json:"spawn_in_flight,omitempty"`
	SpawnInFlightHosts    map[string]int `json:"spawn_in_flight_hosts,omitempty"`
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
	// placementDefaultSpawnBridge: the load1 window is three minutes and the
	// page-in window five, and a spawn takes minutes to settle, so a claim
	// counts for ten minutes before its load is in the metrics.
	placementDefaultSpawnBridge = 10 * time.Minute
	maxPlacementSpawnBridge     = time.Hour
	// placementBridgeClockSkew is how far in the future a job's start may be
	// (node clock ahead of the hub) and still count; a start further ahead
	// is not trusted, so a skewed clock cannot stretch the bridge.
	placementBridgeClockSkew = time.Minute
)

func DefaultPlacementPolicy() PlacementPolicy {
	return PlacementPolicy{LocalMachine: "mac-work", SpillTargets: []string{"desktop"}, MaxActiveJobs: 5, MemoryFreePctMin: memoryFloat(30)}
}

func (p PlacementPolicy) valid() bool {
	if !validPlacementHostTags(p.Tags) {
		return false
	}
	if !machineIDPattern.MatchString(p.LocalMachine) || p.LocalMachine == hubOperatorMachineID || p.MaxActiveJobs < 1 || p.MaxActiveJobs > 10000 || p.LoadRatio < 0 || p.LoadRatio > 100 ||
		p.LaunchFailureThreshold < 0 || p.LaunchFailureThreshold > maxPlacementLaunchFailures || p.LaunchBackoffMinutes < 0 || p.LaunchBackoffMinutes > maxPlacementLaunchBackoffMin ||
		p.SpawnBridgeSeconds < 0 || p.SpawnBridgeSeconds > int(maxPlacementSpawnBridge/time.Second) ||
		!(p.SwapInHalfMBps >= 0 && p.SwapInHalfMBps <= 100000) || !p.Score.valid() ||
		!validOptionalMemoryFloat(p.MemoryFreePctMin, 0, 100) || !validOptionalMemoryFloat(p.SwapUsedMBMax, 0, math.Inf(1)) ||
		!validOptionalMemoryFloat(p.SwapInPerSMax, 0, math.Inf(1)) || !validOptionalMemoryFloat(p.PSISomeAvg10Max, 0, 100) || !validOptionalMemoryFloat(p.PressureLevelMin, 0, 2) {
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
	for machine := range p.Tags {
		if _, candidate := seen[machine]; !candidate {
			return false
		}
	}
	for machine, slots := range p.Machines {
		if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID || slots.TaskSlots < 1 || slots.TaskSlots > 100 {
			return false
		}
	}
	if p.SpawnPlacementWorkers < 0 || p.SpawnPlacementWorkers > maxSpawnConcurrency || p.SpawnInFlight < 0 || p.SpawnInFlight > maxSpawnConcurrency {
		return false
	}
	for machine, n := range p.SpawnInFlightHosts {
		if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID || n < 1 || n > maxSpawnConcurrency {
			return false
		}
	}
	for machine, power := range p.Power {
		if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID || !(power > 0 && power <= maxPlacementPower) {
			return false
		}
	}
	for _, machine := range p.Laptops {
		if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID {
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

func (p PlacementPolicy) spawnBridge() time.Duration {
	if p.SpawnBridgeSeconds <= 0 {
		return placementDefaultSpawnBridge
	}
	return time.Duration(p.SpawnBridgeSeconds) * time.Second
}

// countBridgedJobs counts the jobs that started less than bridge ago. A job
// whose start time is unknown cannot be placed in the window and is left out;
// a start up to placementBridgeClockSkew in the future (node clock ahead of
// the hub) counts, one further ahead does not.
func countBridgedJobs(jobs map[string]HubActiveJob, now time.Time, bridge time.Duration) int {
	count := 0
	for _, job := range jobs {
		started, err := time.Parse(time.RFC3339, job.StartedAt)
		if age := now.Sub(started); err == nil && age < bridge && age > -placementBridgeClockSkew {
			count++
		}
	}
	return count
}

func (p PlacementPolicy) memoryFreePctMin() float64 {
	if p.MemoryFreePctMin == nil {
		return 30
	}
	return *p.MemoryFreePctMin
}

func (p *PlacementPolicy) applyMemoryDefaults() {
	if p.MemoryFreePctMin == nil {
		p.MemoryFreePctMin = memoryFloat(30)
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
	Tasks        *int `json:"tasks,omitempty"`
	TaskSlots    int  `json:"task_slots,omitempty"`
	Connected    bool `json:"connected"`
	MetricsKnown bool `json:"metrics_known"`
	// QuotaDecision/QuotaReason are retired; kept decodable (and unemitted
	// when absent) so a new client still reads an old hub's candidates.
	QuotaDecision *string  `json:"quota_decision,omitempty"`
	QuotaReason   *string  `json:"quota_reason,omitempty"`
	MemoryFreePct *float64 `json:"memory_free_pct"`
	SwapUsedMB    *float64 `json:"swap_used_mb"`
	// SwapInPerS and Pressure serialize only when a node reports the live
	// signal; candidates without them keep the legacy wire byte-identical so
	// older CLI decoders (DisallowUnknownFields) do not reject the response.
	// CapacityWeight is retired and never set; it stays decodable so a new CLI
	// still reads an old hub's candidates.
	SwapInPerS     *float64 `json:"swap_in_per_s,omitempty"`
	Pressure       *float64 `json:"pressure,omitempty"`
	CapacityWeight float64  `json:"capacity_weight,omitempty"`
	MemoryKnown    bool     `json:"memory_known"`
	HoldsActive    bool     `json:"holds_active"`
	BurstReady     bool     `json:"burst_ready"`
	Reason         string   `json:"reason"`
	// Signals are the measurements behind Score. They serialize only when the
	// caller asks with signals=1 (place --explain): a pre-change CLI decodes
	// candidates with DisallowUnknownFields and never asks.
	Signals *PlacementSignals `json:"signals,omitempty"`
	// slotFull is wire-internal: a slot-full machine is filtered out of the
	// candidates array before the response is encoded, so it never serializes.
	slotFull bool
	// accepting is the effective acceptance placement derived (connected,
	// accepting after overrides, above the memory floor); wire-internal too.
	accepting bool
}

// PlacementSignals explains one candidate's score. A nil measurement is one
// the host does not export (macOS has no PSI or MemAvailable gauge); it is
// left out of the score, not guessed. BridgedJobs are the jobs younger than
// the spawn bridge, which the metrics do not cover yet.
type PlacementSignals struct {
	CPUBusy1m    *float64 `json:"cpu_busy_1m"`
	CPUBusy5m    *float64 `json:"cpu_busy_5m"`
	MemAvailable *float64 `json:"mem_available"`
	SwapInMBps   *float64 `json:"swap_in_mbps"`
	// SwapInLinux says SwapInMBps is a Linux swap-in rate, not a macOS page-in
	// rate: the io term halves at a different rate for each.
	SwapInLinux  bool     `json:"swap_in_linux,omitempty"`
	Load1PerCore *float64 `json:"load1_per_core,omitempty"`
	LiveJobs     int      `json:"live_jobs"`
	// The node's live signals (host_activity.go) are recorded for the hk
	// placement record and --explain. No decision reads them yet (hk 3650 is the
	// later decision that may). CPUBusyShort is the 1-minute busy the hk record
	// names cpu_busy_short; SignalAgeS is the age of the heartbeat the node
	// readings came from.
	CPUBusyShort *float64 `json:"cpu_busy_short,omitempty"`
	SignalAgeS   *float64 `json:"signal_age_s,omitempty"`
	PowerSource  string   `json:"power_source,omitempty"`
	SwapInPerS   *float64 `json:"swap_in_per_s,omitempty"`
	SwapOutPerS  *float64 `json:"swap_out_per_s,omitempty"`
	Pressure     *float64 `json:"pressure,omitempty"`
	UserIdleS    *int64   `json:"user_idle_s,omitempty"`
	// Terms are the factors of Free; nil from a hub that predates score v2.
	Terms       *PlacementTerms `json:"terms,omitempty"`
	Free        float64         `json:"free"`
	Power       float64         `json:"power"`
	BridgedJobs int             `json:"bridged_jobs"`
}

type PlacementResult struct {
	Decision     string               `json:"decision"`
	Candidates   []PlacementCandidate `json:"candidates"`
	Source       string               `json:"source"`
	Asof         time.Time            `json:"asof"`
	PolicyStatus string               `json:"policy_status"`
	Reason       string               `json:"reason,omitempty"`
}

// MarshalJSON keeps the internal unavailable sentinel useful to callers while
// presenting the API contract's null decision to external clients.
func (r PlacementResult) MarshalJSON() ([]byte, error) {
	type wirePlacementResult struct {
		Decision     *string              `json:"decision"`
		Candidates   []PlacementCandidate `json:"candidates"`
		Source       string               `json:"source"`
		Asof         time.Time            `json:"asof"`
		PolicyStatus string               `json:"policy_status"`
		Reason       string               `json:"reason,omitempty"`
	}
	var decision *string
	if r.Decision != "unavailable" {
		decision = &r.Decision
	}
	return json.Marshal(wirePlacementResult{Decision: decision, Candidates: r.Candidates, Source: r.Source, Asof: r.Asof, PolicyStatus: r.PolicyStatus, Reason: r.Reason})
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

// The placement signals, chosen by the fleet metric survey in
// docs/placement.md. Every query aggregates by machine_id so remote-write
// labels such as instance and job cannot break vector matching.
const (
	// placementCPUBusyQuery is the busy fraction (0..1) per machine: one minus
	// the idle rate over five minutes, averaged over that machine's cpus.
	// Unlike node_load5 it does not count blocked threads, which on macOS can
	// push the load average far above the core count on a barely busy host.
	placementCPUBusyQuery = `clamp(1 - avg by (machine_id) (rate(node_cpu_seconds_total{mode="idle"}[5m])), 0, 1)`
	// placementCPUBusy1mQuery is the same fraction over the last scrape
	// interval (60s): irate reads the newest two samples in the 3m range.
	placementCPUBusy1mQuery = `clamp(1 - avg by (machine_id) (irate(node_cpu_seconds_total{mode="idle"}[3m])), 0, 1)`
	// placementMemAvailableQuery is the available memory fraction. Linux has
	// MemAvailable. macOS has no such gauge; available is what Activity
	// Monitor does not count as used (app memory, wired, compressed).
	placementMemAvailableQuery = `clamp((max by (machine_id) (node_memory_MemAvailable_bytes) / max by (machine_id) (node_memory_MemTotal_bytes)) or (1 - (max by (machine_id) (node_memory_internal_bytes) - max by (machine_id) (node_memory_purgeable_bytes) + max by (machine_id) (node_memory_wired_bytes) + max by (machine_id) (node_memory_compressed_bytes)) / max by (machine_id) (node_memory_total_bytes)), 0, 1)`
	// placementSwapInQuery is the macOS page-in rate in MB/s over five minutes.
	// node_exporter's swapped_in counter on macOS is the VM page-in count,
	// which includes file-backed reads, not only compressor swap. It still
	// tracks contention: it rises as available memory falls, and on the
	// busiest host 10+ MB/s cost 4.5 failures an hour (docs/placement.md).
	placementSwapInQuery = `sum by (machine_id) (rate(node_memory_swapped_in_bytes_total[5m])) / 1048576`
	// placementSwapInLinuxQuery is the Linux swap-in rate in MB/s over five
	// minutes (pswpin, counted as 4 KiB pages; an aarch64 kernel with larger
	// pages is undercounted). A host that reports both uses the macOS one.
	placementSwapInLinuxQuery = `sum by (machine_id) (rate(node_vmstat_pswpin[5m])) * 4096 / 1048576`
	// placementLoad1Query is load1 per core averaged over three minutes, so one
	// busy scrape does not swing the score and the reading still lags a spawn
	// by less than the bridge. Cores are counted from the cpu series.
	placementLoad1Query   = `max by (machine_id) (avg_over_time(node_load1[3m])) / count by (machine_id) (count by (machine_id, cpu) (node_cpu_seconds_total{mode="idle"}))`
	placementThermalQuery = `min by (machine_id) (min_over_time(node_thermal_cpu_speed_limit_ratio[5m]))`
)

// placementMetricQueries is every query one metrics read issues, in the
// order fetchPlacementMetrics indexes the answers.
var placementMetricQueries = [...]string{placementCPUBusyQuery, placementThermalQuery, placementCPUBusy1mQuery, placementMemAvailableQuery, placementSwapInQuery, placementSwapInLinuxQuery, placementLoad1Query}

type placementMetrics struct {
	load        map[string]float64 // five-minute CPU busy fraction
	cpu1m       map[string]float64
	memAvail    map[string]float64
	swapIn      map[string]float64 // macOS page-ins
	swapInLinux map[string]float64
	load1       map[string]float64 // load1 per core, 3m average
	throttled   map[string]bool
}

// signals assembles one machine's measurements; absent ones stay nil.
func (m placementMetrics) signals(machine string) PlacementSignals {
	lookup := func(values map[string]float64) *float64 {
		if value, ok := values[machine]; ok {
			return &value
		}
		return nil
	}
	signals := PlacementSignals{CPUBusy5m: lookup(m.load), CPUBusy1m: lookup(m.cpu1m), MemAvailable: lookup(m.memAvail), SwapInMBps: lookup(m.swapIn), Load1PerCore: lookup(m.load1)}
	if signals.SwapInMBps == nil {
		if signals.SwapInMBps = lookup(m.swapInLinux); signals.SwapInMBps != nil {
			signals.SwapInLinux = true
		}
	}
	return signals
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
	if len(policy.QuotaExclude)+len(policy.QuotaBoost) > 0 {
		if h.logger != nil {
			h.logger.Warn("placement policy quota rules are retired; remove quota_exclude/quota_boost", "quota_exclude", len(policy.QuotaExclude), "quota_boost", len(policy.QuotaBoost))
		}
		policy.QuotaExclude, policy.QuotaBoost = nil, nil
	}
	if len(policy.CapacityWeight) > 0 {
		if h.logger != nil {
			h.logger.Warn("placement policy capacity_weight is retired and ignored; set power instead")
		}
		policy.CapacityWeight = nil
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
	tags, err := parsePlacementTags(r.URL.Query().Get("tags"))
	if err != nil || len(r.URL.Query()["tags"]) > 1 {
		http.Error(w, "invalid placement tags; supply one --tags a,b list", http.StatusBadRequest)
		return
	}
	result := h.placement(r.Context(), class, r.URL.Query().Get("cwd"), tags...)
	writeJSON(w, http.StatusOK, placementWire(result, r.URL.Query().Get("signals") == "1"))
}

// placementWire is the response as a caller sees it. Signals ride along only
// when asked for: pre-change CLIs decode candidates with
// DisallowUnknownFields and never ask. The candidates are copied before they
// are cleared because a fresh result shares its array with the cache.
func placementWire(result PlacementResult, withSignals bool) PlacementResult {
	if withSignals {
		return result
	}
	candidates := make([]PlacementCandidate, len(result.Candidates))
	for i, candidate := range result.Candidates {
		candidate.Signals = nil
		candidates[i] = candidate
	}
	result.Candidates = candidates
	return result
}

func (h *HubServer) placement(ctx context.Context, class, cwd string, tags ...string) PlacementResult {
	now := h.now().UTC()
	key := class + "\x00" + cwd
	if len(tags) > 0 {
		key += "\x00" + strings.Join(tags, ",")
	}
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
	var result PlacementResult
	if len(tags) > 0 {
		result = h.makeTaggedPlacement(policy, metrics, source, now, tags)
	} else {
		result = h.makePlacement(policy, metrics, source, now)
	}
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

// placementCandidates scores every policy machine, slot-full ones included,
// in policy order. makePlacement decides from them; the fleet state publishes
// them as per-host load values.
func (h *HubServer) placementCandidates(policy PlacementPolicy, metrics placementMetrics, source string, now time.Time) []PlacementCandidate {
	machines := append([]string{policy.LocalMachine}, policy.SpillTargets...)
	candidates := make([]PlacementCandidate, 0, len(machines))
	h.mu.Lock()
	for _, machine := range machines {
		record := h.nodes[machine]
		connected, accepting, jobs, tasks, bridged := false, false, 0, 0, 0
		var memory *HubHostMemory
		if record != nil {
			connected, accepting, jobs, tasks = record.agent != nil && record.state == "connected", h.acceptingEffectiveLocked(machine, record.accepting), len(record.activeJobs), countPlacementTasks(record.activeJobs)
			bridged = countBridgedJobs(record.activeJobs, now, policy.spawnBridge())
			memory = cloneHubHostMemory(record.hostMemory)
		}
		taskSlots, slotCapped := policy.machineTaskSlots(machine)
		slotFull := slotCapped && tasks >= taskSlots
		holdsActive := h.holdsActiveLocked(machine, now)
		burstReady := h.burstPolicyPath != "" && h.burstPolicy.TargetMachine == machine && h.burstState.UpCompleted
		load, metricsKnown := metrics.load[machine]
		// Freshness gate: the last-minute reading exists only while the host
		// sent a sample in the last ~3 minutes, whereas the 5m one survives
		// for several. A host with only the stale 5m value is load_unknown,
		// never idle.
		if _, fresh := metrics.cpu1m[machine]; !fresh {
			metricsKnown = false
		}
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
		memoryKnown := memory != nil && memory.FreePct != nil
		// The memory floor is the one admission refusal left, a crash guard.
		// Swap-in lowers the score (the io term). The reason
		// keeps its historical spelling; consumers match "not_accepting".
		memoryPressure := memory != nil && memory.FreePct != nil && *memory.FreePct < policy.memoryFreePctMin()
		if memoryPressure {
			reasons = append(reasons, "not_accepting(memory_pressure)")
		} else if !memoryKnown {
			reasons = append(reasons, "memory_unknown")
		}
		powerRefusal := ""
		if connected {
			powerRefusal = placementPowerRefusal(policy, machine, record, now)
		}
		if powerRefusal != "" {
			reasons = append(reasons, "not_accepting("+powerRefusal+")")
		}
		if len(reasons) == 0 {
			reasons = append(reasons, "available")
		}
		signals := metrics.signals(machine)
		// Without a Prometheus memory reading, the node's own free_pct is
		// the next real measurement; a missing one must not read as idle.
		if signals.MemAvailable == nil && memory != nil && memory.FreePct != nil {
			fallback := *memory.FreePct / 100
			signals.MemAvailable = &fallback
		}
		recordNodeLiveSignals(&signals, record, memory, now)
		k := policy.scoreK()
		terms := k.terms(signals, jobs)
		signals.Terms, signals.LiveJobs = &terms, jobs
		signals.Free, signals.Power, signals.BridgedJobs = terms.Free(), policy.power(machine), bridged
		score := k.scoreBase(signals.Free, bridged, signals.Power)
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
		var freePct, swapUsedMB, swapInPerS, pressure *float64
		if memory != nil {
			freePct, swapUsedMB = cloneMemoryFloat(memory.FreePct), cloneMemoryFloat(memory.SwapUsedMB)
			swapInPerS, pressure = cloneMemoryFloat(memory.SwapInPerS), cloneMemoryFloat(memory.Pressure)
		}
		var tasksField *int
		if slotCapped {
			tasksField = new(int)
			*tasksField = tasks
		}
		candidates = append(candidates, PlacementCandidate{Machine: machine, Score: score, LoadRatio: load, Throttled: throttled, ActiveJobs: jobs, Tasks: tasksField, TaskSlots: taskSlots, Connected: connected, MetricsKnown: metricsKnown || source == "hub-only", MemoryFreePct: freePct, SwapUsedMB: swapUsedMB, SwapInPerS: swapInPerS, Pressure: pressure, MemoryKnown: memoryKnown, HoldsActive: holdsActive, BurstReady: burstReady, Reason: strings.Join(reasons, ","), Signals: &signals, slotFull: slotFull, accepting: connected && accepting && !memoryPressure && powerRefusal == ""})
	}
	h.mu.Unlock()
	return candidates
}

func (h *HubServer) makePlacement(policy PlacementPolicy, metrics placementMetrics, source string, now time.Time) PlacementResult {
	candidates := h.placementCandidates(policy, metrics, source, now)
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
	reason := ""
	if decision != "unavailable" {
		decision, reason = decideAmongUsable(policy, source, candidates, decision)
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Score > candidates[j].Score })
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

// decideAmongUsable puts the score rules on top of the policy-order winner:
// all hosts loaded, then the hysteresis margin (placement_score.go). A winner
// that is not itself usable (the local fallback, a wake target) is left alone.
func decideAmongUsable(policy PlacementPolicy, source string, candidates []PlacementCandidate, decision string) (string, string) {
	var usable []PlacementCandidate
	winner := -1
	for _, candidate := range candidates {
		if placementUsable(candidate, policy, source) {
			if candidate.Machine == decision {
				winner = len(usable)
			}
			usable = append(usable, candidate)
		}
	}
	if winner < 0 {
		return decision, ""
	}
	picked, reason := policy.scoreK().decide(usable, winner)
	return usable[picked].Machine, reason
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
	// A candidate decoded from an old hub may still carry its quota verdict;
	// deny and unknown are never usable here either.
	if candidate.QuotaDecision != nil && *candidate.QuotaDecision != "allow" && *candidate.QuotaDecision != "boost" && *candidate.QuotaDecision != "" {
		return false
	}
	if source == "prometheus" && !candidate.MetricsKnown {
		return false
	}
	// Load ranks candidates and never filters them; thermal throttling is a
	// hardware state, not load, and still makes a host unusable.
	if source == "prometheus" && candidate.Throttled {
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
	// One failed query fails the whole read: a half-answered Prometheus is
	// treated like a down one (hub-only), never as a host with no load. A
	// query that succeeds without a host's series only leaves that signal out.
	values := make([]map[string]float64, len(placementMetricQueries))
	errs := make([]error, len(placementMetricQueries))
	var wg sync.WaitGroup
	for i, query := range placementMetricQueries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			values[i], errs[i] = prometheusVector(ctx, client, rawURL, query, bearer, user, pass)
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return placementMetrics{}, err
	}
	throttled := make(map[string]bool, len(values[1]))
	for machine, value := range values[1] {
		throttled[machine] = value < 1
	}
	return placementMetrics{load: values[0], throttled: throttled, cpu1m: values[2], memAvail: values[3], swapIn: values[4], swapInLinux: values[5], load1: values[6]}, nil
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
		// A NaN or Inf sample (a 0/0 memory fraction, say) would make the
		// score NaN, and encoding/json refuses NaN: the response would be
		// an empty 200. Skip it so the signal reads as unreported.
		if math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		result[machine] = value
	}
	return result, nil
}

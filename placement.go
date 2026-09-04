package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

// PlacementPolicy is deliberately small and operator-owned. It selects a
// local machine first, then considers spill targets in the listed order.
type PlacementPolicy struct {
	LocalMachine  string   `json:"local_machine"`
	SpillTargets  []string `json:"spill_targets"`
	MaxActiveJobs int      `json:"max_active_jobs"`
	LoadRatio     float64  `json:"load_ratio"`
	WakeOnSpill   bool     `json:"wake_on_spill"`
}

func DefaultPlacementPolicy() PlacementPolicy {
	return PlacementPolicy{LocalMachine: "mac-work", SpillTargets: []string{"desktop"}, MaxActiveJobs: 5, LoadRatio: 0.5}
}

func (p PlacementPolicy) valid() bool {
	if !machineIDPattern.MatchString(p.LocalMachine) || p.LocalMachine == hubOperatorMachineID || p.MaxActiveJobs < 1 || p.MaxActiveJobs > 10000 || p.LoadRatio <= 0 || p.LoadRatio > 100 {
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
	return true
}

func ParsePlacementPolicy(data []byte) (PlacementPolicy, error) {
	var policy PlacementPolicy
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&policy); err != nil || decoder.Decode(&struct{}{}) != io.EOF || !policy.valid() {
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
	Machine      string  `json:"machine"`
	Score        float64 `json:"score"`
	LoadRatio    float64 `json:"load_ratio,omitempty"`
	Throttled    bool    `json:"throttled"`
	ActiveJobs   int     `json:"active_jobs"`
	Connected    bool    `json:"connected"`
	MetricsKnown bool    `json:"metrics_known"`
	HoldsActive  bool    `json:"holds_active"`
	BurstReady   bool    `json:"burst_ready"`
	Reason       string  `json:"reason"`
}

type PlacementResult struct {
	Decision   string               `json:"decision"`
	Candidates []PlacementCandidate `json:"candidates"`
	Source     string               `json:"source"`
	Asof       time.Time            `json:"asof"`
}

type placementCache struct {
	key    string
	at     time.Time
	result PlacementResult
}
type placementMetrics struct {
	load      map[string]float64
	throttled map[string]bool
}

func (h *HubServer) reloadPlacementPolicyLocked() {
	if h.placementPolicyPath == "" {
		h.placementPolicy = DefaultPlacementPolicy()
		return
	}
	info, err := os.Stat(h.placementPolicyPath)
	if err != nil || !info.Mode().IsRegular() {
		return
	}
	policy, modTime, err := LoadPlacementPolicy(h.placementPolicyPath)
	if err == nil {
		h.placementPolicy, h.placementPolicyModTime = policy, modTime
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
	result := h.placement(r.Context(), class, r.URL.Query().Get("cwd"))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func (h *HubServer) placement(ctx context.Context, class, cwd string) PlacementResult {
	now := h.now().UTC()
	key := class + "\x00" + cwd
	h.mu.Lock()
	if h.placementCache.key == key && now.Sub(h.placementCache.at) < 30*time.Second {
		cached := h.placementCache.result
		cached.Candidates = append([]PlacementCandidate(nil), cached.Candidates...)
		h.mu.Unlock()
		return cached
	}
	h.reloadPlacementPolicyLocked()
	policy := h.placementPolicy
	if !policy.valid() {
		policy = DefaultPlacementPolicy()
	}
	h.mu.Unlock()

	metrics, err := h.fetchPlacementMetrics(ctx)
	source := "prometheus"
	if err != nil {
		source = "hub-only"
		metrics = placementMetrics{}
	}
	result := h.makePlacement(policy, metrics, source, now)
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
		connected, accepting, jobs := false, false, 0
		if record != nil {
			connected, accepting, jobs = record.agent != nil && record.state == "connected", h.acceptingEffectiveLocked(machine, record.accepting), len(record.activeJobs)
		}
		holdsActive := h.holdsActiveLocked(machine, now)
		burstReady := h.burstPolicyPath != "" && h.burstPolicy.TargetMachine == machine && h.burstState.UpCompleted
		load, metricsKnown := metrics.load[machine]
		throttled := metrics.throttled[machine]
		reasons := make([]string, 0, 4)
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
		if holdsActive {
			reasons = append(reasons, "hold_active")
		}
		if burstReady {
			reasons = append(reasons, "burst_ready")
		}
		if len(reasons) == 0 {
			reasons = append(reasons, "available")
		}
		score := 100.0 - load*100 - float64(jobs)*10
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
		candidates = append(candidates, PlacementCandidate{Machine: machine, Score: score, LoadRatio: load, Throttled: throttled, ActiveJobs: jobs, Connected: connected, MetricsKnown: metricsKnown || source == "hub-only", HoldsActive: holdsActive, BurstReady: burstReady, Reason: strings.Join(reasons, ",")})
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
	}
	// A disconnected spill target is still the actionable answer when wake is
	// enabled; callers can observe that fact in its reason.
	if decision == policy.LocalMachine && !placementUsable(candidates[0], policy, source) && policy.WakeOnSpill && len(candidates) > 1 {
		decision = candidates[1].Machine
	}
	// Prometheus responding without a local load sample is not the same as a
	// hub-only outage: never silently turn that unknown into a local decision.
	if decision == policy.LocalMachine && source == "prometheus" && !candidates[0].MetricsKnown {
		decision = "unavailable"
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Score > candidates[j].Score })
	return PlacementResult{Decision: decision, Candidates: candidates, Source: source, Asof: now}
}

func placementUsable(candidate PlacementCandidate, policy PlacementPolicy, source string) bool {
	if !candidate.Connected || strings.Contains(candidate.Reason, "not_accepting") || candidate.ActiveJobs >= policy.MaxActiveJobs {
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
	queries := []string{"avg by (machine_id) (avg_over_time(node_load5[5m])) / on (machine_id) count by (machine_id) (count by (machine_id, cpu) (node_cpu_seconds_total))", "min by (machine_id) (min_over_time(node_thermal_cpu_speed_limit_ratio[5m]))"}
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

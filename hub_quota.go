package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	hubQuotaStatusOK          = "ok"
	hubQuotaStatusUnavailable = "unavailable"
	hubQuotaUnknownWindow     = "unknown"
	hubQuotaMaxPools          = 256
)

var (
	hubQuotaPoolPattern      = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,63}$`)
	hubQuotaAccountFPPattern = regexp.MustCompile(`^[0-9a-f]{8}$`)
	hubQuotaWindowPattern    = regexp.MustCompile(`^(?:[a-z0-9][a-z0-9._-]{0,31}|\?)$`)
)

// HubQuotaPool is one account/window observation. Nullable measurements are
// always encoded so an unavailable value can never turn into a fabricated 0.
type HubQuotaPool struct {
	Pool      string   `json:"pool"`
	AccountFP string   `json:"account_fp"`
	Window    string   `json:"window"`
	UsedPct   *float64 `json:"used_pct"`
	ResetAt   *string  `json:"reset_at"`
	Source    string   `json:"source"`
}

func (pool HubQuotaPool) valid() bool {
	if !hubQuotaPoolPattern.MatchString(pool.Pool) || !hubQuotaAccountFPPattern.MatchString(pool.AccountFP) || !hubQuotaWindowPattern.MatchString(pool.Window) {
		return false
	}
	if pool.Source == "" || len(pool.Source) > 128 || strings.ContainsAny(pool.Source, "\x00\r\n") {
		return false
	}
	if !validOptionalMemoryFloat(pool.UsedPct, 0, 100) {
		return false
	}
	return validOptionalRFC3339(pool.ResetAt)
}

// HubQuotaSnapshot is the optional top-level heartbeat quota object. Production
// nodes always send it; nil remains reserved for nodes predating this protocol.
type HubQuotaSnapshot struct {
	Status      string         `json:"status"`
	CollectedAt *string        `json:"collected_at"`
	Pools       []HubQuotaPool `json:"pools"`
}

func (snapshot HubQuotaSnapshot) valid() bool {
	if snapshot.Status == hubQuotaStatusUnavailable {
		return snapshot.CollectedAt == nil && snapshot.Pools != nil && len(snapshot.Pools) == 0
	}
	if snapshot.Status != hubQuotaStatusOK || snapshot.CollectedAt == nil || !validOptionalRFC3339(snapshot.CollectedAt) || len(snapshot.Pools) == 0 || len(snapshot.Pools) > hubQuotaMaxPools {
		return false
	}
	seen := make(map[string]struct{}, len(snapshot.Pools))
	for _, pool := range snapshot.Pools {
		if !pool.valid() {
			return false
		}
		key := pool.Pool + "\x00" + pool.AccountFP + "\x00" + pool.Window
		if _, duplicate := seen[key]; duplicate {
			return false
		}
		seen[key] = struct{}{}
	}
	return true
}

func validOptionalRFC3339(value *string) bool {
	if value == nil {
		return true
	}
	_, err := time.Parse(time.RFC3339, *value)
	return err == nil
}

func unavailableHubQuotaSnapshot() *HubQuotaSnapshot {
	return &HubQuotaSnapshot{Status: hubQuotaStatusUnavailable, Pools: []HubQuotaPool{}}
}

func cloneHubQuotaSnapshot(snapshot *HubQuotaSnapshot) *HubQuotaSnapshot {
	if snapshot == nil {
		return nil
	}
	copy := *snapshot
	copy.CollectedAt = cloneQuotaString(snapshot.CollectedAt)
	copy.Pools = make([]HubQuotaPool, len(snapshot.Pools))
	for index, pool := range snapshot.Pools {
		copy.Pools[index] = pool
		copy.Pools[index].UsedPct = cloneMemoryFloat(pool.UsedPct)
		copy.Pools[index].ResetAt = cloneQuotaString(pool.ResetAt)
	}
	return &copy
}

func equalHubQuotaSnapshot(left, right *HubQuotaSnapshot) bool {
	if left == nil || right == nil {
		return left == right
	}
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

func cloneQuotaString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func decodeHubQuotaSnapshot(raw []byte) (*HubQuotaSnapshot, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 3 {
		return nil, false
	}
	for _, name := range []string{"status", "collected_at", "pools"} {
		if _, exists := fields[name]; !exists {
			return nil, false
		}
	}
	var snapshot HubQuotaSnapshot
	if json.Unmarshal(fields["status"], &snapshot.Status) != nil || json.Unmarshal(fields["collected_at"], &snapshot.CollectedAt) != nil {
		return nil, false
	}
	var rows []map[string]json.RawMessage
	if json.Unmarshal(fields["pools"], &rows) != nil || rows == nil || len(rows) > hubQuotaMaxPools {
		return nil, false
	}
	snapshot.Pools = make([]HubQuotaPool, 0, len(rows))
	for _, fields := range rows {
		if len(fields) != 6 {
			return nil, false
		}
		for _, name := range []string{"pool", "account_fp", "window", "used_pct", "reset_at", "source"} {
			if _, exists := fields[name]; !exists {
				return nil, false
			}
		}
		var pool HubQuotaPool
		if json.Unmarshal(fields["pool"], &pool.Pool) != nil ||
			json.Unmarshal(fields["account_fp"], &pool.AccountFP) != nil ||
			json.Unmarshal(fields["window"], &pool.Window) != nil ||
			json.Unmarshal(fields["used_pct"], &pool.UsedPct) != nil ||
			json.Unmarshal(fields["reset_at"], &pool.ResetAt) != nil ||
			json.Unmarshal(fields["source"], &pool.Source) != nil || !pool.valid() {
			return nil, false
		}
		snapshot.Pools = append(snapshot.Pools, pool)
	}
	if !snapshot.valid() {
		return nil, false
	}
	return &snapshot, true
}

type scopefuelQuotaDocument struct {
	Schema      string                   `json:"schema"`
	GeneratedAt string                   `json:"generated_at"`
	Providers   []scopefuelQuotaProvider `json:"providers"`
}

type scopefuelQuotaProvider struct {
	ID      string                 `json:"id"`
	Status  string                 `json:"status"`
	Source  *string                `json:"source"`
	Buckets []scopefuelQuotaBucket `json:"buckets"`
}

type scopefuelQuotaBucket struct {
	Window   string   `json:"window"`
	UsedPct  *float64 `json:"used_pct"`
	ResetsAt *string  `json:"resets_at"`
}

// collectHubQuota invokes only scopefuel's cached JSON reader. It never opens
// credential files and never sends a model request.
func collectHubQuota(ctx context.Context) (*HubQuotaSnapshot, error) {
	command, err := exec.LookPath("scopefuel")
	if err != nil {
		return nil, errors.New("quota unavailable")
	}
	environment := os.Environ()
	runContext, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	out, err := runHubScopefuelWithEnvironment(runContext, command, hubQuotaScopefuelEnvironment(environment))
	if err != nil {
		return nil, errors.New("quota unavailable")
	}
	return parseScopefuelHubQuota(out, environment)
}

func parseScopefuelHubQuota(data []byte, environment []string) (*HubQuotaSnapshot, error) {
	var document scopefuelQuotaDocument
	if json.Unmarshal(data, &document) != nil || document.Schema != "scopefuel.v1" || document.GeneratedAt == "" || len(document.Providers) == 0 {
		return nil, errors.New("quota unavailable")
	}
	if _, err := time.Parse(time.RFC3339, document.GeneratedAt); err != nil {
		return nil, errors.New("quota unavailable")
	}
	selectors := hubQuotaSelectors(environment)
	rows := make([]HubQuotaPool, 0, len(document.Providers))
	for _, provider := range document.Providers {
		if !hubQuotaPoolPattern.MatchString(provider.ID) {
			return nil, errors.New("quota unavailable")
		}
		fingerprint, ok := hubQuotaAccountFingerprint(provider.ID, selectors)
		if !ok {
			return nil, errors.New("quota unavailable")
		}
		if provider.Status != hubQuotaStatusOK || len(provider.Buckets) == 0 {
			rows = append(rows, HubQuotaPool{Pool: provider.ID, AccountFP: fingerprint, Window: hubQuotaUnknownWindow, Source: hubQuotaStatusUnavailable})
			continue
		}
		if provider.Source == nil || *provider.Source == "" {
			return nil, errors.New("quota unavailable")
		}
		for _, bucket := range provider.Buckets {
			row := HubQuotaPool{Pool: provider.ID, AccountFP: fingerprint, Window: bucket.Window, UsedPct: cloneMemoryFloat(bucket.UsedPct), ResetAt: cloneQuotaString(bucket.ResetsAt), Source: *provider.Source}
			if !row.valid() {
				return nil, errors.New("quota unavailable")
			}
			rows = append(rows, row)
		}
	}
	collectedAt := document.GeneratedAt
	snapshot := &HubQuotaSnapshot{Status: hubQuotaStatusOK, CollectedAt: &collectedAt, Pools: rows}
	if !snapshot.valid() {
		return nil, errors.New("quota unavailable")
	}
	return snapshot, nil
}

func hubQuotaScopefuelEnvironment(environment []string) []string {
	allowed := map[string]bool{
		"PATH": true, "HOME": true, "USER": true, "LANG": true,
		"CODEX_HOME": true, "CLAUDE_CONFIG_DIR": true, "GROK_HOME": true,
	}
	return filterHubEnvironment(environment, allowed)
}

func hubQuotaSelectors(environment []string) map[string]string {
	allowed := map[string]bool{"HOME": true, "CODEX_HOME": true, "CLAUDE_CONFIG_DIR": true, "GROK_HOME": true}
	selectors := make(map[string]string, len(allowed))
	for _, entry := range environment {
		name, value, found := strings.Cut(entry, "=")
		if found && allowed[name] && value != "" {
			selectors[name] = value
		}
	}
	return selectors
}

func hubQuotaAccountFingerprint(provider string, selectors map[string]string) (string, bool) {
	keys := []string{"HOME"}
	switch provider {
	case "claude":
		keys = []string{"CLAUDE_CONFIG_DIR", "HOME"}
	case "codex":
		keys = []string{"CODEX_HOME", "HOME"}
	case "grok":
		keys = []string{"GROK_HOME", "HOME"}
	}
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		if value := selectors[key]; value != "" {
			parts = append(parts, key+"="+value)
		}
	}
	if len(parts) == 0 {
		return "", false
	}
	digest := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(digest[:])[:8], true
}

type hubQuotaCacheKey struct {
	MachineID string
	Pool      string
	AccountFP string
	Window    string
}

type hubQuotaObservation struct {
	machineID   string
	status      string
	collectedAt *string
	rows        map[hubQuotaCacheKey]HubQuotaPool
	receivedAt  time.Time
}

type hubQuotaRecord struct {
	state      string
	latest     *hubQuotaObservation
	lastGood   *hubQuotaObservation
	observedAt *time.Time
}

type HubQuotaObservation struct {
	Status      string         `json:"status"`
	CollectedAt *string        `json:"collected_at"`
	Pools       []HubQuotaPool `json:"pools"`
	ReceivedAt  time.Time      `json:"received_at"`
}

type HubQuotaNode struct {
	MachineID  string               `json:"machine_id"`
	State      string               `json:"state"`
	Latest     *HubQuotaObservation `json:"latest"`
	LastGood   *HubQuotaObservation `json:"last_good"`
	ObservedAt *time.Time           `json:"observed_at"`
}

func newHubQuotaObservation(machineID string, snapshot *HubQuotaSnapshot, receivedAt time.Time) *hubQuotaObservation {
	observation := &hubQuotaObservation{machineID: machineID, status: snapshot.Status, collectedAt: cloneQuotaString(snapshot.CollectedAt), rows: make(map[hubQuotaCacheKey]HubQuotaPool, len(snapshot.Pools)), receivedAt: receivedAt}
	for _, row := range snapshot.Pools {
		key := hubQuotaCacheKey{MachineID: machineID, Pool: row.Pool, AccountFP: row.AccountFP, Window: row.Window}
		copy := row
		copy.UsedPct = cloneMemoryFloat(row.UsedPct)
		copy.ResetAt = cloneQuotaString(row.ResetAt)
		observation.rows[key] = copy
	}
	return observation
}

func (observation *hubQuotaObservation) snapshot() *HubQuotaSnapshot {
	if observation == nil {
		return nil
	}
	rows := make([]HubQuotaPool, 0, len(observation.rows))
	for _, row := range observation.rows {
		copy := row
		copy.UsedPct = cloneMemoryFloat(row.UsedPct)
		copy.ResetAt = cloneQuotaString(row.ResetAt)
		rows = append(rows, copy)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Pool != rows[j].Pool {
			return rows[i].Pool < rows[j].Pool
		}
		if rows[i].AccountFP != rows[j].AccountFP {
			return rows[i].AccountFP < rows[j].AccountFP
		}
		return rows[i].Window < rows[j].Window
	})
	return &HubQuotaSnapshot{Status: observation.status, CollectedAt: cloneQuotaString(observation.collectedAt), Pools: rows}
}

func cloneHubQuotaObservation(observation *hubQuotaObservation) *hubQuotaObservation {
	if observation == nil {
		return nil
	}
	return newHubQuotaObservation(observation.machineID, observation.snapshot(), observation.receivedAt)
}

func hubQuotaObservationView(observation *hubQuotaObservation) *HubQuotaObservation {
	if observation == nil {
		return nil
	}
	snapshot := observation.snapshot()
	return &HubQuotaObservation{Status: snapshot.Status, CollectedAt: snapshot.CollectedAt, Pools: snapshot.Pools, ReceivedAt: observation.receivedAt}
}

func (h *HubServer) recordHubQuotaLocked(machineID string, snapshot *HubQuotaSnapshot, receivedAt time.Time) {
	record := h.nodeQuota[machineID]
	if record == nil {
		record = &hubQuotaRecord{state: "unknown"}
		h.nodeQuota[machineID] = record
	}
	if snapshot == nil {
		if record.state != "legacy" || record.latest != nil {
			record.state, record.latest = "legacy", nil
			observedAt := receivedAt
			record.observedAt = &observedAt
		}
		return
	}
	current := record.latest
	if current != nil && equalHubQuotaSnapshot(current.snapshot(), snapshot) && record.state == snapshot.Status {
		return
	}
	observation := newHubQuotaObservation(machineID, snapshot, receivedAt)
	record.state, record.latest = snapshot.Status, observation
	observedAt := receivedAt
	record.observedAt = &observedAt
	if snapshot.Status == hubQuotaStatusOK {
		record.lastGood = cloneHubQuotaObservation(observation)
	}
}

func (h *HubServer) recordRejectedHubQuota(machineID string, agent *hubAgent, receivedAt time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if node := h.nodes[machineID]; node == nil || node.agent != agent {
		return
	}
	record := h.nodeQuota[machineID]
	if record == nil {
		record = &hubQuotaRecord{}
		h.nodeQuota[machineID] = record
	}
	record.state, record.latest = "unknown", nil
	observedAt := receivedAt
	record.observedAt = &observedAt
}

func hubInboundHeartbeatHasQuota(payload []byte) bool {
	var envelope map[string]json.RawMessage
	if json.Unmarshal(payload, &envelope) != nil {
		return false
	}
	var kind, messageType string
	if json.Unmarshal(envelope["type"], &messageType) != nil || messageType != "event" || json.Unmarshal(envelope["kind"], &kind) != nil || kind != "heartbeat" {
		return false
	}
	var heartbeat map[string]json.RawMessage
	if json.Unmarshal(envelope["payload"], &heartbeat) != nil {
		return false
	}
	_, exists := heartbeat["quota"]
	return exists
}

func (h *HubServer) handleQuotaList(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeOperator(r) {
		hubUnauthorized(w)
		return
	}
	h.mu.Lock()
	nodes := make([]HubQuotaNode, 0, len(h.tokens)-1)
	for machineID := range h.tokens {
		if machineID == hubOperatorMachineID {
			continue
		}
		view := HubQuotaNode{MachineID: machineID, State: "unknown"}
		if record := h.nodeQuota[machineID]; record != nil {
			view.State = record.state
			view.Latest = hubQuotaObservationView(record.latest)
			view.LastGood = hubQuotaObservationView(record.lastGood)
			view.ObservedAt = cloneQuotaTime(record.observedAt)
		}
		nodes = append(nodes, view)
	}
	h.mu.Unlock()
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].MachineID < nodes[j].MachineID })
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Nodes []HubQuotaNode `json:"nodes"`
	}{Nodes: nodes})
}

func cloneQuotaTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

package panewire

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"time"
)

type hubQuotaResult struct {
	Payload string `json:"payload,omitempty"`
	Error   string `json:"error,omitempty"`
}
type hubQuotaCacheEntry struct {
	result  hubQuotaResult
	expires time.Time
}
type hubQuotaReport struct {
	RequestID string
	Payload   string
	Error     string
}

func hubQuotaCacheTTL() time.Duration {
	if value, err := time.ParseDuration(os.Getenv("QUOTA_CACHE_TTL")); err == nil && value > 0 {
		return value
	}
	return 5 * time.Minute
}

func parseHubQuotaReport(raw []byte) (hubQuotaReport, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) < 3 || len(fields) > 3 {
		return hubQuotaReport{}, false
	}
	var report hubQuotaReport
	var kind string
	if json.Unmarshal(fields["type"], &kind) != nil || kind != "quota.report" || json.Unmarshal(fields["request_id"], &report.RequestID) != nil || !validHubRequestID(report.RequestID) {
		return hubQuotaReport{}, false
	}
	if value, found := fields["payload"]; found {
		if json.Unmarshal(value, &report.Payload) != nil || report.Payload == "" {
			return hubQuotaReport{}, false
		}
	}
	if value, found := fields["error"]; found {
		if json.Unmarshal(value, &report.Error) != nil || (report.Error != "unsupported" && report.Error != "scopefuel failed" && report.Error != "output_too_large" && report.Error != "timeout") {
			return hubQuotaReport{}, false
		}
	}
	return report, (report.Payload == "") != (report.Error == "")
}

func (h *HubServer) resolveQuota(machine string, report hubQuotaReport) {
	h.mu.Lock()
	waiter := h.quotaWaiters[report.RequestID]
	if waiter != nil {
		delete(h.quotaWaiters, report.RequestID)
	}
	h.mu.Unlock()
	if waiter != nil {
		select {
		case waiter <- hubQuotaResult{Payload: report.Payload, Error: report.Error}:
		default:
		}
	}
}

func hubQuotaRequestID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "quota-" + hex.EncodeToString(b)
}

func (h *HubServer) handleQuotaGet(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeOperator(r) {
		hubUnauthorized(w)
		return
	}
	machine := r.PathValue("machine")
	if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID {
		http.Error(w, "invalid machine", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	cache, found := h.quotaCache[machine]
	h.mu.Unlock()
	if !found || !h.now().Before(cache.expires) {
		http.Error(w, "quota unavailable", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Cached bool `json:"cached"`
		hubQuotaResult
	}{Cached: true, hubQuotaResult: cache.result})
}

func (h *HubServer) handleQuotaRequest(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeOperator(r) {
		hubUnauthorized(w)
		return
	}
	machine := r.PathValue("machine")
	if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID {
		http.Error(w, "invalid machine", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	if cache, found := h.quotaCache[machine]; found && h.now().Before(cache.expires) {
		h.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Cached bool `json:"cached"`
			hubQuotaResult
		}{Cached: true, hubQuotaResult: cache.result})
		return
	}
	record := h.nodes[machine]
	if record == nil || record.agent == nil || record.state != "connected" {
		h.mu.Unlock()
		http.Error(w, "node unavailable", http.StatusServiceUnavailable)
		return
	}
	id, waiter := hubQuotaRequestID(), make(chan hubQuotaResult, 1)
	h.quotaWaiters[id] = waiter
	agent := record.agent
	h.mu.Unlock()
	if err := agent.writeJSON(struct {
		Type      string `json:"type"`
		RequestID string `json:"request_id"`
		Tool      string `json:"tool"`
	}{"quota.request", id, "scopefuel"}); err != nil {
		h.mu.Lock()
		delete(h.quotaWaiters, id)
		h.mu.Unlock()
		http.Error(w, "node unavailable", http.StatusServiceUnavailable)
		return
	}
	select {
	case result := <-waiter:
		h.mu.Lock()
		h.quotaCache[machine] = hubQuotaCacheEntry{result: result, expires: h.now().Add(h.quotaCacheTTL)}
		h.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Cached bool `json:"cached"`
			hubQuotaResult
		}{Cached: false, hubQuotaResult: result})
	case <-time.After(20 * time.Second):
		h.mu.Lock()
		delete(h.quotaWaiters, id)
		h.mu.Unlock()
		http.Error(w, "quota timeout", http.StatusGatewayTimeout)
	}
}

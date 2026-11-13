package panewire

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

const (
	quotaOperatorToken = "quota-operator-token"
	quotaNodeAToken    = "quota-node-a-token"
	quotaNodeBToken    = "quota-node-b-token"
)

// The heartbeat quota field is retired but the name stays admitted so an
// older node is never rejected; the hub drops the value unread.
func TestHubQuotaLegacyHeartbeatFieldIsAcceptedAndDropped(t *testing.T) {
	for name, payload := range map[string]string{
		"valid snapshot": `{"status":"alive","checks":{"service":"ok"},"quota":{"status":"ok","collected_at":"2026-09-09T05:58:50Z","pools":[{"pool":"claude","account_fp":"a1b2c3d4","window":"5h","used_pct":24,"reset_at":"2026-09-09T10:29:59Z","source":"oauth-usage-api"}]}}`,
		"opaque value":   `{"status":"alive","checks":{"service":"ok"},"quota":1}`,
	} {
		if _, ok := decodeHubHeartbeatPayload([]byte(payload)); !ok {
			t.Fatalf("%s: heartbeat with a quota field was rejected", name)
		}
	}
}

// The node heartbeat carries no quota object at all: the wire never runs a
// quota tool, and an older hub reads the omission as a legacy node.
func TestHubQuotaHeartbeatOmitsQuotaField(t *testing.T) {
	client := &HubClient{
		jobsInboxRoot:       t.TempDir(),
		assignedJobs:        map[string]uint64{},
		completedJobs:       map[string]uint64{},
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, errors.New("fixture") },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, errors.New("fixture") },
	}
	event := client.heartbeatEvent(t.Context())
	if bytes.Contains(event.Payload, []byte(`"quota"`)) {
		t.Fatalf("heartbeat still carries a quota field: %s", event.Payload)
	}
	if _, ok := decodeHubHeartbeatPayload(event.Payload); !ok {
		t.Fatalf("own heartbeat no longer decodes: %s", event.Payload)
	}
}

// The retired v1 quota surface is gone: list, per-machine get, and request all
// return 404 for every credential class.
func TestHubQuotaRetiredEndpointsAreGone(t *testing.T) {
	tokens := map[string]string{hubOperatorMachineID: quotaOperatorToken, "node-a": quotaNodeAToken}
	hub, err := NewHubServer(HubServerConfig{Tokens: tokens, Logger: slog.Default()})
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []struct{ method, path string }{
		{http.MethodGet, "/v1/quota"},
		{http.MethodGet, "/v1/quota/node-a"},
		{http.MethodPost, "/v1/quota/node-a"},
	} {
		for _, token := range []string{"", quotaNodeAToken, quotaOperatorToken} {
			r := httptest.NewRequest(request.method, request.path, nil)
			if token != "" {
				r.Header.Set(hubAuthorizationHeader, "Bearer "+token)
			}
			w := httptest.NewRecorder()
			hub.Handler().ServeHTTP(w, r)
			if w.Code != http.StatusNotFound {
				t.Fatalf("%s %s token=%q status=%d want 404", request.method, request.path, token, w.Code)
			}
		}
	}
}

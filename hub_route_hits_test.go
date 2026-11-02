package panewire

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHubRouteHitsLogsPatternsHourly(t *testing.T) {
	var logs bytes.Buffer
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{hubOperatorMachineID: r6OperatorToken}, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	handler := hub.Handler()
	for _, target := range []string{"/healthz", "/healthz", "/v1/nodes/secret-looking-machine/accepting?token=x", "/no/such/route"} {
		method := http.MethodGet
		if strings.Contains(target, "accepting") {
			method = http.MethodPost
		}
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, target, nil))
	}
	start := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	hub.logRouteHits(start) // starts the clock
	hub.logRouteHits(start.Add(59 * time.Minute))
	if strings.Contains(logs.String(), "hub route hits") {
		t.Fatalf("logged before the interval: %s", logs.String())
	}
	hub.logRouteHits(start.Add(time.Hour))
	output := logs.String()
	if !strings.Contains(output, `route="GET /healthz" count=2`) {
		t.Fatalf("healthz count missing: %s", output)
	}
	if !strings.Contains(output, `route="POST /v1/nodes/{machine}/accepting" count=1`) {
		t.Fatalf("pattern missing: %s", output)
	}
	for _, leaked := range []string{"secret-looking-machine", "token=x", "/no/such/route"} {
		if strings.Contains(output, leaked) {
			t.Fatalf("logged %q: %s", leaked, output)
		}
	}
	logs.Reset()
	hub.logRouteHits(start.Add(2 * time.Hour))
	if strings.Contains(logs.String(), "hub route hits") {
		t.Fatalf("the counts were not reset: %s", logs.String())
	}
}

// A websocket's handler runs while it stays connected, so a request counts on
// arrival. Counting on return would hide an open subscriber.
func TestHubRouteHitsCountOnArrival(t *testing.T) {
	hub := &HubServer{}
	mux := http.NewServeMux()
	var during uint64
	mux.HandleFunc("GET /v1/events", func(http.ResponseWriter, *http.Request) {
		hub.routeHits.mu.Lock()
		during = hub.routeHits.counts["GET /v1/events"]
		hub.routeHits.mu.Unlock()
	})
	hub.countRoutes(mux).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/events", nil))
	if during != 1 {
		t.Fatalf("while the handler ran the route had %d hits, want 1", during)
	}
}

// The hourly line comes from the hub's maintenance sweep.
func TestHubRouteHitsLoggedBySweep(t *testing.T) {
	var logs bytes.Buffer
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{hubOperatorMachineID: r6OperatorToken}, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	hub.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/healthz", nil))
	now := time.Now().UTC()
	hub.now = func() time.Time { return now }
	hub.Sweep()
	now = now.Add(time.Hour)
	hub.Sweep()
	if !strings.Contains(logs.String(), `route="GET /healthz" count=1`) {
		t.Fatalf("the sweep did not log route hits: %s", logs.String())
	}
}

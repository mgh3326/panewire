package panewire

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/hubretry"
)

// retryNodeClient is a daemon's hub client aimed at a fake hub. The endpoint
// is written as the daemon stores it (ws://), and send() maps it to http://.
func retryNodeClient(server *httptest.Server, retry hubretry.Policy) nodeHubClient {
	hub := &HubClient{endpoints: []string{"ws" + strings.TrimPrefix(server.URL, "http")}, machineID: "host-a", token: "node"}
	return nodeHubClient{ctx: context.Background(), client: hub, retry: retry}
}

// The repeatable calls are sent again through 503s and a dropped connection,
// and the answer is the one the hub gives once it recovers. Each send carries
// the same body. Acceptance: a fake hub that fails for a while and then
// recovers, with each listed call succeeding.
func TestNodeHubClientResendsRepeatableCalls(t *testing.T) {
	for _, tc := range []struct {
		name, method, path string
		query              url.Values
		body               string
	}{
		{"get", http.MethodGet, "/v1/inbox/count", url.Values{"lane": {"lane-a"}}, ""},
		{"put lane", http.MethodPut, "/v1/lanes/lane-a", nil, `{"lane":"lane-a"}`},
		{"ack", http.MethodPost, "/v1/inbox/ack", nil, `{"id":7,"all":false,"lane":"lane-a"}`},
		{"read", http.MethodPost, "/v1/inbox/7/read", nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			var mu sync.Mutex
			var bodies []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				mu.Lock()
				bodies = append(bodies, string(raw))
				mu.Unlock()
				switch n := calls.Add(1); {
				case n == 1:
					panic(http.ErrAbortHandler)
				case n <= 3:
					w.WriteHeader(http.StatusServiceUnavailable)
				default:
					_, _ = w.Write([]byte(`{"ok":true}`))
				}
			}))
			defer server.Close()
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			client := retryNodeClient(server, hubretry.Policy{Budget: 5 * time.Second, Initial: time.Millisecond, Max: time.Millisecond})
			response, err := client.do(context.Background(), tc.method, tc.path, tc.query, body)
			if err != nil {
				t.Fatalf("err=%v after %d sends", err, calls.Load())
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("status=%d", response.StatusCode)
			}
			if got := calls.Load(); got != 4 {
				t.Fatalf("hub saw %d sends, want 4", got)
			}
			mu.Lock()
			defer mu.Unlock()
			for i, got := range bodies {
				if got != tc.body {
					t.Fatalf("send %d carried %q, want %q", i+1, got, tc.body)
				}
			}
		})
	}
}

// A call that is not repeatable is sent once: the 503 it gets back is the
// answer, so a job close, reap or node command never acts twice.
func TestNodeHubClientSendsNonIdempotentCallsOnce(t *testing.T) {
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/v1/jobs/close"},
		{http.MethodPost, "/v1/jobs/reap"},
		{http.MethodPost, "/v1/nodes/host-b/command"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer server.Close()
			client := retryNodeClient(server, hubretry.Policy{Budget: 5 * time.Second, Initial: time.Millisecond, Max: time.Millisecond})
			response, err := client.do(context.Background(), tc.method, tc.path, nil, strings.NewReader(`{}`))
			if err != nil {
				t.Fatalf("err=%v", err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusServiceUnavailable || calls.Load() != 1 {
				t.Fatalf("status=%d sends=%d", response.StatusCode, calls.Load())
			}
		})
	}
}

// A repeatable call whose hub never recovers gives up at its budget with the
// last answer, so a stuck hub holds a daemon request for a bounded time.
func TestNodeHubClientRepeatableCallGivesUpAtItsBudget(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client := retryNodeClient(server, hubretry.Policy{Budget: 30 * time.Millisecond, Initial: time.Millisecond, Max: time.Millisecond})
	response, err := client.do(context.Background(), http.MethodGet, "/v1/inbox/count", nil, nil)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || calls.Load() < 2 {
		t.Fatalf("status=%d sends=%d", response.StatusCode, calls.Load())
	}
}

// A daemon's inbox path turns its node-level retry off, because the inbox
// command retries and reports the count once (inbox_daemon.go). Off means one
// send even for a repeatable call.
func TestNodeHubClientDisabledRetrySendsOnce(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client := retryNodeClient(server, hubretry.Policy{Budget: -1})
	response, err := client.do(context.Background(), http.MethodGet, "/v1/inbox/count", nil, nil)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	defer response.Body.Close()
	if calls.Load() != 1 {
		t.Fatalf("sends=%d", calls.Load())
	}
}

func TestNodeHubIdempotentClassifiesCalls(t *testing.T) {
	for _, tc := range []struct {
		method, path string
		want         bool
	}{
		{http.MethodGet, "/v1/inbox/wait", true},
		{http.MethodGet, "/v1/jobs/mine", true},
		{http.MethodPut, "/v1/lanes/lane-a", true},
		{http.MethodDelete, "/v1/lanes/lane-a", true},
		{http.MethodPost, "/v1/inbox/ack", true},
		{http.MethodPost, "/v1/inbox/42/read", true},
		{http.MethodPost, "/v1/inbox/0/read", false},
		{http.MethodPost, "/v1/inbox/read", false},
		{http.MethodPost, "/v1/inbox/x/read", false},
		{http.MethodPost, "/v1/jobs/close", false},
		{http.MethodPost, "/v1/jobs/reap", false},
		{http.MethodPost, "/v1/nodes/host-b/command", false},
	} {
		if got := nodeHubIdempotent(tc.method, tc.path); got != tc.want {
			t.Errorf("nodeHubIdempotent(%s %s)=%v want %v", tc.method, tc.path, got, tc.want)
		}
	}
}

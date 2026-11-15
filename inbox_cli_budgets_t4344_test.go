package panewire

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/hubretry"
)

// slowInboxHub answers an inbox count only after latency and only for token,
// and gives up when the caller's context ends so a timed-out request does not
// hold the server.
func slowInboxHub(t *testing.T, latency time.Duration, token string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		select {
		case <-time.After(latency):
		case <-r.Context().Done():
			return
		}
		if r.URL.Path != "/v1/inbox/count" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = io.WriteString(w, "{\"lane-a\":1}\n")
	}))
}

// A hub answer that takes longer than the 5s budget the node path used to have
// still reaches the CLI through the local daemon: the daemon attempts the call
// under inboxNodeRequestTimeout, and the CLI holds the socket longer than that
// budget so the daemon's answer arrives instead of "daemon unavailable"
// (hk 4344, hk 3730).
func TestInboxNodePathSlowHubSucceeds(t *testing.T) {
	for _, key := range []string{"PANEWIRE_HUB_URL", "PANEWIRE_HUB_TOKEN_ENV", "PANEWIRE_HUB_CF_ENV", "PANEWIRE_LANE"} {
		t.Setenv(key, "")
	}
	const latency = 7 * time.Second
	const token = "synthetic-token-4344"
	server := slowInboxHub(t, latency, token)
	defer server.Close()
	client, err := NewHubClient(HubClientConfig{URL: strings.Replace(server.URL, "http://", "ws://", 1), MachineID: "host-a", Token: token, AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	daemon := NewDaemon(Config{Hub: HubDaemonConfig{Client: client}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	socket := inboxDaemonSocket(t, daemon)
	var stdout, stderr bytes.Buffer
	start := time.Now()
	code := runInboxCLI([]string{"count", "--lane", "lane-a"}, &stdout, &stderr, hubCLIDeps{AllowInsecureForTests: true, SocketPath: socket})
	elapsed := time.Since(start)
	if code != ExitOK || strings.TrimSpace(stdout.String()) != "inbox: 1 unread" || stderr.Len() != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if elapsed < latency || elapsed > latency+inboxDaemonReplySlack {
		t.Fatalf("elapsed=%s, want the %s hub answer inside the daemon socket hold", elapsed, latency)
	}
}

// A direct --hub-url call attempts the same 7s hub answer under its own,
// shorter budget and reports the hub unavailable; the failure comes from that
// budget, not from the hub answering late (hk 4344).
func TestInboxDirectSlowHubTimesOut(t *testing.T) {
	for _, key := range []string{"PANEWIRE_HUB_URL", "PANEWIRE_HUB_TOKEN_ENV", "PANEWIRE_HUB_CF_ENV", "PANEWIRE_LANE"} {
		t.Setenv(key, "")
	}
	const latency = 7 * time.Second
	server := slowInboxHub(t, latency, "node")
	defer server.Close()
	var stdout, stderr bytes.Buffer
	start := time.Now()
	code := runInboxCLI([]string{"count", "--lane", "lane-a", "--hub-url", server.URL, "--hub-token-env", retryInboxTokenEnv(t)}, &stdout, &stderr, hubCLIDeps{
		HTTPClient:            server.Client(),
		AllowInsecureForTests: true,
		HubRetry:              hubretry.Policy{Budget: -1},
	})
	elapsed := time.Since(start)
	if code != ExitInternal || !strings.Contains(stderr.String(), "inbox: hub unavailable") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	// ExitInternal with empty stdout already proves the hub never answered; the
	// lower bound proves the attempt budget was the 5s one, not a shorter one.
	if elapsed < inboxDirectRequestTimeout {
		t.Fatalf("elapsed=%s, want the %s attempt budget to end it", elapsed, inboxDirectRequestTimeout)
	}
}

// The direct client's own timeout stays above the attempt context for both a
// short call and the long poll: the context ends a slow request, not the
// client (hk 4344).
func TestInboxHTTPClientTimeoutExceedsRequestBudget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	deps := hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}
	for _, tc := range []struct {
		command  string
		timeout  time.Duration
		attempt  time.Duration
		describe string
	}{
		{"count", inboxWaitDefaultTimeout, inboxDirectRequestTimeout, "short call"},
		{"wait", time.Second, hubInboxWaitMaxWait + 10*time.Second, "long poll"},
	} {
		client, err := newInboxDirectClient(server.URL, hubTokenEnv{Token: "synthetic-token-4344"}, hubCFAccessEnv{}, deps, tc.command, tc.timeout)
		if err != nil {
			t.Fatal(err)
		}
		if client.client.Timeout <= tc.attempt {
			t.Fatalf("%s: client timeout %s <= attempt budget %s", tc.describe, client.client.Timeout, tc.attempt)
		}
	}
}

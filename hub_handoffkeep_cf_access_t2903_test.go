package panewire

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// cfAccessHandoffkeep answers 403 unless the request carries the bearer and
// the CF Access pair, like a handoffkeep behind Cloudflare Access.
func cfAccessHandoffkeep(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer hk-token" || r.Header.Get("CF-Access-Client-Id") != "cf-id" || r.Header.Get("CF-Access-Client-Secret") != "cf-secret" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte(`{"body":"brief text"}`))
	}))
	t.Cleanup(server.Close)
	return server
}

func writeHandoffkeepEnv(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "handoffkeep.env")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// A node whose handoffkeep sits behind CF Access fetches the brief only when
// the CF Access pair from its env file is sent with the bearer.
func TestBriefFetchSendsCFAccessPair(t *testing.T) {
	server := cfAccessHandoffkeep(t)
	hubSpawnBriefTransport = server.Client().Transport
	t.Cleanup(func() { hubSpawnBriefTransport = nil })
	base := []string{"HANDOFFKEEP_URL=" + server.URL, "HANDOFFKEEP_TOKEN=hk-token"}

	withPair := writeHandoffkeepEnv(t, append(base, "HANDOFFKEEP_CF_ACCESS_CLIENT_ID=cf-id", "HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET=cf-secret")...)
	body, err := (&HubClient{}).fetchHubSpawnBrief(context.Background(), withPair, "briefs/job.md")
	if err != nil || body != "brief text" {
		t.Fatalf("with the CF Access pair: body=%q err=%v", body, err)
	}

	bearerOnly := writeHandoffkeepEnv(t, base...)
	if _, err := (&HubClient{}).fetchHubSpawnBrief(context.Background(), bearerOnly, "briefs/job.md"); err == nil {
		t.Fatal("bearer only must stay refused by a CF Access handoffkeep")
	}
}

func TestHandoffkeepEnvCFAccessPairIsBothOrNeither(t *testing.T) {
	for _, tc := range []struct {
		name  string
		extra []string
		ok    bool
	}{
		{"neither", nil, true},
		{"both", []string{"HANDOFFKEEP_CF_ACCESS_CLIENT_ID=cf-id", "HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET=cf-secret"}, true},
		{"id only", []string{"HANDOFFKEEP_CF_ACCESS_CLIENT_ID=cf-id"}, false},
		{"secret only", []string{"HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET=cf-secret"}, false},
		{"pair over plain http", []string{"HANDOFFKEEP_URL=http://127.0.0.1:9", "HANDOFFKEEP_CF_ACCESS_CLIENT_ID=cf-id", "HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET=cf-secret"}, false},
		{"plain http without a pair", []string{"HANDOFFKEEP_URL=http://127.0.0.1:9"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeHandoffkeepEnv(t, append([]string{"HANDOFFKEEP_URL=https://hk.example", "HANDOFFKEEP_TOKEN=hk-token"}, tc.extra...)...)
			_, err := loadHubHandoffkeepEnv(path)
			if (err == nil) != tc.ok {
				t.Fatalf("err=%v, want ok=%v", err, tc.ok)
			}
			if err != nil && strings.Contains(err.Error(), "cf-") {
				t.Fatalf("error leaks a credential value: %v", err)
			}
		})
	}
}

// The hub's relay and chat clients use the same env and must send the pair too.
func TestHubHandoffkeepClientsSendCFAccessPair(t *testing.T) {
	server := cfAccessHandoffkeep(t)
	env := hubHandoffkeepEnv{URL: server.URL, Token: "hk-token", CFID: "cf-id", CFSecret: "cf-secret"}

	relay, err := newHandoffkeepRelayClient(env, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if status, _, err := relay.do(context.Background(), http.MethodGet, relay.endpoint("/v1/documents/x"), nil); err != nil || status != http.StatusOK {
		t.Fatalf("relay client: status=%d err=%v", status, err)
	}

}

// A redirect never carries the CF Access secret to another host: the clients
// with a CF pair do not follow redirects (the node brief fetch never did).
func TestHandoffkeepClientsDoNotForwardCFSecretOnRedirect(t *testing.T) {
	leaked := false
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("CF-Access-Client-Secret") != "" {
			leaked = true
		}
	}))
	t.Cleanup(other.Close)
	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/elsewhere", http.StatusFound)
	}))
	t.Cleanup(redirecting.Close)
	env := hubHandoffkeepEnv{URL: redirecting.URL, Token: "hk-token", CFID: "cf-id", CFSecret: "cf-secret"}
	relay, err := newHandoffkeepRelayClient(env, redirecting.Client())
	if err != nil {
		t.Fatal(err)
	}
	if status, _, _ := relay.do(context.Background(), http.MethodGet, relay.endpoint("/v1/documents/x"), nil); status != http.StatusFound {
		t.Fatalf("relay client followed the redirect: status=%d", status)
	}
	if leaked {
		t.Fatal("the CF Access secret reached the redirect target")
	}
}

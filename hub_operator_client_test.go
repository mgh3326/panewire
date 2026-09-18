package panewire

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Synthetic fixture credentials only. The "-t441-" markers are deliberately
// unique so any appearance in output, errors, or another server's header set
// is a countable leak assertion, never a false positive from real config.
const (
	t441OperatorToken  = "t441-synthetic-operator-token-3f9a2b71"
	t441CFClientID     = "t441-synthetic-cf-client-id-55c0a1"
	t441CFClientSecret = "t441-synthetic-cf-client-secret-9e04fd"
)

func t441Envs(t *testing.T) (tokenEnv, cfEnv string) {
	t.Helper()
	dir := t.TempDir()
	tokenEnv = filepath.Join(dir, "operator.env")
	if err := os.WriteFile(tokenEnv, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN="+t441OperatorToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfEnv = filepath.Join(dir, "cf.env")
	if err := os.WriteFile(cfEnv, []byte("CF_ACCESS_CLIENT_ID="+t441CFClientID+"\nCF_ACCESS_CLIENT_SECRET="+t441CFClientSecret+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return tokenEnv, cfEnv
}

func t441Deps(server *httptest.Server) hubCLIDeps {
	return hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}
}

func t441AssertNoCredentialLeak(t *testing.T, stdout, stderr, extra string) {
	t.Helper()
	for _, marker := range []string{t441OperatorToken, t441CFClientID, t441CFClientSecret} {
		if strings.Contains(stdout+stderr+extra, marker) {
			t.Fatalf("credential %q leaked into output", marker)
		}
	}
}

type t441RecordedRequest struct {
	method  string
	path    string
	query   string
	headers http.Header
}

// t441HubFixture serves one canned success answer per operator API path and
// records the request for assertions. Every response is deliberately
// credential-free.
func t441HubFixture(t *testing.T, recorded chan<- t441RecordedRequest) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get(hubAuthorizationHeader) != "Bearer "+t441OperatorToken {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		if request.Header.Get("CF-Access-Client-Id") != t441CFClientID || request.Header.Get("CF-Access-Client-Secret") != t441CFClientSecret {
			writer.WriteHeader(http.StatusForbidden)
			return
		}
		if recorded != nil {
			recorded <- t441RecordedRequest{method: request.Method, path: request.URL.Path, query: request.URL.RawQuery, headers: request.Header.Clone()}
		}
		writer.Header().Set("Content-Type", "application/json")
		key := request.Method + " " + request.URL.Path
		switch key {
		case "GET /v1/nodes":
			_, _ = writer.Write([]byte(`{"nodes":[]}`))
		case "GET /v1/jobs":
			_, _ = writer.Write([]byte(`{"jobs":[]}`))
		case "GET /v1/jobs/orphaned":
			_, _ = writer.Write([]byte(`{"jobs":[]}`))
		case "POST /v1/jobs/reassign":
			_, _ = writer.Write([]byte(`{"job_id":"job-a","from":"node-a","to":"node-b","epoch":2}`))
		case "GET /v1/lanes":
			_, _ = writer.Write([]byte(`{"lanes":[{"lane":"lane-a","machine":"machine-a","pane":"w1:p1","parent":"","sink":false}],"control_epoch":7}`))
		case "PUT /v1/lanes/lane-a":
			_, _ = writer.Write([]byte(`{"lane":"lane-a","machine":"machine-a","pane":"w1:p1","parent":"","sink":false}`))
		case "DELETE /v1/lanes/lane-a":
			_, _ = writer.Write([]byte(`{"lane":"lane-a","removed":true}`))
		case "GET /v1/placement":
			_, _ = writer.Write([]byte(`{"decision":"machine-a","source":"hub-only","asof":"2026-09-19T00:00:00Z","candidates":[{"machine":"machine-a","score":1,"reason":"ok"}]}`))
		case "GET /v1/burst":
			_, _ = writer.Write([]byte(`{"policy":"fixture"}`))
		case "POST /v1/burst/request":
			_, _ = writer.Write([]byte(`{"id":"hold-x","status":"active"}`))
		case "POST /v1/burst/release":
			_, _ = writer.Write([]byte(`{"released":true}`))
		case "GET /v1/burst/holds":
			_, _ = writer.Write([]byte(`{"holds":[]}`))
		case "POST /v1/update":
			writer.WriteHeader(http.StatusAccepted)
			_, _ = writer.Write([]byte(`{"accepted":true}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
}

// TestHubOperatorSuccessMatrix proves every in-scope command/method crosses
// the boundary to the injected allowed origin with the operator bearer and
// the optional Cloudflare Access headers attached.
func TestHubOperatorSuccessMatrix(t *testing.T) {
	cases := []struct {
		name       string
		wantMethod string
		wantPath   string
		wantQuery  string
		wantCode   int
		run        func(t *testing.T, hubURL, tokenEnv, cfEnv string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int
	}{
		{name: "hub-status", wantMethod: http.MethodGet, wantPath: "/v1/nodes", wantCode: ExitOK,
			run: func(t *testing.T, hubURL, tokenEnv, cfEnv string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
				return runHubStatusCLI([]string{"--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, stdout, stderr, deps)
			}},
		{name: "jobs list", wantMethod: http.MethodGet, wantPath: "/v1/jobs", wantQuery: "machine=machine-a", wantCode: ExitOK,
			run: func(t *testing.T, hubURL, tokenEnv, cfEnv string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
				return runJobsCLI([]string{"jobs", "--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv, "--machine", "machine-a"}, stdout, stderr, deps)
			}},
		{name: "jobs orphaned", wantMethod: http.MethodGet, wantPath: "/v1/jobs/orphaned", wantCode: ExitOK,
			run: func(t *testing.T, hubURL, tokenEnv, cfEnv string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
				return runJobsCLI([]string{"orphaned", "--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, stdout, stderr, deps)
			}},
		{name: "jobs reassign", wantMethod: http.MethodPost, wantPath: "/v1/jobs/reassign", wantCode: ExitOK,
			run: func(t *testing.T, hubURL, tokenEnv, cfEnv string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
				return runJobsCLI([]string{"reassign", "--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv, "--job-id", "job-a", "--to", "node-b"}, stdout, stderr, deps)
			}},
		{name: "lanes ls", wantMethod: http.MethodGet, wantPath: "/v1/lanes", wantCode: ExitOK,
			run: func(t *testing.T, hubURL, tokenEnv, cfEnv string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
				return runLanesCLI([]string{"ls", "--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, stdout, stderr, deps)
			}},
		{name: "lanes add", wantMethod: http.MethodPut, wantPath: "/v1/lanes/lane-a", wantCode: ExitOK,
			run: func(t *testing.T, hubURL, tokenEnv, cfEnv string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
				return runLanesCLI([]string{"add", "lane-a", "--machine", "machine-a", "--pane", "w1:p1", "--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, stdout, stderr, deps)
			}},
		{name: "lanes rm", wantMethod: http.MethodDelete, wantPath: "/v1/lanes/lane-a", wantCode: ExitOK,
			run: func(t *testing.T, hubURL, tokenEnv, cfEnv string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
				return runLanesCLI([]string{"rm", "lane-a", "--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, stdout, stderr, deps)
			}},
		{name: "lanes self-check", wantMethod: http.MethodGet, wantPath: "/v1/lanes", wantCode: ExitOK,
			run: func(t *testing.T, hubURL, tokenEnv, cfEnv string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
				return runLanesCLI([]string{"self-check", "--lane", "lane-a", "--expect-machine", "machine-a", "--expect-pane", "w1:p1", "--expect-epoch", "7", "--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, stdout, stderr, deps)
			}},
		{name: "place", wantMethod: http.MethodGet, wantPath: "/v1/placement", wantCode: ExitOK,
			run: func(t *testing.T, hubURL, tokenEnv, cfEnv string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
				return runPlaceCLI([]string{"--class", "worker", "--cwd", "repo", "--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, stdout, stderr, deps)
			}},
		{name: "burst request", wantMethod: http.MethodPost, wantPath: "/v1/burst/request", wantCode: ExitOK,
			run: func(t *testing.T, hubURL, tokenEnv, cfEnv string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
				return runBurstCLIWithDeps([]string{"request", "--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv, "--target", "machine-a", "--hold", "1m", "--reason", "t", "--timeout", "5s"}, stdout, stderr, deps)
			}},
		{name: "burst release", wantMethod: http.MethodPost, wantPath: "/v1/burst/release", wantCode: ExitOK,
			run: func(t *testing.T, hubURL, tokenEnv, cfEnv string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
				return runBurstCLIWithDeps([]string{"release", "--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv, "--lease-id", "hold-x", "--timeout", "5s"}, stdout, stderr, deps)
			}},
		{name: "burst holds", wantMethod: http.MethodGet, wantPath: "/v1/burst/holds", wantCode: ExitOK,
			run: func(t *testing.T, hubURL, tokenEnv, cfEnv string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
				return runBurstCLIWithDeps([]string{"holds", "--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, stdout, stderr, deps)
			}},
		{name: "burst show live", wantMethod: http.MethodGet, wantPath: "/v1/burst", wantCode: ExitOK,
			run: func(t *testing.T, hubURL, tokenEnv, cfEnv string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
				return runBurstCLIWithDeps([]string{"show", "--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, stdout, stderr, deps)
			}},
		{name: "update publish", wantMethod: http.MethodPost, wantPath: "/v1/update", wantCode: ExitOK,
			run: func(t *testing.T, hubURL, tokenEnv, cfEnv string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
				return runUpdateCLI([]string{"publish", "--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv, "--version", "v1.2.3", "--sha256", strings.Repeat("ab", 32), "--url", "https://github.com/mgh3326/panewire/releases/download/v1.2.3/panewire", "--machines", "machine-a"}, stdout, stderr, deps)
			}},
	}
	for _, fixture := range cases {
		t.Run(fixture.name, func(t *testing.T) {
			tokenEnv, cfEnv := t441Envs(t)
			recorded := make(chan t441RecordedRequest, 4)
			server := t441HubFixture(t, recorded)
			defer server.Close()
			var stdout, stderr bytes.Buffer
			code := fixture.run(t, server.URL, tokenEnv, cfEnv, t441Deps(server), &stdout, &stderr)
			if code != fixture.wantCode {
				t.Fatalf("code=%d want=%d stdout=%q stderr=%q", code, fixture.wantCode, stdout.String(), stderr.String())
			}
			select {
			case got := <-recorded:
				if got.method != fixture.wantMethod || got.path != fixture.wantPath {
					t.Fatalf("request=%s %s want=%s %s", got.method, got.path, fixture.wantMethod, fixture.wantPath)
				}
				if fixture.wantQuery != "" && got.query != fixture.wantQuery {
					t.Fatalf("query=%q want=%q", got.query, fixture.wantQuery)
				}
				if got.headers.Get(hubAuthorizationHeader) != "Bearer "+t441OperatorToken ||
					got.headers.Get("CF-Access-Client-Id") != t441CFClientID ||
					got.headers.Get("CF-Access-Client-Secret") != t441CFClientSecret {
					t.Fatal("credential headers missing on approved request")
				}
			default:
				t.Fatal("no request reached the approved origin")
			}
			t441AssertNoCredentialLeak(t, stdout.String(), stderr.String(), "")
		})
	}
}

// TestHubOperatorRejectsEveryRedirect proves 301/302/303/307/308 across GET,
// PUT, DELETE, and POST never produce a second request anywhere — the
// redirect target receives zero requests and zero credential headers. The
// same-origin case is rejected identically.
func TestHubOperatorRejectsEveryRedirect(t *testing.T) {
	tokenEnv, cfEnv := t441Envs(t)
	commands := []struct {
		name string
		path string
		run  func(t *testing.T, hubURL string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int
	}{
		{name: "GET", path: "/v1/nodes", run: func(t *testing.T, hubURL string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
			return runHubStatusCLI([]string{"--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, stdout, stderr, deps)
		}},
		{name: "PUT", path: "/v1/lanes/lane-a", run: func(t *testing.T, hubURL string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
			return runLanesCLI([]string{"add", "lane-a", "--machine", "machine-a", "--pane", "w1:p1", "--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, stdout, stderr, deps)
		}},
		{name: "DELETE", path: "/v1/lanes/lane-a", run: func(t *testing.T, hubURL string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
			return runLanesCLI([]string{"rm", "lane-a", "--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, stdout, stderr, deps)
		}},
		{name: "POST", path: "/v1/update", run: func(t *testing.T, hubURL string, deps hubCLIDeps, stdout, stderr *bytes.Buffer) int {
			return runUpdateCLI([]string{"publish", "--hub-url", hubURL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv, "--version", "v1.2.3", "--sha256", strings.Repeat("ab", 32), "--url", "https://github.com/mgh3326/panewire/releases/download/v1.2.3/panewire", "--machines", "machine-a"}, stdout, stderr, deps)
		}},
	}
	for _, status := range []int{http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		for _, command := range commands {
			t.Run(command.name+"/"+http.StatusText(status), func(t *testing.T) {
				var targetHits int64
				var targetHeaders []http.Header
				target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					atomic.AddInt64(&targetHits, 1)
					targetHeaders = append(targetHeaders, request.Header.Clone())
					writer.WriteHeader(http.StatusOK)
				}))
				defer target.Close()
				redirector := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					http.Redirect(writer, request, target.URL+request.URL.Path, status)
				}))
				defer redirector.Close()
				var stdout, stderr bytes.Buffer
				code := command.run(t, redirector.URL, t441Deps(redirector), &stdout, &stderr)
				if code == ExitOK {
					t.Fatalf("redirected request succeeded: code=%d stdout=%q", code, stdout.String())
				}
				if got := atomic.LoadInt64(&targetHits); got != 0 {
					t.Fatalf("redirect target received %d requests", got)
				}
				for _, headers := range targetHeaders {
					if headers.Get(hubAuthorizationHeader) != "" || headers.Get("CF-Access-Client-Id") != "" || headers.Get("CF-Access-Client-Secret") != "" {
						t.Fatal("credential header reached redirect target")
					}
				}
				t441AssertNoCredentialLeak(t, stdout.String(), stderr.String(), "")
			})
		}
	}
	t.Run("same-origin", func(t *testing.T) {
		var alternateHits int64
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.URL.Path == "/v1/jobs" {
				atomic.AddInt64(&alternateHits, 1)
				_, _ = writer.Write([]byte(`{"jobs":[]}`))
				return
			}
			http.Redirect(writer, request, "/v1/jobs", http.StatusFound)
		}))
		defer server.Close()
		var stdout, stderr bytes.Buffer
		code := runHubStatusCLI([]string{"--hub-url", server.URL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, &stdout, &stderr, t441Deps(server))
		if code == ExitOK || atomic.LoadInt64(&alternateHits) != 0 {
			t.Fatalf("same-origin redirect followed: code=%d hits=%d", code, alternateHits)
		}
		t441AssertNoCredentialLeak(t, stdout.String(), stderr.String(), "")
	})
}

// TestHubOperatorClientIsDedicated turns a default-client or follow-redirect
// mutant RED by assertion: the boundary must own a client that is not
// http.DefaultClient, has a finite timeout, rejects every redirect, and wraps
// its base transport in the stamped-URL guard.
func TestHubOperatorClientIsDedicated(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	client, err := newHubOperatorClient(server.URL, t441OperatorToken, hubCFAccessEnv{}, t441Deps(server), 0)
	if err != nil {
		t.Fatal(err)
	}
	if client.client == nil || client.client == http.DefaultClient {
		t.Fatal("operator boundary fell back to http.DefaultClient")
	}
	if client.client.Timeout <= 0 {
		t.Fatal("operator client has no finite timeout")
	}
	if client.client.CheckRedirect == nil {
		t.Fatal("operator client follows redirects")
	}
	if err := client.client.CheckRedirect(&http.Request{}, nil); err == nil {
		t.Fatal("operator client permits a redirect")
	}
	if _, guarded := client.client.Transport.(hubGuardedTransport); !guarded {
		t.Fatal("operator client transport is not the stamped-URL guard")
	}
}

// TestHubOperatorCLISourceNeverUsesDefaultClient is the assertion-level
// mutant tripwire: a direct http.DefaultClient reintroduction in any
// inventoried credentialled file fails the build instead of compiling in.
func TestHubOperatorCLISourceNeverUsesDefaultClient(t *testing.T) {
	for _, file := range []string{
		"hub_operator_client.go", "hub_cli.go", "lanes_cli.go",
		"placement_cli.go", "burst.go", "hub_r19_cli.go",
	} {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, banned := range []string{"http.DefaultClient", "http.Get(", "http.Post(", "http.Head(", "http.Do("} {
			if strings.Contains(string(data), banned) {
				t.Fatalf("%s reintroduces %q outside the credentialled boundary", file, banned)
			}
		}
	}
}

// TestHubOperatorOriginAdversaries covers the initial-parse URL adversaries:
// userinfo, query, fragment, scheme, host, port, trailing dot, case, IDNA,
// encoded/backslash authority tricks, and path-base smuggling.
func TestHubOperatorOriginAdversaries(t *testing.T) {
	rejected := []string{
		"http://hub.robinco.dev",
		"https://hub.robinco.dev:8443",
		"https://hub.robinco.dev:0443",
		"https://user@hub.robinco.dev",
		"https://user:pass@hub.robinco.dev",
		"https://hub.robinco.dev@evil.invalid",
		"https://hub.robinco.dev/?x=1",
		"https://hub.robinco.dev?x=1",
		"https://hub.robinco.dev/#fragment",
		"https://hub.robinco.dev./",
		"https://HUB.ROBINCO.DEV",
		"https://hub.ROBINCO.dev",
		"https://hüb.robinco.dev",
		"https://xn--hb-7la.robinco.dev",
		"https://hub.robinco.dev.evil.invalid",
		"https://evil.invalid",
		"https://hub.robinco.dev;params",
		"https://hub.robinco.dev/api",
		"https://hub.robinco.dev//",
		"https://hub.robinco.dev/%2e%2e/",
		"hub.robinco.dev",
		"https://hub.robinco.dev\\@evil.invalid",
		"https://hub.robinco.dev\\x",
		"https:hub.robinco.dev",
		"https:///v1/nodes",
		"https://:443",
		"wss://hub.robinco.dev",
		"ws://hub.robinco.dev",
		"https://127.0.0.1",
		"https://127.0.0.1:443",
		"https://[::1]",
		" https://hub.robinco.dev",
		"https://hub.robinco.dev ",
		"",
	}
	for _, raw := range rejected {
		if _, err := parseHubOperatorBase(raw, false); err == nil {
			t.Fatalf("production accepted %q", raw)
		}
		// Test injection must not rescue a non-loopback or malformed origin.
		if !strings.Contains(raw, "127.0.0.1") && !strings.Contains(raw, "::1") && !strings.Contains(raw, "localhost") {
			if _, err := parseHubOperatorBase(raw, true); err == nil {
				t.Fatalf("test mode accepted non-loopback %q", raw)
			}
		}
	}
	for _, raw := range []string{"https://hub.robinco.dev", "https://hub.robinco.dev/", "https://hub.robinco.dev:443"} {
		base, err := parseHubOperatorBase(raw, false)
		if err != nil || base == nil {
			t.Fatalf("production rejected approved origin %q: %v", raw, err)
		}
	}
	testRejected := []string{
		"http://127.1:8080",
		"http://localhost.:8080",
		"http://LOCALHOST:8080",
		"http://127.0.0.1:abc",
		"http://127.0.0.1:0",
		"http://127.0.0.1:99999",
		"http://fixture.invalid:8080",
		"http://100.64.0.1:9377",
		"http://127.0.0.1:8080/path",
		"http://127.0.0.1:8080?x=1",
		"http://user@127.0.0.1:8080",
		"http://127.0.0.1:8080#frag",
		"ftp://127.0.0.1:8080",
	}
	for _, raw := range testRejected {
		if _, err := parseHubOperatorBase(raw, true); err == nil {
			t.Fatalf("test mode accepted %q", raw)
		}
	}
	for _, raw := range []string{"http://127.0.0.1:8080", "http://localhost:8080", "http://[::1]:8080", "https://127.0.0.1:8443"} {
		if _, err := parseHubOperatorBase(raw, true); err != nil {
			t.Fatalf("test mode rejected loopback %q: %v", raw, err)
		}
	}
}

// TestHubOperatorCLIRejectsAdversarialURL drives a representative command end
// to end so a --hub-url adversary fails closed with the documented invalid
// condition instead of reaching any origin.
func TestHubOperatorCLIRejectsAdversarialURL(t *testing.T) {
	tokenEnv, _ := t441Envs(t)
	var stdout, stderr bytes.Buffer
	for _, raw := range []string{
		"https://user:pass@hub.robinco.dev",
		"https://hub.robinco.dev.evil.invalid",
		"https://hub.robinco.dev:8443",
		"https://hub.robinco.dev/base",
		"https://hub.robinco.dev/?q=1",
	} {
		stdout.Reset()
		stderr.Reset()
		code := runHubStatusCLI([]string{"--hub-url", raw, "--hub-token-env", tokenEnv}, &stdout, &stderr, hubCLIDeps{})
		if code != ExitConditionInvalid {
			t.Fatalf("hub-url %q code=%d want=%d", raw, code, ExitConditionInvalid)
		}
		t441AssertNoCredentialLeak(t, stdout.String(), stderr.String(), "")
	}
}

// TestHubOperatorAPIPathAdversaries proves only the fixed command paths mint
// credentialled requests; anything else is rejected before a request exists.
func TestHubOperatorAPIPathAdversaries(t *testing.T) {
	for _, path := range []string{
		"/v1/unknown", "/v1/nodes/../admin", "/v1/lanes/a/b", "/v1/lanes/%2e%2e",
		"v1/nodes", "/v1/lanes/", "/v1/lanes/lane-a/../lane-b", "/v2/nodes",
		"/v1/nodes?x=1", "//v1/nodes", "/v1/nodes ", "/V1/nodes", "",
	} {
		if validHubOperatorAPIPath(path) {
			t.Fatalf("API path %q passed validation", path)
		}
	}
	for _, path := range []string{
		"/v1/nodes", "/v1/jobs", "/v1/jobs/orphaned", "/v1/jobs/reassign",
		"/v1/lanes", "/v1/lanes/lane-a", "/v1/placement",
		"/v1/burst", "/v1/burst/request", "/v1/burst/release", "/v1/burst/holds", "/v1/update",
	} {
		if !validHubOperatorAPIPath(path) {
			t.Fatalf("fixed API path %q rejected", path)
		}
	}
}

type t441CountingTransport struct {
	calls *int64
	base  http.RoundTripper
}

func (transport t441CountingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	atomic.AddInt64(transport.calls, 1)
	base := transport.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(request)
}

// TestHubGuardedTransportFailsClosed covers the last-moment adversary: a
// request URL mutated after the boundary stamped it must fail at the
// transport before one byte reaches the wire, and an unstamped request
// (which never crossed the boundary) fails identically.
func TestHubGuardedTransportFailsClosed(t *testing.T) {
	var calls int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte(`{"nodes":[]}`))
	}))
	defer server.Close()
	deps := hubCLIDeps{HTTPClient: &http.Client{Transport: t441CountingTransport{calls: &calls}}, AllowInsecureForTests: true}
	client, err := newHubOperatorClient(server.URL, t441OperatorToken, hubCFAccessEnv{ClientID: t441CFClientID, ClientSecret: t441CFClientSecret}, deps, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// An unstamped request can never come through buildRequest; the guard
	// must refuse it even when it points at the approved origin.
	unstamped, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/v1/nodes", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.client.Do(unstamped); err == nil || atomic.LoadInt64(&calls) != 0 {
		t.Fatalf("unstamped request reached transport: err=%v calls=%d", err, calls)
	}

	mutations := []struct {
		name   string
		mutate func(*http.Request)
	}{
		{"host", func(request *http.Request) { request.URL.Host = "127.0.0.1:1" }},
		{"scheme", func(request *http.Request) { request.URL.Scheme = "https" }},
		{"path", func(request *http.Request) { request.URL.Path = "/v1/jobs" }},
		{"userinfo", func(request *http.Request) { request.URL.User = url.User("u") }},
		{"query", func(request *http.Request) { request.URL.RawQuery = "x=1" }},
		{"port", func(request *http.Request) {
			host := request.URL.Hostname()
			request.URL.Host = host + ":1"
		}},
		{"whole URL", func(request *http.Request) {
			parsed, err := url.Parse("http://127.0.0.1:1/v1/nodes")
			if err == nil {
				request.URL = parsed
			}
		}},
		{"stamped context dropped", func(request *http.Request) {
			*request = *request.WithContext(context.Background())
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			before := atomic.LoadInt64(&calls)
			request, err := client.buildRequest(t.Context(), http.MethodGet, "/v1/nodes", nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			mutation.mutate(request)
			response, err := client.client.Do(request)
			if err == nil {
				if response != nil {
					_ = response.Body.Close()
				}
				t.Fatalf("mutated request succeeded")
			}
			if atomic.LoadInt64(&calls) != before {
				t.Fatal("mutated request reached the base transport")
			}
			if strings.Contains(err.Error(), t441OperatorToken) || strings.Contains(err.Error(), t441CFClientSecret) {
				t.Fatal("rejection error exposed a credential")
			}
		})
	}

	// A valid request still passes the guard end to end.
	response, err := client.do(t.Context(), http.MethodGet, "/v1/nodes", nil, nil)
	if err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	_ = response.Body.Close()
	if atomic.LoadInt64(&calls) == 0 {
		t.Fatal("valid request never reached the base transport")
	}
}

// TestHubOperatorBoundedFailures keeps malformed JSON, oversize bodies,
// timeouts, and 401/403 bounded and credential-free.
func TestHubOperatorBoundedFailures(t *testing.T) {
	tokenEnv, cfEnv := t441Envs(t)
	t.Run("malformed JSON", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			_, _ = writer.Write([]byte("{not json"))
		}))
		defer server.Close()
		var stdout, stderr bytes.Buffer
		code := runPlaceCLI([]string{"--class", "worker", "--hub-url", server.URL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, &stdout, &stderr, t441Deps(server))
		if code != ExitInternal {
			t.Fatalf("code=%d want=%d stdout=%q", code, ExitInternal, stdout.String())
		}
		t441AssertNoCredentialLeak(t, stdout.String(), stderr.String(), "")
	})
	t.Run("oversize body", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			_, _ = writer.Write([]byte(`{"decision":"machine-a","source":"hub-only","asof":"2026-09-19T00:00:00Z","candidates":[{"machine":"machine-a","score":1,"reason":"` + strings.Repeat("a", 2<<20) + `"}]}`))
		}))
		defer server.Close()
		var stdout, stderr bytes.Buffer
		code := runPlaceCLI([]string{"--class", "worker", "--hub-url", server.URL, "--hub-token-env", tokenEnv}, &stdout, &stderr, t441Deps(server))
		if code != ExitInternal {
			t.Fatalf("code=%d want=%d", code, ExitInternal)
		}
		t441AssertNoCredentialLeak(t, stdout.String(), stderr.String(), "")
	})
	t.Run("timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			time.Sleep(300 * time.Millisecond)
			_, _ = writer.Write([]byte(`{}`))
		}))
		defer server.Close()
		client, err := newHubOperatorClient(server.URL, t441OperatorToken, hubCFAccessEnv{}, t441Deps(server), 50*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.do(t.Context(), http.MethodGet, "/v1/nodes", nil, nil)
		if err == nil {
			_ = response.Body.Close()
			t.Fatal("request against a stalled server succeeded")
		}
		t441AssertNoCredentialLeak(t, "", "", err.Error())
	})
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(status)
				_, _ = writer.Write([]byte(`{"error":"denied"}`))
			}))
			defer server.Close()
			var stdout, stderr bytes.Buffer
			code := runPlaceCLI([]string{"--class", "worker", "--hub-url", server.URL, "--hub-token-env", tokenEnv, "--hub-cf-env", cfEnv}, &stdout, &stderr, t441Deps(server))
			if code != ExitDeliveryFailure {
				t.Fatalf("status=%d code=%d want=%d", status, code, ExitDeliveryFailure)
			}
			t441AssertNoCredentialLeak(t, stdout.String(), stderr.String(), "")
		})
	}
}

// TestHubOperatorErrorsCarryNoCredential checks that errors surfaced by the
// boundary (rejected origin, rejected path, transport failure) never contain
// a credential value.
func TestHubOperatorErrorsCarryNoCredential(t *testing.T) {
	cf := hubCFAccessEnv{ClientID: t441CFClientID, ClientSecret: t441CFClientSecret}
	if _, err := newHubOperatorClient("https://user@hub.robinco.dev", t441OperatorToken, cf, hubCLIDeps{}, 0); err == nil || strings.Contains(err.Error(), t441OperatorToken) {
		t.Fatalf("origin rejection leaked credential: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	client, err := newHubOperatorClient(server.URL, t441OperatorToken, cf, t441Deps(server), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.do(t.Context(), http.MethodGet, "/v1/evil", nil, nil); err == nil || strings.Contains(err.Error(), t441OperatorToken) {
		t.Fatalf("path rejection leaked credential: %v", err)
	}
	server.Close()
	_, err = client.do(t.Context(), http.MethodGet, "/v1/nodes", nil, nil)
	if err == nil {
		t.Fatal("request to closed server succeeded")
	}
	if strings.Contains(err.Error(), t441OperatorToken) || strings.Contains(err.Error(), t441CFClientID) || strings.Contains(err.Error(), t441CFClientSecret) {
		t.Fatalf("transport error leaked credential: %v", err)
	}
}

// TestHubOperatorCredentialsOnlyAfterValidation proves buildRequest attaches
// the bearer and CF headers only once every URL check has passed, and that
// rejection paths leave no partially armed request behind.
func TestHubOperatorCredentialsOnlyAfterValidation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	client, err := newHubOperatorClient(server.URL, t441OperatorToken, hubCFAccessEnv{ClientID: t441CFClientID, ClientSecret: t441CFClientSecret}, t441Deps(server), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.buildRequest(t.Context(), http.MethodGet, "/v1/evil", nil, nil); err == nil {
		t.Fatal("invalid API path minted a request")
	}
	request, err := client.buildRequest(t.Context(), http.MethodGet, "/v1/nodes", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := request.Context().Value(hubExpectedURLContextKey{}); got != request.URL.String() {
		t.Fatalf("stamped URL=%v want %q", got, request.URL.String())
	}
	if request.Header.Get(hubAuthorizationHeader) != "Bearer "+t441OperatorToken ||
		request.Header.Get("CF-Access-Client-Id") != t441CFClientID ||
		request.Header.Get("CF-Access-Client-Secret") != t441CFClientSecret {
		t.Fatal("validated request missing credential headers")
	}
}

// TestHubOperatorReadsStayBounded is a body-level bound check on the raw
// copy paths (burst show, update publish): an oversized response is capped
// instead of streamed without limit.
func TestHubOperatorReadsStayBounded(t *testing.T) {
	tokenEnv, _ := t441Envs(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/v1/burst" {
			_, _ = writer.Write([]byte(strings.Repeat("b", 128<<10)))
			return
		}
		writer.WriteHeader(http.StatusAccepted)
		_, _ = writer.Write([]byte(strings.Repeat("u", 128<<10)))
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	code := runBurstCLIWithDeps([]string{"show", "--hub-url", server.URL, "--hub-token-env", tokenEnv}, &stdout, &stderr, t441Deps(server))
	if code != ExitOK || int64(stdout.Len()) > 64<<10 {
		t.Fatalf("burst show unbounded read: code=%d bytes=%d", code, stdout.Len())
	}
	stdout.Reset()
	code = runUpdateCLI([]string{"publish", "--hub-url", server.URL, "--hub-token-env", tokenEnv, "--version", "v1.2.3", "--sha256", strings.Repeat("ab", 32), "--url", "https://github.com/mgh3326/panewire/releases/download/v1.2.3/panewire", "--machines", "machine-a"}, &stdout, &stderr, t441Deps(server))
	if code != ExitOK || int64(stdout.Len()) > 64<<10 {
		t.Fatalf("update publish unbounded read: code=%d bytes=%d", code, stdout.Len())
	}
	t441AssertNoCredentialLeak(t, stdout.String(), stderr.String(), "")
}

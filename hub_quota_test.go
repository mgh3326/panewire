package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
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

// A legacy hub's quota.request is still decoded, and the node answers
// unsupported so the hub's waiter completes instead of timing out.
func TestHubQuotaRequestAnswersUnsupported(t *testing.T) {
	message, ok := parseHubOutbound([]byte(`{"type":"quota.request","request_id":"quota-abc123","tool":"scopefuel"}`))
	if !ok || message.RequestID != "quota-abc123" {
		t.Fatalf("quota.request no longer decodes: %+v ok=%t", message, ok)
	}

	reports := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_, payload, err := conn.Read(r.Context())
		if err == nil {
			reports <- payload
		}
	}))
	defer server.Close()
	conn, _, err := websocket.Dial(t.Context(), r6WSURL(server.URL, ""), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()

	client := &HubClient{}
	client.handleHubQuota(t.Context(), &hubClientConnection{connection: conn}, message)
	select {
	case payload := <-reports:
		var report struct {
			Type      string `json:"type"`
			RequestID string `json:"request_id"`
			Error     string `json:"error"`
		}
		if json.Unmarshal(payload, &report) != nil || report.Type != "quota.report" || report.RequestID != "quota-abc123" || report.Error != "unsupported" {
			t.Fatalf("quota.report=%s", payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("node did not answer quota.request")
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

func TestPlaceQuotaAdapterClassificationAndLocalDenyPrecedence(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "wrk-quota-adapter.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Name           string          `json:"name"`
		HubStatus      int             `json:"hub_status"`
		HubBody        json.RawMessage `json:"hub_body"`
		TransportError string          `json:"transport_error"`
		LocalExit      int             `json:"local_exit"`
		LocalStdout    string          `json:"local_stdout"`
		LocalStderr    string          `json:"local_stderr"`
		WantHub        string          `json:"want_hub"`
		WantFinal      string          `json:"want_final"`
	}
	if json.Unmarshal(data, &fixtures) != nil || len(fixtures) < 8 {
		t.Fatalf("adapter fixture invalid: %s", data)
	}
	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(fixture.Name, func(t *testing.T) {
			validateScopefuelGateFixture(t, fixture.LocalExit, fixture.LocalStdout, fixture.LocalStderr)
			tokenPath := filepath.Join(t.TempDir(), "operator.env")
			if err := os.WriteFile(tokenPath, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN="+quotaOperatorToken+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			// The credentialled hub boundary admits only loopback test origins;
			// the fixture transport answers before any dial, so the discard
			// port keeps the transport-error case identical to a real one.
			hubURL := "http://127.0.0.1:9"
			client := &http.Client{Transport: quotaRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, context.DeadlineExceeded
			})}
			if fixture.TransportError == "" {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Query().Get("pool") != "claude" || r.URL.Query().Get("account_fp") != "a1b2c3d4" || r.Header.Get(hubAuthorizationHeader) != "Bearer "+quotaOperatorToken {
						http.Error(w, "bad fixture request", http.StatusBadRequest)
						return
					}
					w.WriteHeader(fixture.HubStatus)
					_, _ = w.Write(fixture.HubBody)
				}))
				defer server.Close()
				hubURL, client = server.URL, server.Client()
			}
			var stdout, stderr bytes.Buffer
			code := runPlaceCLI([]string{"--class", "worker", "--cwd", "repo-key", "--pool", "claude", "--account-fp", "a1b2c3d4", "--hub-url", hubURL, "--hub-token-env", tokenPath}, &stdout, &stderr, hubCLIDeps{HTTPClient: client, AllowInsecureForTests: true})
			hubOutcome := quotaHubOutcomeFromCLI(t, code, stdout.String(), stderr.String())
			if string(hubOutcome) != fixture.WantHub {
				t.Fatalf("hub outcome=%q want=%q code=%d stdout=%q stderr=%q", hubOutcome, fixture.WantHub, code, stdout.String(), stderr.String())
			}
			final := ResolveWrkQuotaGate(hubOutcome, fixture.LocalExit)
			if string(final) != fixture.WantFinal {
				t.Fatalf("final=%q want=%q hub=%q local_exit=%d", final, fixture.WantFinal, hubOutcome, fixture.LocalExit)
			}
		})
	}
}

func TestHubQuotaDocumentationContract(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("docs", "r29-hub-quota.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, required := range []string{"## Heartbeat field", "| field | JSON type | `null` meaning", "## Quota policy", "## Deployment order", "## Example JSON", "## wrk adapter contract", "retired", "unsupported", "scopefuel", "profile-to-pool", "path-derived hint", "exit 3", "exit 4"} {
		if !strings.Contains(text, required) {
			t.Fatalf("quota design document missing %q", required)
		}
	}
	if !strings.Contains(text, `"host-a"`) {
		t.Fatal("quota design document does not use a generic example identifier")
	}
}

type quotaRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn quotaRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func quotaHubOutcomeFromCLI(t *testing.T, code int, stdout, stderr string) HubQuotaGateOutcome {
	t.Helper()
	switch {
	case code == ExitOK && stdout != "" && stderr == "":
		return HubQuotaGateAllow
	case code == ExitConditionInvalid && strings.Contains(stderr, "place denied"):
		return HubQuotaGateDeny
	case code == ExitDaemonUnavailable && strings.Contains(stderr, "place unavailable"):
		return HubQuotaGateUnavailable
	case code == ExitDeliveryFailure && strings.Contains(stderr, "authentication"):
		return HubQuotaGateAuthentication
	case code == ExitInternal && strings.Contains(stderr, "fail-closed") || code == ExitInternal && strings.Contains(stderr, "malformed"):
		return HubQuotaGateMalformed
	default:
		t.Fatalf("unclassified place result: code=%d stdout=%q stderr=%q", code, stdout, stderr)
		return HubQuotaGateMalformed
	}
}

func validateScopefuelGateFixture(t *testing.T, exit int, stdout, stderr string) {
	t.Helper()
	switch exit {
	case 0:
		lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
		firstLine := regexp.MustCompile(`^profile=[a-z0-9._-]+ pool=[a-z][a-z0-9._-]* used_pct=[0-9]+(?:\.[0-9]+)? class=(?:preserve|spend)$`)
		if len(lines) != 2 || !firstLine.MatchString(lines[0]) || lines[1] == "" || stderr != "" {
			t.Fatalf("scopefuel exit 0 must be exactly two stdout lines: stdout=%q stderr=%q", stdout, stderr)
		}
	case 3:
		if stdout != "" || stderr == "" || !strings.Contains(stderr, "alternatives=") {
			t.Fatalf("scopefuel exit 3 must put denial/alternatives on stderr: stdout=%q stderr=%q", stdout, stderr)
		}
	case 4:
		if stdout != "" || stderr == "" {
			t.Fatalf("scopefuel exit 4 must be measurement-unavailable: stdout=%q stderr=%q", stdout, stderr)
		}
	default:
		t.Fatalf("unknown scopefuel gate fixture exit=%d", exit)
	}
}

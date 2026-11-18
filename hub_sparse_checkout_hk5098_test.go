package panewire

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 5098, hub side: the hub carries the sparse fields and lists the feature a
// client reads; a node that does not advertise checkout_sparse_v1 never sees
// the lists, so it prepares the full checkout instead.

func sparseSpecSha() string { return strings.Repeat("a", 40) }

func sparseSpecCheckout(exclude, include []string) *fleet.HubCheckout {
	return &fleet.HubCheckout{
		Origin:  "https://github.com/example/project.git",
		Mode:    "detached",
		Sha:     sparseSpecSha(),
		Fetch:   "refs/heads/main",
		Dir:     "project.test-job",
		Exclude: exclude,
		Include: include,
	}
}

func TestSpawnFeaturesListTheSparseCheckout(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	status, _, body := t14HTTP(t, http.MethodGet, server.URL+"/v1/spawns/features", t14OperatorToken, nil)
	if status != http.StatusOK {
		t.Fatalf("features: %d %s", status, body)
	}
	var features struct {
		Features []string `json:"features"`
	}
	if err := json.Unmarshal(body, &features); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(features.Features, hubCapabilityCheckoutSparse) {
		t.Fatalf("features=%v, want %s", features.Features, hubCapabilityCheckoutSparse)
	}
}

func TestHubRefusesABadSparsePatternNamingItsIndex(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	for _, tc := range []struct {
		name    string
		exclude []string
		include []string
		want    string
	}{
		{"leading bang", []string{"Assets", "!docs"}, nil, "checkout-exclude entry 2"},
		{"parent segment", nil, []string{"ok", "Assets/../etc"}, "checkout-include entry 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := durableSpawnRequest()
			request.Repository.Checkout = sparseSpecCheckout(tc.exclude, tc.include)
			status, _, body := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
			if status != http.StatusBadRequest || !strings.Contains(string(body), tc.want) {
				t.Fatalf("submit=%d %s, want a refusal naming %q", status, body, tc.want)
			}
			// The refusal names the entry, never its text.
			for _, entry := range append(append([]string(nil), tc.exclude...), tc.include...) {
				if strings.Contains(entry, "..") && strings.Contains(string(body), entry) {
					t.Fatalf("refusal repeats the entry: %s", body)
				}
			}
		})
	}
}

func TestHubRefusesAnOverBoundSparseList(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	exclude := make([]string, 65)
	for i := range exclude {
		exclude[i] = "dir" + strings.Repeat("x", i%5)
	}
	request := durableSpawnRequest()
	request.Repository.Checkout = sparseSpecCheckout(exclude, nil)
	status, _, body := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
	if status != http.StatusBadRequest || !strings.Contains(string(body), "limit is 64") {
		t.Fatalf("submit=%d %s, want the exclude bound", status, body)
	}
}

// sparseNode connects a node advertising capabilities and returns its
// connection, so a test can read the frames the hub sends it.
func sparseNode(t *testing.T, h *HubServer, server *httptest.Server, machine string, capabilities []string) *websocket.Conn {
	t.Helper()
	headers := http.Header{}
	headers.Set(hubMachineIDHeader, machine)
	headers.Set(hubAuthorizationHeader, "Bearer node-token")
	connection, _, err := websocket.Dial(t.Context(), t14WSURL(server.URL, "/v1/agent"), &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.CloseNow() })
	hello := map[string]any{"type": "hello", "machine_id": machine, "version": "t14", "accepting": true, "capabilities": capabilities}
	if err := wsjson.Write(t.Context(), connection, hello); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, "the node to connect", func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		record := h.nodes[machine]
		return record != nil && record.agent != nil && record.state == "connected"
	})
	return connection
}

// readSpawnFrame reads the node's frames until the job.spawn one arrives.
func readSpawnFrame(t *testing.T, connection *websocket.Conn) map[string]json.RawMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	for {
		var message map[string]json.RawMessage
		if err := wsjson.Read(ctx, connection, &message); err != nil {
			t.Fatalf("no job.spawn frame: %v", err)
		}
		var kind string
		if json.Unmarshal(message["type"], &kind) == nil && kind == "job.spawn" {
			return message
		}
	}
}

func TestHubSendsSparseListsOnlyToACapableNode(t *testing.T) {
	for _, tc := range []struct {
		name         string
		capabilities []string
		want         bool
	}{
		{"node without the capability", []string{"spawn_v2", "spawn_ops_v1"}, false},
		{"node advertising checkout_sparse_v1", []string{"spawn_v2", "spawn_ops_v1", hubCapabilityCheckoutSparse}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, server := t14Hub(t)
			node := sparseNode(t, h, server, "machine-a", tc.capabilities)
			body := t14SpawnBody(t14RequestID(950))
			body["checkout"] = map[string]any{
				"origin": "https://github.com/example/project.git", "mode": "detached",
				"sha": sparseSpecSha(), "fetch": "refs/heads/main", "dir": "project.test-job",
				"exclude": []string{"Assets"}, "include": []string{"Assets/Changed.prefab"},
			}
			reply := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
			message := readSpawnFrame(t, node)
			var checkout struct {
				Exclude []string `json:"exclude"`
				Include []string `json:"include"`
			}
			if err := json.Unmarshal(message["checkout"], &checkout); err != nil {
				t.Fatalf("checkout frame: %v", err)
			}
			if got := len(checkout.Exclude) == 1 && len(checkout.Include) == 1; got != tc.want {
				t.Fatalf("checkout lists=%+v, want lists present=%v", checkout, tc.want)
			}
			if result := <-reply; result.status != http.StatusGatewayTimeout {
				t.Fatalf("unanswered send: %d %s", result.status, result.body)
			}
		})
	}
}

// sparseCheckoutForNode never mutates the stored request's lists: a replay to a
// capable node must still carry them.
func TestSparseCheckoutForNodeKeepsTheRequest(t *testing.T) {
	checkout := &hubSpawnCheckout{Origin: "https://github.com/example/project.git", Mode: "detached", Sha: sparseSpecSha(), Fetch: "refs/heads/main", Dir: "project.test-job", Exclude: []string{"Assets"}, Include: []string{"Assets/Changed.prefab"}}
	incapable := map[string]bool{"spawn_v2": true}
	stripped := sparseCheckoutForNode(checkout, incapable)
	if stripped == checkout || len(stripped.Exclude) != 0 || len(stripped.Include) != 0 {
		t.Fatalf("stripped=%+v, want a copy without the lists", stripped)
	}
	if len(checkout.Exclude) != 1 || len(checkout.Include) != 1 {
		t.Fatalf("the request's lists were mutated: %+v", checkout)
	}
	capable := map[string]bool{hubCapabilityCheckoutSparse: true}
	if kept := sparseCheckoutForNode(checkout, capable); kept != checkout || len(kept.Exclude) != 1 {
		t.Fatalf("capable node got %+v", kept)
	}
}

// The node's answer carries the mode to the hub, which answers the submitter
// and records it in the job; the legacy wire shape an older hub reads stays
// byte-shaped.
func TestSpawnAnswerCarriesCheckoutMode(t *testing.T) {
	extended, err := json.Marshal(spawnResultWire(hubSpawnResult{RequestID: t14RequestID(951), CheckoutMode: nodeCheckoutSparse}, true))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(extended), `"checkout_mode":"sparse"`) {
		t.Fatalf("extended answer=%s", extended)
	}
	// A direct /v1/spawn never sets operator_authorized, so its answer uses the
	// legacy receipt shape; the hub-feature gate, not that shape, decides
	// whether checkout_mode travels (hk 5098 review).
	legacy, err := json.Marshal(spawnResultWire(spawnResultForHub(hubSpawnResult{RequestID: t14RequestID(952), CheckoutMode: nodeCheckoutSparse}, true, true), false))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(legacy), `"checkout_mode":"sparse"`) {
		t.Fatalf("a capable hub's legacy-shaped answer lost checkout_mode: %s", legacy)
	}
}

// A list is valid only on a detached spec: the hub applies the same rule the
// node does, so a branch-mode request with lists is refused on submit instead
// of being accepted and refused (or silently run full) at the node (hk 5098
// review).
func TestHubRefusesSparseListsOnABranchCheckout(t *testing.T) {
	_, server := t14Hub(t)
	body := t14SpawnBody(t14RequestID(954))
	body["checkout"] = map[string]any{
		"origin": "https://github.com/example/project.git", "mode": "branch",
		"branch": "wk/task", "sha": sparseSpecSha(), "dir": "project.test-job",
		"exclude": []string{"Assets"},
	}
	status, _, answer := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
	if status != http.StatusBadRequest {
		t.Fatalf("branch checkout with lists: %d %s", status, answer)
	}
	// The submit path refuses the same request too, naming the rule.
	request := durableSpawnRequest()
	request.Repository.Checkout = &fleet.HubCheckout{Origin: "https://github.com/example/project.git", Mode: "branch", Branch: "wk/task", Sha: sparseSpecSha(), Dir: "project.test-job", Exclude: []string{"Assets"}}
	status, _, answer = t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
	if status != http.StatusBadRequest || !strings.Contains(string(answer), "detached checkout spec") {
		t.Fatalf("branch checkout with lists: %d %s", status, answer)
	}
}

// The per-entry bound alone allows megabytes of pattern text; the lists
// together must fit the transport that carries them (hk 5098 review).
func TestHubRefusesAnOverSizeSparseList(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	include := make([]string, 40)
	for i := range include {
		include[i] = "dir" + strconv.Itoa(i) + "/" + strings.Repeat("p", 4090)
	}
	request := durableSpawnRequest()
	request.Repository.Checkout = sparseSpecCheckout(nil, include)
	status, _, body := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
	if status != http.StatusBadRequest || !strings.Contains(string(body), "together") {
		t.Fatalf("over-size list: %d %s", status, body)
	}
}

// A node from before the feature cannot answer checkout_mode, but the hub
// knows it dropped the lists and that the node therefore prepared the full
// checkout; the answer says "full" rather than nothing (hk 5098 review).
func TestHubReportsFullForAStrippedSparseRequest(t *testing.T) {
	h, server := t14Hub(t)
	node := sparseNode(t, h, server, "machine-a", []string{"spawn_v2", "spawn_ops_v1"})
	requestID := t14RequestID(953)
	body := t14SpawnBody(requestID)
	body["checkout"] = map[string]any{
		"origin": "https://github.com/example/project.git", "mode": "detached",
		"sha": sparseSpecSha(), "fetch": "refs/heads/main", "dir": "project.test-job",
		"exclude": []string{"Assets"},
	}
	reply := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawn", t14OperatorToken, body)
	message := readSpawnFrame(t, node)
	var decoded struct {
		Checkout map[string]json.RawMessage `json:"checkout"`
	}
	if err := json.Unmarshal(message["checkout"], &decoded.Checkout); err == nil {
		if _, present := decoded.Checkout["exclude"]; present {
			t.Fatalf("the incapable node received the lists: %s", message["checkout"])
		}
	}
	// The pre-feature node's answer carries no checkout_mode at all.
	if err := wsjson.Write(t.Context(), node, map[string]any{
		"type": "job.spawn.result", "request_id": requestID, "rc": 81, "job_id": "project.test-job",
		"pane": "", "stdout_tail": "", "error": "checkout_refused:git-failed",
	}); err != nil {
		t.Fatal(err)
	}
	result := <-reply
	if result.status != http.StatusOK || !strings.Contains(string(result.body), `"checkout_mode":"full"`) {
		t.Fatalf("answer=%d %s, want checkout_mode full", result.status, result.body)
	}
}

// The hub names checkout_sparse_v1 in the frame it sends a node once per
// connection, which is the signal the node gates checkout_mode on.
func TestHubFeaturesFrameNamesTheSparseCheckout(t *testing.T) {
	if !slices.Contains(hubFeaturesFrame().Features, hubFeatureCheckoutSparse) {
		t.Fatalf("hub.features=%v", hubFeaturesFrame().Features)
	}
}

// A node must not put checkout_mode on the wire to a hub that did not name the
// feature: an older hub's strict result parser drops the whole answer for one
// unknown key (hk 5098 review).
func TestSpawnResultHidesCheckoutModeFromAnOldHub(t *testing.T) {
	result := hubSpawnResult{RequestID: t14RequestID(955), CheckoutMode: nodeCheckoutSparse}
	if got := spawnResultForHub(result, true, false); got.CheckoutMode != "" {
		t.Fatalf("old hub result = %+v, want no checkout_mode", got)
	}
	if got := spawnResultForHub(result, true, true); got.CheckoutMode != nodeCheckoutSparse {
		t.Fatalf("new hub result = %+v, want checkout_mode sparse", got)
	}
	raw, err := json.Marshal(spawnResultWire(spawnResultForHub(result, true, false), true))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "checkout_mode") {
		t.Fatalf("old hub wire=%s", raw)
	}
}

// The asynchronous operation's stored outcome keeps the node's checkout mode,
// so a status read after the immediate answer still says which checkout ran
// (hk 5098 review).
func TestStoredSpawnOutcomeCarriesCheckoutMode(t *testing.T) {
	task := hubSpawnTask{Request: fleet.Submission{RequestID: t14RequestID(956)}}
	node := hubSpawnRequest{RequestID: t14RequestID(956), Machine: "machine-a", Args: []string{"--job", "job-a"}}
	outcome := spawnNodeOutcome(task, node, hubSpawnResult{RC: 0, CheckoutMode: nodeCheckoutSparse})
	if outcome.CheckoutMode != nodeCheckoutSparse {
		t.Fatalf("stored outcome = %+v, want checkout_mode sparse", outcome)
	}
	raw, err := json.Marshal(outcome)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"checkout_mode":"sparse"`) {
		t.Fatalf("stored outcome JSON = %s", raw)
	}
}

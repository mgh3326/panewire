package panewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	quotaOperatorToken = "quota-operator-token"
	quotaNodeAToken    = "quota-node-a-token"
	quotaNodeBToken    = "quota-node-b-token"
)

func TestHubQuotaHeartbeatCompatibilityAndStrictValidation(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("testdata", "heartbeat-quota.json"))
	if err != nil {
		t.Fatal(err)
	}
	heartbeat, ok := decodeHubHeartbeatPayload(fixture)
	if !ok || heartbeat.Quota == nil || !heartbeat.Quota.valid() || heartbeat.Quota.Pools[2].UsedPct != nil || heartbeat.Quota.Pools[2].ResetAt != nil {
		t.Fatalf("quota fixture rejected or null changed: %+v ok=%t", heartbeat.Quota, ok)
	}
	roundTrip, err := json.Marshal(heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(roundTrip, []byte(`"used_pct":null`)) || !bytes.Contains(roundTrip, []byte(`"reset_at":null`)) {
		t.Fatalf("nullable quota fields were omitted or fabricated: %s", roundTrip)
	}

	legacy := []byte(`{"status":"alive","checks":{"service":"ok"}}`)
	decodedLegacy, ok := decodeHubHeartbeatPayload(legacy)
	if !ok || decodedLegacy.Quota != nil {
		t.Fatalf("legacy heartbeat compatibility lost: %+v ok=%t", decodedLegacy, ok)
	}
	now := time.Date(2026, 9, 9, 6, 0, 0, 0, time.UTC)
	policyPath := filepath.Join(t.TempDir(), "placement.json")
	writeQuotaPolicy(t, policyPath, nil, nil)
	hub := newQuotaTestHub(t, []string{"host-a"}, policyPath, func() time.Time { return now }, slog.Default())
	agent := &hubAgent{}
	hub.connect("host-a", "fixture", "fixture", agent, true)
	before, err := json.Marshal(hub.placement(t.Context(), "worker", "legacy-fixture"))
	if err != nil {
		t.Fatal(err)
	}
	baseGolden := []byte(`{"decision":"host-a","candidates":[{"machine":"host-a","score":100,"throttled":false,"active_jobs":0,"connected":true,"metrics_known":true,"memory_free_pct":null,"swap_used_mb":null,"memory_known":false,"holds_active":false,"burst_ready":false,"reason":"memory_unknown"}],"source":"hub-only","asof":"2026-09-09T06:00:00Z","policy_status":"current"}`)
	if !bytes.Equal(before, baseGolden) {
		t.Fatalf("legacy placement drifted beyond the required policy-status field: got=%s want=%s", before, baseGolden)
	}
	sendRawQuotaHeartbeat(t, hub, "host-a", agent, legacy)
	after, err := json.Marshal(hub.placement(t.Context(), "worker", "legacy-fixture"))
	if err != nil {
		t.Fatal(err)
	}
	hub.mu.Lock()
	legacyRecord := hub.nodeQuota["host-a"]
	hub.mu.Unlock()
	if legacyRecord == nil || legacyRecord.latest != nil || legacyRecord.state != "legacy" || !bytes.Equal(before, after) {
		t.Fatalf("legacy quota changed placement or became a value: before=%s after=%s record=%+v", before, after, legacyRecord)
	}

	base := `{"status":"alive","quota":{"status":"ok","collected_at":"2026-09-09T05:58:50Z","pools":[%s]}}`
	validRow := `{"pool":"claude","account_fp":"a1b2c3d4","window":"5h","used_pct":null,"reset_at":null,"source":"unavailable"}`
	if _, ok := decodeHubHeartbeatPayload([]byte(fmt.Sprintf(base, validRow))); !ok {
		t.Fatal("explicit null quota measurement was rejected")
	}
	invalidRows := map[string]string{
		"over 100":       strings.Replace(validRow, `"used_pct":null`, `"used_pct":100.1`, 1),
		"negative":       strings.Replace(validRow, `"used_pct":null`, `"used_pct":-0.1`, 1),
		"NaN":            strings.Replace(validRow, `"used_pct":null`, `"used_pct":NaN`, 1),
		"positive inf":   strings.Replace(validRow, `"used_pct":null`, `"used_pct":+Inf`, 1),
		"negative inf":   strings.Replace(validRow, `"used_pct":null`, `"used_pct":-Inf`, 1),
		"string":         strings.Replace(validRow, `"used_pct":null`, `"used_pct":"24"`, 1),
		"missing":        strings.Replace(validRow, `"used_pct":null,`, "", 1),
		"extra":          strings.Replace(validRow, `"source":"unavailable"`, `"source":"unavailable","extra":1`, 1),
		"bad pool":       strings.Replace(validRow, `"pool":"claude"`, `"pool":"../claude"`, 1),
		"bad account fp": strings.Replace(validRow, `"account_fp":"a1b2c3d4"`, `"account_fp":"identity"`, 1),
		"bad window":     strings.Replace(validRow, `"window":"5h"`, `"window":"../../5h"`, 1),
		"bad reset":      strings.Replace(validRow, `"reset_at":null`, `"reset_at":"tomorrow"`, 1),
	}
	for name, row := range invalidRows {
		t.Run(name, func(t *testing.T) {
			if _, ok := decodeHubHeartbeatPayload([]byte(fmt.Sprintf(base, row))); ok {
				t.Fatalf("invalid quota heartbeat was accepted: %s", row)
			}
		})
	}
	invalidTop := []byte(`{"status":"alive","quota":{"status":"ok","collected_at":"2026-09-09T05:58:50Z","pools":[],"extra":true}}`)
	if _, ok := decodeHubHeartbeatPayload(invalidTop); ok {
		t.Fatal("quota object with an extra field was accepted")
	}
	if (HubQuotaPool{Pool: "claude", AccountFP: "a1b2c3d4", Window: "5h", UsedPct: memoryFloat(math.Inf(1)), Source: "fixture"}).valid() {
		t.Fatal("infinite quota measurement was accepted by the value validator")
	}
}

func TestHubQuotaRejectedAndUnavailablePreserveLastGood(t *testing.T) {
	now := time.Date(2026, 9, 9, 6, 0, 0, 0, time.UTC)
	hub := newQuotaTestHub(t, []string{"host-a"}, "", func() time.Time { return now }, slog.Default())
	agent := &hubAgent{}
	hub.connect("host-a", "fixture", "fixture", agent, true)
	good := quotaSnapshot("2026-09-09T05:58:50Z", HubQuotaPool{Pool: "claude", AccountFP: "a1b2c3d4", Window: "5h", UsedPct: memoryFloat(24), ResetAt: quotaString("2026-09-09T10:29:59Z"), Source: "oauth-usage-api"})
	sendQuotaHeartbeat(t, hub, "host-a", agent, good)

	now = now.Add(time.Minute)
	invalid := []byte(`{"status":"alive","quota":{"status":"ok","collected_at":"2026-09-09T05:59:50Z","pools":[{"pool":"claude","account_fp":"a1b2c3d4","window":"5h","used_pct":100.1,"reset_at":null,"source":"oauth-usage-api"}]}}`)
	sendRawQuotaHeartbeat(t, hub, "host-a", agent, invalid)
	nodes, _ := readQuotaAPI(t, hub)
	if len(nodes) != 1 || nodes[0].State != "unknown" || nodes[0].Latest != nil || nodes[0].LastGood == nil || !sameMemoryFloat(nodes[0].LastGood.Pools[0].UsedPct, 24) {
		t.Fatalf("rejected quota erased or presented last-good as latest: %+v", nodes)
	}

	now = now.Add(time.Minute)
	sendQuotaHeartbeat(t, hub, "host-a", agent, unavailableHubQuotaSnapshot())
	nodes, _ = readQuotaAPI(t, hub)
	if nodes[0].State != "unavailable" || nodes[0].Latest == nil || nodes[0].Latest.Status != "unavailable" || nodes[0].Latest.CollectedAt != nil || len(nodes[0].Latest.Pools) != 0 || nodes[0].LastGood == nil || nodes[0].LastGood.CollectedAt == nil || *nodes[0].LastGood.CollectedAt != "2026-09-09T05:58:50Z" {
		t.Fatalf("unavailable latest did not preserve last-good: %+v", nodes[0])
	}
}

func TestHubQuotaAPIKeepsTupleRowsWithoutFleetAggregate(t *testing.T) {
	now := time.Date(2026, 9, 9, 6, 0, 0, 0, time.UTC)
	hub := newQuotaTestHub(t, []string{"host-a", "host-b"}, "", func() time.Time { return now }, slog.Default())
	agentA, agentB := &hubAgent{}, &hubAgent{}
	hub.connect("host-a", "fixture", "fixture", agentA, true)
	hub.connect("host-b", "fixture", "fixture", agentB, true)
	sendQuotaHeartbeat(t, hub, "host-a", agentA, quotaSnapshot("2026-09-09T05:58:50Z",
		HubQuotaPool{Pool: "claude", AccountFP: "a1b2c3d4", Window: "5h", UsedPct: memoryFloat(20), Source: "fixture"},
		HubQuotaPool{Pool: "claude", AccountFP: "b1c2d3e4", Window: "5h", UsedPct: memoryFloat(30), Source: "fixture"},
	))
	sendQuotaHeartbeat(t, hub, "host-b", agentB, quotaSnapshot("2026-09-09T05:59:50Z",
		HubQuotaPool{Pool: "claude", AccountFP: "a1b2c3d4", Window: "5h", UsedPct: memoryFloat(60), Source: "fixture"},
	))
	nodes, raw := readQuotaAPI(t, hub)
	if len(nodes) != 2 || len(nodes[0].Latest.Pools)+len(nodes[1].Latest.Pools) != 3 {
		t.Fatalf("four-tuple rows collapsed: %+v", nodes)
	}
	observed := map[string]float64{}
	for _, node := range nodes {
		for _, row := range node.Latest.Pools {
			if row.UsedPct == nil {
				t.Fatalf("fixture value became null: %+v", row)
			}
			observed[node.MachineID+"/"+row.AccountFP] = *row.UsedPct
		}
	}
	want := map[string]float64{"host-a/a1b2c3d4": 20, "host-a/b1c2d3e4": 30, "host-b/a1b2c3d4": 60}
	if len(observed) != len(want) {
		t.Fatalf("distinct/same account rows not preserved: got=%v want=%v", observed, want)
	}
	for key, value := range want {
		if observed[key] != value {
			t.Fatalf("row %s=%v want=%v", key, observed[key], value)
		}
	}
	var top map[string]json.RawMessage
	if json.Unmarshal(raw, &top) != nil || len(top) != 1 || top["nodes"] == nil {
		t.Fatalf("fleet aggregate appeared beside nodes: %s", raw)
	}
	for _, forbidden := range []string{`"used_pct":110`, `"used_pct":40`, `"sum"`, `"average"`, `"avg"`} {
		if bytes.Contains(bytes.ToLower(raw), []byte(forbidden)) {
			t.Fatalf("fleet sum/average leaked into response (%s): %s", forbidden, raw)
		}
	}
}

func TestHubQuotaRestartIsUnknownUntilHeartbeat(t *testing.T) {
	root := t.TempDir()
	policyPath := filepath.Join(root, "placement.json")
	writeQuotaPolicy(t, policyPath, nil, nil)
	before, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 9, 6, 0, 0, 0, time.UTC)
	original := newQuotaTestHub(t, []string{"host-a"}, policyPath, func() time.Time { return now }, slog.Default())
	originalAgent := &hubAgent{}
	original.connect("host-a", "fixture", "fixture", originalAgent, true)
	sendQuotaHeartbeat(t, original, "host-a", originalAgent, quotaSnapshot("2026-09-09T05:58:50Z", HubQuotaPool{Pool: "claude", AccountFP: "a1b2c3d4", Window: "5h", UsedPct: memoryFloat(24), Source: "fixture"}))
	originalNodes, _ := readQuotaAPI(t, original)
	if len(originalNodes) != 1 || originalNodes[0].State != "ok" {
		t.Fatalf("pre-restart quota fixture was not populated: %+v", originalNodes)
	}

	hub := newQuotaTestHub(t, []string{"host-a"}, policyPath, func() time.Time { return now }, slog.Default())
	nodes, raw := readQuotaAPI(t, hub)
	if len(nodes) != 1 || nodes[0].State != "unknown" || nodes[0].Latest != nil || nodes[0].LastGood != nil || bytes.Contains(raw, []byte(`"used_pct":0`)) {
		t.Fatalf("fresh hub fabricated healthy quota: %s", raw)
	}
	after, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) || len(after) != 1 || before[0].Name() != after[0].Name() {
		t.Fatalf("in-memory quota cache wrote persistence files: before=%v after=%v", before, after)
	}
	agent := &hubAgent{}
	hub.connect("host-a", "fixture", "fixture", agent, true)
	sendQuotaHeartbeat(t, hub, "host-a", agent, quotaSnapshot("2026-09-09T05:58:50Z", HubQuotaPool{Pool: "claude", AccountFP: "a1b2c3d4", Window: "5h", UsedPct: memoryFloat(24), Source: "fixture"}))
	nodes, _ = readQuotaAPI(t, hub)
	if nodes[0].State != "ok" || nodes[0].Latest == nil || !sameMemoryFloat(nodes[0].Latest.Pools[0].UsedPct, 24) {
		t.Fatalf("heartbeat did not rebuild restart-empty cache: %+v", nodes[0])
	}
}

func TestPlacementQuotaPolicyHotReloadAndExpiry(t *testing.T) {
	now := time.Date(2026, 9, 9, 6, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "placement.json")
	writeQuotaPolicy(t, path, nil, nil)
	hub := newQuotaTestHub(t, []string{"host-a"}, path, func() time.Time { return now }, slog.Default())
	hub.connect("host-a", "fixture", "fixture", &hubAgent{}, true)
	allowed := hub.placementWithQuota(t.Context(), "worker", "reload", "claude", "a1b2c3d4")
	if allowed.Decision != "host-a" || allowed.Quota == nil || allowed.Quota.Decision != "allow" {
		t.Fatalf("initial quota decision=%+v", allowed)
	}

	writeQuotaPolicy(t, path, []PlacementQuotaRule{{Pool: "claude", AccountFP: "a1b2c3d4"}}, nil)
	touchQuotaPolicy(t, path, now.Add(time.Minute))
	excluded := hub.placementWithQuota(t.Context(), "worker", "reload", "claude", "a1b2c3d4")
	if excluded.Decision != "unavailable" || excluded.Quota == nil || excluded.Quota.Decision != "deny" || excluded.Quota.PolicyStatus != "current" || !strings.Contains(excluded.Candidates[0].Reason, "quota_excluded") {
		t.Fatalf("changed policy did not bypass 30s cache: %+v", excluded)
	}

	past := now.Add(-time.Minute).Format(time.RFC3339)
	writeQuotaPolicy(t, path, []PlacementQuotaRule{{Pool: "claude", AccountFP: "a1b2c3d4", Until: past}}, nil)
	touchQuotaPolicy(t, path, now.Add(2*time.Minute))
	expired := hub.placementWithQuota(t.Context(), "worker", "reload", "claude", "a1b2c3d4")
	if expired.Decision != "host-a" || expired.Quota == nil || expired.Quota.Decision != "allow" {
		t.Fatalf("expired exclude remained active: %+v", expired)
	}

	writeQuotaPolicy(t, path, nil, []PlacementQuotaRule{{Pool: "claude"}})
	touchQuotaPolicy(t, path, now.Add(3*time.Minute))
	boosted := hub.placementWithQuota(t.Context(), "worker", "reload", "claude", "a1b2c3d4")
	if boosted.Decision != "host-a" || boosted.Quota == nil || boosted.Quota.Decision != "boost" || boosted.Candidates[0].Score != allowed.Candidates[0].Score+10 {
		t.Fatalf("quota boost was not reflected in candidate judgement: base=%+v boost=%+v", allowed, boosted)
	}
}

func TestPlacementQuotaPolicyCorruptionRejectsStartupOrStaysStale(t *testing.T) {
	now := time.Date(2026, 9, 9, 6, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "placement.json")
	writeQuotaPolicy(t, path, []PlacementQuotaRule{{Pool: "claude"}}, nil)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	hub := newQuotaTestHub(t, []string{"host-a"}, path, func() time.Time { return now }, logger)
	hub.connect("host-a", "fixture", "fixture", &hubAgent{}, true)
	first := hub.placementWithQuota(t.Context(), "worker", "stale", "claude", "")
	if first.Decision != "unavailable" || first.Quota == nil || first.Quota.Decision != "deny" {
		t.Fatalf("valid exclude was not active: %+v", first)
	}
	if err := os.WriteFile(path, []byte(`{"local_machine":`), 0600); err != nil {
		t.Fatal(err)
	}
	touchQuotaPolicy(t, path, now.Add(time.Minute))
	stale := hub.placementWithQuota(t.Context(), "worker", "stale", "claude", "")
	if stale.Decision != "unavailable" || stale.Quota == nil || stale.Quota.Decision != "deny" || stale.Quota.PolicyStatus != "stale" || !strings.Contains(logs.String(), "placement policy reload failed") || !strings.Contains(logs.String(), "policy_status=stale") {
		t.Fatalf("last-good policy was not retained/observable: result=%+v logs=%q", stale, logs.String())
	}

	invalidPath := filepath.Join(t.TempDir(), "placement.json")
	if err := os.WriteFile(invalidPath, []byte(`{"local_machine":`), 0600); err != nil {
		t.Fatal(err)
	}
	invalidLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	invalidHub, err := NewHubServer(HubServerConfig{
		Tokens:              map[string]string{hubOperatorMachineID: quotaOperatorToken, "mac-work": quotaNodeAToken},
		PlacementPolicyPath: invalidPath,
		Now:                 func() time.Time { return now },
		Logger:              invalidLogger,
	})
	if invalidHub != nil || err == nil || err.Error() != "hub placement policy is invalid" {
		t.Fatalf("invalid startup policy was accepted: hub=%v err=%v", invalidHub != nil, err)
	}

	authPath := filepath.Join(t.TempDir(), "hub-auth.env")
	if err := os.WriteFile(authPath, []byte("HUB_TOKEN_operator="+quotaOperatorToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cliHub, _, code, err := newHubServerForCLI([]string{"--hub-auth", authPath, "--placement-policy", invalidPath}, invalidLogger)
	if cliHub != nil || code != ExitConditionInvalid || err == nil {
		t.Fatalf("CLI did not map invalid startup policy to condition-invalid: hub=%v code=%d err=%v", cliHub != nil, code, err)
	}
}

func TestPlacementPolicyStatusVisibleWithoutQuotaSelector(t *testing.T) {
	now := time.Date(2026, 9, 9, 6, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "placement.json")
	writeQuotaPolicy(t, path, nil, nil)
	hub := newQuotaTestHub(t, []string{"host-a"}, path, func() time.Time { return now }, slog.Default())
	hub.connect("host-a", "fixture", "fixture", &hubAgent{}, true)

	request := httptest.NewRequest(http.MethodGet, "/v1/placement?class=worker&cwd=no-pool", nil)
	request.Header.Set(hubAuthorizationHeader, "Bearer "+quotaOperatorToken)
	response := httptest.NewRecorder()
	hub.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("placement status=%d body=%s", response.Code, response.Body.String())
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &wire); err != nil {
		t.Fatal(err)
	}
	var policyStatus string
	if raw, ok := wire["policy_status"]; !ok || json.Unmarshal(raw, &policyStatus) != nil || policyStatus != "current" {
		t.Fatalf("pool-free placement omitted top-level policy status: %s", response.Body.Bytes())
	}
	if _, ok := wire["quota"]; ok {
		t.Fatalf("pool-free placement fabricated quota decision: %s", response.Body.Bytes())
	}
}

func TestHubQuotaRoutesCoexistWithR19(t *testing.T) {
	now := time.Date(2026, 9, 9, 6, 0, 0, 0, time.UTC)
	hub := newQuotaTestHub(t, []string{"host-a"}, "", func() time.Time { return now }, slog.Default())
	hub.quotaCache["host-a"] = hubQuotaCacheEntry{result: hubQuotaResult{Payload: `{"opaque":"r19"}`}, expires: now.Add(time.Minute)}

	unauthorized := httptest.NewRecorder()
	hub.Handler().ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/v1/quota", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("new quota route bypassed operator authentication: %d", unauthorized.Code)
	}

	listRequest := httptest.NewRequest(http.MethodGet, "/v1/quota", nil)
	listRequest.Header.Set(hubAuthorizationHeader, "Bearer "+quotaOperatorToken)
	listResponse := httptest.NewRecorder()
	hub.Handler().ServeHTTP(listResponse, listRequest)
	if listResponse.Code != http.StatusOK || !strings.Contains(listResponse.Body.String(), `"nodes"`) || strings.Contains(listResponse.Body.String(), `"opaque"`) {
		t.Fatalf("new quota route response=%d %q", listResponse.Code, listResponse.Body.String())
	}

	r19Request := httptest.NewRequest(http.MethodGet, "/v1/quota/host-a", nil)
	r19Request.Header.Set(hubAuthorizationHeader, "Bearer "+quotaOperatorToken)
	r19Response := httptest.NewRecorder()
	hub.Handler().ServeHTTP(r19Response, r19Request)
	if r19Response.Code != http.StatusOK || !strings.Contains(r19Response.Body.String(), `\"opaque\"`) || strings.Contains(r19Response.Body.String(), `"nodes"`) {
		t.Fatalf("R19 quota route response=%d %q", r19Response.Code, r19Response.Body.String())
	}
}

func TestHubQuotaCollectorFingerprintAndRedaction(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "scopefuel-quota.json"))
	if err != nil {
		t.Fatal(err)
	}
	environment := []string{
		"PATH=bin-selector",
		"HOME=home-selector-a",
		"CLAUDE_CONFIG_DIR=claude-selector-a",
		"CODEX_HOME=codex-selector-a",
		"GROK_HOME=grok-selector-a",
		"HUB_TOKEN=credential-token-value",
	}
	snapshot, err := parseScopefuelHubQuota(data, environment)
	if err != nil || len(snapshot.Pools) != 4 {
		t.Fatalf("scopefuel fixture parse=%+v err=%v", snapshot, err)
	}
	wantIdentity := map[string]string{
		"claude": "CLAUDE_CONFIG_DIR=claude-selector-a|HOME=home-selector-a",
		"codex":  "CODEX_HOME=codex-selector-a|HOME=home-selector-a",
		"grok":   "GROK_HOME=grok-selector-a|HOME=home-selector-a",
		"kiro":   "HOME=home-selector-a",
	}
	selectors := hubQuotaSelectors(environment)
	for pool, identity := range wantIdentity {
		got, ok := hubQuotaAccountFingerprint(pool, selectors)
		if !ok || len(got) != 8 || got == identity || strings.Contains(got, "selector") {
			t.Fatalf("%s account fingerprint exposed identity: got=%q ok=%t", pool, got, ok)
		}
	}
	for _, row := range snapshot.Pools {
		digest := sha256.Sum256([]byte(wantIdentity[row.Pool]))
		want := hex.EncodeToString(digest[:])[:8]
		if row.AccountFP != want {
			t.Fatalf("%s account_fp=%q want sha8=%q", row.Pool, row.AccountFP, want)
		}
		if row.Pool == "kiro" && (row.Window != "unknown" || row.UsedPct != nil || row.ResetAt != nil || row.Source != "unavailable") {
			t.Fatalf("unavailable provider mapping=%+v", row)
		}
	}
	heartbeat, err := json.Marshal(hubHeartbeatPayload{Status: "alive", Quota: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range append([]string{"credential-token-value"}, selectorValues(environment)...) {
		if strings.Contains(string(heartbeat), forbidden) {
			t.Fatalf("credential selector leaked into heartbeat (%q): %s", forbidden, heartbeat)
		}
	}
	for _, identity := range wantIdentity {
		if strings.Contains(string(heartbeat), identity) {
			t.Fatalf("raw identity leaked into heartbeat: %s", heartbeat)
		}
	}
	filtered := hubQuotaScopefuelEnvironment(environment)
	if strings.Contains(strings.Join(filtered, "\n"), "HUB_TOKEN") || !strings.Contains(strings.Join(filtered, "\n"), "GROK_HOME=grok-selector-a") {
		t.Fatalf("scopefuel environment allowlist=%v", filtered)
	}
}

func TestHeartbeatQuotaCollectionFailureIsExplicitAndDistinct(t *testing.T) {
	client := &HubClient{
		jobsInboxRoot:       t.TempDir(),
		assignedJobs:        map[string]uint64{},
		completedJobs:       map[string]uint64{},
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, errors.New("fixture") },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, errors.New("fixture") },
		quotaCollector:      func(context.Context) (*HubQuotaSnapshot, error) { return nil, errors.New("fixture") },
	}
	event := client.heartbeatEvent(t.Context())
	heartbeat, ok := decodeHubHeartbeatPayload(event.Payload)
	if !ok || heartbeat.Quota == nil || heartbeat.Quota.Status != "unavailable" || heartbeat.Quota.CollectedAt != nil || heartbeat.Quota.Pools == nil || len(heartbeat.Quota.Pools) != 0 || !bytes.Contains(event.Payload, []byte(`"quota":{"status":"unavailable","collected_at":null,"pools":[]}`)) {
		t.Fatalf("collector failure was omitted or fabricated: %s", event.Payload)
	}

	now := time.Date(2026, 9, 9, 6, 0, 0, 0, time.UTC)
	hub := newQuotaTestHub(t, []string{"host-a", "host-b", "host-c"}, "", func() time.Time { return now }, slog.Default())
	agentA, agentB, agentC := &hubAgent{}, &hubAgent{}, &hubAgent{}
	hub.connect("host-a", "fixture", "fixture", agentA, true)
	hub.connect("host-b", "fixture", "fixture", agentB, true)
	hub.connect("host-c", "fixture", "fixture", agentC, true)
	sendQuotaHeartbeat(t, hub, "host-a", agentA, quotaSnapshot("2026-09-09T05:58:50Z", HubQuotaPool{Pool: "claude", AccountFP: "a1b2c3d4", Window: "5h", UsedPct: memoryFloat(24), Source: "fixture"}))
	sendQuotaHeartbeat(t, hub, "host-b", agentB, quotaSnapshot("2026-09-09T05:58:50Z", HubQuotaPool{Pool: "kiro", AccountFP: "b1c2d3e4", Window: "7d", UsedPct: nil, ResetAt: nil, Source: "unavailable"}))
	now = now.Add(time.Minute)
	sendRawQuotaHeartbeat(t, hub, "host-a", agentA, event.Payload)
	sendRawQuotaHeartbeat(t, hub, "host-c", agentC, []byte(`{"status":"alive"}`))
	nodes, _ := readQuotaAPI(t, hub)
	byMachine := map[string]HubQuotaNode{}
	for _, node := range nodes {
		byMachine[node.MachineID] = node
	}
	if byMachine["host-a"].State != "unavailable" || byMachine["host-a"].Latest == nil || byMachine["host-a"].LastGood == nil || !sameMemoryFloat(byMachine["host-a"].LastGood.Pools[0].UsedPct, 24) {
		t.Fatalf("explicit unavailable lost last-good or became zero: %+v", byMachine["host-a"])
	}
	if byMachine["host-b"].State != "ok" || byMachine["host-b"].Latest == nil || len(byMachine["host-b"].Latest.Pools) != 1 || byMachine["host-b"].Latest.Pools[0].UsedPct != nil {
		t.Fatalf("explicit null was fabricated as healthy zero: %+v", byMachine["host-b"])
	}
	if byMachine["host-c"].State != "legacy" || byMachine["host-c"].Latest != nil {
		t.Fatalf("legacy absence was not distinct from unavailable: %+v", byMachine["host-c"])
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
			hubURL := "http://fixture.invalid"
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
	for _, required := range []string{"## Heartbeat field", "| field | JSON type | `null` meaning", "## Quota policy", "## Deployment order", "## Example JSON", "## wrk adapter contract", "last-good", "stale", "restart", "partial", "scopefuel", "profile-to-pool", "path-derived hint", "rollback", "exit 3", "exit 4"} {
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

func newQuotaTestHub(t *testing.T, machines []string, policyPath string, now func() time.Time, logger *slog.Logger) *HubServer {
	t.Helper()
	tokens := map[string]string{hubOperatorMachineID: quotaOperatorToken}
	for index, machine := range machines {
		if index == 0 {
			tokens[machine] = quotaNodeAToken
		} else {
			tokens[machine] = quotaNodeBToken
		}
	}
	hub, err := NewHubServer(HubServerConfig{Tokens: tokens, PlacementPolicyPath: policyPath, Now: now, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

func writeQuotaPolicy(t *testing.T, path string, exclude, boost []PlacementQuotaRule) {
	t.Helper()
	policy := PlacementPolicy{LocalMachine: "host-a", MaxActiveJobs: 5, LoadRatio: .5, QuotaExclude: exclude, QuotaBoost: boost}
	data, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func touchQuotaPolicy(t *testing.T, path string, modTime time.Time) {
	t.Helper()
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatal(err)
	}
}

func quotaSnapshot(collectedAt string, rows ...HubQuotaPool) *HubQuotaSnapshot {
	return &HubQuotaSnapshot{Status: "ok", CollectedAt: &collectedAt, Pools: rows}
}

func quotaString(value string) *string { return &value }

func sendQuotaHeartbeat(t *testing.T, hub *HubServer, machine string, agent *hubAgent, snapshot *HubQuotaSnapshot) {
	t.Helper()
	payload, err := json.Marshal(hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}, Quota: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	sendRawQuotaHeartbeat(t, hub, machine, agent, payload)
}

func sendRawQuotaHeartbeat(t *testing.T, hub *HubServer, machine string, agent *hubAgent, heartbeat []byte) {
	t.Helper()
	envelope := append([]byte(`{"type":"event","kind":"heartbeat","payload":`), heartbeat...)
	envelope = append(envelope, '}')
	hub.handleAgentMessage(machine, "fixture", agent, envelope)
}

func readQuotaAPI(t *testing.T, hub *HubServer) ([]HubQuotaNode, []byte) {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/v1/quota", nil)
	request.Header.Set(hubAuthorizationHeader, "Bearer "+quotaOperatorToken)
	response := httptest.NewRecorder()
	hub.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("quota API status=%d body=%q", response.Code, response.Body.String())
	}
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Nodes []HubQuotaNode `json:"nodes"`
	}
	if json.Unmarshal(raw, &document) != nil {
		t.Fatalf("quota API malformed: %s", raw)
	}
	return document.Nodes, raw
}

func selectorValues(environment []string) []string {
	values := make([]string, 0, len(environment))
	for _, entry := range environment {
		name, value, found := strings.Cut(entry, "=")
		if found && (name == "HOME" || name == "CLAUDE_CONFIG_DIR" || name == "CODEX_HOME" || name == "GROK_HOME") {
			values = append(values, value)
		}
	}
	sort.Strings(values)
	return values
}

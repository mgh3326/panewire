package panewire

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"testing"
)

// fleetEventLabels are the only labels the Alloy pipeline puts on fleet
// events (plus source and the external machine_id/env); a query that selects
// on job, reason or detail as a stream label would match nothing.
var fleetEventStreamSelector = regexp.MustCompile(`\{([^{}]*source="fleet_events"[^{}]*)\}`)

func checkFleetEventQuery(t *testing.T, where, expr string) {
	t.Helper()
	selector := fleetEventStreamSelector.FindStringSubmatch(expr)
	if selector == nil {
		t.Fatalf("%s: query does not select source=\"fleet_events\": %s", where, expr)
	}
	for _, matcher := range strings.Split(selector[1], ",") {
		name, _, _ := strings.Cut(strings.TrimSpace(matcher), "=")
		name = strings.TrimRight(name, "!~")
		switch name {
		case "source", "host", "os_host", "tool", "kind", "outcome":
		default:
			t.Fatalf("%s: selects on %q, which is not a fleet event label: %s", where, name, expr)
		}
	}
	if strings.Count(expr, "(") != strings.Count(expr, ")") || strings.Count(expr, "[") != strings.Count(expr, "]") {
		t.Fatalf("%s: unbalanced query: %s", where, expr)
	}
}

func TestGrafanaDashboardIsValidAndUsesTheLabelContract(t *testing.T) {
	raw, err := os.ReadFile("deploy/grafana/fleet-dashboard.json")
	if err != nil {
		t.Fatal(err)
	}
	var dashboard struct {
		UID    string `json:"uid"`
		Panels []struct {
			Title   string `json:"title"`
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(raw, &dashboard); err != nil {
		t.Fatal(err)
	}
	titles := map[string]bool{}
	for _, panel := range dashboard.Panels {
		titles[panel.Title] = true
		if len(panel.Targets) == 0 {
			t.Fatalf("panel %q has no query", panel.Title)
		}
		for _, target := range panel.Targets {
			checkFleetEventQuery(t, panel.Title, target.Expr)
		}
	}
	for _, want := range []string{"Spawn refusals by host and reason", "Review outcomes", "Node spawn refusals by reason", "Quota observation age (minutes) at refusal"} {
		if !titles[want] {
			t.Fatalf("dashboard lacks panel %q", want)
		}
	}
	panelsByTitle := map[string]string{}
	for _, panel := range dashboard.Panels {
		if len(panel.Targets) > 0 {
			panelsByTitle[panel.Title] = panel.Targets[0].Expr
		}
	}
	placement := panelsByTitle["Placements by chosen host and ranking"]
	if !strings.Contains(placement, `kind="placement"`) || !strings.Contains(placement, `request_id="request_id"`) || !strings.Contains(placement, `request_id != ""`) {
		t.Errorf("placements must select hub records by request_id presence: %s", placement)
	}
	refusals := panelsByTitle["Spawn refusals by host and reason"]
	if !strings.Contains(refusals, `kind="spawn"`) || strings.Contains(refusals, `node_spawn`) {
		t.Errorf("refusal panel must count the hub's final spawn result only: %s", refusals)
	}
	nodeRefusals := panelsByTitle["Node spawn refusals by reason"]
	if !strings.Contains(nodeRefusals, `outcome=~"refused|failed"`) {
		t.Errorf("node refusal panel must filter outcomes: %s", nodeRefusals)
	}
	for title, kind := range map[string]string{
		"Placement refusals by reason": "kind=\"placement\"",
		"Quota shadow agreement by kind": "kind=~\"quota_gate_shadow|quota_pools_shadow\"",
		"Placement candidate exclusion records": "kind=\"placement\"",
		"Placement decisions: placed vs final refusals by host": "kind=\"placement\"",
		"node-local placement records (direct spawn + hub spawn node record)": "kind=\"placement\"",
	} {
		if !strings.Contains(panelsByTitle[title], kind) {
			t.Errorf("dashboard lacks a query for %q with %s: %s", title, kind, panelsByTitle[title])
		}
	}
	if !strings.Contains(panelsByTitle["Placement refusals by reason"], `request_id="request_id"`) || !strings.Contains(panelsByTitle["Placement refusals by reason"], `request_id != ""`) {
		t.Errorf("placement refusals must select hub records by request_id: %s", panelsByTitle["Placement refusals by reason"])
	}
	if !strings.Contains(panelsByTitle["Placement decisions: placed vs final refusals by host"], `outcome=~"placed|refused"`) || !strings.Contains(panelsByTitle["Placement decisions: placed vs final refusals by host"], `request_id != ""`) {
		t.Errorf("placement decisions must compare hub placed and refused outcomes: %s", panelsByTitle["Placement decisions: placed vs final refusals by host"])
	}
	if !strings.Contains(panelsByTitle["node-local placement records (direct spawn + hub spawn node record)"], `request_id = ""`) {
		t.Errorf("node-local placement panel must select missing request_id records: %s", panelsByTitle["node-local placement records (direct spawn + hub spawn node record)"])
	}
	if !strings.Contains(panelsByTitle["Spawn stuck and held by branch"], `kind="spawn"`) || !strings.Contains(panelsByTitle["Spawn stuck and held by branch"], `outcome="stuck"`) {
		t.Errorf("stuck series must select spawn outcome=stuck: %s", panelsByTitle["Spawn stuck and held by branch"])
	}
	for _, panel := range dashboard.Panels {
		if panel.Title == "Spawn stuck and held by branch" && (len(panel.Targets) != 2 || !strings.Contains(panel.Targets[1].Expr, `kind="spawn_held"`)) {
			t.Errorf("held series must separately select spawn_held: %+v", panel.Targets)
		}
	}
	if strings.Contains(strings.ToLower(panelsByTitle["Placement decisions: placed vs final refusals by host"]), "completed") {
		t.Error("placement decision panel must not claim completion")
	}
	if dashboard.UID != "fleet-observability" {
		t.Fatalf("uid %q", dashboard.UID)
	}
}

type grafanaRuleGroup struct {
	Title     string `json:"title"`
	FolderUID string `json:"folderUid"`
	Interval  int    `json:"interval"`
	Rules     []struct {
		UID       string `json:"uid"`
		Title     string `json:"title"`
		Condition string `json:"condition"`
		FolderUID string `json:"folderUID"`
		RuleGroup string `json:"ruleGroup"`
		Data      []struct {
			RefID         string         `json:"refId"`
			DatasourceUID string         `json:"datasourceUid"`
			Model         map[string]any `json:"model"`
		} `json:"data"`
	} `json:"rules"`
}

func TestGrafanaAlertRulesAreValid(t *testing.T) {
	raw, err := os.ReadFile("deploy/grafana/fleet-alert-rules.json")
	if err != nil {
		t.Fatal(err)
	}
	var group grafanaRuleGroup
	if err := json.Unmarshal(raw, &group); err != nil {
		t.Fatal(err)
	}
	if group.Interval <= 0 || group.FolderUID == "" || group.Title == "" {
		t.Fatalf("group: %+v", group)
	}
	titles := map[string]bool{}
	for _, rule := range group.Rules {
		titles[rule.Title] = true
		if rule.UID == "" || rule.Condition != "C" || rule.FolderUID != group.FolderUID || rule.RuleGroup != group.Title || len(rule.Data) != 2 {
			t.Fatalf("rule %q is incomplete", rule.Title)
		}
		query, threshold := rule.Data[0], rule.Data[1]
		if query.RefID != "A" || query.DatasourceUID != "${LOKI_DATASOURCE_UID}" || threshold.RefID != "C" || threshold.DatasourceUID != "__expr__" || threshold.Model["expression"] != "A" {
			t.Fatalf("rule %q data wiring", rule.Title)
		}
		expr, _ := query.Model["expr"].(string)
		checkFleetEventQuery(t, rule.Title, expr)
		// Count rules see "no data" when healthy; only the heartbeat may
		// stand in for a dead pipeline, through absent_over_time.
		if rule.Title == "Fleet events pipeline silent" && !strings.HasPrefix(expr, "absent_over_time(") {
			t.Fatalf("heartbeat must use absent_over_time: %s", expr)
		}
		if strings.Contains(expr, "by (host, reason)") && !strings.Contains(expr, "unspecified") {
			t.Fatalf("%s: a missing reason must group as unspecified", rule.Title)
		}
		if rule.Title == "Repeated spawn refusals" && (!strings.Contains(expr, `kind="spawn"`) || strings.Contains(expr, "node_spawn")) {
			t.Errorf("refusal alert must count the hub's final spawn result only: %s", expr)
		}
	}
	for _, want := range []string{"Repeated spawn refusals", "Review missing", "Reviewer failing repeatedly", "Quota observation stale", "Fleet events pipeline silent"} {
		if !titles[want] {
			t.Fatalf("alert rules lack %q", want)
		}
	}
}

// grafanaApplyRequests splits apply.sh --dry-run output into requests.
func grafanaApplyRequests(t *testing.T, output string) map[string]string {
	t.Helper()
	requests := map[string]string{}
	for _, part := range strings.Split(output, "### ")[1:] {
		head, body, _ := strings.Cut(part, "\n")
		if strings.HasPrefix(head, "GET ") {
			requests[head] = ""
			continue
		}
		var value any
		if err := json.Unmarshal([]byte(body), &value); err != nil {
			t.Fatalf("dry-run body for %s is not JSON: %v", head, err)
		}
		requests[head] = body
	}
	return requests
}

func TestGrafanaApplyDryRunNeedsNoCredentials(t *testing.T) {
	command := exec.Command("sh", "deploy/grafana/apply.sh", "--dry-run")
	command.Env = []string{"PATH=" + os.Getenv("PATH")}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("dry run: %v\n%s", err, output)
	}
	requests := grafanaApplyRequests(t, string(output))
	for _, want := range []string{
		"GET https://grafana.invalid/api/datasources",
		"POST https://grafana.invalid/api/folders",
		"POST https://grafana.invalid/api/dashboards/db",
		"PUT https://grafana.invalid/api/v1/provisioning/folder/fleet-observability/rule-groups/fleet-observability",
	} {
		if _, ok := requests[want]; !ok {
			t.Fatalf("dry run lacks %s:\n%s", want, output)
		}
	}
	if strings.Contains(string(output), "Authorization") || strings.Contains(string(output), "${LOKI_DATASOURCE_UID}") {
		t.Fatalf("dry run leaks a header or an unsubstituted uid:\n%s", output)
	}
	missing := exec.Command("sh", "deploy/grafana/apply.sh")
	missing.Env = []string{"PATH=" + os.Getenv("PATH")}
	if output, err := missing.CombinedOutput(); err == nil || !strings.Contains(string(output), "GRAFANA_CLOUD_URL is required") {
		t.Fatalf("a real run without credentials must refuse: %v %s", err, output)
	}
}

func TestGrafanaApplySendsIdempotentRequests(t *testing.T) {
	if _, err := exec.LookPath("curl"); err != nil {
		t.Skip("curl is not installed")
	}
	var mu sync.Mutex
	seen := map[string]string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		seen[r.Method+" "+r.URL.Path] = r.Header.Get("Authorization") + "\n" + string(body)
		mu.Unlock()
		if r.URL.Path == "/api/datasources" {
			_, _ = w.Write([]byte(`[{"uid":"prom-uid","type":"prometheus","name":"grafanacloud-x-prom"},{"uid":"loki-uid-1","type":"loki","name":"grafanacloud-x-logs"},{"uid":"loki-other","type":"loki","name":"scratch-loki"}]`))
			return
		}
		if r.URL.Path == "/api/folders" {
			w.WriteHeader(http.StatusConflict) // the folder already exists
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	command := exec.Command("sh", "deploy/grafana/apply.sh")
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "GRAFANA_CLOUD_URL=" + server.URL + "/", "GRAFANA_CLOUD_SA_TOKEN=fake-token-0123"}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("apply: %v\n%s", err, output)
	}
	mu.Lock()
	defer mu.Unlock()
	rules := seen["PUT /api/v1/provisioning/folder/fleet-observability/rule-groups/fleet-observability"]
	if len(seen) != 4 || !strings.HasPrefix(seen["GET /api/datasources"], "Bearer fake-token-0123\n") || !strings.HasPrefix(rules, "Bearer fake-token-0123\n") || !strings.Contains(rules, `"datasourceUid": "loki-uid-1"`) {
		t.Fatalf("requests: %v", seen)
	}
	if strings.Contains(string(output), "fake-token-0123") {
		t.Fatal("apply printed the token")
	}
}

func TestGrafanaApplyReportsAMissingEditorRole(t *testing.T) {
	if _, err := exec.LookPath("curl"); err != nil {
		t.Skip("curl is not installed")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/datasources" {
			_, _ = w.Write([]byte(`[{"uid":"loki-uid-1","type":"loki","name":"grafanacloud-x-logs"}]`))
			return
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()
	command := exec.Command("sh", "deploy/grafana/apply.sh")
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "GRAFANA_CLOUD_URL=" + server.URL, "GRAFANA_CLOUD_SA_TOKEN=fake-token-0123"}
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "needs Editor") {
		t.Fatalf("a viewer token must fail with the role named: %v %s", err, output)
	}
}

func TestGrafanaApplyRefusesAMalformedDatasourceUID(t *testing.T) {
	if _, err := exec.LookPath("curl"); err != nil {
		t.Skip("curl is not installed")
	}
	var writes int
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/datasources" {
			_, _ = w.Write([]byte(`[{"uid":"../x y","type":"loki","name":"grafanacloud-x-logs"}]`))
			return
		}
		mu.Lock()
		writes++
		mu.Unlock()
	}))
	defer server.Close()
	command := exec.Command("sh", "deploy/grafana/apply.sh")
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "GRAFANA_CLOUD_URL=" + server.URL, "GRAFANA_CLOUD_SA_TOKEN=fake-token-0123"}
	output, err := command.CombinedOutput()
	mu.Lock()
	defer mu.Unlock()
	if err == nil || !strings.Contains(string(output), "unexpected Loki data source uid") || writes != 0 {
		t.Fatalf("a malformed uid must stop before any write: %v writes=%d %s", err, writes, output)
	}
}

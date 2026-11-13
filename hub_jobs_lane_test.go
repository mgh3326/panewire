package panewire

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestJobsLaneListing(t *testing.T) {
	hub, server := pw2Hub(t, nil, "", nil)
	hub.jobs["job-a"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-a", OwnerLane: "lane-a", Pane: "pane-a", StartedAt: "start", LastEventKind: "job.done"}, Node: "node-b"}
	hub.jobs["job-b"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-b", OwnerLane: "lane-a"}, Node: "node-a", Completed: true}
	hub.jobs["job-c"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-c", OwnerLane: "lane-b"}, Node: "node-a"}
	hub.jobs["job-d"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-d", OwnerLane: "lane-a"}, Node: "node-b", Orphaned: true}
	node := pw2Caller{"node", "node-a", pw2NodeAToken}
	for _, tc := range []struct {
		path   string
		status int
	}{{"/v1/jobs", 401}, {"/v1/jobs?owner_lane=lane-b", 403}, {"/v1/jobs?owner_lane=bad%2Fname", 400}} {
		status, _ := node.do(t, server, http.MethodGet, tc.path, "")
		if status != tc.status {
			t.Fatalf("%s: %d", tc.path, status)
		}
	}
	status, body := node.do(t, server, http.MethodGet, "/v1/jobs?owner_lane=lane-a", "")
	var result struct {
		Jobs []hubConsoleJob `json:"jobs"`
	}
	if status != 200 || json.Unmarshal([]byte(body), &result) != nil || len(result.Jobs) != 3 || result.Jobs[0].JobID != "job-a" || result.Jobs[0].State != "active" || result.Jobs[1].State != "completed" || result.Jobs[2].State != "orphaned" {
		t.Fatalf("status=%d body=%s", status, body)
	}
	operator := pw2Caller{"operator", "", pw2OperatorToken}
	_, body = operator.do(t, server, http.MethodGet, "/v1/jobs", "")
	if strings.Contains(body, "job-b") || !strings.Contains(body, "job-c") {
		t.Fatal(body)
	}
	_, body = operator.do(t, server, http.MethodGet, "/v1/jobs?include_completed=true", "")
	if !strings.Contains(body, `"state":"completed"`) {
		t.Fatal(body)
	}
}

func TestJobsListCLI(t *testing.T) {
	hub, server := pw2Hub(t, nil, "", nil)
	hub.jobs["job-a"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-a", OwnerLane: "lane-a", Pane: "pane-a", StartedAt: "start", LastEventKind: "job.done"}, Node: "node-b", Completed: true}
	token := filepath.Join(t.TempDir(), "node.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=node-a\nHUB_TOKEN="+pw2NodeAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PANEWIRE_HUB_URL", server.URL)
	t.Setenv("PANEWIRE_HUB_TOKEN_ENV", token)
	t.Setenv("PANEWIRE_HUB_CF_ENV", "")
	var out, errs bytes.Buffer
	deps := t441Deps(server)
	if code := runJobsCLI([]string{"list", "--lane", "lane-a"}, &out, &errs, deps); code != 0 || out.String() != "job-a\tpane-a\tnode-b\tcompleted\tstart\tjob.done\n" {
		t.Fatalf("code=%d out=%q err=%q", code, out.String(), errs.String())
	}
	out.Reset()
	errs.Reset()
	if code := runJobsCLI([]string{"list", "--lane", "lane-a", "--json", "--hub-url", server.URL, "--hub-token-env", token}, &out, &errs, deps); code != 0 {
		t.Fatalf("code=%d err=%q", code, errs.String())
	}
	_, body := (pw2Caller{"node", "node-a", pw2NodeAToken}).do(t, server, http.MethodGet, "/v1/jobs?owner_lane=lane-a&include_completed=true", "")
	if out.String() != body {
		t.Fatalf("got %q want %q", out.String(), body)
	}
}

func TestJobsListEstablishedLaneName(t *testing.T) {
	hub, server := pw2Hub(t, nil, "", nil)
	if err := os.WriteFile(hub.reportRelayPath, []byte(`{"lanes":{"legacy.parent":{"machine":"node-a","pane":"pane-a"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	hub.jobs["job-a"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-a", OwnerLane: "legacy.parent"}, Node: "node-b"}
	token := filepath.Join(t.TempDir(), "node.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=node-a\nHUB_TOKEN="+pw2NodeAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	status, body := (pw2Caller{"node", "node-a", pw2NodeAToken}).do(t, server, http.MethodGet, "/v1/jobs?owner_lane=legacy.parent", "")
	if status != http.StatusOK || !strings.Contains(body, `"job_id":"job-a"`) {
		t.Errorf("status=%d body=%s", status, body)
	}
	var out, errs bytes.Buffer
	code := runJobsCLI([]string{"list", "--lane", "legacy.parent", "--hub-url", server.URL, "--hub-token-env", token, "--json"}, &out, &errs, t441Deps(server))
	if code != ExitOK || !strings.Contains(out.String(), `"job_id":"job-a"`) {
		t.Errorf("code=%d out=%q err=%q", code, out.String(), errs.String())
	}
}

func TestNormalizeHubActiveJobRetainsHistoricalOwnerLane(t *testing.T) {
	job := HubActiveJob{JobID: "job-a", AgentLabel: "agent-a", OwnerLane: "legacy.parent", Epoch: 1}
	normalizeHubActiveJobMetadata(&job)
	if job.OwnerLane != "legacy.parent" {
		t.Fatalf("historical owner lane was dropped: %+v", job)
	}
}

func TestJobsListLateCompletion(t *testing.T) {
	hub, server := pw2Hub(t, nil, "", nil)
	agent := &hubAgent{transient: true}
	hub.handleAgentMessage("node-b", "fixture", agent, []byte(`{"type":"event","kind":"job.completed","payload":{"job_id":"job-late","epoch":1,"agent_label":"agent-a","owner_lane":"lane-a"}}`))
	status, body := (pw2Caller{"node", "node-a", pw2NodeAToken}).do(t, server, http.MethodGet, "/v1/jobs?owner_lane=lane-a", "")
	var result struct {
		Jobs []hubConsoleJob `json:"jobs"`
	}
	if status != http.StatusOK || json.Unmarshal([]byte(body), &result) != nil || len(result.Jobs) != 1 || result.Jobs[0].JobID != "job-late" || result.Jobs[0].State != "completed" || result.Jobs[0].OwnerLane != "lane-a" {
		t.Fatalf("status=%d body=%s", status, body)
	}
}

func TestJobsListTrustsRemoteNodeAttribution(t *testing.T) {
	hub, server := pw2Hub(t, nil, "", nil)
	remote := pw2Caller{"remote", "node-b", pw2NodeBToken}
	if status, _ := remote.do(t, server, http.MethodGet, "/v1/jobs?owner_lane=lane-a", ""); status != http.StatusForbidden {
		t.Fatalf("remote lane read: got %d, want 403", status)
	}
	// A node may report an owner lane on another machine even though it cannot
	// read that lane's listing. The reporting node is trusted for attribution.
	agent := &hubAgent{transient: true}
	hub.handleAgentMessage("node-b", "fixture", agent, []byte(`{"type":"event","kind":"job.completed","payload":{"job_id":"job-reported","epoch":1,"agent_label":"agent-a","owner_lane":"lane-a"}}`))
	status, body := (pw2Caller{"owner", "node-a", pw2NodeAToken}).do(t, server, http.MethodGet, "/v1/jobs?owner_lane=lane-a", "")
	var result struct {
		Jobs []hubConsoleJob `json:"jobs"`
	}
	if status != http.StatusOK || json.Unmarshal([]byte(body), &result) != nil || len(result.Jobs) != 1 || result.Jobs[0].JobID != "job-reported" || result.Jobs[0].Machine != "node-b" || result.Jobs[0].State != "completed" {
		t.Fatalf("status=%d body=%s", status, body)
	}
}

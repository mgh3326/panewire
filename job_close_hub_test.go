package panewire

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
)

func TestJobCloseViaHub(t *testing.T) {
	f := newR19eFixture(t)
	stopWorker := f.connectWorker(t)
	eventually(t, time.Second, "worker connected", func() bool {
		f.hub.mu.Lock()
		defer f.hub.mu.Unlock()
		n := f.hub.nodes[r19eWorkerNode]
		return n != nil && n.agent != nil
	})
	sender, err := NewHubClient(HubClientConfig{URL: r6WSURL(f.server.URL, ""), MachineID: r19eLaneNode, Token: r6NodeBToken, AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	socket := inboxDaemonSocket(t, NewDaemon(Config{Hub: HubDaemonConfig{Client: sender}}))
	t.Setenv("ARBITER_INBOX_ROOT", t.TempDir())
	jobDir := filepath.Join(f.inbox, "jobs", "remote", "events")
	if err := os.MkdirAll(jobDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(jobDir, "00001-job.claim.json"), []byte(`{"kind":"job.claim","payload":{"owner_lane":"lane-a"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	f.hub.mu.Lock()
	f.hub.jobs["remote"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "remote", OwnerLane: "lane-a", Epoch: 1}, Node: r19eWorkerNode}
	f.hub.mu.Unlock()
	run := func(job, lane string, override bool) (int, string, string) {
		var out, errOut bytes.Buffer
		args := []string{"close", job, "--lane", lane, "--outcome", "abandoned", "--reason", "test reason"}
		if override {
			args = append(args, "--operator-override")
		}
		code := runJobCLI(args, &out, &errOut, CLIConfig{SocketPath: socket})
		return code, out.String(), errOut.String()
	}
	code, _, errOut := run("remote", "wrong", false)
	if code != jobCloseRefused || !strings.Contains(errOut, "owner_mismatch") {
		t.Fatalf("wrong lane: %d %s", code, errOut)
	}
	code, _, errOut = run("unknown", "lane-a", false)
	if code != jobCloseRefused || !strings.Contains(errOut, "job_unknown") {
		t.Fatalf("unknown: %d %s", code, errOut)
	}
	entries, _ := os.ReadDir(jobDir)
	if len(entries) != 1 {
		t.Fatalf("wrong lane wrote records: %v", entries)
	}
	code, out, errOut := run("remote", "wrong", true)
	if code != 0 || !strings.Contains(out, "OK job=remote kind=job.revoked") || !strings.Contains(out, "override=operator") {
		t.Fatalf("close: %d %q %q", code, out, errOut)
	}
	files, err := filepath.Glob(filepath.Join(jobDir, "*-job.revoked.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("files %v %v", files, err)
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var record jobCloseRecord
	if err = json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if record.OwnerLane != "lane-a" || record.ClosedBy != "wrong" || record.Override != "operator" || record.Source != jobCloseHubSource || record.Reason != "test reason" {
		t.Fatalf("record %+v", record)
	}
	stopWorker()
	f.connectWorker(t)
	eventually(t, 3*time.Second, "hub observes revocation", func() bool {
		f.hub.mu.Lock()
		defer f.hub.mu.Unlock()
		return f.hub.jobs["remote"].Completed
	})
}

func TestJobCloseHubOwnerAndOffline(t *testing.T) {
	f := newR19eFixture(t)
	stop := f.connectWorker(t)
	eventually(t, time.Second, "worker connected", func() bool {
		f.hub.mu.Lock()
		defer f.hub.mu.Unlock()
		n := f.hub.nodes[r19eWorkerNode]
		return n != nil && n.agent != nil
	})
	sender, err := NewHubClient(HubClientConfig{URL: r6WSURL(f.server.URL, ""), MachineID: r19eLaneNode, Token: r6NodeBToken, AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	socket := inboxDaemonSocket(t, NewDaemon(Config{Hub: HubDaemonConfig{Client: sender}}))
	t.Setenv("ARBITER_INBOX_ROOT", t.TempDir())
	for _, job := range []string{"active", "offline"} {
		dir := filepath.Join(f.inbox, "jobs", job, "events")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "00001-job.claim.json"), []byte(`{"kind":"job.claim","payload":{"owner_lane":"lane-a"}}`), 0600); err != nil {
			t.Fatal(err)
		}
		f.hub.mu.Lock()
		f.hub.jobs[job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: job, OwnerLane: "lane-a", Epoch: 1}, Node: r19eWorkerNode}
		f.hub.mu.Unlock()
	}
	run := func(job string) (int, string, string) {
		var out, errOut bytes.Buffer
		rc := runJobCLI([]string{"close", job, "--lane", "lane-a", "--outcome", "completed", "--reason", "done"}, &out, &errOut, CLIConfig{SocketPath: socket})
		return rc, out.String(), errOut.String()
	}
	rc, out, errOut := run("active")
	if rc != 0 || !strings.Contains(out, "OK job=active kind=job.revoked outcome=completed closed_by=lane-a owner_lane=lane-a") {
		t.Fatalf("rc=%d out=%s err=%s", rc, out, errOut)
	}
	data, err := os.ReadFile(filepath.Join(f.inbox, "jobs", "active", "events", "00002-job.revoked.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rec jobCloseRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		t.Fatal(err)
	}
	if rec.Kind != jobCloseKind || rec.JobID != "active" || rec.OwnerLane != "lane-a" || rec.ClosedBy != "lane-a" || rec.Outcome != "completed" || rec.Reason != "done" || rec.Override != "" || rec.Source != jobCloseHubSource || rec.Host == "" || rec.CreatedAt == "" || rec.Epoch != 1 {
		t.Fatalf("record %+v", rec)
	}
	stop()
	eventually(t, time.Second, "worker disconnected", func() bool { f.hub.mu.Lock(); defer f.hub.mu.Unlock(); return f.hub.nodes[r19eWorkerNode].agent == nil })
	rc, _, errOut = run("offline")
	if rc != jobCloseRefused || !strings.Contains(errOut, "node_offline") {
		t.Fatalf("offline rc=%d err=%s", rc, errOut)
	}
	files, _ := filepath.Glob(filepath.Join(f.inbox, "jobs", "offline", "events", "*-job.revoked.json"))
	if len(files) != 0 {
		t.Fatalf("offline wrote %v", files)
	}
}

func TestJobCloseHubRoleRelease(t *testing.T) {
	hub, server, _, held := roleHolderHub(t, hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}, "spawned", "succeeded")
	node := t14Node(t, hub, server, true)
	root := t.TempDir()
	eventsDir := filepath.Join(root, "jobs", "test-job", "events")
	if err := os.MkdirAll(eventsDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(eventsDir, "00001-job.claim.json"), []byte(`{"kind":"job.claim","payload":{"owner_lane":"lane-a","agent_label":"test-job"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	hub.mu.Lock()
	hub.jobs["test-job"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "test-job", OwnerLane: "lane-a", Epoch: 1}, Node: "machine-a"}
	hub.mu.Unlock()
	sender, err := NewHubClient(HubClientConfig{URL: t14WSURL(server.URL, ""), MachineID: "machine-a", Token: "node-token", AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	socket := inboxDaemonSocket(t, NewDaemon(Config{Hub: HubDaemonConfig{Client: sender}}))
	t.Setenv("ARBITER_INBOX_ROOT", t.TempDir())
	type cliResult struct {
		code     int
		out, err string
	}
	done := make(chan cliResult, 1)
	go func() {
		var out, errOut bytes.Buffer
		code := runJobCLI([]string{"close", "test-job", "--lane", "lane-a", "--outcome", "completed", "--reason", "finished"}, &out, &errOut, CLIConfig{SocketPath: socket})
		done <- cliResult{code, out.String(), errOut.String()}
	}()
	cmd := nodeCommandRead(t, node)
	nodeClient := &HubClient{jobsInboxRoot: root}
	result, code := nodeClient.runNodeCommand(t.Context(), nodeCommandJobClose, cmd["args"])
	if code != "" {
		t.Fatalf("node error %s", code)
	}
	nodeCommandReplyTo(t, node, cmd, map[string]any{"result": result})
	cli := <-done
	if cli.code != 0 || !strings.Contains(cli.out, "OK job=test-job") {
		t.Fatalf("cli %+v", cli)
	}
	events := nodeClient.jobCompletionEvents()
	if len(events) != 1 {
		t.Fatalf("events %+v", events)
	}
	if err := wsjson.Write(t.Context(), node, hubClientWireEvent(events[0])); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, "hub marks job terminal", func() bool { hub.mu.Lock(); defer hub.mu.Unlock(); return hub.jobs["test-job"].Completed })
	assertRoleHeldBy(t, hub, held.Request.RequestID)
	response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{{PaneID: roleHeldPane, Cwd: "/work/project.test-job"}}, true)
	if r := t14Result(t, response); r.status != http.StatusConflict {
		t.Fatalf("present pane: %d %s", r.status, r.body)
	}
	assertRoleHeldBy(t, hub, held.Request.RequestID)
	response = t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{}, true)
	roleReplyMarkLost(t, node, nodeJobMarkLostLost)
	if r := t14Result(t, response); r.status != http.StatusAccepted {
		t.Fatalf("gone pane: %d %s", r.status, r.body)
	}
	owner, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:worker")
	if err != nil || owner == held.Request.RequestID || job != "test-job-r2" {
		t.Fatalf("role owner=%s job=%s err=%v", owner, job, err)
	}
}

func TestJobCloseHubAuthorization(t *testing.T) {
	_, server := t14Hub(t)
	req := hubJobCloseRequest{Job: "test-job", Lane: "lane-a", Outcome: "completed", Reason: "done"}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ machine, token string }{{"", ""}, {"", t14OperatorToken}, {"machine-a", "wrong"}} {
		r, err := http.NewRequest(http.MethodPost, server.URL+"/v1/jobs/close", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set(hubMachineIDHeader, tc.machine)
		r.Header.Set(hubAuthorizationHeader, "Bearer "+tc.token)
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("machine=%s status=%d", tc.machine, resp.StatusCode)
		}
	}
}

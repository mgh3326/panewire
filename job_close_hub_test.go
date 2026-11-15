package panewire

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
)

func TestJobCloseViaHub(t *testing.T) {
	f := newR19eFixture(t)
	if err := os.WriteFile(f.hub.reportRelayPath, []byte(`{"lanes":{"lane-a":{"machine":"node-b","pane":"w1:p1"},"wrong":{"machine":"node-b","pane":"w1:p2"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
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
	code, _, errOut := run("remote", "unrouted", false)
	if code != jobCloseRefused || !strings.Contains(errOut, "lane_not_routed_to_caller") {
		t.Fatalf("unrouted: %d %s", code, errOut)
	}
	code, _, errOut = run("remote", "wrong", false)
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
	code, _, errOut = run("remote", "wrong", true)
	if code != jobCloseRefused || !strings.Contains(errOut, "override_requires_operator") {
		t.Fatalf("node override: %d %s", code, errOut)
	}
	operator, err := newHubOperatorClient(f.server.URL, r6OperatorToken, hubCFAccessEnv{}, hubCLIDeps{AllowInsecureForTests: true}, nodeReapClientTimeout)
	if err != nil {
		t.Fatal(err)
	}
	result, err := requestHubJobClose(t.Context(), operator, hubJobCloseRequest{Job: "remote", Lane: "wrong", Outcome: "abandoned", Reason: "test reason", Override: true})
	if err != nil {
		t.Fatal(err)
	}
	var output, errOutput bytes.Buffer
	cli := &jobCLI{stdout: &output, stderr: &errOutput}
	code = cli.printHubClose(jobCloseOptions{job: "remote"}, result)
	out, errOut := output.String(), errOutput.String()
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
	hub.reportRelayPath = r20LanesFile(t, `{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1"}}}`)
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
	owner, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
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
	for _, tc := range []struct{ machine, token string }{{"", ""}, {"machine-a", "wrong"}} {
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

func TestJobCloseServiceScope(t *testing.T) {
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "operator-token", "machine-a": "node-token", "svc-close": "service-close-token", "svc-other": "service-other-token"}, Scopes: map[string]string{"svc-close": "jobs.close:lane-a", "svc-other": "inbox.read:lane-a"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hub.Close() })
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	node := t14Node(t, hub, server, true)
	hub.mu.Lock()
	hub.jobs["remote"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "remote", OwnerLane: "lane-a", Epoch: 1}, Node: "machine-a"}
	hub.mu.Unlock()
	makeClient := func(id, token string) *hubOperatorClient {
		c, err := newHubOperatorClient(server.URL, token, hubCFAccessEnv{}, hubCLIDeps{AllowInsecureForTests: true}, nodeReapClientTimeout)
		if err != nil {
			t.Fatal(err)
		}
		c.machineID = id
		return c
	}
	allowed := makeClient("svc-close", "service-close-token")
	denied := makeClient("svc-other", "service-other-token")
	req := hubJobCloseRequest{Job: "remote", Lane: "lane-a", Outcome: "completed", Reason: "done"}
	result, err := requestHubJobClose(t.Context(), denied, req)
	if err != nil || result.Refusal != "scope_forbidden" {
		t.Fatalf("denied %+v %v", result, err)
	}
	req.Override = true
	result, err = requestHubJobClose(t.Context(), allowed, req)
	if err != nil || result.Refusal != "override_requires_operator" {
		t.Fatalf("override %+v %v", result, err)
	}
	req.Override = false
	done := make(chan hubJobCloseResult, 1)
	go func() { result, _ := requestHubJobClose(t.Context(), allowed, req); done <- result }()
	cmd := nodeCommandRead(t, node)
	root := t.TempDir()
	dir := filepath.Join(root, "jobs", "remote", "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "00001-job.claim.json"), []byte(`{"kind":"job.claim","payload":{"owner_lane":"lane-a"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	worker := &HubClient{jobsInboxRoot: root}
	nodeResult, nodeErr := worker.runNodeCommand(t.Context(), nodeCommandJobClose, cmd["args"])
	if nodeErr != "" {
		t.Fatal(nodeErr)
	}
	nodeCommandReplyTo(t, node, cmd, map[string]any{"result": nodeResult})
	result = <-done
	if result.Record.JobID != "remote" || result.Refusal != "" {
		t.Fatalf("allowed %+v", result)
	}
}

func TestJobCloseUnconfirmedAfterSend(t *testing.T) {
	hub, server := t14Hub(t)
	node := t14Node(t, hub, server, true)
	hub.mu.Lock()
	hub.jobs["remote"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "remote", OwnerLane: "lane-a", Epoch: 1}, Node: "machine-a"}
	hub.mu.Unlock()
	client, err := newHubOperatorClient(server.URL, t14OperatorToken, hubCFAccessEnv{}, hubCLIDeps{AllowInsecureForTests: true}, nodeReapClientTimeout)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan hubJobCloseResult, 1)
	go func() {
		result, _ := requestHubJobClose(t.Context(), client, hubJobCloseRequest{Job: "remote", Lane: "lane-a", Outcome: "completed", Reason: "done"})
		done <- result
	}()
	cmd := nodeCommandRead(t, node)
	root := t.TempDir()
	dir := filepath.Join(root, "jobs", "remote", "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "00001-job.claim.json"), []byte(`{"kind":"job.claim","payload":{"owner_lane":"lane-a"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	worker := &HubClient{jobsInboxRoot: root}
	_, code := worker.runNodeCommand(t.Context(), nodeCommandJobClose, cmd["args"])
	if code != "" {
		t.Fatal(code)
	}
	if _, err := os.Stat(filepath.Join(dir, "00002-job.revoked.json")); err != nil {
		t.Fatal(err)
	}
	node.CloseNow()
	result := <-done
	if result.Failure != "close_unconfirmed" || result.Refusal != "" {
		t.Fatalf("result %+v", result)
	}
}

func TestJobCloseNodeFailures(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "jobs", "remote")
	events := filepath.Join(dir, "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(events, "00001-job.claim.json"), []byte(`{"kind":"job.claim","payload":{"owner_lane":"lane-a"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	worker := &HubClient{jobsInboxRoot: root}
	req := hubJobCloseRequest{Job: "remote", Lane: "other", Outcome: "completed", Reason: "done"}
	raw, _ := worker.nodeJobClose(req)
	refusal := raw.(hubJobCloseResult)
	if !strings.Contains(refusal.Refusal, "owned by lane") || refusal.Failure != "" {
		t.Fatalf("ownership %+v", refusal)
	}
	req.Lane = "lane-a"
	if err := os.Remove(filepath.Join(dir, ".wrk-events.lock")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, ".wrk-events.lock"), 0700); err != nil {
		t.Fatal(err)
	}
	raw, _ = worker.nodeJobClose(req)
	failure := raw.(hubJobCloseResult)
	if failure.Failure != "close_failed" || failure.Refusal != "" {
		t.Fatalf("IO %+v", failure)
	}
}

func TestJobCloseConnectionOptions(t *testing.T) {
	opts, err := parseJobCloseArgs([]string{"remote", "--lane", "lane-a", "--outcome", "completed", "--reason", "done", "--hub-url", "https://hub.robinco.dev", "--hub-token-env", "credential-path", "--hub-cf-env", "cf-path"})
	if err != nil || opts.hubURL != "https://hub.robinco.dev" || opts.tokenEnv != "credential-path" || opts.cfEnv != "cf-path" {
		t.Fatalf("options %+v error %v", opts, err)
	}
	if _, err := parseHubScopes("jobs.close:*"); err == nil {
		t.Fatal("wildcard lane scope accepted")
	}
}

func TestJobCloseDoesNotRetryAfterLostResponse(t *testing.T) {
	var first, second atomic.Int32
	firstHub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close()
	}))
	t.Cleanup(firstHub.Close)
	secondHub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		second.Add(1)
		writeJSON(w, http.StatusOK, hubJobCloseResult{Refusal: "already_terminal"})
	}))
	t.Cleanup(secondHub.Close)
	client, err := NewHubClient(HubClientConfig{URLs: []string{r6WSURL(firstHub.URL, ""), r6WSURL(secondHub.URL, "")}, MachineID: "node-a", Token: "fixture-token", AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	daemon := NewDaemon(Config{Hub: HubDaemonConfig{Client: client}})
	value, err := daemon.handleJobClose(t.Context(), &hubJobCloseRequest{Job: "remote", Lane: "lane-a", Outcome: "completed", Reason: "done"})
	if err != nil {
		t.Fatal(err)
	}
	result := value.(hubJobCloseResult)
	if result.Failure != "close_unconfirmed" || first.Load() != 1 || second.Load() != 0 {
		t.Fatalf("result=%+v first=%d second=%d", result, first.Load(), second.Load())
	}
}

func TestJobCloseHubUnauthorized(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hubUnauthorized(w) }))
	t.Cleanup(server.Close)
	client, err := newHubOperatorClient(server.URL, "fixture-token", hubCFAccessEnv{}, hubCLIDeps{AllowInsecureForTests: true}, nodeReapClientTimeout)
	if err != nil {
		t.Fatal(err)
	}
	result, err := requestHubJobClose(t.Context(), client, hubJobCloseRequest{Job: "remote", Lane: "lane-a", Outcome: "completed", Reason: "done"})
	if err != nil || result.Failure != "hub_unauthorized" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

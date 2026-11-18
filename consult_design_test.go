package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDesignConsultAuthorization(t *testing.T) {
	f := newConsultFixture(t)
	f.hub.reportRelayPath = r20LanesFile(t, `{"lanes":{"project-design":{"machine":"machine-b","pane":"w1:p1"},"other-design":{"machine":"machine-a","pane":"w1:p2"},"worker":{"machine":"machine-b","pane":"w1:p3"},"operator-desk":{"machine":"machine-b","pane":"w1:p4"}}}`)
	body := consultRequestBody("42", "example/project#7", strings.Repeat("d", 40))
	body["requester"] = "project-design"
	body["caller_pane"] = "w1:p1"
	code, out := f.post(t, "machine-b", "node-token-b", body)
	if code != http.StatusCreated {
		t.Fatalf("create: %d %v", code, out)
	}
	c, err := f.hub.inbox.store.consultByID(context.Background(), out["id"].(string))
	if err != nil || c.Requester != "project-design" || c.Machine != "machine-b" || c.CallerPane != "w1:p1" {
		t.Fatalf("row: %+v %v", c, err)
	}
	tests := []struct{ name, lane, pane string }{{"missing", "project-design", ""}, {"other machine", "other-design", "w1:p2"}, {"other pane", "project-design", "w1:p3"}, {"worker lane", "worker", "w1:p3"}, {"desk", "operator-desk", "w1:p4"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := consultRequestBody("42", "", "")
			b["requester"] = tt.lane
			b["caller_pane"] = tt.pane
			code, _ := f.post(t, "machine-b", "node-token-b", b)
			if code != http.StatusForbidden {
				t.Fatalf("got %d", code)
			}
		})
	}
	implicit := consultRequestBody("43", "", "")
	implicit["requester"] = ""
	implicit["caller_pane"] = "w1:p1"
	code, out = f.post(t, "machine-b", "node-token-b", implicit)
	if code != http.StatusCreated || out["requester"] != "project-design" {
		t.Fatalf("inferred: %d %v", code, out)
	}
	f.hub.reportRelayPath = r20LanesFile(t, `{"lanes":{"project-design":{"machine":"machine-b","pane":"w1:p1"},"other-design":{"machine":"machine-b","pane":"w1:p1"}}}`)
	code, out = f.post(t, "machine-b", "node-token-b", implicit)
	if code != http.StatusForbidden || !strings.Contains(out["reason"].(string), "--requester") {
		t.Fatalf("ambiguous: %d %v", code, out)
	}
	f.hub.mu.Lock()
	f.hub.jobs["job"] = &hubJobRecord{Node: "machine-b", HubActiveJob: HubActiveJob{Pane: "w1:p1"}}
	f.hub.mu.Unlock()
	code, out = f.post(t, "machine-b", "node-token-b", body)
	if code != http.StatusForbidden || !strings.Contains(out["reason"].(string), "ESC") {
		t.Fatalf("job: %d %v", code, out)
	}
	for _, state := range []string{"completed", "orphaned"} {
		f.hub.mu.Lock()
		f.hub.jobs["job"].Completed = state == "completed"
		f.hub.jobs["job"].Orphaned = state == "orphaned"
		f.hub.mu.Unlock()
		code, out = f.post(t, "machine-b", "node-token-b", body)
		if code != http.StatusOK {
			t.Fatalf("%s job on reused pane: %d %v", state, code, out)
		}
	}
}

func TestConsultPaneResolution(t *testing.T) {
	run := func(_ context.Context, args ...string) ([]byte, error) {
		if len(args) == 2 {
			return []byte(`{"panes":[{"pane_id":"p1"},{"pane_id":"p2"}]}`), nil
		}
		shell := 10
		if args[3] == "p2" {
			shell = 20
		}
		return json.Marshal(map[string]any{"process_info": map[string]int{"shell_pid": shell}})
	}
	parents := map[int]int{42: 30, 30: 20, 20: 1}
	got, err := consultPane(context.Background(), run, 42, func(pid int) (int, error) { return parents[pid], nil })
	if err != nil || got != "p2" {
		t.Fatalf("pane %q: %v", got, err)
	}
}

func TestConsultSocketPeerPID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "socket")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	accepted, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
	pid, err := localPeerPID(accepted)
	if err != nil || pid != os.Getpid() {
		t.Fatalf("peer pid %d, want %d: %v", pid, os.Getpid(), err)
	}
}

func TestConsultLocalDaemonForwardsAndRefusesJob(t *testing.T) {
	f := newConsultFixture(t)
	f.hub.reportRelayPath = r20LanesFile(t, `{"lanes":{"project-design":{"machine":"machine-b","pane":"w1:p1"}}}`)
	server := httptest.NewServer(f.hub.Handler())
	defer server.Close()
	root := t.TempDir()
	client := &HubClient{machineID: "machine-b", token: "node-token-b", endpoints: []string{strings.Replace(server.URL, "http://", "ws://", 1)}, spawnJournalDir: root,
		nodeHerdr: func(_ context.Context, args ...string) ([]byte, error) {
			if len(args) == 2 && args[0] == "pane" {
				return []byte(`{"panes":[{"pane_id":"w1:p1"}]}`), nil
			}
			if len(args) == 2 && args[0] == "agent" {
				return []byte(`{"result":{"agents":[{"pane_id":"w1:p1","name":"design"}]}}`), nil
			}
			return json.Marshal(map[string]any{"process_info": map[string]int{"shell_pid": os.Getpid()}})
		}}
	daemon := NewDaemon(Config{})
	daemon.cfg.Hub.Client = client
	// A unix socket path is capped at 104 bytes on macOS, and the t.TempDir()
	// path there is longer than that, so the socket lives in a short dir (hk 5282).
	sockets, err := os.MkdirTemp("", "pwcd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockets) })
	path := filepath.Join(sockets, "sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			go daemon.handle(context.Background(), c)
		}
	}()
	defer func() { listener.Close(); <-done }()
	req := hubConsultRequest{Task: "42", QuestionDoc: "hk:doc/design/q", AuthorJob: "author-job"}
	var out, errOut bytes.Buffer
	if code := runConsultLocal(req, false, &out, &errOut, path); code != ExitOK || !strings.Contains(out.String(), "CONSULT") {
		t.Fatalf("request: %d %s %s", code, out.String(), errOut.String())
	}
	j, err := client.acquireSpawnJournal()
	if err != nil {
		t.Fatal(err)
	}
	_, err = j.db.Exec(`INSERT INTO spawn_inbox(request_id,fingerprint,status,result) VALUES('worker','x','completed',?)`, `{"pane":"w1:p1","receipt":{"label":"design"}}`)
	client.releaseSpawnJournal()
	if err != nil {
		t.Fatal(err)
	}
	out.Reset()
	errOut.Reset()
	if code := runConsultLocal(req, false, &out, &errOut, path); code != ExitConditionInvalid || !strings.Contains(errOut.String(), "ESC") {
		t.Fatalf("worker: %d %s", code, errOut.String())
	}
	// The old journal's pane ID may later belong to a different pane label.
	j, err = client.acquireSpawnJournal()
	if err != nil {
		t.Fatal(err)
	}
	_, err = j.db.Exec(`UPDATE spawn_inbox SET result=? WHERE request_id='worker'`, `{"pane":"w1:p1","receipt":{"label":"old-worker"}}`)
	client.releaseSpawnJournal()
	if err != nil {
		t.Fatal(err)
	}
	out.Reset()
	errOut.Reset()
	if code := runConsultLocal(req, false, &out, &errOut, path); code != ExitOK {
		t.Fatalf("reused pane: %d %s", code, errOut.String())
	}
}

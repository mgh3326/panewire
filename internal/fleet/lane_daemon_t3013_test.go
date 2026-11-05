package fleet

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// laneDaemonStub answers the spawn engine the way the node daemon does: one
// request line in, one response line out. answer is called per request, so a
// test can replay a canned answer or forward to a fake hub.
type laneDaemonStub struct {
	path     string
	requests chan map[string]any
}

func newLaneDaemonStub(t *testing.T, answer func(map[string]any) string) *laneDaemonStub {
	t.Helper()
	// The socket path must stay inside the short macOS sun_path limit.
	root, err := os.MkdirTemp("/tmp", "pw-lane-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	path := filepath.Join(root, "daemon.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	stub := &laneDaemonStub{path: path, requests: make(chan map[string]any, 8)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				line, err := bufio.NewReader(conn).ReadString('\n')
				if err != nil {
					return
				}
				var request map[string]any
				if json.Unmarshal([]byte(line), &request) != nil {
					return
				}
				stub.requests <- request
				fmt.Fprintln(conn, answer(request))
			}(conn)
		}
	}()
	t.Cleanup(func() { listener.Close(); <-done })
	return stub
}

// laneAddArgs is the fleet-config registration command, if it ran at all.
func laneAddArgs(f *fakeRunner) string {
	for _, c := range f.Calls {
		if c.Name == "panewire" && len(c.Args) > 1 && c.Args[0] == "lanes" && c.Args[1] == "add" {
			return strings.Join(c.Args, " ")
		}
	}
	return ""
}

// laneDaemonSpawn is a cmd spawn whose fleet config carries no [hub] token
// keys at all: the lane has to be registered through the local daemon.
func laneDaemonSpawn(t *testing.T) (*App, Spawn, *fakeRunner, *[]Command) {
	t.Helper()
	a, s, f, _ := cmdFixture(t)
	s.Owner = "desk"
	a.Config.Hub = map[string]any{}
	a.Config.Local["machine_id"] = "test-host"
	emitted := &[]Command{}
	original := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "panewire" && strings.HasPrefix(strings.Join(c.Args, " "), "emit") {
			*emitted = append(*emitted, c)
			return Result{}
		}
		return original(c)
	}
	return a, s, f, emitted
}

// Acceptance 1: a spawn with no fleet [hub] credential registers its lane over
// the daemon socket, and the daemon's PUT reaches the hub with the node
// credential.
func TestSpawnRegistersLaneThroughDaemonSocket(t *testing.T) {
	a, s, f, emitted := laneDaemonSpawn(t)
	var mu sync.Mutex
	var hubPuts []string
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		hubPuts = append(hubPuts, r.Method+" "+r.URL.Path+" "+r.Header.Get("X-Panewire-Machine-ID")+" "+string(body))
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"lane":"worker-a","machine":"test-host","pane":"w:p1","parent":"desk","sink":false}`+"\n")
	}))
	defer hub.Close()
	stub := newLaneDaemonStub(t, func(request map[string]any) string {
		body, _ := json.Marshal(map[string]any{"machine": request["machine"], "pane": request["pane"], "parent": request["parent"]})
		put, err := http.NewRequest(http.MethodPut, hub.URL+"/v1/lanes/"+request["lane"].(string), bytes.NewReader(body))
		if err != nil {
			return `{"ok":false,"code":70,"error":"lanes unavailable"}`
		}
		put.Header.Set("Authorization", "Bearer node-token")
		put.Header.Set("X-Panewire-Machine-ID", request["machine"].(string))
		response, err := http.DefaultClient.Do(put)
		if err != nil {
			return `{"ok":false,"code":70,"error":"lanes unavailable"}`
		}
		defer response.Body.Close()
		projection, _ := io.ReadAll(response.Body)
		return `{"ok":true,"code":0,"result":` + strings.TrimSpace(string(projection)) + `}`
	})
	t.Setenv("PANEWIRE_SOCKET", stub.path)
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if r := spawnedReceipt(t, a, s.Job); r["hub_lane"] != "worker-a" || r["lane_error"] != nil {
		t.Fatalf("receipt=%v", r)
	}

	request := <-stub.requests
	if request["op"] != "lanes.put" || request["lane"] != "worker-a" || request["machine"] != "test-host" || request["pane"] != "w:p1" || request["parent"] != "desk" {
		t.Fatalf("socket request=%v", request)
	}
	if timeout, _ := request["timeout_ms"].(float64); timeout <= 0 {
		t.Fatalf("socket request has no deadline: %v", request)
	}
	if laneAddCalls(f) != 0 {
		t.Fatal("the fleet-config command also ran")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(hubPuts) != 1 || hubPuts[0] != `PUT /v1/lanes/worker-a test-host {"machine":"test-host","pane":"w:p1","parent":"desk"}` {
		t.Fatalf("hub PUTs=%v", hubPuts)
	}
	r := spawnedReceipt(t, a, s.Job)
	if r["hub_lane"] != "worker-a" || r["hub_lane_machine"] != "test-host" || r["lane_unregistered"] != nil || r["lane_error"] != nil {
		t.Fatalf("receipt=%v", r)
	}
	if len(*emitted) != 0 {
		t.Fatalf("success announced an unregistered lane: %v", *emitted)
	}
}

// Acceptance 2: a refusal through the socket reaches the receipt unchanged —
// the same text and exit status the credentialled command path reports — and
// is retried once like any other hub refusal.
func TestDaemonLaneRefusalRecordsSameReasonAsDirectPath(t *testing.T) {
	refusal := "lanes rejected by hub: lane_machine_mismatch (check that parent lane desk is registered on the hub)"
	a, s, f, emitted := laneDaemonSpawn(t)
	stub := newLaneDaemonStub(t, func(map[string]any) string {
		encoded, _ := json.Marshal(refusal)
		return `{"ok":false,"code":5,"error":` + string(encoded) + `}`
	})
	t.Setenv("PANEWIRE_SOCKET", stub.path)
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	want := "lanes: " + refusal
	r := spawnedReceipt(t, a, s.Job)
	if r["lane_error"] != want || r["lane_unregistered"] != true || r["hub_lane"] != nil {
		t.Fatalf("receipt=%v, want lane_error=%q", r, want)
	}
	if laneAddCalls(f) != 0 {
		t.Fatal("a socket refusal also ran the fleet-config command")
	}
	if got := len(stub.requests); got != 2 {
		t.Fatalf("socket attempts=%d, want one retry", got)
	}
	if len(*emitted) != 1 || !strings.Contains(flagValue((*emitted)[0].Args, "--text"), "reason="+want) {
		t.Fatalf("unregistered notice=%v", *emitted)
	}
}

// Acceptance 3: with no daemon socket the fleet-config command registers the
// lane as before.
func TestSpawnRegistersLaneThroughFleetConfigWithoutDaemon(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Owner = "desk"
	token := filepath.Join(t.TempDir(), "node.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=test-host\nHUB_TOKEN=fixture\n"), 0600); err != nil {
		t.Fatal(err)
	}
	a.Config.Local["machine_id"] = "test-host"
	a.Config.Hub = map[string]any{"hub_url": "https://hub.example.invalid", "lanes_token_env": token}
	t.Setenv("PANEWIRE_SOCKET", filepath.Join(t.TempDir(), "absent.sock"))
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if laneAddCalls(f) != 1 {
		t.Fatalf("lanes add calls=%d, want 1", laneAddCalls(f))
	}
	if args := laneAddArgs(f); !strings.Contains(args, "--hub-token-env "+token) {
		t.Fatalf("fleet-config route lost its credential: %q", args)
	}
	if r := spawnedReceipt(t, a, s.Job); r["hub_lane"] != "worker-a" || r["lane_error"] != nil {
		t.Fatalf("receipt=%v", r)
	}
}

// A daemon that answers "I cannot serve this op" is the same as no daemon: the
// fleet-config route registers the lane.
func TestDaemonWithoutHubClientFallsBackToFleetConfig(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Owner = "desk"
	a.Config.Local["machine_id"] = "test-host"
	a.Config.Hub = map[string]any{"hub_url": "https://hub.example.invalid"}
	stub := newLaneDaemonStub(t, func(map[string]any) string {
		return `{"ok":false,"code":4,"error":"lanes: daemon has no hub client"}`
	})
	t.Setenv("PANEWIRE_SOCKET", stub.path)
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if laneAddCalls(f) != 1 {
		t.Fatalf("lanes add calls=%d, want the fallback", laneAddCalls(f))
	}
	if r := spawnedReceipt(t, a, s.Job); r["hub_lane"] != "worker-a" || r["lane_error"] != nil {
		t.Fatalf("receipt=%v", r)
	}
}

// A session with its own lane credential registers with that credential, not
// with the daemon's node identity.
func TestSessionLaneCredentialKeepsFleetConfigRoute(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Owner = "desk"
	a.Config.Local["machine_id"] = "test-host"
	a.Config.Hub = map[string]any{
		"hub_url":                  "https://hub.example.invalid",
		"lanes_token_env":          "shared.env",
		"session_lanes_token_envs": map[string]any{a.herdrSession(): "session.env"},
	}
	stub := newLaneDaemonStub(t, func(map[string]any) string {
		return `{"ok":true,"code":0}`
	})
	t.Setenv("PANEWIRE_SOCKET", stub.path)
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if laneAddCalls(f) != 1 {
		t.Fatalf("lanes add calls=%d, want the session credential route", laneAddCalls(f))
	}
	select {
	case request := <-stub.requests:
		t.Fatalf("session route dialed the daemon: %v", request)
	default:
	}
}

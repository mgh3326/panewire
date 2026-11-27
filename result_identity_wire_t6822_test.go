package panewire

// hk 6822: the result_id's way from `panewire emit` to the hub, and the
// hub.features gate between a node and a hub that deploy at different times.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/laneevent"
)

// t6822OldHubCompletionFields is the job.completed vocabulary of a hub before
// result_id (decodeHubJobCompletionPayloadDetailed at c28eb8ea hub_jobs.go:65):
// it refuses any other name, and more than 19 fields.
var t6822OldHubCompletionFields = []string{"job_id", "epoch", "agent_label", "owner_lane", "label", "host", "report_path", "report_last_line", "question", "pr", "head", "pane_id", "event_id", "replay", "delivery_class", "notify_type", "late", "cause", "final"}

func t6822WriteCompletion(t *testing.T, root, job, resultID string) {
	t.Helper()
	record := emitRecord{Type: "job.completed", JobID: job, Epoch: 1, CreatedAt: time.Now().UTC().Format(time.RFC3339), OwnerLane: t3426Design, Label: job, Host: "host-a", ReportPath: "/jobs/" + job + "/report.md", ReportLastLine: "done", ResultID: resultID}
	if _, err := writeEmitRecord(root, record); err != nil {
		t.Fatal(err)
	}
}

// A new node and an old hub: the node holds the completion until it knows the
// hub's features, sends it without result_id, and the old hub's acknowledgement
// retires the outbox row, so nothing is resent. Mutant: always send the field.
func TestResultIDStrippedForOldHub(t *testing.T) {
	held := hubClientEvent{Kind: "job.completed", Payload: json.RawMessage(`{"job_id":"j","epoch":1}`), resultID: "ready-1"}
	if !relayHeldForFeatures(held) {
		t.Fatal("a completion with a result_id is not held for the hub's features")
	}
	raw, err := json.Marshal((&hubClientConnection{}).relayWire(held))
	if err != nil || strings.Contains(string(raw), "result_id") {
		t.Fatalf("a hub that listed no result-id-v1 got %s (%v)", raw, err)
	}

	var mu sync.Mutex
	var payloads []map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer func() { _ = connection.CloseNow() }()
		ctx := request.Context()
		_ = wsjson.Write(ctx, connection, map[string]any{"type": "hub.features", "features": []string{"host_signals", "lane_event_pane", "final_lost"}})
		for {
			var message map[string]json.RawMessage
			if wsjson.Read(ctx, connection, &message) != nil {
				return
			}
			var kind, eventKind string
			_ = json.Unmarshal(message["type"], &kind)
			_ = json.Unmarshal(message["kind"], &eventKind)
			switch {
			case kind == "ping":
				_ = wsjson.Write(ctx, connection, map[string]string{"type": "pong"})
			case kind == "event" && eventKind == "job.completed":
				var payload map[string]json.RawMessage
				_ = json.Unmarshal(message["payload"], &payload)
				mu.Lock()
				payloads = append(payloads, payload)
				mu.Unlock()
				// The old hub's strict decoder: a refused frame gets no answer.
				refused := len(payload) > 19
				for name := range payload {
					refused = refused || !slices.Contains(t6822OldHubCompletionFields, name)
				}
				if !refused {
					_ = wsjson.Write(ctx, connection, map[string]any{"type": "relay.persisted", "kind": "job.completed", "job_id": payload["job_id"], "epoch": payload["epoch"], "report_path": payload["report_path"], "reason": "", "event_id": 7, "producer_event_id": payload["event_id"]})
				}
			}
		}
	}))
	t.Cleanup(server.Close)
	root := t.TempDir()
	store := NewMemoryStore(t)
	t.Cleanup(func() { _ = store.Close() })
	t6822WriteCompletion(t, root, "t6822-old-hub", "ready-1")
	fsRunNode(t, server.URL, func(client *HubClient) {
		client.jobsInboxRoot = root
		client.SetRelayOutbox(store)
	})
	key := relayOutboxKey{Kind: "job.completed", JobID: "t6822-old-hub", Epoch: 1, ReportPath: "/jobs/t6822-old-hub/report.md", EventID: "00001-job.completed.json"}
	eventually(t, 10*time.Second, "the outbox row retired by the old hub's acknowledgement", func() bool {
		state, err := store.RelayOutboxState(context.Background(), key)
		return err == nil && state.Persisted
	})
	mu.Lock()
	defer mu.Unlock()
	if len(payloads) != 1 || payloads[0]["result_id"] != nil {
		t.Fatalf("frames=%v, want one without result_id", payloads)
	}
}

// A new node and a new hub, over a real socket: the hub lists result-id-v1, the
// node sends the field, and the hub merges the completion into the READY item.
// Mutants: the hub refuses the field; the node never stores the accept bit.
func TestResultIDAcceptedWhenAdvertised(t *testing.T) {
	if !slices.Contains(hubFeaturesFrame().Features, "result-id-v1") {
		t.Fatalf("hub.features=%v lacks result-id-v1", hubFeaturesFrame().Features)
	}
	// Every name the decoder takes, at once: the old bound of 19 and result_id.
	full := map[string]any{"job_id": "j", "epoch": 1}
	for _, name := range t6822OldHubCompletionFields[2:] {
		full[name] = "x"
	}
	full["replay"], full["late"], full["final"], full["cause"] = false, false, false, "job:j"
	full["delivery_class"], full["notify_type"], full["agent_label"] = "inbox", "report", "agent-j"
	full["result_id"] = "ready-1"
	raw, _ := json.Marshal(full)
	if got, ok := decodeHubJobCompletionPayload(raw); !ok || got.ResultID != "ready-1" || len(full) != 20 {
		t.Fatalf("20 fields: %d fields, decoded ok=%t result_id=%q", len(full), ok, got.ResultID)
	}
	// An id is read whole, never cut; a value that is no id is dropped and the
	// completion itself is kept.
	long := strings.Repeat("r", 512)
	for value, want := range map[string]string{
		`"` + long + `"`:  long,
		`"` + long + `r"`: "",
		`"bad\nid"`:       "",
		`7`:               "",
		`""`:              "",
	} {
		got, ok := decodeHubJobCompletionPayload([]byte(`{"job_id":"j","epoch":1,"result_id":` + value + `}`))
		if !ok || got.ResultID != want {
			t.Fatalf("result_id %.20s: ok=%t got %d bytes, want %d", value, ok, len(got.ResultID), len(want))
		}
	}
	// A stored READY's set is the hub's alone: no wire payload may set it.
	if _, ok := decodeHubJobCompletionPayload([]byte(`{"job_id":"j","epoch":1,"merged_completions":["k"]}`)); ok {
		t.Fatal("a node set merged_completions")
	}

	_, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	h, err := NewHubServer(HubServerConfig{
		Tokens:             map[string]string{"operator": "op", "host-a": "node"},
		ReportRelayPath:    r20LanesFile(t, t3426Lanes),
		DeliveryPolicyPath: "default",
		InboxStorePath:     filepath.Join(t.TempDir(), "inbox.sqlite3"),
		handoffkeep:        hk,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: t3426Records, Out: out, Err: errout}, nil
	}
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	root := t.TempDir()
	client, err := NewHubClient(HubClientConfig{
		URL: r6WSURL(server.URL, ""), MachineID: "host-a", Token: "node", Accepting: true,
		AllowInsecureForTests: true, PingInterval: 20 * time.Millisecond, Warn: func(string) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	client.jobsInboxRoot = root
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); client.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	ready := emitRecord{Type: "lane.event", Epoch: 1, CreatedAt: time.Now().UTC().Format(time.RFC3339), OwnerLane: t3426Design, EventID: "e2e-ready", NotifyType: "ready", Text: "[" + t6822Job + "] READY | #12"}
	if _, err := writeLaneEmitRecord(root, ready); err != nil {
		t.Fatal(err)
	}
	item := func() (laneInboxItem, int) {
		items, err := h.inbox.store.list(t3426Design, false)
		if err != nil {
			return laneInboxItem{}, -1
		}
		for _, i := range items {
			if i.Event.EventID == "e2e-ready" {
				return i, len(items)
			}
		}
		return laneInboxItem{}, len(items)
	}
	eventually(t, 10*time.Second, "the READY in the design lane's inbox", func() bool { i, _ := item(); return i.ID != 0 })
	t6822WriteCompletion(t, root, t6822Job, "e2e-ready")
	eventually(t, 10*time.Second, "the completion merged into the READY", func() bool {
		i, n := item()
		return n == 1 && len(i.Event.MergedCompletions) == 1
	})
}

// `emit --result-id` writes the id into the event file, hands it to the daemon
// with the id of that file, and the node scan reads it back.
// Mutants: drop the field on any of the three paths.
func TestEmitResultIDReachesTheDaemonAndTheScan(t *testing.T) {
	inbox := t.TempDir()
	socket := filepath.Join(r20SocketRoot(t), "emit.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	pushed := make(chan localRequest, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		defer connection.Close()
		scanner := bufio.NewScanner(connection)
		if scanner.Scan() {
			var request localRequest
			_ = json.Unmarshal(scanner.Bytes(), &request)
			pushed <- request
			body, _ := json.Marshal(localResponse{OK: true})
			_, _ = connection.Write(append(body, '\n'))
		}
	}()
	args := []string{"--kind", "job.completed", "--job", "t6822-emit", "--report", "report.md", "--owner-lane", "lane-a", "--inbox-root", inbox, "--result-id", "ready-1"}
	var stderr bytes.Buffer
	if code := runEmitCLI(args, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: socket}); code != ExitOK {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	request := <-pushed
	if request.ResultID != "ready-1" {
		t.Fatalf("daemon request=%+v", request)
	}
	if name, _ := emitJobEventFileID(inbox, request, 1); name != "00001-job.completed.json" {
		t.Fatalf("the pushed record names file %q", name)
	}
	events := scanHubRelayEvents(inbox)
	if len(events) != 1 || events[0].ResultID != "ready-1" {
		t.Fatalf("scanned=%+v", events)
	}
	node := r20Node(inbox, NewMemoryStore(t))
	offered := node.jobCompletionEvents()
	if len(offered) != 1 || offered[0].resultID != "ready-1" || strings.Contains(string(offered[0].Payload), "result_id") {
		t.Fatalf("offered=%+v, want the id held beside the payload", offered)
	}
	peer := &hubClientConnection{}
	peer.acceptsResultID.Store(true)
	wire, err := json.Marshal(peer.relayWire(offered[0]))
	if err != nil || !strings.Contains(string(wire), `"result_id":"ready-1"`) {
		t.Fatalf("wire=%s %v", wire, err)
	}
	// The same record under another result_id is another record: the file is
	// not reused, so the new id cannot be lost silently.
	args[len(args)-1] = "ready-2"
	if code := runEmitCLI(args, &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: socket}); code != ExitDeliveryFailure {
		t.Fatalf("a changed result_id: code=%d", code)
	}
}

// The lifecycle envelope stands in for a flat record that was never written,
// and names the READY too.
func TestLifecycleEnvelopeCarriesResultID(t *testing.T) {
	inbox := t.TempDir()
	old := time.Now().Add(-2 * lifecycleEnvelopeSettle)
	r20WriteEvent(t, inbox, "t6822-envelope", "00001-job.completed.json", `{"kind":"job.completed","job_id":"t6822-envelope","time":"`+old.UTC().Format(time.RFC3339)+`","payload":{"owner_lane":"lane-a","report":"report.md","result_id":"ready-1"}}`, old)
	events := scanHubRelayEvents(inbox)
	if len(events) != 1 || events[0].ResultID != "ready-1" {
		t.Fatalf("scanned=%+v", events)
	}
}

// result_id is a job.completed's own, and an event id: emit and the daemon
// refuse anything else before it is recorded.
func TestResultIDRefusedWhereItNamesNothing(t *testing.T) {
	inbox := t.TempDir()
	base := []string{"--inbox-root", inbox, "--report", "report.md", "--owner-lane", "lane-a", "--job", "t6822-bad"}
	for _, extra := range [][]string{
		{"--kind", "job.completed", "--result-id", ""},
		{"--kind", "job.completed", "--result-id", "bad\nid"},
		{"--kind", "job.completed", "--result-id", strings.Repeat("r", 513)},
		{"--kind", "job.escalate", "--reason", "r", "--result-id", "ready-1"},
		{"--kind", "lane.event", "--lane", "lane-a", "--text", "READY", "--result-id", "ready-1"},
	} {
		var stderr bytes.Buffer
		code := runEmitCLI(append(slices.Clone(base), extra...), &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: filepath.Join(inbox, "absent.sock")})
		if code != ExitUsage || !strings.Contains(stderr.String(), "--result-id") {
			t.Fatalf("%.40q: code=%d stderr=%q", extra, code, stderr.String())
		}
	}
	if events := scanHubRelayEvents(inbox); len(events) != 0 {
		t.Fatalf("a refused emit left a record: %+v", events)
	}
	daemon := &Daemon{}
	for _, request := range []localRequest{
		{Kind: "job.lost", ResultID: "ready-1"},
		{Kind: "job.completed", ResultID: "bad\nid"},
	} {
		if err := daemon.emitRelayEvent(request); ExitCode(err) != ExitUsage {
			t.Fatalf("%+v: %v", request, err)
		}
	}
}

// spawn --wait matches a merged completion by the start of its entry.
func TestCompletionKeyPrefixMatchesTheHubKey(t *testing.T) {
	c := t6822Completion(t3426Design, "ready-1", "00001-job.completed.json")
	key := t6822Merged(c)[0]
	if !strings.HasPrefix(key, laneevent.CompletionKeyPrefix(c.JobID)) || strings.HasPrefix(key, laneevent.CompletionKeyPrefix(c.JobID+"-2")) {
		t.Fatalf("key=%q", key)
	}
}

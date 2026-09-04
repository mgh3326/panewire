package panewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// scopefuelEmitting writes a fixture scopefuel that prints exactly body.
func scopefuelEmitting(t *testing.T, body []byte) string {
	t.Helper()
	directory := t.TempDir()
	payload := filepath.Join(directory, "payload")
	if err := os.WriteFile(payload, body, 0644); err != nil {
		t.Fatal(err)
	}
	command := filepath.Join(directory, "scopefuel")
	if err := os.WriteFile(command, []byte("#!/bin/sh\ncat "+payload+"\n"), 0755); err != nil {
		t.Fatal(err)
	}
	return command
}

// TestR19QuotaOutputBoundedByEncodedSize pins the bound on the side that
// matters. A 16 KiB stdout is within the raw cap, but JSON escaping doubles
// quotes and newlines and sextuples control bytes, so the same output can still
// exceed the 32 KiB message the hub is willing to read; such a report must be
// refused at the node rather than silently closing the connection.
func TestR19QuotaOutputBoundedByEncodedSize(t *testing.T) {
	for _, testCase := range []struct {
		name string
		fill byte
	}{
		{"quote", '"'},
		{"newline", '\n'},
		{"control", 0x01},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			body := bytes.Repeat([]byte{testCase.fill}, hubQuotaOutputLimit)
			if encoded := hubQuotaEncodedSize(body); encoded <= hubQuotaEncodedLimit {
				t.Fatalf("fixture does not exercise the encoded bound: encoded=%d", encoded)
			}
			out, err := runHubScopefuel(context.Background(), scopefuelEmitting(t, body))
			if err == nil || err.Error() != "output_too_large" {
				t.Fatalf("raw=%d error=%v, want output_too_large", len(body), err)
			}
			if len(out) != 0 {
				t.Fatalf("rejected report still returned %d bytes", len(out))
			}
		})
	}
	// The encoded bound must not shrink the documented raw allowance for
	// ordinary output: 16 KiB that needs no escaping still passes.
	plain := bytes.Repeat([]byte{'a'}, hubQuotaOutputLimit)
	out, err := runHubScopefuel(context.Background(), scopefuelEmitting(t, plain))
	if err != nil || len(out) != hubQuotaOutputLimit {
		t.Fatalf("unescaped %d bytes rejected: len=%d err=%v", hubQuotaOutputLimit, len(out), err)
	}
	// Whatever passes must fit the hub's message limit once wrapped.
	report, marshalErr := json.Marshal(struct {
		Type      string `json:"type"`
		RequestID string `json:"request_id"`
		Payload   string `json:"payload"`
	}{"quota.report", strings.Repeat("r", 96), string(out)})
	if marshalErr != nil || len(report) > hubMaxMessageBytes {
		t.Fatalf("accepted report does not fit the protocol limit: len=%d limit=%d err=%v", len(report), hubMaxMessageBytes, marshalErr)
	}
}

// TestR19ConcurrentUpdateInstructionsApplyOnce drives two update.available
// messages through the real node read loop. Without an in-flight guard both
// would download and rename against the same executable, leaving the surviving
// version up to whichever finished last.
func TestR19ConcurrentUpdateInstructionsApplyOnce(t *testing.T) {
	asset := []byte("new binary")
	digest := sha256.Sum256(asset)
	executable := filepath.Join(t.TempDir(), "panewire")
	if err := os.WriteFile(executable, []byte("old binary"), 0755); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	requests := 0
	firstRequest := make(chan struct{}, 4)
	release := make(chan struct{})
	updateClient := &http.Client{Transport: hubRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		mu.Lock()
		requests++
		mu.Unlock()
		firstRequest <- struct{}{}
		// Hold the download open long enough for the second instruction to be
		// read and answered while this one is still in flight.
		select {
		case <-release:
		case <-time.After(5 * time.Second):
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(asset)), Request: request}, nil
	})}

	busy := make(chan struct{}, 8)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		instruction := map[string]any{
			"type":    "update.available",
			"version": "r19c",
			"sha256":  hex.EncodeToString(digest[:]),
			"url":     "https://github.com/mgh3326/panewire/releases/download/r19c/panewire_linux_amd64",
		}
		_ = wsjson.Write(request.Context(), conn, instruction)
		_ = wsjson.Write(request.Context(), conn, instruction)
		for {
			var message map[string]any
			if wsjson.Read(request.Context(), conn, &message) != nil {
				return
			}
			if message["type"] == "update.busy" {
				select {
				case busy <- struct{}{}:
				default:
				}
			}
		}
	}))
	defer server.Close()

	restarted := make(chan struct{}, 4)
	client, err := NewHubClient(HubClientConfig{
		URL: r6WSURL(server.URL, ""), MachineID: "node-a", Token: "node-token", AllowInsecureForTests: true,
		PingInterval: time.Hour, PreferRetry: time.Hour, ExecutablePath: executable, UpdateHTTPClient: updateClient,
		Restart: func() { restarted <- struct{}{} },
	})
	if err != nil {
		t.Fatal(err)
	}
	conn, _, err := websocket.Dial(t.Context(), client.endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- client.serve(ctx, conn) }()

	select {
	case <-firstRequest:
	case <-time.After(5 * time.Second):
		t.Fatal("the first update instruction never started a download")
	}
	answeredBusy := false
	select {
	case <-busy:
		answeredBusy = true
	case <-time.After(3 * time.Second):
	}
	close(release)
	select {
	case <-restarted:
	case <-time.After(5 * time.Second):
		t.Fatal("the admitted update never completed")
	}
	// Give a second, unguarded download the chance to be counted before the
	// assertion reads the counter.
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	total := requests
	mu.Unlock()
	if total != 1 {
		t.Fatalf("concurrent update.available instructions produced %d downloads, want 1", total)
	}
	if !answeredBusy {
		t.Fatal("the declined update instruction was not answered with update.busy")
	}

	// The guard admits one update at a time, not one update ever. A guard that
	// is never released would decline every later instruction for the life of
	// the process, which looks identical to correct behaviour above.
	if !client.beginHubUpdate() {
		t.Fatal("the in-flight guard was not released after the update completed")
	}
	client.endHubUpdate()
	installed, readErr := os.ReadFile(executable)
	if readErr != nil || string(installed) != string(asset) {
		t.Fatalf("executable=%q err=%v", installed, readErr)
	}
	backups, globErr := filepath.Glob(executable + ".bak-*")
	if globErr != nil || len(backups) != 1 {
		t.Fatalf("one applied update left backups=%v err=%v", backupNames(backups), globErr)
	}
	cancel()
	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("client did not stop")
	}
}

// TestR19UpdateBusyIsAFirstClassInboundMessage keeps the node's reply out of the
// hub's unknown-message counter and visible to an operator.
func TestR19UpdateBusyIsAFirstClassInboundMessage(t *testing.T) {
	if _, ok := parseHubInbound([]byte(`{"type":"update.busy"}`)); !ok {
		t.Fatal("update.busy rejected by the hub parser")
	}
	if _, ok := parseHubInbound([]byte(`{"type":"update.busy","version":"r19c"}`)); ok {
		t.Fatal("update.busy carrier field accepted")
	}
}

// TestR19UpdateBackupsArePruned bounds the rollback copies kept beside the
// executable. Each one is a full binary, so an unpruned series fills the
// install directory in proportion to how often the node updates.
func TestR19UpdateBackupsArePruned(t *testing.T) {
	directory := t.TempDir()
	executable := filepath.Join(directory, "panewire")
	if err := os.WriteFile(executable, []byte("old binary"), 0755); err != nil {
		t.Fatal(err)
	}
	stale := []string{"20200101T000000Z", "20200102T000000Z", "20200103T000000Z", "20200104T000000Z"}
	for _, timestamp := range stale {
		if err := os.WriteFile(executable+".bak-"+timestamp, []byte(timestamp), 0755); err != nil {
			t.Fatal(err)
		}
	}
	// An unrelated sibling must survive: pruning is scoped to this executable's
	// own rollback copies.
	if err := os.WriteFile(filepath.Join(directory, "other.bak-20200101T000000Z"), []byte("other"), 0644); err != nil {
		t.Fatal(err)
	}

	asset := []byte("new binary")
	digest := sha256.Sum256(asset)
	updateClient := &http.Client{Transport: hubRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(asset)), Request: request}, nil
	})}
	url := "https://github.com/mgh3326/panewire/releases/download/r19c/panewire_linux_amd64"
	if err := applyHubUpdate(context.Background(), updateClient, executable, url, hex.EncodeToString(digest[:])); err != nil {
		t.Fatal(err)
	}

	backups, globErr := filepath.Glob(executable + ".bak-*")
	if globErr != nil {
		t.Fatal(globErr)
	}
	if len(backups) != hubUpdateBackupsKept {
		t.Fatalf("backups=%v, want %d retained", backupNames(backups), hubUpdateBackupsKept)
	}
	sort.Strings(backups)
	// The newest stale copy and the one this update just wrote are what remain.
	if filepath.Base(backups[0]) != "panewire.bak-"+stale[len(stale)-1] {
		t.Fatalf("pruning removed the newest copies: %v", backupNames(backups))
	}
	newest, readErr := os.ReadFile(backups[1])
	if readErr != nil || string(newest) != "old binary" {
		t.Fatalf("rollback copy=%q err=%v", newest, readErr)
	}
	if _, err := os.Stat(filepath.Join(directory, "other.bak-20200101T000000Z")); err != nil {
		t.Fatalf("pruning removed an unrelated sibling: %v", err)
	}
	// Below the retention count nothing is removed.
	pruneHubUpdateBackups(executable, hubUpdateBackupsKept)
	if remaining, _ := filepath.Glob(executable + ".bak-*"); len(remaining) != hubUpdateBackupsKept {
		t.Fatalf("a second prune removed retained copies: %v", backupNames(remaining))
	}
}

func backupNames(paths []string) []string {
	names := make([]string, 0, len(paths))
	for _, path := range paths {
		names = append(names, filepath.Base(path))
	}
	sort.Strings(names)
	return names
}

// TestR19PreferenceSwitchesDoNotNest walks the node up five preferred endpoints
// in one session. Each switch used to be a recursive serve call, so the
// abandoned frame kept its ping ticker, its preference ticker, and its stack
// alive until the whole chain unwound.
func TestR19PreferenceSwitchesDoNotNest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			if _, _, err := conn.Read(request.Context()); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	endpoints := []string{
		"ws://127.0.0.1:9001", "ws://127.0.0.1:9002", "ws://127.0.0.1:9003",
		"ws://127.0.0.1:9004", "ws://127.0.0.1:9005", "ws://127.0.0.1:9006",
	}
	const switchCount = 5
	client, err := NewHubClient(HubClientConfig{
		URLs: endpoints, MachineID: "node-a", Token: "node-token", AllowInsecureForTests: true,
		PingInterval: time.Hour, PreferRetry: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	target := switchCount - 1
	switches := 0
	// Only the endpoint one step above the current one accepts a dial, so each
	// preference tick moves the session up exactly one endpoint.
	client.dial = func(ctx context.Context, endpoint string, _ *websocket.DialOptions) (*websocket.Conn, *http.Response, error) {
		mu.Lock()
		wanted := ""
		if target >= 0 {
			wanted = client.endpoints[target]
		}
		mu.Unlock()
		if endpoint != wanted {
			return nil, nil, errors.New("down")
		}
		conn, response, dialErr := websocket.Dial(ctx, r6WSURL(server.URL, ""), nil)
		if dialErr != nil {
			return nil, nil, dialErr
		}
		mu.Lock()
		target--
		switches++
		mu.Unlock()
		return conn, response, nil
	}
	client.endpoint = client.endpoints[switchCount]

	first, _, err := websocket.Dial(t.Context(), r6WSURL(server.URL, ""), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- client.serve(ctx, first) }()

	baseline := 0
	deadline := time.Now().Add(15 * time.Second)
	for {
		mu.Lock()
		done := switches
		mu.Unlock()
		if done == 1 && baseline == 0 {
			// Measured once the session is fully established, so the reader
			// goroutine and the preference probe are already counted.
			baseline = runtime.NumGoroutine()
		}
		if done == switchCount {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d preference switches completed", done, switchCount)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Let the superseded session's reader goroutine observe its closed
	// connection and exit before anything is counted.
	time.Sleep(300 * time.Millisecond)

	if nested := serveConnectionFrames(); nested != 1 {
		t.Fatalf("%d serveConnection frames are live after %d switches, want 1", nested, switchCount)
	}
	if grown := runtime.NumGoroutine() - baseline; grown > 2 {
		t.Fatalf("%d preference switches leaked %d goroutines", switchCount, grown)
	}
	cancel()
	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("client did not stop")
	}
}

// serveConnectionFrames counts the live serveConnection frames across all
// goroutines. One session must occupy exactly one frame; a recursive switch
// shows up as a frame per abandoned session, each still holding two tickers.
func serveConnectionFrames() int {
	buffer := make([]byte, 1<<20)
	for {
		length := runtime.Stack(buffer, true)
		if length < len(buffer) {
			return strings.Count(string(buffer[:length]), "(*HubClient).serveConnection(")
		}
		buffer = make([]byte, 2*len(buffer))
	}
}

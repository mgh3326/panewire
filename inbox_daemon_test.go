package panewire

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func inboxDaemonSocket(t *testing.T, d *Daemon) string {
	t.Helper()
	path := filepath.Join(r20SocketRoot(t), "inbox.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go d.handle(context.Background(), conn)
		}
	}()
	t.Cleanup(func() { listener.Close(); <-done })
	return path
}

func TestInboxDaemonParity(t *testing.T) {
	for _, key := range []string{"PANEWIRE_HUB_URL", "PANEWIRE_HUB_TOKEN_ENV", "PANEWIRE_HUB_CF_ENV", "PANEWIRE_LANE"} {
		t.Setenv(key, "")
	}
	const token, cfID, cfSecret = "synthetic-token-2917", "synthetic-cf-id-2917", "synthetic-cf-secret-2917"
	item := laneInboxItem{ID: 1, Lane: "lane-a", Kind: "lane.event", Class: "inbox", Event: hubJobEventPayload{Text: "hello"}}
	itemJSON, _ := json.Marshal(item)
	var nodeRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get(hubMachineIDHeader) != "host-a" {
			w.WriteHeader(401)
			return
		}
		if id, secret := r.Header.Get("CF-Access-Client-Id"), r.Header.Get("CF-Access-Client-Secret"); id != "" || secret != "" {
			if id != cfID || secret != cfSecret {
				w.WriteHeader(403)
				return
			}
			nodeRequests.Add(1)
		}
		if r.URL.Query().Get("lane") == "lane-denied" {
			w.WriteHeader(403)
			return
		}
		switch r.URL.Path {
		case "/v1/inbox":
			fmt.Fprintf(w, "[%s]\n", itemJSON)
		case "/v1/inbox/1/read":
			fmt.Fprintf(w, "%s\n", itemJSON)
		case "/v1/inbox/ack":
			fmt.Fprint(w, "{\"acked\":1}\n")
		case "/v1/inbox/count":
			fmt.Fprint(w, "{\"lane-a\":1}\n")
		case "/v1/inbox/ack-state":
			if r.URL.Query().Get("lane") != "" {
				w.WriteHeader(400)
				return
			}
			if r.URL.Query().Get("event_id") == "evt-missing" {
				w.WriteHeader(404)
				fmt.Fprint(w, "{\"events\":[],\"not_found\":[\"evt-missing\"]}\n")
				return
			}
			fmt.Fprint(w, "{\"events\":[{\"event_id\":\"evt-1\",\"acked\":true,\"acked_at\":\"2026-11-05T00:00:00Z\"}],\"not_found\":[]}\n")
		case "/v1/inbox/wait":
			if r.URL.Query().Get("lane") == "lane-empty" {
				d, _ := time.ParseDuration(r.URL.Query().Get("timeout"))
				select {
				case <-time.After(d):
				case <-r.Context().Done():
					return
				}
				fmt.Fprint(w, "{\"item\":null}\n")
				return
			}
			fmt.Fprintf(w, "{\"item\":%s}\n", itemJSON)
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	tokenPath := filepath.Join(t.TempDir(), "fixture.env")
	if err := os.WriteFile(tokenPath, []byte("HUB_MACHINE_ID=host-a\nHUB_TOKEN="+token+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	client, err := NewHubClient(HubClientConfig{URL: strings.Replace(server.URL, "http://", "ws://", 1), MachineID: "host-a", Token: token, CFAccessClientID: cfID, CFAccessClientSecret: cfSecret, AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	daemon := NewDaemon(Config{Hub: HubDaemonConfig{Client: client}, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	socket := inboxDaemonSocket(t, daemon)
	deps := hubCLIDeps{AllowInsecureForTests: true, SocketPath: socket}
	cases := [][]string{{"list", "--lane", "lane-a", "--unread"}, {"list", "--lane", "legacy.parent"}, {"read", "1", "--lane", "lane-a"}, {"ack", "1", "--lane", "lane-a"}, {"count", "--lane", "lane-a"}, {"wait", "--lane", "lane-a", "--timeout", "1s"}, {"wait", "--lane", "legacy.parent", "--timeout", "1s"}, {"wait", "--lane", "lane-empty", "--timeout", "25ms"}, {"list", "--lane", "lane-denied"}, {"status", "--event-id", "evt-1"}, {"status", "--event-id", "evt-missing"}}
	for _, args := range cases {
		for _, jsonMode := range []bool{false, true} {
			args := append([]string(nil), args...)
			if jsonMode {
				args = append(args, "--json")
			}
			var directOut, directErr, localOut, localErr bytes.Buffer
			directArgs := append(append([]string(nil), args...), "--hub-url", server.URL, "--hub-token-env", tokenPath)
			directDeps := deps
			directDeps.SocketPath = filepath.Join(t.TempDir(), "absent.sock")
			directCode := runInboxCLI(directArgs, &directOut, &directErr, directDeps)
			localCode := runInboxCLI(args, &localOut, &localErr, deps)
			if directCode != localCode || directOut.String() != localOut.String() || directErr.String() != localErr.String() {
				t.Errorf("%v: direct %d %q %q; daemon %d %q %q", args, directCode, directOut.String(), directErr.String(), localCode, localOut.String(), localErr.String())
			}
			if strings.Contains(strings.Join(args, " "), "lane-empty") && (localCode != ExitTimeout || localOut.Len() != 0 || localErr.Len() != 0) {
				t.Errorf("timeout: %d %q %q", localCode, localOut.String(), localErr.String())
			}
			if strings.Contains(strings.Join(args, " "), "lane-denied") && localCode != ExitDeliveryFailure {
				t.Errorf("refusal: %d", localCode)
			}
			if strings.Contains(strings.Join(args, " "), "evt-missing") && localCode != ExitPartial {
				t.Errorf("status not found: %d", localCode)
			}
			if strings.Contains(strings.Join(args, " "), "evt-1") && localCode != ExitOK {
				t.Errorf("status: %d %q", localCode, localErr.String())
			}
		}
	}
	if nodeRequests.Load() == 0 {
		t.Fatal("daemon did not forward node CF credentials")
	}
	// An ambient lane must not leak into status: the hub rejects a lane there.
	t.Setenv("PANEWIRE_LANE", "lane-a")
	var ambientOut, ambientErr bytes.Buffer
	if code := runInboxCLI([]string{"status", "--event-id", "evt-1", "--json"}, &ambientOut, &ambientErr, deps); code != ExitOK {
		t.Errorf("status with ambient lane: %d %q %q", code, ambientOut.String(), ambientErr.String())
	}
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	json.NewEncoder(conn).Encode(localRequest{Op: "inbox", TimeoutMS: 5000, Inbox: &inboxRequest{Command: "count", Lane: "lane-a", Timeout: time.Second}})
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		t.Fatal(scanner.Err())
	}
	response := scanner.Text()
	for _, secret := range []string{token, cfID, cfSecret, tokenPath} {
		if strings.Contains(response+logs.String(), secret) {
			t.Error("credential or path leaked")
		}
	}
	t.Setenv("PANEWIRE_HUB_URL", server.URL)
	t.Setenv("PANEWIRE_HUB_TOKEN_ENV", tokenPath)
	var out, errOut bytes.Buffer
	deps.SocketPath = filepath.Join(t.TempDir(), "absent.sock")
	if code := runInboxCLI([]string{"count", "--lane", "lane-a"}, &out, &errOut, deps); code != ExitOK {
		t.Fatalf("env direct path: %d %s", code, errOut.String())
	}
}

func TestInboxDaemonUnavailable(t *testing.T) {
	for _, key := range []string{"PANEWIRE_HUB_URL", "PANEWIRE_HUB_TOKEN_ENV", "PANEWIRE_HUB_CF_ENV", "PANEWIRE_LANE"} {
		t.Setenv(key, "")
	}
	socket := inboxDaemonSocket(t, NewDaemon(Config{}))
	for _, path := range []string{socket, filepath.Join(t.TempDir(), "absent.sock")} {
		var out, errOut bytes.Buffer
		code := runInboxCLI([]string{"count", "--lane", "lane-a"}, &out, &errOut, hubCLIDeps{SocketPath: path})
		if code != ExitDaemonUnavailable || out.Len() != 0 || strings.Count(errOut.String(), "\n") != 1 {
			t.Errorf("unavailable: %d %q %q", code, out.String(), errOut.String())
		}
	}
}

package panewire

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mgh3326/panewire/internal/daemonop"
)

type recordedLanePut struct {
	Method, Path, Auth, Machine, Body string
}

type lanePutAnswer struct {
	OK     bool            `json:"ok"`
	Code   int             `json:"code"`
	Error  string          `json:"error"`
	Result json.RawMessage `json:"result"`
}

// lanePutSocket is the request a node's spawn engine sends: the shared
// daemonop fields inside the local request envelope.
func lanePutSocket(t *testing.T, socket string, put daemonop.LanesPut) lanePutAnswer {
	t.Helper()
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	envelope := struct {
		Op string `json:"op"`
		daemonop.LanesPut
		TimeoutMS int64 `json:"timeout_ms"`
	}{Op: daemonop.LanesPutOp, LanesPut: put, TimeoutMS: 5000}
	if err := json.NewEncoder(conn).Encode(envelope); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	var answer lanePutAnswer
	if err := json.Unmarshal([]byte(line), &answer); err != nil {
		t.Fatalf("%v: %s", err, line)
	}
	return answer
}

// The daemon's lanes.put and the credentialled `lanes add` command must make
// the same hub PUT and report the same refusal text and exit status, because
// the spawn receipt records whichever route answered.
func TestLanesPutDaemonParity(t *testing.T) {
	const token = "synthetic-lanes-token-3013"
	var mu sync.Mutex
	var requests []recordedLanePut
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		lane := strings.TrimPrefix(r.URL.Path, "/v1/lanes/")
		mu.Lock()
		requests = append(requests, recordedLanePut{Method: r.Method, Path: r.URL.Path, Auth: r.Header.Get("Authorization"), Machine: r.Header.Get(hubMachineIDHeader), Body: string(body)})
		mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get(hubMachineIDHeader) != "host-a" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		projection := fmt.Sprintf(`{"lane":%q,"machine":"host-a","pane":"w1:p1","parent":"desk","sink":false}`, lane)
		switch lane {
		case "lane-ok":
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintln(w, projection)
		case "lane-replace":
			w.WriteHeader(http.StatusOK)
			fmt.Fprintln(w, projection)
		case "lane-mismatch":
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprintln(w, `{"error":"lane_machine_mismatch"}`)
		case "lane-cap":
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprintln(w, `{"error":"lane_quota_exceeded"}`)
		case "lane-hint":
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprintln(w, `{"error":"lane_machine_mismatch","use":"register the parent first"}`)
		case "lane-bad":
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintln(w, `{"error":"invalid_lane_request"}`)
		case "lane-down":
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintln(w, `{"error":"lanes_invalid"}`)
		default:
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintln(w, "not json")
		}
	}))
	defer server.Close()

	tokenPath := filepath.Join(t.TempDir(), "node.env")
	if err := os.WriteFile(tokenPath, []byte("HUB_MACHINE_ID=host-a\nHUB_TOKEN="+token+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	client, err := NewHubClient(HubClientConfig{URL: strings.Replace(server.URL, "http://", "ws://", 1), MachineID: "host-a", Token: token, AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	daemon := NewDaemon(Config{Hub: HubDaemonConfig{Client: client}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	socket := inboxDaemonSocket(t, daemon)

	cases := []daemonop.LanesPut{
		{Lane: "lane-ok", Machine: "host-a", Pane: "w1:p1", Parent: "desk"},
		{Lane: "lane-replace", Machine: "host-a", Pane: "w1:p1", Parent: "desk"},
		{Lane: "lane-mismatch", Machine: "host-a", Pane: "w1:p1", Parent: "desk"},
		{Lane: "lane-cap", Machine: "host-a", Pane: "w1:p1", Parent: "desk"},
		{Lane: "lane-hint", Machine: "host-a", Pane: "w1:p1", Parent: "desk"},
		{Lane: "lane-bad", Machine: "host-a", Pane: "w1:p1", Parent: "desk"},
		{Lane: "lane-down", Machine: "host-a", Pane: "w1:p1", Parent: "desk"},
		{Lane: "lane-garbage", Machine: "host-a", Pane: "w1:p1", Parent: "desk"},
		{Lane: "lane-ok", Machine: "operator", Pane: "w1:p1"},
		{Lane: "lane-ok", Machine: "host-a", Pane: "not a pane"},
		{Lane: "lane-ok", Machine: "host-a", Pane: "w1:p1", Parent: "lane-ok"},
	}
	for _, put := range cases {
		t.Run(put.Lane+"/"+put.Machine+"/"+put.Pane+"/"+put.Parent, func(t *testing.T) {
			args := []string{"add", put.Lane, "--machine", put.Machine, "--pane", put.Pane, "--hub-url", server.URL, "--hub-token-env", tokenPath}
			if put.Parent != "" {
				args = append(args, "--parent", put.Parent)
			}
			var directOut, directErr bytes.Buffer
			mu.Lock()
			before := len(requests)
			mu.Unlock()
			directCode := runLanesCLI(args, &directOut, &directErr, hubCLIDeps{AllowInsecureForTests: true})

			answer := lanePutSocket(t, socket, put)

			if answer.Code != directCode {
				t.Fatalf("exit: daemon %d, direct %d (%q)", answer.Code, directCode, directErr.String())
			}
			if answer.Error != strings.TrimSpace(directErr.String()) {
				t.Fatalf("refusal text: daemon %q, direct %q", answer.Error, directErr.String())
			}
			if got, want := string(answer.Result), strings.TrimSpace(directOut.String()); got != want {
				t.Fatalf("projection: daemon %q, direct %q", got, want)
			}
			mu.Lock()
			defer mu.Unlock()
			reached := len(requests) - before
			if put.Machine == "operator" || !validLanePane(put.Pane) || put.Parent == put.Lane {
				if reached != 0 {
					t.Fatalf("invalid write reached the hub: %+v", requests[before:])
				}
				return
			}
			if reached != 2 {
				t.Fatalf("hub PUTs=%d, want one per route", reached)
			}
			direct, local := requests[len(requests)-2], requests[len(requests)-1]
			if direct != local {
				t.Fatalf("daemon PUT differs from the credentialled CLI PUT:\ndirect %+v\ndaemon %+v", direct, local)
			}
			if direct.Method != http.MethodPut || direct.Path != "/v1/lanes/"+put.Lane || direct.Machine != "host-a" || !strings.Contains(direct.Body, `"machine":"`+put.Machine+`"`) {
				t.Fatalf("hub request: %+v", direct)
			}
		})
	}
}

// A daemon with no hub client cannot serve the op; the response code is the
// one the spawn engine reads as "use the fleet-config route".
func TestLanesPutDaemonWithoutHubClient(t *testing.T) {
	socket := inboxDaemonSocket(t, NewDaemon(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}))
	answer := lanePutSocket(t, socket, daemonop.LanesPut{Lane: "lane-ok", Machine: "host-a", Pane: "w1:p1"})
	if answer.OK || answer.Code != ExitDaemonUnavailable || answer.Error != "lanes: daemon has no hub client" {
		t.Fatalf("answer=%+v", answer)
	}
}

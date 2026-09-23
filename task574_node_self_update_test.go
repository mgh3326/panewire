package panewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const (
	t574Version    = "pw-abc1234"
	t574OldVersion = "pw-0000000"
	t574SHA        = "abababababababababababababababababababababababababababababababab"
)

func t574ReleaseURL(version string) string {
	return "https://github.com/mgh3326/panewire/releases/download/v0.1.0/panewire_" + version + "_linux_amd64"
}

// t574WSPair returns a client connection whose server side records every JSON
// message the connection writes.
func t574WSPair(t *testing.T) (*websocket.Conn, <-chan map[string]any) {
	t.Helper()
	received := make(chan map[string]any, 32)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			var message map[string]any
			if wsjson.Read(request.Context(), conn, &message) != nil {
				return
			}
			received <- message
		}
	}))
	t.Cleanup(server.Close)
	conn, _, err := websocket.Dial(t.Context(), r6WSURL(server.URL, ""), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn, received
}

func t574Drain(received <-chan map[string]any, wait time.Duration) []map[string]any {
	var out []map[string]any
	deadline := time.After(wait)
	for {
		select {
		case message := <-received:
			out = append(out, message)
		case <-deadline:
			return out
		}
	}
}

// t574PublishHub has two live nodes: node-a and the NCP root node itself, so a
// refused ncp target can never be explained by "node unavailable".
func t574PublishHub(t *testing.T) (*HubServer, map[string]<-chan map[string]any) {
	t.Helper()
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op", "node-a": "node-a-token", "ncp": "ncp-token"}})
	if err != nil {
		t.Fatal(err)
	}
	channels := make(map[string]<-chan map[string]any)
	for _, machine := range []string{"node-a", "ncp"} {
		conn, received := t574WSPair(t)
		hub.nodes[machine] = &hubNodeRecord{machineID: machine, agent: &hubAgent{conn: conn}, state: "connected", remoteMeta: map[string]string{"version": t574OldVersion}}
		channels[machine] = received
	}
	return hub, channels
}

func t574Publish(t *testing.T, hub *HubServer, version, url string, machines []string) int {
	t.Helper()
	body, err := json.Marshal(map[string]any{"version": version, "sha256": t574SHA, "url": url, "machines": machines})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/update", bytes.NewReader(body))
	request.Header.Set(hubAuthorizationHeader, "Bearer op")
	writer := httptest.NewRecorder()
	hub.handleUpdatePublish(writer, request)
	return writer.Code
}

// Mutants (a) and (e): a GitHub URL outside the pinned repository's release
// path is refused with 400 and nothing reaches the node.
func TestT574PublishRejectsReleaseURLOutsidePinnedRepository(t *testing.T) {
	hub, channels := t574PublishHub(t)
	const tail = "/releases/download/v0.1.0/panewire_" + t574Version + "_linux_amd64"
	for _, url := range []string{
		"https://github.com/evil/panewire" + tail,
		"https://github.com/mgh3326/other" + tail,
		"https://github.com/MGH3326/panewire" + tail,
		"https://github.com/mgh3326/PANEWIRE" + tail,
		"https://github.com/mgh3326/panewire/../../evil/panewire" + tail,
		"https://github.com/mgh3326/panewire/releases/download/../panewire_" + t574Version + "_linux_amd64",
		"https://github.com/mgh3326/panewire/releases/download/./panewire_" + t574Version + "_linux_amd64",
		"https://github.com/mgh3326%2Fpanewire" + tail,
		"https://github.com/%6dgh3326/panewire" + tail,
		"https://github.com/mgh3326/panewire/releases/download/v0.1.0/panewire_" + t574Version + "_linux_amd64%2F..%2Fx",
		"https://github.com/mgh3326/panewire\\..\\evil" + tail,
		"https://github.com/mgh3326/panewire" + tail + "?x=1",
		"https://github.com/mgh3326/panewire" + tail + "?",
		"https://github.com/mgh3326/panewire" + tail + "#frag",
		"https://mgh3326@github.com/mgh3326/panewire" + tail,
		"https://github.com:8443/mgh3326/panewire" + tail,
		"http://github.com/mgh3326/panewire" + tail,
		"https://github.com.evil.example/mgh3326/panewire" + tail,
		"https://objects.githubusercontent.com/mgh3326/panewire" + tail,
		"https://release-assets.githubusercontent.com/mgh3326/panewire" + tail,
		"https://github.com/mgh3326/panewire/releases/download/v0.1.0/extra/panewire_" + t574Version + "_linux_amd64",
		"https://github.com/mgh3326/panewire/releases/latest/download/panewire_" + t574Version + "_linux_amd64",
		"https://github.com/mgh3326/panewire/releases/download/v0.1.0/evil_panewire_" + t574Version + "_linux_amd64",
		"https://github.com/mgh3326/panewire/releases/download/v0.1.0/panewire_" + t574Version + "_windows_amd64",
		// Asset for a different version than the one published.
		t574ReleaseURL("pw-def5678"),
	} {
		if code := t574Publish(t, hub, t574Version, url, []string{"node-a"}); code != http.StatusBadRequest {
			t.Errorf("publish %s = %d, want 400", url, code)
		}
	}
	if len(hub.expectedVersion) != 0 {
		t.Fatalf("rejected publishes recorded expectations: %+v", hub.expectedVersion)
	}
	if got := t574Drain(channels["node-a"], 200*time.Millisecond); len(got) != 0 {
		t.Fatalf("rejected publishes reached the node: %v", got)
	}
	// Positive control: the pinned release URL for the published version.
	if code := t574Publish(t, hub, t574Version, t574ReleaseURL(t574Version), []string{"node-a"}); code != http.StatusAccepted {
		t.Fatalf("pinned release URL = %d, want 202", code)
	}
	got := t574Drain(channels["node-a"], 500*time.Millisecond)
	if len(got) != 1 || got[0]["type"] != "update.available" || got[0]["url"] != t574ReleaseURL(t574Version) {
		t.Fatalf("pinned publish delivered %v", got)
	}
}

// Mutant (d): every spelling of the NCP root node in --machines is refused as
// a whole request, even with ncp connected and another valid target present.
func TestT574PublishRejectsNCPInEveryShape(t *testing.T) {
	hub, channels := t574PublishHub(t)
	for _, machines := range [][]string{
		{"ncp"},
		{"NCP"},
		{"Ncp"},
		{" ncp"},
		{"ncp "},
		{"\tncp"},
		{"node-a", "ncp"},
		{"ncp", "node-a"},
		{"ncp", "ncp"},
		{"node-a", "NCP"},
	} {
		if code := t574Publish(t, hub, t574Version, t574ReleaseURL(t574Version), machines); code != http.StatusBadRequest {
			t.Errorf("publish to %q = %d, want 400", machines, code)
		}
	}
	// Duplicate targets are refused too, so no repetition trick reaches the
	// per-machine loop.
	if code := t574Publish(t, hub, t574Version, t574ReleaseURL(t574Version), []string{"node-a", "node-a"}); code != http.StatusBadRequest {
		t.Errorf("duplicate targets = %d, want 400", code)
	}
	if len(hub.expectedVersion) != 0 {
		t.Fatalf("refused publishes recorded expectations: %+v", hub.expectedVersion)
	}
	for machine, received := range channels {
		if got := t574Drain(received, 200*time.Millisecond); len(got) != 0 {
			t.Fatalf("refused publish reached %s: %v", machine, got)
		}
	}
	if code := t574Publish(t, hub, t574Version, t574ReleaseURL(t574Version), []string{"node-a"}); code != http.StatusAccepted {
		t.Fatalf("control publish = %d, want 202", code)
	}
	if got := t574Drain(channels["ncp"], 200*time.Millisecond); len(got) != 0 {
		t.Fatalf("control publish reached ncp: %v", got)
	}
}

// The operator CLI refuses ncp before contacting any hub, which is the only
// guard while the hub still runs code older than this check.
func TestT574UpdateCLIRefusesNCPWithoutContactingHub(t *testing.T) {
	tokenEnv, _ := t441Envs(t)
	var hits int
	var mu sync.Mutex
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		writer.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	for _, machines := range []string{"ncp", "NCP", "node-a, ncp", "node-a,Ncp"} {
		var stdout, stderr bytes.Buffer
		code := runUpdateCLI([]string{"publish", "--hub-url", server.URL, "--hub-token-env", tokenEnv, "--version", t574Version, "--sha256", t574SHA, "--url", t574ReleaseURL(t574Version), "--machines", machines}, &stdout, &stderr, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true})
		if code != ExitConditionInvalid {
			t.Errorf("--machines %q exit=%d, want %d", machines, code, ExitConditionInvalid)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if hits != 0 {
		t.Fatalf("refused publishes contacted the hub %d times", hits)
	}
}

// A node whose own identity is ncp never downloads, whatever the hub sends.
func TestT574NCPNodeNeverAppliesUpdate(t *testing.T) {
	executable := filepath.Join(t.TempDir(), "panewire")
	if err := os.WriteFile(executable, []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}
	downloads := 0
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1:1", MachineID: "ncp", Token: "fixture", AllowInsecureForTests: true, ExecutablePath: executable, Restart: func() { t.Error("ncp restarted") },
		UpdateHTTPClient: &http.Client{Transport: hubRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
			downloads++
			return nil, errors.New("unexpected download")
		})}})
	if err != nil {
		t.Fatal(err)
	}
	client.beginHubUpdate()
	client.handleHubUpdate(t.Context(), nil, hubOutboundMessage{Type: "update.available", Version: t574Version, SHA256: t574SHA, URL: t574ReleaseURL(t574Version)})
	if downloads != 0 {
		t.Fatalf("ncp node downloaded %d times", downloads)
	}
}

func t574AssetClient(asset []byte) *http.Client {
	return &http.Client{Transport: hubRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(asset)), Request: request}, nil
	})}
}

func t574Install(t *testing.T) (string, string) {
	t.Helper()
	directory := t.TempDir()
	executable := filepath.Join(directory, "panewire")
	if err := os.WriteFile(executable, []byte("old binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	// One earlier rollback copy: a refused candidate must not add another.
	if err := os.WriteFile(executable+".bak-20200101T000000Z", []byte("older binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	return directory, executable
}

func t574Backups(t *testing.T, executable string) []string {
	t.Helper()
	backups, err := filepath.Glob(executable + ".bak-*")
	if err != nil {
		t.Fatal(err)
	}
	return backups
}

// Mutants (b) and (f): a candidate whose checksum matches but whose smoke run
// fails leaves the executable and the rollback copies exactly as they were.
//
// Every case except "timeout" runs under the production 5s bound: macOS scans
// a freshly written executable on its first exec (0.4-0.9s observed), so a
// short bound would reject every case by timeout and hide what each one tests.
func TestT574SmokeFailureLeavesExecutableAndBackups(t *testing.T) {
	for _, fixture := range []struct {
		name  string
		asset string
	}{
		{"exit non-zero", "#!/bin/sh\necho " + t574Version + "\nexit 1\n"},
		{"version mismatch", "#!/bin/sh\necho pw-def5678\n"},
		{"no output", "#!/bin/sh\nexit 0\n"},
		{"stderr only", "#!/bin/sh\necho " + t574Version + " >&2\n"},
		{"extra output", "#!/bin/sh\necho " + t574Version + "\necho debug\n"},
		{"timeout", "#!/bin/sh\nsleep 30\necho " + t574Version + "\n"},
		{"not executable format", "new binary"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			if fixture.name == "timeout" {
				previous := hubUpdateSmokeTimeout
				hubUpdateSmokeTimeout = 2 * time.Second
				defer func() { hubUpdateSmokeTimeout = previous }()
			}
			directory, executable := t574Install(t)
			asset := []byte(fixture.asset)
			digest := sha256.Sum256(asset)
			started := time.Now()
			err := applyHubUpdate(context.Background(), t574AssetClient(asset), executable, hubTestUpdate(t574ReleaseURL(t574Version), hex.EncodeToString(digest[:]), t574Version))
			if !errors.Is(err, errHubUpdateSmoke) {
				t.Fatalf("err=%v, want smoke rejection", err)
			}
			// Only the timeout case may be refused by the deadline; every other
			// case must be refused by its own check.
			if elapsed := time.Since(started); fixture.name != "timeout" && elapsed >= hubUpdateSmokeTimeout {
				t.Fatalf("refused only by the %v deadline (elapsed %v)", hubUpdateSmokeTimeout, elapsed)
			}
			if got, _ := os.ReadFile(executable); string(got) != "old binary" {
				t.Fatalf("executable replaced: %q", got)
			}
			if backups := t574Backups(t, executable); len(backups) != 1 {
				t.Fatalf("backups=%v, want the one pre-existing copy", backupNames(backups))
			}
			if _, err := os.Stat(hubUpdateStatePath(executable)); !os.IsNotExist(err) {
				t.Fatalf("probation state written for a refused candidate: %v", err)
			}
			entries, _ := os.ReadDir(directory)
			if len(entries) != 2 {
				t.Fatalf("leftover files: %v", entries)
			}
		})
	}
	t.Run("positive control", func(t *testing.T) {
		_, executable := t574Install(t)
		asset := hubTestSmokeAsset(t574Version)
		digest := sha256.Sum256(asset)
		if err := applyHubUpdate(context.Background(), t574AssetClient(asset), executable, hubTestUpdate(t574ReleaseURL(t574Version), hex.EncodeToString(digest[:]), t574Version)); err != nil {
			t.Fatal(err)
		}
		if got, _ := os.ReadFile(executable); !bytes.Equal(got, asset) {
			t.Fatalf("executable=%q", got)
		}
		if backups := t574Backups(t, executable); len(backups) != 2 {
			t.Fatalf("backups=%v, want one new copy", backupNames(backups))
		}
		state, found := readHubUpdateState(executable)
		if !found || state.Version != t574Version || state.Starts != 0 || !strings.HasPrefix(state.Backup, "panewire.bak-") {
			t.Fatalf("probation state=%+v found=%v", state, found)
		}
	})
}

// The smoke rejection travels through the real node read loop to the hub as
// update.rejected, and the hub shows it as the node's LAST_NOTE.
func TestT574SmokeRejectionIsReportedToHub(t *testing.T) {
	_, executable := t574Install(t)
	asset := []byte("#!/bin/sh\necho pw-def5678\n")
	digest := sha256.Sum256(asset)
	sent := make(chan map[string]any, 8)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_ = wsjson.Write(request.Context(), conn, map[string]any{"type": "update.available", "version": t574Version, "sha256": hex.EncodeToString(digest[:]), "url": t574ReleaseURL(t574Version)})
		for {
			var message map[string]any
			if wsjson.Read(request.Context(), conn, &message) != nil {
				return
			}
			if message["type"] == "update.rejected" {
				sent <- message
			}
		}
	}))
	defer server.Close()
	client, err := NewHubClient(HubClientConfig{URL: r6WSURL(server.URL, ""), MachineID: "node-a", Token: "node-token", AllowInsecureForTests: true, PingInterval: time.Hour, PreferRetry: time.Hour, ExecutablePath: executable, UpdateHTTPClient: t574AssetClient(asset), Restart: func() { t.Error("restarted after a refused candidate") }})
	if err != nil {
		t.Fatal(err)
	}
	conn, _, err := websocket.Dial(t.Context(), client.endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = client.serve(ctx, conn) }()
	var rejected map[string]any
	select {
	case rejected = <-sent:
	case <-time.After(10 * time.Second):
		t.Fatal("no update.rejected reached the hub")
	}
	if rejected["reason"] != "smoke" || rejected["version"] != t574Version {
		t.Fatalf("update.rejected=%v", rejected)
	}
	raw, _ := json.Marshal(rejected)

	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op", "node-a": "node-token"}})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{}
	hub.connect("node-a", t574OldVersion, "test", agent, false)
	hub.handleAgentMessage("node-a", "test", agent, raw)
	nodes := hub.Nodes()
	if len(nodes) != 1 || nodes[0].LastNote == nil || nodes[0].LastNote.Text != "update rejected(smoke) "+t574Version {
		t.Fatalf("LAST_NOTE=%+v", nodes[0].LastNote)
	}
	for _, bad := range []string{
		`{"type":"update.rejected","reason":"other","version":"pw-abc1234"}`,
		`{"type":"update.rejected","reason":"smoke","version":"bad version"}`,
		`{"type":"update.rejected","reason":"smoke","version":"pw-abc1234","extra":1}`,
		`{"type":"update.rejected","reason":"smoke"}`,
	} {
		if _, ok := parseHubInbound([]byte(bad)); ok {
			t.Errorf("parseHubInbound accepted %s", bad)
		}
	}
}

func t574Probation(t *testing.T, starts int, withBackup bool) (string, string) {
	t.Helper()
	directory := t.TempDir()
	executable := filepath.Join(directory, "panewire")
	if err := os.WriteFile(executable, []byte("new binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	backup := "panewire.bak-20260923T000000Z"
	if withBackup {
		if err := os.WriteFile(filepath.Join(directory, backup), []byte("old binary"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeHubUpdateState(executable, hubUpdateState{Version: t574Version, Backup: backup, Starts: starts}); err != nil {
		t.Fatal(err)
	}
	return directory, executable
}

// AC4 through the real call site: each `panewire daemon` start of the new
// version that dies (here on its flags) counts; the fourth start restores the
// rollback copy and exits for the supervisor.
func TestT574DaemonStartRollsBackAfterThreeFailedStarts(t *testing.T) {
	_, executable := t574Probation(t, 0, true)
	deps := daemonCLIDeps{Version: t574Version, ExecutablePath: executable}
	for start := 1; start <= hubUpdateRollbackAfter; start++ {
		if code := runDaemonCLIWithDeps([]string{"--not-a-flag"}, deps); code != ExitUsage {
			t.Fatalf("start %d exit=%d, want the new binary's own failure", start, code)
		}
		if got, _ := os.ReadFile(executable); string(got) != "new binary" {
			t.Fatalf("start %d rolled back early", start)
		}
		if state, _ := readHubUpdateState(executable); state.Starts != start {
			t.Fatalf("start %d recorded starts=%d", start, state.Starts)
		}
	}
	if code := runDaemonCLIWithDeps([]string{"--not-a-flag"}, deps); code != ExitInternal {
		t.Fatalf("fourth start exit=%d, want rollback exit %d", code, ExitInternal)
	}
	if got, _ := os.ReadFile(executable); string(got) != "old binary" {
		t.Fatalf("fourth start left executable=%q", got)
	}
	if _, err := os.Stat(hubUpdateStatePath(executable)); !os.IsNotExist(err) {
		t.Fatalf("probation state survived rollback: %v", err)
	}
	if backups := t574Backups(t, executable); len(backups) != 1 {
		t.Fatalf("rollback copy not kept: %v", backupNames(backups))
	}
	info, err := os.Stat(executable)
	if err != nil || info.Mode().Perm()&0o100 == 0 {
		t.Fatalf("restored executable mode=%v err=%v", info.Mode(), err)
	}
}

// t574EarlyDeath is one daemon start that ends inside the probation window.
func t574EarlyDeath(executable, version string) bool {
	rolledBack, stop := hubUpdateStartup(executable, version, func(string) {})
	stop()
	return rolledBack
}

func TestT574ProbationBoundariesAndReset(t *testing.T) {
	t.Run("two failures then hello resets", func(t *testing.T) {
		_, executable := t574Probation(t, 0, true)
		t574EarlyDeath(executable, t574Version)
		t574EarlyDeath(executable, t574Version)
		t574EarlyDeath(executable, t574Version) // third start, which reaches the hub
		client := &HubClient{executablePath: executable, version: t574Version}
		client.confirmHubUpdate()
		for i := 0; i < 5; i++ {
			if t574EarlyDeath(executable, t574Version) {
				t.Fatal("rolled back a version that reached the hub")
			}
		}
		if got, _ := os.ReadFile(executable); string(got) != "new binary" {
			t.Fatalf("executable=%q", got)
		}
	})
	t.Run("hello from another version keeps probation", func(t *testing.T) {
		_, executable := t574Probation(t, 3, true)
		(&HubClient{executablePath: executable, version: t574OldVersion}).confirmHubUpdate()
		if _, found := readHubUpdateState(executable); !found {
			t.Fatal("a different version's hello cleared probation")
		}
	})
	t.Run("running version differs drops state", func(t *testing.T) {
		_, executable := t574Probation(t, 3, true)
		if t574EarlyDeath(executable, t574OldVersion) {
			t.Fatal("rolled back while not running the probation version")
		}
		if _, err := os.Stat(hubUpdateStatePath(executable)); !os.IsNotExist(err) {
			t.Fatal("stale probation state kept")
		}
	})
	for _, fixture := range []struct {
		name   string
		backup string
		create bool
	}{
		{"backup missing", "panewire.bak-20260923T000000Z", false},
		{"backup outside directory", "../panewire.bak-20260923T000000Z", true},
		{"backup of another executable", "other.bak-20260923T000000Z", true},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			directory, executable := t574Probation(t, 3, false)
			if fixture.create {
				_ = os.WriteFile(filepath.Join(directory, fixture.backup), []byte("old binary"), 0o755)
			}
			if err := writeHubUpdateState(executable, hubUpdateState{Version: t574Version, Backup: fixture.backup, Starts: 3}); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				if t574EarlyDeath(executable, t574Version) {
					t.Fatal("reported a rollback without a usable copy")
				}
			}
			if got, _ := os.ReadFile(executable); string(got) != "new binary" {
				t.Fatalf("executable=%q", got)
			}
			if state, _ := readHubUpdateState(executable); !state.RollbackUnavailable || state.Starts != 3 {
				t.Fatalf("state=%+v", state)
			}
		})
	}
}

// The real hello path ends probation.
func TestT574HelloEndsProbation(t *testing.T) {
	_, executable := t574Probation(t, 1, true)
	conn, received := t574WSPair(t)
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1:1", MachineID: "node-a", Token: "fixture", Version: t574Version, AllowInsecureForTests: true, PingInterval: time.Hour, PreferRetry: time.Hour, ExecutablePath: executable})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = client.serve(ctx, conn) }()
	select {
	case message := <-received:
		if message["type"] != "hello" {
			t.Fatalf("first message=%v", message)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no hello")
	}
	r6Eventually(t, "probation cleared", func() bool {
		_, err := os.Stat(hubUpdateStatePath(executable))
		return os.IsNotExist(err)
	})
}

type t574Notifier struct {
	mu    sync.Mutex
	sends int
}

func (n *t574Notifier) Send(context.Context, HubAlert) error {
	n.mu.Lock()
	n.sends++
	n.mu.Unlock()
	return nil
}

func t574OverdueHub(t *testing.T, lanes, overdueLane string) (*HubServer, *fakeHandoffkeep, *t574Notifier, *time.Time) {
	t.Helper()
	fake, client, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	now := time.Date(2026, 9, 23, 3, 0, 0, 0, time.UTC)
	notifier := &t574Notifier{}
	hub, err := NewHubServer(HubServerConfig{
		Tokens:            map[string]string{"operator": "op", "node-a": "node", "host-b": "node-b"},
		Now:               func() time.Time { return now },
		ReportRelayPath:   r20LanesFile(t, lanes),
		UpdateOverdueLane: overdueLane,
		// node-a is presence-only so its disconnect cannot page; any Telegram
		// send in these tests would have to come from the overdue path.
		AlertNodes:  map[string]struct{}{},
		Notifier:    notifier,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		handoffkeep: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	hub.nodes["node-a"] = &hubNodeRecord{machineID: "node-a", state: "disconnected", remoteMeta: map[string]string{"version": t574OldVersion}}
	return hub, fake, notifier, &now
}

// t574Sweep runs one Sweep and waits for the overdue flush it started, which
// runs off the Sweep goroutine.
func t574Sweep(hub *HubServer) {
	hub.Sweep()
	hub.updateOverdueFlushes.Wait()
}

func t574OverdueRows(fake *fakeHandoffkeep) []map[string]any {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	var rows []map[string]any
	for _, call := range fake.calls {
		if call.Method == http.MethodPost && call.Path == "/v1/relay/events" && strings.HasPrefix(asString(call.Body["text"]), "update.overdue ") {
			rows = append(rows, call.Body)
		}
	}
	return rows
}

// Mutant (c): a node still on its old version past the deadline yields exactly
// one update.overdue row in the operator sink, and LAST_NOTE shows it.
func TestT574OverdueReachesSinkExactlyOnce(t *testing.T) {
	hub, fake, notifier, now := t574OverdueHub(t, `{"lanes":{"ops-sink":{"sink":true}}}`, "ops-sink")
	hub.expectedVersion["node-a"] = hubExpectedVersion{version: t574Version, deadline: *now}
	t574Sweep(hub)
	rows := t574OverdueRows(fake)
	if len(rows) != 1 || rows[0]["owner_lane"] != "ops-sink" || !strings.Contains(asString(rows[0]["text"]), "machine=node-a version="+t574Version) {
		t.Fatalf("overdue rows=%v", rows)
	}
	if fake.count(http.MethodPost, "/v1/relay/events/101/delivered") != 1 {
		t.Fatalf("sink row not marked delivered: %v", fake.sequence())
	}
	nodes := hub.Nodes()
	if len(nodes) != 1 || nodes[0].LastNote == nil || nodes[0].LastNote.Text != "update overdue "+t574Version {
		t.Fatalf("LAST_NOTE=%+v", nodes[0].LastNote)
	}
	var rendered bytes.Buffer
	renderHubStatus(&rendered, nodes)
	if !strings.Contains(rendered.String(), "update overdue "+t574Version) {
		t.Fatalf("hub-status does not show the overdue note:\n%s", rendered.String())
	}

	// More sweeps, and republishing the same version, never repeat it.
	t574Sweep(hub)
	*now = now.Add(time.Hour)
	hub.expectedVersion["node-a"] = hubExpectedVersion{version: t574Version, deadline: *now}
	t574Sweep(hub)
	t574Sweep(hub)
	if rows := t574OverdueRows(fake); len(rows) != 1 {
		t.Fatalf("same machine and version notified %d times", len(rows))
	}
	// The in-memory notice is once per (machine, version) too: one overdue UI
	// event, and a LAST_NOTE the operator can overwrite without it coming back.
	overdueEvents := func() int {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		total := 0
		for _, event := range hub.uiEvents {
			if event.Kind == "update" && event.Phase == "overdue" && event.MachineID == "node-a" {
				total++
			}
		}
		return total
	}
	if got := overdueEvents(); got != 1 {
		t.Fatalf("overdue UI events=%d, want 1", got)
	}
	hub.recordNote("node-a", "operator acknowledged", *now)
	hub.expectedVersion["node-a"] = hubExpectedVersion{version: t574Version, deadline: *now}
	t574Sweep(hub)
	if note := hub.Nodes()[0].LastNote; note == nil || note.Text != "operator acknowledged" || overdueEvents() != 1 {
		t.Fatalf("repeat notice: LAST_NOTE=%+v overdue events=%d", note, overdueEvents())
	}
	// A restarted hub has lost that memory; the (machine, version) event id
	// still keeps the durable sink at one row.
	restarted, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op", "node-a": "node"}, Now: func() time.Time { return *now }, ReportRelayPath: hub.reportRelayPath, UpdateOverdueLane: "ops-sink", AlertNodes: map[string]struct{}{}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), handoffkeep: hub.handoffkeep})
	if err != nil {
		t.Fatal(err)
	}
	restarted.nodes["node-a"] = &hubNodeRecord{machineID: "node-a", state: "disconnected", remoteMeta: map[string]string{"version": t574OldVersion}}
	restarted.expectedVersion["node-a"] = hubExpectedVersion{version: t574Version, deadline: *now}
	t574Sweep(restarted)
	if rows := fake.rowCount(); rows != 1 {
		t.Fatalf("durable sink rows after hub restart=%d, want 1", rows)
	}
	// A different version is a new notice.
	hub.expectedVersion["node-a"] = hubExpectedVersion{version: "pw-def5678", deadline: *now}
	t574Sweep(hub)
	if rows := fake.rowCount(); rows != 2 {
		t.Fatalf("new version durable rows=%d, want 2", rows)
	}
	notifier.mu.Lock()
	defer notifier.mu.Unlock()
	if notifier.sends != 0 {
		t.Fatalf("overdue used the Telegram notifier %d times", notifier.sends)
	}
}

func TestT574OverdueOnlyForLaggingNodesAndOnlyToSinks(t *testing.T) {
	t.Run("node on the version is not overdue", func(t *testing.T) {
		hub, fake, _, now := t574OverdueHub(t, `{"lanes":{"ops-sink":{"sink":true}}}`, "ops-sink")
		hub.nodes["node-a"].remoteMeta["version"] = t574Version
		hub.expectedVersion["node-a"] = hubExpectedVersion{version: t574Version, deadline: *now}
		t574Sweep(hub)
		if rows := t574OverdueRows(fake); len(rows) != 0 {
			t.Fatalf("rows=%v", rows)
		}
	})
	t.Run("before the deadline nothing is sent", func(t *testing.T) {
		hub, fake, _, now := t574OverdueHub(t, `{"lanes":{"ops-sink":{"sink":true}}}`, "ops-sink")
		hub.expectedVersion["node-a"] = hubExpectedVersion{version: t574Version, deadline: now.Add(time.Second)}
		t574Sweep(hub)
		if rows := t574OverdueRows(fake); len(rows) != 0 {
			t.Fatalf("rows=%v", rows)
		}
	})
	t.Run("a pane lane never receives it", func(t *testing.T) {
		hub, fake, _, now := t574OverdueHub(t, `{"lanes":{"ops-pane":{"machine":"host-b","pane":"w1:p1"}}}`, "ops-pane")
		destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 4)}
		// A fresh keepalive keeps Sweep from pinging this socketless agent.
		hub.nodes["host-b"] = &hubNodeRecord{machineID: "host-b", agent: destination, state: "connected", lastPing: *now, lastKeepaliveSent: *now, remoteMeta: map[string]string{}}
		hub.expectedVersion["node-a"] = hubExpectedVersion{version: t574Version, deadline: *now}
		t574Sweep(hub)
		if got := fake.sequence(); len(got) != 0 {
			t.Fatalf("handoffkeep calls=%v", got)
		}
		if injected := drainRelays(destination); injected != 0 {
			t.Fatalf("pane injections=%d", injected)
		}
		for _, node := range hub.Nodes() {
			if node.MachineID == "node-a" && (node.LastNote == nil || node.LastNote.Text != "update overdue "+t574Version) {
				t.Fatalf("LAST_NOTE=%+v", node.LastNote)
			}
		}
	})
}

// AC1 + operator condition 3: the release script names, stamps, and sums every
// asset with one pw-<sha> string, and that string is what the hub and the
// node's smoke run compare against.
func TestT574ReleaseBuildStampsNamesAndSums(t *testing.T) {
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatalf("git rev-parse: %v", err)
	}
	want := "pw-" + strings.TrimSpace(string(head))[:7]
	host := runtime.GOOS + "/" + runtime.GOARCH
	targets := "linux/arm64 " + host
	if host == "linux/arm64" {
		targets = "darwin/amd64 " + host
	}
	output := t.TempDir()
	cmd := exec.Command("sh", "scripts/build-release.sh", output)
	cmd.Env = append(os.Environ(), "PANEWIRE_RELEASE_TARGETS="+targets)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("build-release.sh: %v", err)
	}
	if got := strings.TrimSpace(string(out)); got != want {
		t.Fatalf("release version=%q, want %q", got, want)
	}
	sums, err := os.ReadFile(filepath.Join(output, "SHA256SUMS"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(sums)), "\n")
	if len(lines) != 2 {
		t.Fatalf("SHA256SUMS=%q", sums)
	}
	for _, line := range lines {
		sum, name, found := strings.Cut(line, "  ")
		if !found {
			t.Fatalf("SHA256SUMS line %q", line)
		}
		asset, err := os.ReadFile(filepath.Join(output, name))
		if err != nil {
			t.Fatal(err)
		}
		if digest := sha256.Sum256(asset); hex.EncodeToString(digest[:]) != sum {
			t.Fatalf("%s checksum mismatch", name)
		}
		url := "https://github.com/mgh3326/panewire/releases/download/v9.9.9/" + name
		if !validHubUpdateURLForVersion(url, hubUpdateDefaultRepository, want) {
			t.Fatalf("release asset %s is not a publishable URL for %s", name, want)
		}
		info, err := exec.Command("go", "version", "-m", filepath.Join(output, name)).CombinedOutput()
		if err != nil || !strings.Contains(string(info), "-X main.version="+want) {
			t.Fatalf("%s lacks -X main.version=%s: %v\n%s", name, want, err, info)
		}
	}
	hostAsset := filepath.Join(output, "panewire_"+want+"_"+runtime.GOOS+"_"+runtime.GOARCH)
	if err := smokeHubUpdate(context.Background(), hostAsset, want); err != nil {
		t.Fatalf("host release asset fails the node smoke run: %v", err)
	}
	script, err := os.ReadFile("scripts/build-release.sh")
	if err != nil || !strings.Contains(string(script), `"darwin/amd64 darwin/arm64 linux/amd64 linux/arm64"`) {
		t.Fatalf("default release targets changed: %v", err)
	}
}

// The node enforces the pin itself (a hub running older code does not): a
// foreign repository is refused before any download, the decoder drops a
// malformed instruction, and a redirect may leave github.com only for a GitHub
// release-asset host.
func TestT574NodePinsRepositoryAndRedirectChain(t *testing.T) {
	asset := hubTestSmokeAsset(t574Version)
	digest := hex.EncodeToString(func() []byte { d := sha256.Sum256(asset); return d[:] }())
	foreign := "https://github.com/evil/panewire/releases/download/v0.1.0/panewire_" + t574Version + "_linux_amd64"
	_, executable := t574Install(t)
	downloads := 0
	counting := &http.Client{Transport: hubRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		downloads++
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(asset)), Request: request}, nil
	})}
	if err := applyHubUpdate(context.Background(), counting, executable, hubTestUpdate(foreign, digest, t574Version)); err == nil || downloads != 0 {
		t.Fatalf("foreign repository err=%v downloads=%d", err, downloads)
	}
	if got, _ := os.ReadFile(executable); string(got) != "old binary" {
		t.Fatalf("executable=%q", got)
	}
	instruction := func(url string) []byte {
		raw, _ := json.Marshal(map[string]any{"type": "update.available", "version": t574Version, "sha256": digest, "url": url})
		return raw
	}
	if _, ok := parseHubOutbound(instruction(t574ReleaseURL(t574Version))); !ok {
		t.Fatal("decoder dropped a pinned instruction")
	}
	for _, url := range []string{
		"https://objects.githubusercontent.com/asset?sig=1",
		"https://github.com/mgh3326/panewire/releases/download/v0.1.0/panewire_linux_amd64",
		t574ReleaseURL("pw-def5678"),
	} {
		if _, ok := parseHubOutbound(instruction(url)); ok {
			t.Errorf("decoder accepted %s", url)
		}
	}

	for _, fixture := range []struct {
		name     string
		location string
		allowed  bool
	}{
		{"release-assets store", "https://release-assets.githubusercontent.com/github-production-release-asset/1/2?sig=x", true},
		{"objects store", "https://objects.githubusercontent.com/github-production-release-asset/1/2?sig=x", true},
		{"pinned github.com hop", t574ReleaseURL(t574Version), true},
		{"foreign repository hop", foreign, false},
		{"foreign host", "https://evil.example.com/panewire_" + t574Version + "_linux_amd64", false},
		{"asset store over http", "http://release-assets.githubusercontent.com/x", false},
		{"asset store with userinfo", "https://u@release-assets.githubusercontent.com/x", false},
		{"asset store on another port", "https://release-assets.githubusercontent.com:8443/x", false},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			_, executable := t574Install(t)
			hops := 0
			client := &http.Client{Transport: hubRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
				hops++
				if hops == 1 {
					return &http.Response{StatusCode: http.StatusFound, Header: http.Header{"Location": []string{fixture.location}}, Body: io.NopCloser(bytes.NewReader(nil)), Request: request}, nil
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(asset)), Request: request}, nil
			})}
			err := applyHubUpdate(context.Background(), client, executable, hubTestUpdate(t574ReleaseURL(t574Version), digest, t574Version))
			got, _ := os.ReadFile(executable)
			if fixture.allowed != (err == nil) || fixture.allowed != bytes.Equal(got, asset) {
				t.Fatalf("redirect to %s: err=%v replaced=%v", fixture.location, err, bytes.Equal(got, asset))
			}
			if !fixture.allowed && hops != 1 {
				t.Fatalf("followed a refused redirect: hops=%d", hops)
			}
		})
	}
}

// The node read loop pins the repository at decode time: a foreign-repository
// instruction is dropped before dispatch (no download, no in-flight claim),
// while the next pinned instruction on the same connection is applied.
func TestT574ReadLoopDropsForeignRepositoryInstruction(t *testing.T) {
	foreign := "https://github.com/evil/panewire/releases/download/v0.1.0/panewire_" + t574Version + "_linux_amd64"
	raw := func(url string) []byte {
		out, _ := json.Marshal(map[string]any{"type": "update.available", "version": t574Version, "sha256": t574SHA, "url": url})
		return out
	}
	if _, ok := parseHubOutbound(raw(foreign)); ok {
		t.Fatal("decoder accepted a foreign repository")
	}
	if _, ok := parseHubOutboundPinned(raw(foreign), "evil/panewire"); !ok {
		t.Fatal("decoder refused the configured repository")
	}
	if _, ok := parseHubOutboundPinned(raw(t574ReleaseURL(t574Version)), "evil/panewire"); ok {
		t.Fatal("decoder accepted the default repository under another pin")
	}

	_, executable := t574Install(t)
	asset := hubTestSmokeAsset(t574Version)
	digest := sha256.Sum256(asset)
	var mu sync.Mutex
	var downloaded []string
	client := &http.Client{Transport: hubRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		mu.Lock()
		downloaded = append(downloaded, request.URL.String())
		mu.Unlock()
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(asset)), Request: request}, nil
	})}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for _, url := range []string{foreign, t574ReleaseURL(t574Version)} {
			_ = wsjson.Write(request.Context(), conn, map[string]any{"type": "update.available", "version": t574Version, "sha256": hex.EncodeToString(digest[:]), "url": url})
		}
		for {
			var message map[string]any
			if wsjson.Read(request.Context(), conn, &message) != nil {
				return
			}
		}
	}))
	defer server.Close()
	restarted := make(chan struct{}, 2)
	node, err := NewHubClient(HubClientConfig{URL: r6WSURL(server.URL, ""), MachineID: "node-a", Token: "node-token", AllowInsecureForTests: true, PingInterval: time.Hour, PreferRetry: time.Hour, ExecutablePath: executable, UpdateHTTPClient: client, Restart: func() { restarted <- struct{}{} }})
	if err != nil {
		t.Fatal(err)
	}
	conn, _, err := websocket.Dial(t.Context(), node.endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = node.serve(ctx, conn) }()
	select {
	case <-restarted:
	case <-time.After(10 * time.Second):
		t.Fatal("the pinned instruction after the foreign one was not applied")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(downloaded) != 1 || downloaded[0] != t574ReleaseURL(t574Version) {
		t.Fatalf("downloads=%v, want only the pinned asset", downloaded)
	}
}

// AC4's 60-second bound: a start that outlives the probation window is not an
// early death. Long-lived runs without a hello (hub unreachable) followed by
// planned restarts never roll back; only consecutive early deaths do.
func TestT574ProbationWindowSeparatesEarlyDeathFromLongRuns(t *testing.T) {
	previous := hubUpdateProbationWindow
	hubUpdateProbationWindow = 150 * time.Millisecond
	defer func() { hubUpdateProbationWindow = previous }()
	starts := func(executable string) int {
		state, _ := readHubUpdateState(executable)
		return state.Starts
	}
	survive := func(t *testing.T, executable string) {
		t.Helper()
		rolledBack, stop := hubUpdateStartup(executable, t574Version, func(string) {})
		defer stop() // the process ends only after the window
		if rolledBack {
			t.Fatal("rolled back a long-lived start")
		}
		r6EventuallyWithin(t, "window reset", 3*time.Second, func() bool { return starts(executable) == 0 })
	}

	t.Run("hub unreachable, long runs, planned restarts", func(t *testing.T) {
		_, executable := t574Probation(t, 0, true)
		for i := 0; i < 5; i++ {
			survive(t, executable)
		}
		if got, _ := os.ReadFile(executable); string(got) != "new binary" {
			t.Fatalf("executable=%q after long-lived restarts", got)
		}
		if _, found := readHubUpdateState(executable); !found {
			t.Fatal("probation ended without a hello")
		}
	})
	t.Run("a long run breaks the early-death streak", func(t *testing.T) {
		_, executable := t574Probation(t, 0, true)
		t574EarlyDeath(executable, t574Version)
		t574EarlyDeath(executable, t574Version)
		survive(t, executable)
		for i := 0; i < hubUpdateRollbackAfter; i++ {
			if t574EarlyDeath(executable, t574Version) {
				t.Fatalf("rolled back after %d early deaths since the long run", i)
			}
		}
		if !t574EarlyDeath(executable, t574Version) {
			t.Fatal("three consecutive early deaths did not roll back")
		}
		if got, _ := os.ReadFile(executable); string(got) != "old binary" {
			t.Fatalf("executable=%q", got)
		}
	})
	t.Run("an early death is not rescued by the window", func(t *testing.T) {
		_, executable := t574Probation(t, 0, true)
		t574EarlyDeath(executable, t574Version)
		time.Sleep(3 * hubUpdateProbationWindow)
		if got := starts(executable); got != 1 {
			t.Fatalf("a cancelled window reset the count: starts=%d", got)
		}
	})
	t.Run("the window never recreates a record a hello removed", func(t *testing.T) {
		_, executable := t574Probation(t, 0, true)
		_, stop := hubUpdateStartup(executable, t574Version, func(string) {})
		defer stop()
		(&HubClient{executablePath: executable, version: t574Version}).confirmHubUpdate()
		time.Sleep(3 * hubUpdateProbationWindow)
		if _, err := os.Stat(hubUpdateStatePath(executable)); !os.IsNotExist(err) {
			t.Fatalf("probation record reappeared: %v", err)
		}
	})
}

func r6EventuallyWithin(t *testing.T, label string, limit time.Duration, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", label)
}

// AC5 "exactly one" survives a failed sink write: the notice stays queued and
// every Sweep retries it until the row exists, then stops.
func TestT574OverdueRetriesUntilRecorded(t *testing.T) {
	hub, fake, _, now := t574OverdueHub(t, `{"lanes":{"ops-sink":{"sink":true}}}`, "ops-sink")
	fake.mu.Lock()
	fake.status = http.StatusInternalServerError
	fake.mu.Unlock()
	hub.expectedVersion["node-a"] = hubExpectedVersion{version: t574Version, deadline: *now}
	t574Sweep(hub)
	t574Sweep(hub)
	if rows := fake.rowCount(); rows != 0 {
		t.Fatalf("rows while handoffkeep fails=%d", rows)
	}
	if attempts := len(t574OverdueRows(fake)); attempts != 2 {
		t.Fatalf("attempts while failing=%d, want one per sweep", attempts)
	}
	fake.mu.Lock()
	fake.status = http.StatusCreated
	fake.mu.Unlock()
	t574Sweep(hub)
	if rows := fake.rowCount(); rows != 1 {
		t.Fatalf("rows after recovery=%d, want 1", rows)
	}
	t574Sweep(hub)
	t574Sweep(hub)
	if attempts := len(t574OverdueRows(fake)); attempts != 3 || fake.rowCount() != 1 {
		t.Fatalf("recorded notice retried: attempts=%d rows=%d", attempts, fake.rowCount())
	}
}

// Without --update-overdue-lane the one sink lane in lanes.json is the
// operator sink. With none (or several) the notice waits in the queue and is
// delivered once exactly one sink lane is configured.
func TestT574OverdueDefaultsToTheSoleSinkLane(t *testing.T) {
	t.Run("sole sink", func(t *testing.T) {
		hub, fake, _, now := t574OverdueHub(t, `{"lanes":{"ops-sink":{"sink":true},"lane-a":{"machine":"host-b","pane":"w1:p1"}}}`, "")
		hub.expectedVersion["node-a"] = hubExpectedVersion{version: t574Version, deadline: *now}
		t574Sweep(hub)
		if rows := t574OverdueRows(fake); len(rows) != 1 || rows[0]["owner_lane"] != "ops-sink" {
			t.Fatalf("rows=%v", rows)
		}
	})
	t.Run("ambiguous then configured", func(t *testing.T) {
		hub, fake, _, now := t574OverdueHub(t, `{"lanes":{"sink-a":{"sink":true},"sink-b":{"sink":true}}}`, "")
		hub.expectedVersion["node-a"] = hubExpectedVersion{version: t574Version, deadline: *now}
		t574Sweep(hub)
		if got := fake.sequence(); len(got) != 0 {
			t.Fatalf("ambiguous sinks were written: %v", got)
		}
		if err := os.WriteFile(hub.reportRelayPath, []byte(`{"lanes":{"sink-a":{"sink":true}}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		t574Sweep(hub)
		if rows := t574OverdueRows(fake); len(rows) != 1 || rows[0]["owner_lane"] != "sink-a" || fake.rowCount() != 1 {
			t.Fatalf("rows after configuring one sink=%v", rows)
		}
	})
}

// A failing overdue backlog must not hold up maintenance: Sweep returns and
// sends its keepalive ping at once while the retries run in the background,
// on this sweep and on the next retry sweep alike.
func TestT574OverdueBacklogNeverDelaysKeepalive(t *testing.T) {
	const postDelay = 300 * time.Millisecond
	slow := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		time.Sleep(postDelay)
		http.Error(writer, `{"error":"boom"}`, http.StatusInternalServerError)
	}))
	defer slow.Close()
	store, err := newHandoffkeepRelayClient(hubHandoffkeepEnv{URL: slow.URL, Token: "test-token"}, slow.Client())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 23, 3, 0, 0, 0, time.UTC)
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": "op", "node-a": "node", "node-b": "node-b"},
		Now:             func() time.Time { return now },
		ReportRelayPath: r20LanesFile(t, `{"lanes":{"ops-sink":{"sink":true}}}`),
		AlertNodes:      map[string]struct{}{},
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		handoffkeep:     store,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, version := range []string{"pw-1111111", "pw-2222222", "pw-3333333"} {
		machine := "lag-" + string(rune('a'+i))
		hub.nodes[machine] = &hubNodeRecord{machineID: machine, state: "disconnected", remoteMeta: map[string]string{"version": t574OldVersion}}
		hub.expectedVersion[machine] = hubExpectedVersion{version: version, deadline: now}
	}
	conn, received := t574WSPair(t)
	hub.nodes["node-b"] = &hubNodeRecord{machineID: "node-b", agent: &hubAgent{conn: conn}, state: "connected", lastPing: now, remoteMeta: map[string]string{}}
	for sweep := 1; sweep <= 2; sweep++ {
		hub.mu.Lock()
		hub.nodes["node-b"].lastKeepaliveSent = time.Time{}
		hub.mu.Unlock()
		started := time.Now()
		hub.Sweep()
		if elapsed := time.Since(started); elapsed >= postDelay {
			t.Fatalf("sweep %d took %v behind the failing backlog", sweep, elapsed)
		}
		select {
		case message := <-received:
			if message["type"] != "ping" {
				t.Fatalf("sweep %d first message=%v", sweep, message)
			}
			if elapsed := time.Since(started); elapsed >= postDelay {
				t.Fatalf("sweep %d ping arrived after %v", sweep, elapsed)
			}
		case <-time.After(postDelay):
			t.Fatalf("sweep %d sent no ping before one backlog POST could finish", sweep)
		}
		hub.updateOverdueFlushes.Wait()
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if len(hub.updateOverduePending) != 3 {
		t.Fatalf("failing notices left the queue: %d pending", len(hub.updateOverduePending))
	}
}

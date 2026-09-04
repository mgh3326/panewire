package panewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestR19ListenListsAcceptLoopbackAndTailnet(t *testing.T) {
	addresses, err := hubListenAddresses([]string{"--hub-auth", "ignored", "--listen", "127.0.0.1:9377,100.64.0.1:9377"})
	if err != nil || len(addresses) != 2 || addresses[1] != "100.64.0.1:9377" {
		t.Fatalf("addresses=%v err=%v", addresses, err)
	}
	if _, err := hubListenAddress("192.0.2.1:9377"); err == nil {
		t.Fatal("non-tailnet public bind accepted")
	}
}

func TestR19HubURLFallbackAndPreference(t *testing.T) {
	attempts := make([]string, 0, 3)
	dial := func(_ context.Context, endpoint string, _ *websocket.DialOptions) (*websocket.Conn, *http.Response, error) {
		attempts = append(attempts, endpoint)
		if len(attempts) == 1 {
			return nil, nil, errors.New("down")
		}
		return nil, nil, nil
	}
	client, err := NewHubClient(HubClientConfig{URLs: []string{"ws://100.64.0.1:9377", "ws://127.0.0.1:9377"}, MachineID: "node-a", Token: "fixture", AllowInsecureForTests: true, Dial: dial})
	if err != nil {
		t.Fatal(err)
	}
	_, endpoint, err := client.dialAny(context.Background())
	if err != nil || endpoint != client.endpoints[1] {
		t.Fatalf("fallback endpoint=%q err=%v", endpoint, err)
	}
	client.endpoint = client.endpoints[1]
	_, endpoint, switched := client.dialPreferred(context.Background())
	if !switched || endpoint != client.endpoints[0] {
		t.Fatalf("preference endpoint=%q switched=%v", endpoint, switched)
	}
}

type hubRoundTripperFunc func(*http.Request) (*http.Response, error)

func (fn hubRoundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func TestR19UpdateChecksumFailureDoesNotReplace(t *testing.T) {
	asset := []byte("new binary")
	client := &http.Client{Transport: hubRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Hostname() != "github.com" {
			t.Fatalf("download host=%q", request.URL.Hostname())
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(asset)), Request: request}, nil
	})}
	path := filepath.Join(t.TempDir(), "panewire")
	if err := os.WriteFile(path, []byte("old binary"), 0755); err != nil {
		t.Fatal(err)
	}
	url := "https://github.com/mgh3326/panewire/releases/download/r19b/panewire_darwin_arm64"
	if err := applyHubUpdate(context.Background(), client, path, url, "0000000000000000000000000000000000000000000000000000000000000000"); err == nil {
		t.Fatal("checksum mismatch accepted")
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "old binary" {
		t.Fatalf("binary changed: %q err=%v", got, err)
	}
	hash := sha256.Sum256(asset)
	if err := applyHubUpdate(context.Background(), client, path, url, hex.EncodeToString(hash[:])); err != nil {
		t.Fatal(err)
	}
	got, _ = os.ReadFile(path)
	if string(got) != string(asset) {
		t.Fatalf("got %q", got)
	}
	backups, globErr := filepath.Glob(path + ".bak-*")
	if globErr != nil || len(backups) != 1 {
		t.Fatalf("backups=%v err=%v", backups, globErr)
	}
	previous, readErr := os.ReadFile(backups[0])
	if readErr != nil || string(previous) != "old binary" {
		t.Fatalf("backup=%q err=%v", previous, readErr)
	}
}

func TestR19UpdateURLAllowlistAndRedirectDowngrade(t *testing.T) {
	for _, raw := range []string{
		"https://github.com/mgh3326/panewire/releases/download/r19b/panewire_darwin_arm64",
		"https://objects.githubusercontent.com/asset?X-Amz-Signature=fixture",
	} {
		if !validHubUpdateURL(raw) {
			t.Fatalf("allowed URL rejected: %s", raw)
		}
	}
	if validHubUpdateURL("https://example.invalid/panewire_darwin_arm64") {
		t.Fatal("non-GitHub update host accepted")
	}
	path := filepath.Join(t.TempDir(), "panewire")
	if err := os.WriteFile(path, []byte("old binary"), 0755); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: hubRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusFound, Header: http.Header{"Location": []string{"http://127.0.0.1/panewire"}}, Body: io.NopCloser(bytes.NewReader(nil)), Request: request}, nil
	})}
	err := applyHubUpdate(context.Background(), client, path, "https://github.com/mgh3326/panewire/releases/download/r19b/panewire_darwin_arm64", hex.EncodeToString(make([]byte, 32)))
	if err == nil {
		t.Fatal("HTTPS redirect downgrade accepted")
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil || string(got) != "old binary" {
		t.Fatalf("original was changed: %q err=%v", got, readErr)
	}
}

func TestR19QuotaOutputAndEnvironmentAreBounded(t *testing.T) {
	if got := hubScopefuelEnvironment([]string{"PATH=/bin", "HOME=/tmp/home", "HUB_TOKEN=secret", "CF_ACCESS_CLIENT_SECRET=secret", "CODEX_HOME=/tmp/codex"}); len(got) != 3 {
		t.Fatalf("allowlisted environment=%q", got)
	}
	command := filepath.Join(t.TempDir(), "scopefuel")
	if err := os.WriteFile(command, []byte("#!/bin/sh\nhead -c 16385 /dev/zero\n"), 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := runHubScopefuel(context.Background(), command); err == nil || err.Error() != "output_too_large" {
		t.Fatalf("oversized output error=%v", err)
	}
	t.Setenv("HUB_TOKEN", "quota-secret-sentinel")
	if err := os.WriteFile(command, []byte("#!/bin/sh\nprintf '%s' \"$HUB_TOKEN\"\n"), 0755); err != nil {
		t.Fatal(err)
	}
	output, err := runHubScopefuel(context.Background(), command)
	if err != nil {
		t.Fatalf("scopefuel run failed: err=%v", err)
	}
	// Report the variable name only: the value is the secret under assertion,
	// and a failure message lands in CI logs.
	if strings.Contains(string(output), "quota-secret-sentinel") {
		t.Fatal("daemon secret reached the child: HUB_TOKEN")
	}
}

func TestR19QuotaTimeoutKillsProcessGroup(t *testing.T) {
	directory := t.TempDir()
	command := filepath.Join(directory, "scopefuel")
	pidPath := filepath.Join(directory, "child.pid")
	script := "#!/bin/sh\nsleep 60 &\necho $! > " + strconv.Quote(pidPath) + "\nwait\n"
	if err := os.WriteFile(command, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	// The liveness poll below tolerates a zombie. Confirm first that it still
	// reports a plainly running process as running, so that tolerance cannot
	// decay into "everything is dead".
	if dead, state := hubTestProcessIsDead(os.Getpid()); dead {
		t.Fatalf("the liveness check calls this running test process dead (%s)", state)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// Bound the call: without the group kill a descendant holds the stdout pipe
	// open and runHubScopefuel never returns, which must read as a failure
	// rather than a hung test binary.
	runErrors := make(chan error, 1)
	go func() {
		_, err := runHubScopefuel(ctx, command)
		runErrors <- err
	}()
	select {
	case err := <-runErrors:
		if err == nil || err.Error() != "timeout" {
			t.Fatalf("timeout error=%v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runHubScopefuel never returned: a descendant still holds the stdout pipe")
	}
	rawPID, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
	if err != nil {
		t.Fatal(err)
	}
	// The grandchild's parent dies in the same group kill, so the grandchild is
	// reparented and reaped asynchronously. Until the reaper runs it is a
	// zombie, and kill(pid, 0) succeeds for a zombie: on Linux that made this
	// assertion report a process that is already dead as "still running".
	deadline := time.Now().Add(5 * time.Second)
	for {
		dead, state := hubTestProcessIsDead(pid)
		if dead {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("descendant %d still running (%s)", pid, state)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// hubTestProcessIsDead reports whether pid names no live process, treating a
// not-yet-reaped zombie as dead. The returned string describes what was
// observed and is used only in failure messages.
func hubTestProcessIsDead(pid int) (bool, string) {
	err := syscall.Kill(pid, 0)
	if errors.Is(err, syscall.ESRCH) {
		return true, "ESRCH"
	}
	if err != nil {
		return false, "status=" + err.Error()
	}
	if state, ok := hubTestProcessState(pid); ok {
		return state == "Z", "state=" + state
	}
	return false, "signalable"
}

// hubTestProcessState reads the scheduler state letter from /proc/<pid>/stat.
// It reports false on systems without procfs, where the ESRCH poll above is the
// only signal available.
func hubTestProcessState(pid int) (string, bool) {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return "", false
	}
	// The comm field is parenthesised and may itself contain spaces, so the
	// state letter is the first field after the final ')'.
	close := bytes.LastIndexByte(raw, ')')
	if close < 0 {
		return "", false
	}
	fields := strings.Fields(string(raw[close+1:]))
	if len(fields) == 0 {
		return "", false
	}
	return fields[0], true
}

func TestR19ExpectedVersionConfirmationAndTimeout(t *testing.T) {
	now := time.Date(2026, 9, 4, 16, 0, 0, 0, time.UTC)
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "operator-token", "node-a": "node-token"}, Now: func() time.Time { return now }, UpdateConfirmationTimeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	hub.expectedVersion["node-a"] = hubExpectedVersion{version: "r19c", deadline: now.Add(time.Minute)}
	hub.connect("node-a", "r19c", "127.0.0.1:1", &hubAgent{}, false)
	confirmed := false
	for _, event := range hub.uiEvents {
		confirmed = confirmed || (event.Kind == "update" && event.Phase == "succeeded")
	}
	if _, waiting := hub.expectedVersion["node-a"]; waiting || !confirmed {
		t.Fatalf("success confirmation missing: %+v", hub.uiEvents)
	}
	hub.expectedVersion["node-a"] = hubExpectedVersion{version: "r19d", deadline: now}
	hub.nodes["node-a"].agent = nil // confirmation timeout is independent of a live socket.
	hub.Sweep()
	unconfirmed := false
	for _, event := range hub.uiEvents {
		unconfirmed = unconfirmed || (event.Kind == "update" && event.Phase == "unconfirmed")
	}
	if _, waiting := hub.expectedVersion["node-a"]; waiting || !unconfirmed {
		t.Fatalf("timeout confirmation missing: %+v", hub.uiEvents)
	}
}

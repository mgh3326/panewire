package panewire

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// hk 3729: `lanes add --sink` registers a lane with no machine/pane route —
// the hub discards transport fields for a sink — so the CLI must not demand
// --machine and --pane it would throw away.
func TestLanesAddSinkNeedsNoRouteT3729(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	hub := lanesWriteHub(t, path)
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	envPath := filepath.Join(t.TempDir(), "operator.env")
	if err := os.WriteFile(envPath, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN=fixture-operator-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	deps := hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}

	var stdout, stderr strings.Builder
	code := runLanesCLI([]string{"add", "lane-sink", "--sink", "--hub-url", server.URL, "--hub-token-env", envPath}, &stdout, &stderr, deps)
	if code != ExitOK || stderr.Len() != 0 {
		t.Fatalf("sink add code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if got := decodeLaneProjection(t, []byte(stdout.String())); got != (hubLaneProjection{Lane: "lane-sink", Sink: true}) {
		t.Fatalf("sink projection=%+v", got)
	}

	// Route flags beside --sink are accepted and discarded by the hub, the
	// same precedence the lanes loader gives an explicit sink.
	stdout.Reset()
	stderr.Reset()
	code = runLanesCLI([]string{"add", "lane-sink-2", "--sink", "--machine", "machine-a", "--pane", "w1:p1", "--hub-url", server.URL, "--hub-token-env", envPath}, &stdout, &stderr, deps)
	if code != ExitOK || stderr.Len() != 0 {
		t.Fatalf("sink add with route code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if got := decodeLaneProjection(t, []byte(stdout.String())); got != (hubLaneProjection{Lane: "lane-sink-2", Sink: true}) {
		t.Fatalf("sink-with-route projection=%+v", got)
	}

	// Without --sink the route flags stay required.
	stdout.Reset()
	stderr.Reset()
	code = runLanesCLI([]string{"add", "lane-a", "--hub-url", server.URL, "--hub-token-env", envPath}, &stdout, &stderr, deps)
	if code != ExitUsage {
		t.Fatalf("routeless add code=%d want %d; stderr=%q", code, ExitUsage, stderr.String())
	}
	if !strings.Contains(stderr.String(), "route flags are required") {
		t.Fatalf("routeless add stderr=%q", stderr.String())
	}

	// --sink=false is not a sink: the route flags stay required.
	stdout.Reset()
	stderr.Reset()
	code = runLanesCLI([]string{"add", "lane-b", "--sink=false", "--hub-url", server.URL, "--hub-token-env", envPath}, &stdout, &stderr, deps)
	if code != ExitUsage {
		t.Fatalf("sink=false add code=%d want %d; stderr=%q", code, ExitUsage, stderr.String())
	}
}

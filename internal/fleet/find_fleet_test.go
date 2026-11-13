package fleet

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// findHubApp points [hub] at a fixture hub and returns the App. The fixture
// answers the three projections find reads; each test sets the bodies it
// needs through the returned setter before running diagnostics.
type findHubFixture struct {
	nodes, jobs, lanes string
}

func findHubApp(t *testing.T) (*App, *findHubFixture) {
	t.Helper()
	fixture := &findHubFixture{
		nodes: `{"nodes":[]}`,
		jobs:  `{"jobs":[]}`,
		lanes: `{"lanes":[]}`,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/nodes":
			fmt.Fprint(w, fixture.nodes)
		case "/v1/jobs":
			fmt.Fprint(w, fixture.jobs)
		case "/v1/lanes":
			fmt.Fprint(w, fixture.lanes)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	dir := t.TempDir()
	envPath := filepath.Join(dir, "operator.env")
	if err := os.WriteFile(envPath, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN=test-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	a := testApp(t)
	a.Config.Hub = map[string]any{"hub_url": server.URL, "operator_token_env": envPath, "machine_id": "test-host"}
	return a, fixture
}

// findStubHerdr answers the local inventory find reads: agent list, tab
// list, and the per-match preview read.
func findStubHerdr(runner *fakeRunner, agents, tabs string) {
	runner.Do = func(c Command) Result {
		if c.Name != "herdr" {
			return Result{Code: 1, Err: "unexpected " + c.Name}
		}
		joined := strings.Join(c.Args, " ")
		switch {
		case joined == "agent list":
			return Result{Out: agents}
		case joined == "tab list":
			return Result{Out: tabs}
		case strings.HasPrefix(joined, "agent read "):
			return Result{Out: "preview line\n"}
		default:
			return Result{Code: 1, Err: "unexpected herdr " + joined}
		}
	}
}

const findEmptyInventory = `{"agents":[]}`
const findEmptyTabs = `{"tabs":[]}`

func findSessionJSON(t *testing.T, a *App, machine, pane, agentName, label, displayLabel, status string) string {
	t.Helper()
	received := a.Now().UTC().Format(time.RFC3339)
	return fmt.Sprintf(`{"machine_id":%q,"state":"connected","session_snapshot":{"sessions":[{"pane_id":%q,"workspace_id":"w9","label":%q,"agent_name":%q,"label_source":"agent_name","display_label":%q,"status":%q,"revision":1,"state_change_seq":1}],"snapshot_status":"ok","truncated":false,"received_at":%q,"stale":false}}`,
		machine, pane, label, agentName, displayLabel, status, received)
}

func findRun(t *testing.T, a *App, args ...string) (string, error) {
	t.Helper()
	out := &strings.Builder{}
	a.Out = out
	err := a.diagnostics(t.Context(), "find", args)
	return out.String(), err
}

func TestFindFleetSessionMatchPrintsMachineAndPane(t *testing.T) {
	a, fixture := findHubApp(t)
	fixture.nodes = `{"nodes":[` + findSessionJSON(t, a, "remote-a", "w2:p7", "pw-target", "pw-target", "pw-target", "idle") + `]}`
	runner := &fakeRunner{}
	findStubHerdr(runner, findEmptyInventory, findEmptyTabs)
	a.Runner = runner

	out, err := findRun(t, a, "pw-target")
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	want := "machine=remote-a pane_id=w2:p7 name=pw-target cwd=- status=idle via=session\n"
	if !strings.Contains(out, want) {
		t.Fatalf("output missing fleet match %q in:\n%s", want, out)
	}
}

func TestFindFleetJobMatch(t *testing.T) {
	a, fixture := findHubApp(t)
	fixture.jobs = `{"jobs":[{"machine":"remote-b","state":"active","job_id":"pw-target","pane":"w1:p1"}]}`
	runner := &fakeRunner{}
	findStubHerdr(runner, findEmptyInventory, findEmptyTabs)
	a.Runner = runner

	out, err := findRun(t, a, "pw-target")
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	want := "machine=remote-b pane_id=w1:p1 name=- cwd=- status=- via=job\n"
	if !strings.Contains(out, want) {
		t.Fatalf("output missing job match %q in:\n%s", want, out)
	}
}

func TestFindFleetLaneMatch(t *testing.T) {
	a, fixture := findHubApp(t)
	fixture.lanes = `{"lanes":[{"lane":"pw-target","machine":"remote-c","pane":"w3:p3","parent":"desk","sink":false}]}`
	runner := &fakeRunner{}
	findStubHerdr(runner, findEmptyInventory, findEmptyTabs)
	a.Runner = runner

	out, err := findRun(t, a, "pw-target")
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	want := "machine=remote-c pane_id=w3:p3 name=- cwd=- status=- via=lane\n"
	if !strings.Contains(out, want) {
		t.Fatalf("output missing lane match %q in:\n%s", want, out)
	}
}

func TestFindFleetLabelMatchIsFallbackUnderNameTier(t *testing.T) {
	a, fixture := findHubApp(t)
	// remote-a knows the pane only by display label; remote-b has the
	// canonical agent_name. The name-tier match must suppress the label one.
	fixture.nodes = `{"nodes":[` +
		findSessionJSON(t, a, "remote-a", "w2:p1", "other-agent", "pw-target", "pw-target", "idle") + `,` +
		findSessionJSON(t, a, "remote-b", "w4:p2", "pw-target", "pw-target", "pw-target", "working") + `]}`
	runner := &fakeRunner{}
	findStubHerdr(runner, findEmptyInventory, findEmptyTabs)
	a.Runner = runner

	out, err := findRun(t, a, "pw-target")
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	if strings.Contains(out, "remote-a") {
		t.Fatalf("label-tier match must lose to the agent_name match:\n%s", out)
	}
	if !strings.Contains(out, "machine=remote-b pane_id=w4:p2") {
		t.Fatalf("name-tier match missing:\n%s", out)
	}
}

func TestFindFleetMergesLocalAndSessionOnSamePane(t *testing.T) {
	a, fixture := findHubApp(t)
	// This host's machine id is test-host; the hub reports the same pane
	// under the same machine, so local detail and the snapshot merge.
	fixture.nodes = `{"nodes":[` + findSessionJSON(t, a, "test-host", "w1:p2", "pw-target", "pw-target", "pw-target", "idle") + `]}`
	agents := `{"agents":[{"pane_id":"w1:p2","tab_id":"w1:t1","name":"pw-target","agent":"claude","agent_status":"working","cwd":"/work/job"}]}`
	tabs := `{"tabs":[{"tab_id":"w1:t1","label":"pw-target"}]}`
	runner := &fakeRunner{}
	findStubHerdr(runner, agents, tabs)
	a.Runner = runner

	out, err := findRun(t, a, "pw-target")
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	matchLines := 0
	for _, line := range lines {
		if strings.HasPrefix(line, "machine=") {
			matchLines++
			if !strings.Contains(line, "machine=test-host pane_id=w1:p2") || !strings.Contains(line, "via=local,session") || !strings.Contains(line, "cwd=/work/job") {
				t.Fatalf("merged match line wrong: %q", line)
			}
		}
	}
	if matchLines != 1 {
		t.Fatalf("local and snapshot reports of one pane must merge, got %d match lines:\n%s", matchLines, out)
	}
	if !strings.Contains(out, "preview line") {
		t.Fatalf("local match must keep its pane preview:\n%s", out)
	}
}

func TestFindFleetPaneOnlyPrintsMachineQualifiedRemote(t *testing.T) {
	a, fixture := findHubApp(t)
	fixture.nodes = `{"nodes":[` + findSessionJSON(t, a, "remote-a", "w2:p7", "pw-target", "pw-target", "pw-target", "idle") + `]}`
	runner := &fakeRunner{}
	findStubHerdr(runner, findEmptyInventory, findEmptyTabs)
	a.Runner = runner

	out, err := findRun(t, a, "pw-target", "--pane-only")
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	if strings.TrimSpace(out) != "remote-a:w2:p7" {
		t.Fatalf("remote --pane-only must print machine:pane_id, got %q", out)
	}
}

func TestFindFleetPaneOnlyKeepsBareLocalPane(t *testing.T) {
	a, _ := findHubApp(t)
	agents := `{"agents":[{"pane_id":"w1:p2","tab_id":"w1:t1","name":"pw-target","agent":"claude","agent_status":"idle","cwd":"/work/job"}]}`
	runner := &fakeRunner{}
	findStubHerdr(runner, agents, findEmptyTabs)
	a.Runner = runner

	out, err := findRun(t, a, "pw-target", "--pane-only")
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	if strings.TrimSpace(out) != "w1:p2" {
		t.Fatalf("local --pane-only must keep the bare pane id, got %q", out)
	}
}

func TestFindFleetMultipleHostsIsAmbiguous(t *testing.T) {
	a, fixture := findHubApp(t)
	fixture.nodes = `{"nodes":[` +
		findSessionJSON(t, a, "remote-a", "w2:p7", "pw-target", "pw-target", "pw-target", "idle") + `,` +
		findSessionJSON(t, a, "remote-b", "w4:p2", "pw-target", "pw-target", "pw-target", "idle") + `]}`
	runner := &fakeRunner{}
	findStubHerdr(runner, findEmptyInventory, findEmptyTabs)
	a.Runner = runner

	out, err := findRun(t, a, "pw-target")
	if err == nil || !strings.Contains(err.Error(), "multiple panes matched") {
		t.Fatalf("same name on two hosts must stay ambiguous, err=%v", err)
	}
	for _, want := range []string{"machine=remote-a pane_id=w2:p7", "machine=remote-b pane_id=w4:p2"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q in:\n%s", want, out)
		}
	}
}

func TestFindFleetStaleSnapshotIsMarked(t *testing.T) {
	a, fixture := findHubApp(t)
	stale := findSessionJSON(t, a, "remote-a", "w2:p7", "pw-target", "pw-target", "pw-target", "idle")
	// Flip the node to disconnected: the snapshot is still decodable but the
	// observation is no longer fresh.
	stale = strings.Replace(stale, `"state":"connected"`, `"state":"disconnected"`, 1)
	fixture.nodes = `{"nodes":[` + stale + `]}`
	runner := &fakeRunner{}
	findStubHerdr(runner, findEmptyInventory, findEmptyTabs)
	a.Runner = runner

	out, err := findRun(t, a, "pw-target")
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	if !strings.Contains(out, "machine=remote-a pane_id=w2:p7") || !strings.Contains(out, "stale=true") {
		t.Fatalf("stale snapshot match must be marked, got:\n%s", out)
	}
}

func TestFindFleetUnavailableKeepsLocalLookup(t *testing.T) {
	// No [hub] section at all: find stays local, exactly as before.
	a := testApp(t)
	agents := `{"agents":[{"pane_id":"w1:p2","tab_id":"w1:t1","name":"pw-target","agent":"claude","agent_status":"idle","cwd":"/work/job"}]}`
	runner := &fakeRunner{}
	findStubHerdr(runner, agents, findEmptyTabs)
	a.Runner = runner

	out, err := findRun(t, a, "pw-target")
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	want := "machine=local pane_id=w1:p2 name=pw-target cwd=/work/job status=idle via=local"
	if !strings.Contains(out, want) {
		t.Fatalf("local-only output wrong, want substring %q in:\n%s", want, out)
	}
}

func TestFindFleetJobAndLaneAndSessionMerge(t *testing.T) {
	a, fixture := findHubApp(t)
	// The same pane is the job's pane, a registered lane, and a snapshot
	// session: one line, every source named.
	fixture.nodes = `{"nodes":[` + findSessionJSON(t, a, "remote-a", "w2:p7", "pw-target", "pw-target", "pw-target", "idle") + `]}`
	fixture.jobs = `{"jobs":[{"machine":"remote-a","state":"active","job_id":"pw-target","pane":"w2:p7"}]}`
	fixture.lanes = `{"lanes":[{"lane":"pw-target","machine":"remote-a","pane":"w2:p7"}]}`
	runner := &fakeRunner{}
	findStubHerdr(runner, findEmptyInventory, findEmptyTabs)
	a.Runner = runner

	out, err := findRun(t, a, "pw-target")
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	want := "machine=remote-a pane_id=w2:p7 name=pw-target cwd=- status=idle via=job,lane,session"
	if !strings.Contains(out, want+"\n") {
		t.Fatalf("merged fleet match wrong, want %q in:\n%s", want, out)
	}
}

package panewire

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// t501Now is the fixture clock: pinned to real now so fixture ages agree on
// both sides of the equivalence comparison — wrk reads the wall clock, the
// census reads deps.Now.
var t501Now = time.Now().UTC().Truncate(time.Second)

// t501Deps pins the census clock so fixture ages are deterministic.
func t501Deps(server *httptest.Server) hubCLIDeps {
	deps := hubCLIDeps{Now: func() time.Time { return t501Now }}
	if server != nil {
		deps.HTTPClient = server.Client()
		deps.AllowInsecureForTests = true
	}
	return deps
}

// t501JobEvent writes one flat inbox event record (the shape wrk itself
// writes) or, when envelope is true, a {"payload": {...}} envelope.
func t501JobEvent(t *testing.T, root, job, name string, document map[string]any) {
	t.Helper()
	dir := filepath.Join(root, job, "events")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), append(raw, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func t501FlatEvent(kind, at string, fields map[string]any) map[string]any {
	document := map[string]any{"kind": kind}
	if at != "" {
		document["at"] = at
	}
	for key, value := range fields {
		document[key] = value
	}
	return document
}

func t501EnvelopedEvent(kind, createdAt string, payload map[string]any) map[string]any {
	document := map[string]any{"kind": kind, "payload": payload}
	if createdAt != "" {
		document["created_at"] = createdAt
	}
	return document
}

// t501WriteJobsInbox materializes one fixture jobs root and returns it.
func t501WriteJobsInbox(t *testing.T, jobs map[string][]map[string]any) string {
	t.Helper()
	root := t.TempDir()
	for job, events := range jobs {
		for index, document := range events {
			kind, _ := document["kind"].(string)
			t501JobEvent(t, root, job, fmt.Sprintf("%05d-%s.json", index+1, kind), document)
		}
	}
	return root
}

// t501OldEnough is comfortably past the default 10m grace.
func t501OldEnough() string { return t501Now.Add(-2 * time.Hour).UTC().Format(time.RFC3339) }

func t501Fresh() string { return t501Now.Add(-30 * time.Second).UTC().Format(time.RFC3339) }

// t501StandardInbox is the fixture every reap-equivalence case shares: each
// job exercises one #508 rule branch.
func t501StandardInbox() map[string][]map[string]any {
	old := t501OldEnough()
	fresh := t501Fresh()
	return map[string][]map[string]any{
		// Clean terminal job on an idle pane in a single-pane tab.
		"j-reapable": {
			t501EnvelopedEvent("job.claim", "", map[string]any{"owner_lane": "lane-a", "role": "worker"}),
			t501EnvelopedEvent("job.spawned", "", map[string]any{"pane_id": "w1:p1", "tab_id": "w1:t1"}),
			t501FlatEvent("job.completed", old, nil),
		},
		// Terminal then claim again: revived, must never reap.
		"j-revived": {
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p2", "tab_id": "w1:t2"}),
			t501FlatEvent("job.completed", old, nil),
			t501FlatEvent("job.claim", "", map[string]any{"owner_lane": "lane-b", "role": "worker"}),
		},
		// Completed, but the recorded tab holds two panes.
		"j-shared": {
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p3", "tab_id": "w1:t3"}),
			t501FlatEvent("job.completed", old, nil),
		},
		// Recorded tab differs from the pane's current tab.
		"j-tabmismatch": {
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p4", "tab_id": "w1:t4old"}),
			t501FlatEvent("job.completed", old, nil),
		},
		// Newest spawn receipt lacks pane_id: malformed, never patched from
		// the earlier good receipt.
		"j-malformed": {
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p5", "tab_id": "w1:t5"}),
			t501FlatEvent("job.spawned", "", map[string]any{"tab_id": "w1:t5b"}),
			t501FlatEvent("job.completed", old, nil),
		},
		// Terminal job on a pane herdr cannot resolve.
		"j-nopane": {
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p6", "tab_id": "w1:t6"}),
			t501FlatEvent("job.completed", old, nil),
		},
		// Terminal event younger than --grace: silent skip.
		"j-grace": {
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p7", "tab_id": "w1:t7"}),
			t501FlatEvent("job.completed", fresh, nil),
		},
		// Builder claim role: excluded from reap without --include-builders.
		"j-builder": {
			t501FlatEvent("job.claim", "", map[string]any{"owner_lane": "lane-a", "role": "builder"}),
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p8", "tab_id": "w1:t8"}),
			t501FlatEvent("job.completed", old, nil),
		},
		// Legacy captain role counts as builder too.
		"j-captain": {
			t501FlatEvent("job.claim", "", map[string]any{"owner_lane": "lane-a", "role": "captain"}),
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p8b", "tab_id": "w1:t8b"}),
			t501FlatEvent("job.completed", old, nil),
		},
		// Already reaped once.
		"j-reaped": {
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p9", "tab_id": "w1:t9"}),
			t501FlatEvent("job.completed", old, nil),
			t501FlatEvent("job.reaped", old, nil),
		},
		// No terminal event at all.
		"j-running": {
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p10", "tab_id": "w1:t10"}),
		},
		// Recorded tab absent from tab.list.
		"j-tabmissing": {
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p11", "tab_id": "w1:t11"}),
			t501FlatEvent("job.completed", old, nil),
		},
		// Non-integer pane_count is wrk's "?": unknown, never shared.
		"j-tabcountq": {
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p12", "tab_id": "w1:t12"}),
			t501FlatEvent("job.completed", old, nil),
		},
		// pane_count 0 is unknown, not "shared with zero panes".
		"j-tabzero": {
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p14", "tab_id": "w1:t14"}),
			t501FlatEvent("job.completed", old, nil),
		},
		// Pane alive but still working.
		"j-status": {
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p13", "tab_id": "w1:t13"}),
			t501FlatEvent("job.completed", old, nil),
		},
		// Spawn receipt with no tab_id: the probe's tab is the fallback.
		"j-tabfallback": {
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p15"}),
			t501FlatEvent("job.completed", old, nil),
		},
		// Newest spawn receipt wins pane AND tab as a unit: the earlier
		// receipt's tab must not leak in. Newest receipt has no tab, so the
		// probe tab (w1:t16b) is used; a field-wise merge would see w1:t16a
		// and hit tab-mismatch instead.
		"j-unit": {
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p99", "tab_id": "w1:t16a"}),
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p16"}),
			t501FlatEvent("job.completed", old, nil),
		},
		// No spawn receipt: pane_id/tab_id from a flat completed event still
		// identify the job's pane (wrk accumulates payload fields).
		"j-nospawn": {
			t501FlatEvent("job.completed", old, map[string]any{"pane_id": "w1:p17", "tab_id": "w1:t17"}),
		},
		// Whitespace in the recorded pane: malformed.
		"j-whitespace": {
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p1 x", "tab_id": "w1:t18"}),
			t501FlatEvent("job.completed", old, nil),
		},
		// Job dir without an events dir at all (covered by t501ExtraDirs).
	}
}

// t501StandardAgents is the probe view matching t501StandardInbox.
func t501StandardAgents() map[string]map[string]string {
	return map[string]map[string]string{
		"w1:p1":  {"status": "idle", "tab_id": "w1:t1"},
		"w1:p2":  {"status": "idle", "tab_id": "w1:t2"},
		"w1:p3":  {"status": "idle", "tab_id": "w1:t3"},
		"w1:p4":  {"status": "idle", "tab_id": "w1:t4new"},
		"w1:p7":  {"status": "idle", "tab_id": "w1:t7"},
		"w1:p8":  {"status": "idle", "tab_id": "w1:t8"},
		"w1:p8b": {"status": "idle", "tab_id": "w1:t8b"},
		"w1:p9":  {"status": "idle", "tab_id": "w1:t9"},
		"w1:p10": {"status": "busy", "tab_id": "w1:t10"},
		"w1:p11": {"status": "idle", "tab_id": "w1:t11"},
		"w1:p12": {"status": "done", "tab_id": "w1:t12"},
		"w1:p13": {"status": "working", "tab_id": "w1:t13"},
		"w1:p14": {"status": "idle", "tab_id": "w1:t14"},
		"w1:p15": {"status": "idle", "tab_id": "w1:t15"},
		"w1:p16": {"status": "idle", "tab_id": "w1:t16b"},
		"w1:p17": {"status": "idle", "tab_id": "w1:t17"},
	}
}

func t501StandardTabs() []map[string]any {
	return []map[string]any{
		{"tab_id": "w1:t1", "pane_count": 1},
		{"tab_id": "w1:t2", "pane_count": 1},
		{"tab_id": "w1:t3", "pane_count": 2},
		{"tab_id": "w1:t4old", "pane_count": 1},
		{"tab_id": "w1:t4new", "pane_count": 1},
		{"tab_id": "w1:t6", "pane_count": 1},
		{"tab_id": "w1:t7", "pane_count": 1},
		{"tab_id": "w1:t8", "pane_count": 1},
		{"tab_id": "w1:t8b", "pane_count": 1},
		{"tab_id": "w1:t9", "pane_count": 1},
		{"tab_id": "w1:t10", "pane_count": 1},
		{"tab_id": "w1:t12", "pane_count": "?"},
		{"tab_id": "w1:t13", "pane_count": 1},
		{"tab_id": "w1:t14", "pane_count": 0},
		{"tab_id": "w1:t15", "pane_count": 1},
		{"tab_id": "w1:t16a", "pane_count": 1},
		{"tab_id": "w1:t16b", "pane_count": 1},
		{"tab_id": "w1:t17", "pane_count": 1},
		{"tab_id": "w1:t18", "pane_count": 1},
	}
}

// t501ViewFromSpec builds the census local observation from the same fixture
// values the stub herdr serves to the reference script.
func t501ViewFromSpec(agents map[string]map[string]string, tabs []map[string]any, tabsOK bool) fleetCensusLocalView {
	view := fleetCensusLocalView{
		agents:     map[string]fleetCensusPaneObs{},
		paneListOK: true,
		tabsOK:     tabsOK,
	}
	for pane, spec := range agents {
		obs := fleetCensusPaneObs{PaneID: pane, Status: spec["status"], TabID: spec["tab_id"], HasAgent: true, AgentName: spec["name"], Harness: spec["harness"]}
		view.agents[pane] = obs
		view.panes = append(view.panes, obs)
	}
	if tabsOK {
		view.tabs = map[string]*int{}
		for _, tab := range tabs {
			id, _ := tab["tab_id"].(string)
			if id == "" {
				continue
			}
			if _, dup := view.tabs[id]; dup {
				view.tabs[id] = nil
				continue
			}
			switch value := tab["pane_count"].(type) {
			case int:
				count := value
				view.tabs[id] = &count
			case float64:
				count := int(value)
				if float64(count) == value {
					view.tabs[id] = &count
				} else {
					view.tabs[id] = nil
				}
			default:
				view.tabs[id] = nil
			}
		}
	}
	sort.Slice(view.panes, func(i, j int) bool { return view.panes[i].PaneID < view.panes[j].PaneID })
	return view
}

// t501RunReference executes the vendored wrk ae1f544 reap dry-run against the
// fixture inbox and stub herdr, returning job → verdict/reason pairs.
func t501RunReference(t *testing.T, jobsRoot string, agents map[string]map[string]string, tabs []map[string]any, tabListFails bool, graceSeconds string) map[string][2]string {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 unavailable")
	}
	fixtureAgents := map[string]any{}
	for pane, spec := range agents {
		if spec["error"] != "" {
			fixtureAgents[pane] = map[string]any{"error": spec["error"]}
		} else {
			fixtureAgents[pane] = map[string]any{"status": spec["status"], "tab_id": spec["tab_id"]}
		}
	}
	fixtureDoc := map[string]any{"agents": fixtureAgents, "tabs": tabs, "tab_list_fails": tabListFails}
	fixtureRaw, err := json.Marshal(fixtureDoc)
	if err != nil {
		t.Fatal(err)
	}
	fixturePath := filepath.Join(t.TempDir(), "herdr-fixture.json")
	if err := os.WriteFile(fixturePath, fixtureRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("bash", "testdata/fleet_census_reap_reference.sh", graceSeconds)
	command.Env = append(os.Environ(),
		"ARBITER_INBOX_ROOT="+jobsRoot,
		"HERDR_BIN=testdata/fleet_census_herdr_stub.sh",
		"STUB_HERDR_FIXTURE="+fixturePath,
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("reference reap failed: %v\n%s", err, output)
	}
	verdicts := map[string][2]string{}
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		var job, verdict, reason string
		switch fields[0] {
		case "would-close":
			verdict = "would-close"
		case "skip":
			verdict = "skip"
		default:
			continue
		}
		for _, field := range fields[1:] {
			if strings.HasPrefix(field, "job=") {
				job = strings.TrimPrefix(field, "job=")
			}
			if strings.HasPrefix(field, "reason=") {
				reason = strings.TrimPrefix(field, "reason=")
			}
		}
		if job != "" {
			verdicts[job] = [2]string{verdict, reason}
		}
	}
	return verdicts
}

// t501CensusVerdicts runs the census job pipeline (scan + verdict) against
// the same fixture and returns job → verdict/reason pairs.
func t501CensusVerdicts(t *testing.T, jobsRoot string, view fleetCensusLocalView, grace time.Duration) map[string][2]string {
	t.Helper()
	scans, ok := scanFleetCensusJobs(jobsRoot)
	if !ok {
		t.Fatalf("jobs root unreadable: %s", jobsRoot)
	}
	verdicts := map[string][2]string{}
	for _, scan := range scans {
		verdict, reason, _ := fleetCensusJobVerdict(scan, view, grace, t501Now)
		verdicts[scan.jobID] = [2]string{verdict, reason}
	}
	return verdicts
}

// TestFleetCensusReapEquivalence pins A-4: on one shared fixture the census
// verdict for every job equals the vendored wrk ae1f544 dry-run verdict —
// would-close for would-close, the same reason for every printed skip, and
// silence for the jobs wrk drops without a line. A mismatch anywhere is an
// assertion failure, not a panic.
func TestFleetCensusReapEquivalence(t *testing.T) {
	jobs := t501StandardInbox()
	// A directory that is not a jobs entry and a job dir without events.
	root := t501WriteJobsInbox(t, jobs)
	if err := os.MkdirAll(filepath.Join(root, "j-noevents"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "stray-file"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	agents := t501StandardAgents()
	tabs := t501StandardTabs()

	reference := t501RunReference(t, root, agents, tabs, false, "600")
	view := t501ViewFromSpec(agents, tabs, true)
	census := t501CensusVerdicts(t, root, view, 10*time.Minute)

	for job, pair := range census {
		gotVerdict, gotReason := pair[0], pair[1]
		want, listed := reference[job]
		switch gotVerdict {
		case fleetCensusVerdictWouldClose:
			if !listed || want[0] != "would-close" {
				t.Errorf("job %s: census=would-close, wrk=%v", job, want)
			}
		case fleetCensusVerdictSkip:
			if !listed || want[0] != "skip" {
				t.Errorf("job %s: census=skip(%s), wrk=%v", job, gotReason, want)
				continue
			}
			if want[1] != gotReason {
				t.Errorf("job %s: census reason %q != wrk %q", job, gotReason, want[1])
			}
		default:
			if listed {
				t.Errorf("job %s: census silent, wrk=%v", job, want)
			}
		}
	}
	for job, want := range reference {
		if _, seen := census[job]; !seen {
			t.Errorf("job %s: wrk=%v but census has no row", job, want)
		}
	}
	if t.Failed() {
		t.Logf("reference=%v census=%v", reference, census)
	}
}

// TestFleetCensusReapEquivalenceTabListDown repeats the comparison with the
// tab list unreadable: every candidate that reaches the tab gate must land
// on tab-count-unknown on both sides.
func TestFleetCensusReapEquivalenceTabListDown(t *testing.T) {
	root := t501WriteJobsInbox(t, t501StandardInbox())
	agents := t501StandardAgents()
	reference := t501RunReference(t, root, agents, t501StandardTabs(), true, "600")
	view := t501ViewFromSpec(agents, t501StandardTabs(), false)
	census := t501CensusVerdicts(t, root, view, 10*time.Minute)
	for job, pair := range census {
		want, listed := reference[job]
		switch pair[0] {
		case fleetCensusVerdictWouldClose:
			if !listed || want[0] != "would-close" {
				t.Errorf("job %s: census=would-close, wrk=%v", job, want)
			}
		case fleetCensusVerdictSkip:
			if !listed || want[0] != "skip" || want[1] != pair[1] {
				t.Errorf("job %s: census=skip(%s), wrk=%v", job, pair[1], want)
			}
		default:
			if listed {
				t.Errorf("job %s: census silent, wrk=%v", job, want)
			}
		}
	}
	if len(reference) == 0 {
		t.Fatal("reference produced no verdict lines")
	}
}

// TestFleetCensusReapEquivalenceClasses asserts the pane-level class mapping
// on the same fixture: would-close → reapable, builder role → builder, and
// terminal-state skips → terminal-held, revived → no-terminal-event.
func TestFleetCensusReapEquivalenceClasses(t *testing.T) {
	root := t501WriteJobsInbox(t, t501StandardInbox())
	view := t501ViewFromSpec(t501StandardAgents(), t501StandardTabs(), true)
	result := buildFleetCensusResult(fleetCensusOptions{grace: 10 * time.Minute}, "machine-local", view, true, true, mustScans(t, root), nil, nil, nil, "", false, t501Now)
	classByJob := map[string]string{}
	paneClass := map[string]string{}
	for _, pane := range result.Panes {
		paneClass[pane.PaneID] = pane.Class
	}
	for _, job := range result.Jobs {
		classByJob[job.JobID] = job.Verdict
	}
	if paneClass["w1:p1"] != fleetCensusClassReapable {
		t.Errorf("w1:p1 class=%s want reapable", paneClass["w1:p1"])
	}
	if paneClass["w1:p8"] != fleetCensusClassBuilder || paneClass["w1:p8b"] != fleetCensusClassBuilder {
		t.Errorf("builder classes: w1:p8=%s w1:p8b=%s", paneClass["w1:p8"], paneClass["w1:p8b"])
	}
	if paneClass["w1:p2"] != fleetCensusClassNoTerminalEvent {
		t.Errorf("w1:p2 class=%s want no-terminal-event (revived)", paneClass["w1:p2"])
	}
	for _, pane := range []string{"w1:p3", "w1:p4", "w1:p9", "w1:p14"} {
		if paneClass[pane] != fleetCensusClassTerminalHeld {
			t.Errorf("%s class=%s want terminal-held", pane, paneClass[pane])
		}
	}
	// w1:p6 resolves to no live pane, so there is no pane row — the job row
	// carries the skip verdict instead.
	if classByJob["j-nopane"] != fleetCensusVerdictSkip {
		t.Errorf("j-nopane verdict=%s want skip", classByJob["j-nopane"])
	}
	if paneClass["w1:p10"] != fleetCensusClassNoTerminalEvent {
		t.Errorf("w1:p10 class=%s want no-terminal-event", paneClass["w1:p10"])
	}
	if paneClass["w1:p7"] != fleetCensusClassTerminalHeld {
		t.Errorf("w1:p7 class=%s want terminal-held (within grace)", paneClass["w1:p7"])
	}
	if classByJob["j-reapable"] != fleetCensusVerdictWouldClose {
		t.Errorf("j-reapable verdict=%s", classByJob["j-reapable"])
	}
	if classByJob["j-revived"] != fleetCensusVerdictSkip {
		t.Errorf("j-revived verdict=%s want skip", classByJob["j-revived"])
	}
}

func mustScans(t *testing.T, root string) []fleetCensusJobScan {
	t.Helper()
	scans, ok := scanFleetCensusJobs(root)
	if !ok {
		t.Fatalf("jobs root unreadable: %s", root)
	}
	return scans
}

// t501HerdrServer is a minimal unix-socket herdr double for internal-package
// tests: canned per-method results, every method string recorded.
type t501HerdrServer struct {
	path     string
	listener net.Listener
	mu       sync.Mutex
	methods  []string
	results  map[string]any
	errors   map[string]bool
}

func t501NewHerdr(t *testing.T, results map[string]any, errors map[string]bool) *t501HerdrServer {
	t.Helper()
	listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "herdr.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := &t501HerdrServer{path: listener.Addr().String(), listener: listener, results: results, errors: errors}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.serve(conn)
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return server
}

func (s *t501HerdrServer) serve(conn net.Conn) {
	defer conn.Close()
	scan := bufio.NewScanner(conn)
	for scan.Scan() {
		var request map[string]any
		if json.Unmarshal(scan.Bytes(), &request) != nil {
			continue
		}
		method, _ := request["method"].(string)
		s.mu.Lock()
		s.methods = append(s.methods, method)
		s.mu.Unlock()
		var reply map[string]any
		if s.errors[method] {
			reply = map[string]any{"id": request["id"], "error": map[string]any{"code": "stub_error"}}
		} else {
			result := s.results[method]
			if result == nil {
				result = map[string]any{}
			}
			reply = map[string]any{"id": request["id"], "result": result}
		}
		raw, _ := json.Marshal(reply)
		_, _ = fmt.Fprintf(conn, "%s\n", raw)
	}
}

func (s *t501HerdrServer) Methods() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string{}, s.methods...)
}

// t501HubFixture serves the three GET endpoints the census reads and records
// every request so tests can count exactly what was contacted.
func t501HubFixture(t *testing.T, recorded chan<- t441RecordedRequest, lanesBody, jobsBody, nodesBody string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get(hubAuthorizationHeader) != "Bearer "+t441OperatorToken {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		if recorded != nil {
			recorded <- t441RecordedRequest{method: request.Method, path: request.URL.Path, query: request.URL.RawQuery, headers: request.Header.Clone()}
		}
		writer.Header().Set("Content-Type", "application/json")
		switch request.Method + " " + request.URL.Path {
		case "GET /v1/lanes":
			_, _ = writer.Write([]byte(lanesBody))
		case "GET /v1/jobs":
			_, _ = writer.Write([]byte(jobsBody))
		case "GET /v1/nodes":
			_, _ = writer.Write([]byte(nodesBody))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
}

func t501Snapshot(status, sessions string, receivedAt time.Time, truncated, stale bool) string {
	return fmt.Sprintf(`{"sessions":%s,"snapshot_status":%q,"truncated":%t,"received_at":%q,"stale":%t}`,
		sessions, status, truncated, receivedAt.UTC().Format(time.RFC3339), stale)
}

func t501SessionRow(paneID, agentName, status string) string {
	return fmt.Sprintf(`{"pane_id":%q,"workspace_id":"w1","label":%q,"agent_name":%q,"label_source":"agent_name","status":%q,"revision":1,"state_change_seq":1}`, paneID, agentName, agentName, status)
}

func t501Node(machineID, state, snapshot string) string {
	return fmt.Sprintf(`{"machine_id":%q,"state":%q,"session_snapshot":%s}`, machineID, state, snapshot)
}

// t501EmptyHerdrResults is a healthy local observation with one agent pane
// and one agent-less pane.
func t501EmptyHerdrResults() map[string]any {
	return map[string]any{
		"agent.list": map[string]any{"agents": []any{
			map[string]any{"pane_id": "w1:p1", "workspace_id": "w1", "tab_id": "w1:t1", "name": "flag-1", "agent": "claude", "agent_status": "idle"},
		}},
		"pane.list": map[string]any{"panes": []any{
			map[string]any{"pane_id": "w1:p1", "workspace_id": "w1", "tab_id": "w1:t1", "agent_status": "idle"},
			map[string]any{"pane_id": "w1:p2", "workspace_id": "w1", "tab_id": "w1:t2"},
		}},
		"tab.list": map[string]any{"tabs": []any{
			map[string]any{"tab_id": "w1:t1", "pane_count": 1},
			map[string]any{"tab_id": "w1:t2", "pane_count": 1},
		}},
	}
}

// TestFleetCensusLocalCLI runs the command end-to-end against a fixture
// socket: the agent-less pane appears (pane.list is the pane-set source, not
// agent.list), the reapable job classifies, and only read methods were used.
func TestFleetCensusLocalCLI(t *testing.T) {
	jobs := map[string][]map[string]any{
		"j-done": {
			t501FlatEvent("job.claim", "", map[string]any{"owner_lane": "lane-a", "role": "worker"}),
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p1", "tab_id": "w1:t1"}),
			t501FlatEvent("job.completed", t501OldEnough(), nil),
		},
	}
	root := t501WriteJobsInbox(t, jobs)
	server := t501NewHerdr(t, t501EmptyHerdrResults(), nil)
	var stdout, stderr bytes.Buffer
	code := runFleetCensusCLI([]string{
		"--jobs-root", root, "--herdr-socket", server.path, "--machine-id", "machine-local", "--json",
	}, &stdout, &stderr, t501Deps(nil))
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	var result fleetCensusResult
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &result); err != nil {
		t.Fatalf("output not JSON: %v %q", err, stdout.String())
	}
	if len(result.Panes) != 2 {
		t.Fatalf("panes=%d want 2 (agent pane + agent-less pane): %+v", len(result.Panes), result.Panes)
	}
	byPane := map[string]fleetCensusPaneRow{}
	for _, pane := range result.Panes {
		byPane[pane.PaneID] = pane
	}
	row := byPane["w1:p1"]
	if row.Class != fleetCensusClassReapable || row.JobID != "j-done" || row.OwnerLane != "lane-a" || row.Role != "worker" {
		t.Errorf("w1:p1 row=%+v", row)
	}
	if row.AgentName != "flag-1" || row.Harness != "claude" || row.Status != "idle" {
		t.Errorf("w1:p1 identity=%+v", row)
	}
	if row.TerminalKind != "job.completed" || row.TerminalAt == "" {
		t.Errorf("w1:p1 terminal=%q %q", row.TerminalKind, row.TerminalAt)
	}
	if row.AgeSeconds == nil {
		t.Errorf("w1:p1 age missing")
	}
	agentless := byPane["w1:p2"]
	if agentless.Class != fleetCensusClassNoJobRecord || agentless.HasAgent {
		t.Errorf("w1:p2 agent-less row=%+v", agentless)
	}
	// Read-only proof: exactly the three read methods, in any order.
	methods := server.Methods()
	sort.Strings(methods)
	want := []string{"agent.list", "pane.list", "tab.list"}
	if fmt.Sprint(methods) != fmt.Sprint(want) {
		t.Fatalf("herdr methods=%v want %v", methods, want)
	}
}

// TestFleetCensusReadOnly pins A-2: the hub sees only GETs on the three read
// endpoints, herdr sees only list methods, and the jobs inbox is untouched —
// same file set, same bytes, same mtimes before and after the run.
func TestFleetCensusReadOnly(t *testing.T) {
	root := t501WriteJobsInbox(t, t501StandardInbox())
	before := t501InboxFingerprint(t, root)
	server := t501NewHerdr(t, t501EmptyHerdrResults(), nil)
	recorded := make(chan t441RecordedRequest, 16)
	snapshot := t501Snapshot(hubSnapshotStatusOK, "["+t501SessionRow("w2:p1", "rem-1", "idle")+"]", t501Now.Add(-10*time.Second), false, false)
	nodesBody := `{"nodes":[` + t501Node("machine-remote", "connected", snapshot) + `]}`
	jobsBody := `{"jobs":[{"machine":"machine-remote","job_id":"j-remote","owner_lane":"lane-r","pane":"w2:p1","role":"worker","started_at":"` + t501OldEnough() + `","last_event_kind":"job.completed","last_event_at":"` + t501OldEnough() + `"}]}`
	lanesBody := `{"lanes":[{"lane":"lane-r","machine":"machine-remote","pane":"w2:p1","parent":"","sink":false}],"control_epoch":1}`
	hub := t501HubFixture(t, recorded, lanesBody, jobsBody, nodesBody)
	defer hub.Close()
	tokenEnv, _ := t441Envs(t)
	var stdout, stderr bytes.Buffer
	code := runFleetCensusCLI([]string{
		"--jobs-root", root, "--herdr-socket", server.path, "--machine-id", "machine-local",
		"--hub-url", hub.URL, "--hub-token-env", tokenEnv, "--json",
	}, &stdout, &stderr, t501Deps(hub))
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	seen := map[string]int{}
	drained := false
	for !drained {
		select {
		case request := <-recorded:
			seen[request.method+" "+request.path]++
		default:
			drained = true
		}
	}
	want := map[string]int{"GET /v1/lanes": 1, "GET /v1/jobs": 1, "GET /v1/nodes": 1}
	if fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Fatalf("hub requests=%v want %v", seen, want)
	}
	for _, method := range server.Methods() {
		switch method {
		case "agent.list", "pane.list", "tab.list":
		default:
			t.Fatalf("unexpected herdr method %q", method)
		}
	}
	after := t501InboxFingerprint(t, root)
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatalf("jobs inbox changed: before=%v after=%v", before, after)
	}
	t441AssertNoCredentialLeak(t, stdout.String(), stderr.String(), "")
}

func t501InboxFingerprint(t *testing.T, root string) []string {
	t.Helper()
	var entries []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		entries = append(entries, fmt.Sprintf("%s|%d|%x", path, info.Size(), raw))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(entries)
	return entries
}

// TestFleetCensusStaleNodeNotEmpty is the A-7 mutation guard: a node whose
// snapshot is stale still contributes its rows, marked unobservable — never
// silently counted as an empty node.
func TestFleetCensusStaleNodeNotEmpty(t *testing.T) {
	stale := t501Snapshot(hubSnapshotStatusOK, "["+t501SessionRow("w2:p1", "rem-1", "idle")+","+t501SessionRow("w2:p2", "rem-2", "idle")+"]", t501Now.Add(-time.Hour), false, false)
	nodes := []sessionsFindNodeWire{{MachineID: "machine-remote", State: "connected", SessionSnapshot: json.RawMessage(stale)}}
	result := buildFleetCensusResult(fleetCensusOptions{grace: time.Minute, hubURL: "https://h"}, "machine-local", fleetCensusLocalView{}, false, true, nil, nodes, nil, nil, "", false, t501Now)
	if len(result.Panes) != 2 {
		t.Fatalf("stale node contributed %d panes, want 2 (visible but unobservable)", len(result.Panes))
	}
	for _, pane := range result.Panes {
		if pane.Class != fleetCensusClassUnobservable || pane.Detail != fleetCensusDetailStaleSnapshot {
			t.Fatalf("stale pane row=%+v", pane)
		}
	}
	if result.Coverage.Stale != 1 || result.Outcome != fleetCensusOutcomePartial {
		t.Fatalf("coverage=%+v outcome=%s", result.Coverage, result.Outcome)
	}
}

// TestFleetCensusCoverageStates pins the sessions-find vocabulary on each
// node state: unobserved, unavailable, invalid, truncated and observed-empty
// all keep their distinct labels.
func TestFleetCensusCoverageStates(t *testing.T) {
	fresh := t501Now.Add(-10 * time.Second)
	nodes := []sessionsFindNodeWire{
		{MachineID: "m-unobserved", State: "connected"},
		{MachineID: "m-unavail", State: "connected", SessionSnapshot: json.RawMessage(t501Snapshot(hubSnapshotStatusUnavailable, "null", fresh, false, false))},
		{MachineID: "m-invalid", State: "connected", SessionSnapshot: json.RawMessage(t501Snapshot(hubSnapshotStatusOK, `"oops"`, fresh, false, false))},
		{MachineID: "m-partial", State: "connected", SessionSnapshot: json.RawMessage(t501Snapshot(hubSnapshotStatusOK, "["+t501SessionRow("w3:p1", "a", "idle")+"]", fresh, true, false))},
		{MachineID: "m-empty", State: "connected", SessionSnapshot: json.RawMessage(t501Snapshot(hubSnapshotStatusOK, "[]", fresh, false, false))},
	}
	result := buildFleetCensusResult(fleetCensusOptions{grace: time.Minute, hubURL: "https://h"}, "machine-local", fleetCensusLocalView{}, false, true, nil, nodes, nil, nil, "", false, t501Now)
	states := map[string]string{}
	for _, node := range result.Coverage.Nodes {
		states[node.Machine] = node.State
	}
	want := map[string]string{
		"m-unobserved": sessionsFindStateUnobserved,
		"m-unavail":    sessionsFindStateCollectorUnavailable,
		"m-invalid":    sessionsFindStateInvalidSnapshot,
		"m-partial":    sessionsFindStatePartial,
		"m-empty":      sessionsFindStateObservedEmpty,
	}
	for machine, state := range want {
		if states[machine] != state {
			t.Errorf("node %s state=%q want %q", machine, states[machine], state)
		}
	}
	// The truncated node still surfaces its visible pane, flagged partial.
	found := false
	for _, pane := range result.Panes {
		if pane.Machine == "m-partial" && pane.PaneID == "w3:p1" {
			found = true
			if pane.Detail != fleetCensusDetailPartialSnapshot {
				t.Errorf("partial pane detail=%q", pane.Detail)
			}
		}
	}
	if !found {
		t.Error("truncated node's visible pane missing")
	}
	// Unobserved/unavailable/invalid nodes contribute zero rows.
	for _, pane := range result.Panes {
		if pane.Machine == "m-unobserved" || pane.Machine == "m-unavail" || pane.Machine == "m-invalid" {
			t.Errorf("row from %s should not exist", pane.Machine)
		}
	}
	if result.Outcome != fleetCensusOutcomePartial {
		t.Fatalf("outcome=%s want partial", result.Outcome)
	}
}

// TestFleetCensusLanesIssues covers A-5: two lanes pointing at one pane and a
// lane whose pane is absent from a usable observation are each reported.
func TestFleetCensusLanesIssues(t *testing.T) {
	fresh := t501Now.Add(-10 * time.Second)
	nodes := []sessionsFindNodeWire{
		{MachineID: "machine-remote", State: "connected", SessionSnapshot: json.RawMessage(t501Snapshot(hubSnapshotStatusOK, "["+t501SessionRow("w2:p1", "a", "idle")+"]", fresh, false, false))},
	}
	lanes := []hubLaneProjection{
		{Lane: "lane-a", Machine: "machine-remote", Pane: "w2:p1"},
		{Lane: "lane-b", Machine: "machine-remote", Pane: "w2:p1"},
		{Lane: "lane-c", Machine: "machine-remote", Pane: "w2:p9"},
		{Lane: "sink-lane", Sink: true},
	}
	result := buildFleetCensusResult(fleetCensusOptions{grace: time.Minute, hubURL: "https://h"}, "machine-local", fleetCensusLocalView{}, false, true, nil, nodes, nil, lanes, "", false, t501Now)
	var multi, missing *fleetCensusLaneIssue
	for index := range result.LaneIssues {
		switch result.LaneIssues[index].Kind {
		case "multi-lane-pane":
			multi = &result.LaneIssues[index]
		case "lane-pane-missing":
			missing = &result.LaneIssues[index]
		}
	}
	if multi == nil || multi.Pane != "w2:p1" || len(multi.Lanes) != 2 {
		t.Fatalf("multi-lane issue=%+v", multi)
	}
	if missing == nil || missing.Pane != "w2:p9" || len(missing.Lanes) != 1 || missing.Lanes[0] != "lane-c" {
		t.Fatalf("missing-pane issue=%+v", missing)
	}
	// Per-pane lane list carries both lanes.
	for _, pane := range result.Panes {
		if pane.PaneID == "w2:p1" && len(pane.Lanes) != 2 {
			t.Errorf("pane lanes=%v", pane.Lanes)
		}
	}
	// The dead verdict comes from lanes-audit.
	dead := 0
	for _, lane := range result.Lanes {
		if lane.Verdict == lanesAuditVerdictDead {
			dead++
		}
	}
	if dead != 1 {
		t.Fatalf("dead verdicts=%d lanes=%+v", dead, result.Lanes)
	}
}

// TestFleetCensusIndeterminateLane pins that a lane to an unobserved machine
// is indeterminate — never reported as missing.
func TestFleetCensusIndeterminateLane(t *testing.T) {
	nodes := []sessionsFindNodeWire{{MachineID: "machine-remote", State: "stale", SessionSnapshot: json.RawMessage(t501Snapshot(hubSnapshotStatusOK, "[]", t501Now.Add(-time.Hour), false, false))}}
	lanes := []hubLaneProjection{{Lane: "lane-x", Machine: "machine-remote", Pane: "w2:p9"}}
	result := buildFleetCensusResult(fleetCensusOptions{grace: time.Minute, hubURL: "https://h"}, "machine-local", fleetCensusLocalView{}, false, true, nil, nodes, nil, lanes, "", false, t501Now)
	for _, issue := range result.LaneIssues {
		if issue.Kind == "lane-pane-missing" {
			t.Fatalf("unobserved machine reported missing pane: %+v", issue)
		}
	}
	for _, lane := range result.Lanes {
		if lane.Verdict != lanesAuditVerdictIndeterminate {
			t.Fatalf("lane verdict=%s want indeterminate", lane.Verdict)
		}
	}
}

// TestFleetCensusHerdrDown: no socket → the local node is reported
// unavailable, never "empty", and the exit is partial.
func TestFleetCensusHerdrDown(t *testing.T) {
	root := t501WriteJobsInbox(t, nil)
	var stdout, stderr bytes.Buffer
	code := runFleetCensusCLI([]string{
		"--jobs-root", root, "--herdr-socket", filepath.Join(t.TempDir(), "absent.sock"), "--machine-id", "machine-local", "--json",
	}, &stdout, &stderr, t501Deps(nil))
	if code != ExitPartial {
		t.Fatalf("code=%d want partial, stdout=%q", code, stdout.String())
	}
	var result fleetCensusResult
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &result); err != nil {
		t.Fatalf("output not JSON: %v", err)
	}
	if len(result.Panes) != 0 {
		t.Fatalf("panes=%v", result.Panes)
	}
	local := result.Coverage.Nodes[0]
	if local.State != sessionsFindStateCollectorUnavailable || local.Covered {
		t.Fatalf("local coverage=%+v", local)
	}
}

// TestFleetCensusLocalHubMerge: the local machine listed by the hub merges
// into one coverage entry and one row set — never double-counted.
func TestFleetCensusLocalHubMerge(t *testing.T) {
	fresh := t501Now.Add(-10 * time.Second)
	nodes := []sessionsFindNodeWire{
		{MachineID: "machine-local", State: "connected", SessionSnapshot: json.RawMessage(t501Snapshot(hubSnapshotStatusOK, "["+t501SessionRow("w1:p1", "a", "idle")+"]", fresh, false, false))},
		{MachineID: "machine-remote", State: "connected", SessionSnapshot: json.RawMessage(t501Snapshot(hubSnapshotStatusOK, "["+t501SessionRow("w2:p1", "b", "idle")+"]", fresh, false, false))},
	}
	view := fleetCensusLocalView{
		agents:     map[string]fleetCensusPaneObs{"w1:p1": {PaneID: "w1:p1", Status: "idle", HasAgent: true}},
		panes:      []fleetCensusPaneObs{{PaneID: "w1:p1", Status: "idle", HasAgent: true}},
		paneListOK: true,
		tabsOK:     true,
		tabs:       map[string]*int{},
	}
	result := buildFleetCensusResult(fleetCensusOptions{grace: time.Minute, hubURL: "https://h"}, "machine-local", view, true, true, nil, nodes, nil, nil, "", false, t501Now)
	if result.Coverage.Expected != 2 || result.Coverage.Observed != 2 {
		t.Fatalf("coverage=%+v", result.Coverage)
	}
	localRows, remoteRows := 0, 0
	for _, pane := range result.Panes {
		switch pane.Machine {
		case "machine-local":
			localRows++
		case "machine-remote":
			remoteRows++
		}
	}
	if localRows != 1 || remoteRows != 1 {
		t.Fatalf("rows local=%d remote=%d", localRows, remoteRows)
	}
	var localEntry *fleetCensusNodeCoverage
	for index := range result.Coverage.Nodes {
		if result.Coverage.Nodes[index].Machine == "machine-local" {
			localEntry = &result.Coverage.Nodes[index]
		}
	}
	if localEntry == nil || localEntry.Source != "local+hub" || localEntry.HubState == "" {
		t.Fatalf("local entry=%+v", localEntry)
	}
}

// TestFleetCensusArgs pins the closed flag set: no --apply-style flag exists.
func TestFleetCensusArgs(t *testing.T) {
	for _, args := range [][]string{
		{"extra"},
		{"--bogus"},
		{"--apply"},
		{"--lane", "lane-a"},
		{"--json", "--json"},
		{"--grace"},
		{"--grace", "bogus"},
		{"--hub-url", "http://x"},
		{"--hub-token-env", "/tmp/x"},
		{"--machine-id", "BAD ID"},
	} {
		if _, err := parseFleetCensusArgs(args); err == nil {
			t.Errorf("args=%v should be rejected", args)
		}
	}
	options, err := parseFleetCensusArgs([]string{"--json", "--grace", "2h", "--jobs-root", "/tmp/j"})
	if err != nil || !options.jsonOut || options.grace != 2*time.Hour || options.jobsRoot != "/tmp/j" {
		t.Fatalf("options=%+v err=%v", options, err)
	}
}

// TestFleetCensusTableOutput: the human-readable form lists every field A-1
// names, and the porcelain form of an empty fleet stays stable.
func TestFleetCensusTableOutput(t *testing.T) {
	root := t501WriteJobsInbox(t, map[string][]map[string]any{
		"j-done": {
			t501FlatEvent("job.spawned", "", map[string]any{"pane_id": "w1:p1", "tab_id": "w1:t1"}),
			t501FlatEvent("job.completed", t501OldEnough(), nil),
		},
	})
	server := t501NewHerdr(t, t501EmptyHerdrResults(), nil)
	var stdout, stderr bytes.Buffer
	code := runFleetCensusCLI([]string{
		"--jobs-root", root, "--herdr-socket", server.path, "--machine-id", "machine-local",
	}, &stdout, &stderr, t501Deps(nil))
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{"machine\tmachine-local", "class=reapable", "agent=flag-1", "harness=claude", "job=j-done", "terminal=job.completed", "panes\t2", "summary\tpanes=2"} {
		if !strings.Contains(out, want) {
			t.Errorf("table output missing %q:\n%s", want, out)
		}
	}
}

// TestFleetCensusPorcelainEmpty: an empty fleet emits the porcelain skeleton
// (coverage + zero counts), not an error.
func TestFleetCensusPorcelainEmpty(t *testing.T) {
	root := t501WriteJobsInbox(t, nil)
	server := t501NewHerdr(t, map[string]any{
		"agent.list": map[string]any{"agents": []any{}},
		"pane.list":  map[string]any{"panes": []any{}},
		"tab.list":   map[string]any{"tabs": []any{}},
	}, nil)
	var stdout, stderr bytes.Buffer
	code := runFleetCensusCLI([]string{
		"--jobs-root", root, "--herdr-socket", server.path, "--machine-id", "machine-local",
	}, &stdout, &stderr, t501Deps(nil))
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "outcome\tok") || !strings.Contains(out, "panes\t0") || !strings.Contains(out, "summary\tpanes=0") {
		t.Fatalf("empty porcelain output:\n%s", out)
	}
}

package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/mgh3326/panewire/internal/fleet"
)

// concurrencyRig is a node whose fake git prepare writes an owner marker into
// the worktree and whose fake herdr engine plays launch, brief delivery and
// landing proof: it reads the marker at launch, waits on release, and proves
// at landing that the marker and its brief are still its own.
type concurrencyRig struct {
	client     *HubClient
	root       string
	release    chan struct{}
	mu         sync.Mutex
	running    int
	maxRunning int
	preparing  int
	maxPrepare int
	cwds       []string
	disturbed  []string
	started    chan string
}

func newConcurrencyRig(t *testing.T, slots int) *concurrencyRig {
	t.Helper()
	rig := &concurrencyRig{root: t.TempDir(), release: make(chan struct{}), started: make(chan string, 16)}
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	config := filepath.Join(t.TempDir(), "spawn.json")
	// alias names the same directory as the checkout dir job-x.
	contents := `{"cwd_map":{"repo-a":"` + t.TempDir() + `","alias":"` + filepath.Join(rig.root, "job-x") + `"},"work_root":"` + rig.root + `","workspace":"worker","herdr_session":"worker","enabled":true}`
	if err := os.WriteFile(config, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{spawnConfigPath: config, spawnJournalDir: t.TempDir(), fleetEvents: (&fleetEventRecorder{}).write, spawnSlotsOverride: slots}
	client.spawnPrepare = func(_ context.Context, args nodeWorktreePrepareArgs) (any, string) {
		rig.mu.Lock()
		rig.preparing++
		rig.maxPrepare = max(rig.maxPrepare, rig.preparing)
		rig.mu.Unlock()
		// Simulated latency: a prepare long enough that two of one clone
		// would overlap if they were not serialized.
		time.Sleep(20 * time.Millisecond)
		dir := filepath.Join(rig.root, args.Dir)
		_ = os.MkdirAll(dir, 0o700)
		_ = os.WriteFile(filepath.Join(dir, "owner"), []byte(args.Sha), 0o600)
		rig.mu.Lock()
		rig.preparing--
		rig.mu.Unlock()
		return nodeWorktreePrepareResult{Outcome: nodePreparePrepared, Worktree: &nodeWorktree{Path: dir, Head: args.Sha}}, ""
	}
	client.spawnEngine = func(_ context.Context, cwd, brief, _, _, _ string, _ time.Time, args []string) (*hubSpawnOutput, fleet.Execution) {
		owner, _ := os.ReadFile(filepath.Join(cwd, "owner"))
		text, _ := os.ReadFile(brief)
		rig.mu.Lock()
		rig.running++
		rig.maxRunning = max(rig.maxRunning, rig.running)
		rig.cwds = append(rig.cwds, cwd)
		rig.mu.Unlock()
		rig.started <- string(owner)
		<-rig.release
		landed, _ := os.ReadFile(filepath.Join(cwd, "owner"))
		delivered, _ := os.ReadFile(brief)
		rig.mu.Lock()
		rig.running--
		ownBrief := !strings.HasPrefix(hubSpawnArg(args, "--job"), "job-") || string(text) == "brief "+string(owner)
		if string(landed) != string(owner) || string(delivered) != string(text) || !ownBrief {
			rig.disturbed = append(rig.disturbed, fmt.Sprintf("%s: launch owner %q, landing owner %q, brief %q", hubSpawnArg(args, "--job"), owner, landed, delivered))
		}
		rig.mu.Unlock()
		return t14EngineOK()
	}
	rig.client = client
	return rig
}

func concurrencyMessage(n int, dir string) hubOutboundMessage {
	sha := fmt.Sprintf("%040x", n)
	message := admissionMessage(457800+n, true)
	message.Args = []string{"--job", fmt.Sprintf("job-%d", n)}
	message.BriefInline = "brief " + sha
	message.Checkout = &hubSpawnCheckout{Origin: "https://github.com/mgh3326/panewire.git", Mode: "detached", Sha: sha, Dir: dir}
	return message
}

func (rig *concurrencyRig) run(t *testing.T, messages []hubOutboundMessage) <-chan hubSpawnResult {
	t.Helper()
	peer, results, _ := grantPeer(t, rig.client, "grant")
	for _, message := range messages {
		go rig.client.handleHubSpawn(t.Context(), peer, message)
	}
	return results
}

func TestNodeSpawnRunsNAtOnceInDistinctWorktrees_t4578(t *testing.T) {
	const n = 3
	rig := newConcurrencyRig(t, n)
	var messages []hubOutboundMessage
	for i := range n {
		messages = append(messages, concurrencyMessage(i+1, fmt.Sprintf("job-%d", i+1)))
	}
	results := rig.run(t, messages)
	for range n {
		select {
		case <-rig.started:
		case <-time.After(5 * time.Second):
			t.Fatal("N spawns did not all reach launch at once")
		}
	}
	close(rig.release)
	for range n {
		if result := nextSpawnResult(t, results); result.RC != 0 {
			t.Fatalf("spawn failed: %+v", result)
		}
	}
	rig.mu.Lock()
	defer rig.mu.Unlock()
	slices.Sort(rig.cwds)
	if rig.maxRunning != n || len(slices.Compact(rig.cwds)) != n || len(rig.disturbed) != 0 {
		t.Fatalf("maxRunning=%d cwds=%v disturbed=%v", rig.maxRunning, rig.cwds, rig.disturbed)
	}
	if rig.maxPrepare != 1 {
		t.Fatalf("one clone prepared %d worktrees at once", rig.maxPrepare)
	}
}

func TestNodeSpawnSerializesOneWorktree_t4578(t *testing.T) {
	const n = 3
	rig := newConcurrencyRig(t, n)
	// One directory three ways: a checkout dir, the same dir in another
	// case, and a cwd_map entry without a checkout that names it.
	alias := admissionMessage(457809, true)
	alias.CWDKey, alias.Args, alias.BriefInline = "alias", []string{"--job", "alias-9"}, "alias brief"
	messages := []hubOutboundMessage{concurrencyMessage(1, "job-x"), concurrencyMessage(2, "JOB-X"), alias}
	results := rig.run(t, messages)
	for i := range n {
		select {
		case <-rig.started:
		case <-time.After(5 * time.Second):
			t.Fatalf("spawn %d never launched", i+1)
		}
		// Window: no second spawn of this worktree may launch while one is
		// between launch and landing.
		select {
		case owner := <-rig.started:
			t.Fatalf("spawn for %s launched in a held worktree", owner)
		case <-time.After(150 * time.Millisecond):
		}
		rig.release <- struct{}{}
		if result := nextSpawnResult(t, results); result.RC != 0 {
			t.Fatalf("spawn failed: %+v", result)
		}
	}
	rig.mu.Lock()
	defer rig.mu.Unlock()
	if rig.maxRunning != 1 || len(rig.disturbed) != 0 {
		t.Fatalf("maxRunning=%d disturbed=%v", rig.maxRunning, rig.disturbed)
	}
}

// Default-off: with spawn_concurrency unset the node runs one spawn at a time
// and never advertises the capability, so the hub dispatches one at a time
// (hubNodeSpawnSlots of a hello without it is 1). Only an explicit value above
// one is advertised and enforced, and a change binds at the next hello.
func TestNodeSpawnSlotsDefaultOffAndOptIn_t4578(t *testing.T) {
	writeConfig := func(path, extra string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(`{"workspace":"w","herdr_session":"w","enabled":true`+extra+`}`), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, machine := range []string{"mbp-server", "rpi", "oci"} {
		client := &HubClient{machineID: machine, spawnConfigPath: t14SpawnConfig(t, t.TempDir())}
		caps := client.helloCapabilities(hubSpawnCapabilities)
		if !slices.Equal(caps, hubSpawnCapabilities) || client.nodeSpawnSlots() != 1 {
			t.Fatalf("%s unset: capabilities %v slots %d", machine, caps, client.nodeSpawnSlots())
		}
		if got := hubNodeSpawnSlots(map[string]bool{"spawn_v2": true, "spawn_ops_v1": true}); got != 1 {
			t.Fatalf("hub slots for a node without the capability=%d", got)
		}
	}
	client := &HubClient{machineID: "m1", spawnConfigPath: t14SpawnConfig(t, t.TempDir())}
	writeConfig(client.spawnConfigPath, `,"spawn_concurrency":2`)
	caps := client.helloCapabilities(hubSpawnCapabilities)
	if !slices.Contains(caps, hubCapabilitySpawnSlotsPrefix+"2") || client.nodeSpawnSlots() != 2 {
		t.Fatalf("opt-in capabilities %v slots %d", caps, client.nodeSpawnSlots())
	}
	if got := hubNodeSpawnSlots(map[string]bool{hubCapabilitySpawnSlotsPrefix + "2": true}); got != 2 {
		t.Fatalf("hub slots for an opted-in node=%d", got)
	}
	// The node enforces what it advertised until the next hello.
	writeConfig(client.spawnConfigPath, ``)
	if got := client.nodeSpawnSlots(); got != 2 {
		t.Fatalf("slots=%d before re-registering, want the advertised 2", got)
	}
	if caps := client.helloCapabilities(hubSpawnCapabilities); !slices.Equal(caps, hubSpawnCapabilities) || client.nodeSpawnSlots() != 1 {
		t.Fatalf("after opting out: capabilities %v slots %d", caps, client.nodeSpawnSlots())
	}
	if err := os.WriteFile(client.spawnConfigPath, []byte(`{"workspace":"w","herdr_session":"w","enabled":false,"spawn_concurrency":3}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if caps := client.helloCapabilities(hubSpawnCapabilities); !slices.Equal(caps, hubSpawnCapabilities) || client.nodeSpawnSlots() != 1 {
		t.Fatalf("unloadable config: capabilities %v slots %d", caps, client.nodeSpawnSlots())
	}
	if client.helloCapabilities(nil) != nil {
		t.Fatal("a hello without spawn capabilities advertised slots")
	}
}

func TestSpawnConcurrencyConfigRange_t4578(t *testing.T) {
	for value, rule := range map[string]string{"2": "", "-1": "spawn_concurrency_range", "9": "spawn_concurrency_range"} {
		path := filepath.Join(t.TempDir(), "spawn.json")
		contents := `{"workspace":"w","herdr_session":"w","enabled":true,"spawn_concurrency":` + value + `}`
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		config, err := loadHubSpawnConfig(path)
		var refusal *hubSpawnConfigError
		switch {
		case rule == "" && (err != nil || config.SpawnConcurrency != 2):
			t.Fatalf("%s: config=%+v err=%v", value, config, err)
		case rule != "" && (err == nil || !asConfigError(err, &refusal) || refusal.Rule != rule):
			t.Fatalf("%s: err=%v, want %s", value, err, rule)
		}
	}
}

// slotNode connects machine-a advertising slots (0 = an older node).
func slotNode(t *testing.T, hub *HubServer, server *httptest.Server, slots int) *websocket.Conn {
	t.Helper()
	headers := http.Header{}
	headers.Set(hubMachineIDHeader, "machine-a")
	headers.Set(hubAuthorizationHeader, "Bearer node-token")
	connection, _, err := websocket.Dial(t.Context(), t14WSURL(server.URL, "/v1/agent"), &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.CloseNow() })
	capabilities := []string{"spawn_v2", "spawn_ops_v1", hubCapabilityCloseQuestion}
	if slots > 0 {
		capabilities = append(capabilities, fmt.Sprintf("%s%d", hubCapabilitySpawnSlotsPrefix, slots))
	}
	if err := wsjson.Write(t.Context(), connection, map[string]any{"type": "hello", "machine_id": "machine-a", "version": "t4578", "accepting": true, "capabilities": capabilities}); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, "the node to connect", func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		record := hub.nodes["machine-a"]
		return record != nil && record.agent != nil && record.state == "connected"
	})
	return connection
}

func TestNodeSpawnHubDispatchesUpToAdvertisedSlots_t4578(t *testing.T) {
	for _, tc := range []struct {
		name  string
		slots int
		want  int
	}{{"advertised 2", 2, 2}, {"older node", 0, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			h := asyncSpawnHub(t)
			server := httptest.NewServer(h.Handler())
			defer server.Close()
			node := slotNode(t, h, server, tc.slots)
			tasks := map[string]hubSpawnTask{}
			for i := range 3 {
				task := checkoutDispatchFixture(t, h, i+1, fmt.Sprintf("job-%d", i+1))
				tasks[task.Node.RequestID] = task
			}
			h.startSpawnCoordinator(t.Context())
			frames := make(chan hubOutboundMessage, 3)
			go func() {
				// Ends when the test closes the connection.
				for {
					_, payload, err := node.Read(context.Background())
					if err != nil {
						return
					}
					var frame struct {
						Type      string `json:"type"`
						RequestID string `json:"request_id"`
					}
					if json.Unmarshal(payload, &frame) == nil && frame.Type == "job.spawn" {
						frames <- hubOutboundMessage{Type: frame.Type, RequestID: frame.RequestID}
					}
				}
			}()
			var open []string
			for i := range tc.want {
				select {
				case frame := <-frames:
					open = append(open, frame.RequestID)
				case <-time.After(3 * time.Second):
					t.Fatalf("hub sent %d of %d slots", i, tc.want)
				}
			}
			// Window: no frame beyond the node's slots is sent while every
			// dispatched spawn is unresolved.
			select {
			case extra := <-frames:
				t.Fatalf("hub sent %s beyond %d slots", extra.RequestID, tc.want)
			case <-time.After(250 * time.Millisecond):
			}
			// Answer the newest dispatch first: it settles on its own result,
			// the older one stays bound and unresolved, and exactly one slot
			// frees for the queued request.
			answered := open[len(open)-1]
			if err := wsjson.Write(t.Context(), node, nodeRefusal(answered, "spawn_disabled", false)); err != nil {
				t.Fatal(err)
			}
			select {
			case frame := <-frames:
				if tasks[frame.RequestID].Node == nil || slices.Contains(open, frame.RequestID) {
					t.Fatalf("freed slot sent %s", frame.RequestID)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("the freed slot sent nothing")
			}
			eventually(t, 3*time.Second, "the answered dispatch to settle", func() bool {
				current, err := h.inbox.store.spawnTask(t.Context(), tasks[answered].Request.RequestID)
				return err == nil && (current.Node == nil || current.Node.RequestID != answered || !spawnPending(current.Status))
			})
			for _, id := range open[:len(open)-1] {
				current, err := h.inbox.store.spawnTask(t.Context(), tasks[id].Request.RequestID)
				if err != nil || current.Node == nil || current.Node.RequestID != id || !spawnPending(current.Status) {
					t.Fatalf("unanswered dispatch %s changed: %+v err=%v", id, current, err)
				}
			}
		})
	}
}

// checkoutDispatchFixture binds a checkout spawn into dir on machine-a; an
// empty dir binds a spawn without a checkout.
func checkoutDispatchFixture(t *testing.T, h *HubServer, n int, dir string) hubSpawnTask {
	t.Helper()
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(457900 + n)
	job := fmt.Sprintf("overlap-%d", n)
	request.Args = append([]string(nil), request.Args...)
	for i, arg := range request.Args {
		switch arg {
		case "test-job":
			request.Args[i] = job
		case "42":
			request.Args[i] = fmt.Sprint(457900 + n)
		}
	}
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, job, "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	node := boundaryTestNode(task, 457950+n, "machine-a")
	node.CWDKey = "repo-a"
	node.Args = []string{"--job", job, "-m", "sonnet"}
	if dir != "" {
		node.Checkout = &hubSpawnCheckout{Origin: "https://github.com/mgh3326/panewire.git", Mode: "detached", Sha: fmt.Sprintf("%040x", n), Dir: dir}
	}
	task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, node, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return task
}

// The hub overlaps only checkout spawns into distinct dirs: a second spawn of
// one dir, or a spawn without a checkout, stays queued at the hub instead of
// waiting on the node past its start window.
func TestNodeSpawnHubKeepsNonOverlappingWorkQueued_t4578(t *testing.T) {
	for _, tc := range []struct {
		name string
		dirs []string
	}{{"same dir", []string{"job-x", "JOB-X"}}, {"no checkout first", []string{"", "job-y"}}, {"no checkout second", []string{"job-y", ""}}} {
		t.Run(tc.name, func(t *testing.T) {
			h := asyncSpawnHub(t)
			server := httptest.NewServer(h.Handler())
			defer server.Close()
			node := slotNode(t, h, server, 3)
			for i, dir := range tc.dirs {
				checkoutDispatchFixture(t, h, i+1, dir)
			}
			h.startSpawnCoordinator(t.Context())
			frames := make(chan hubOutboundMessage, 2)
			go func() {
				// Ends when the test closes the connection.
				for {
					_, payload, err := node.Read(context.Background())
					if err != nil {
						return
					}
					var frame struct {
						Type      string `json:"type"`
						RequestID string `json:"request_id"`
					}
					if json.Unmarshal(payload, &frame) == nil && frame.Type == "job.spawn" {
						frames <- hubOutboundMessage{Type: frame.Type, RequestID: frame.RequestID}
					}
				}
			}()
			select {
			case <-frames:
			case <-time.After(3 * time.Second):
				t.Fatal("hub sent nothing")
			}
			// Window: the second request must stay at the hub.
			select {
			case extra := <-frames:
				t.Fatalf("hub overlapped %s", extra.RequestID)
			case <-time.After(250 * time.Millisecond):
			}
		})
	}
}

// A symlinked job dir or lane may name another job's worktree: such a spawn
// is admitted alone, and a lane that leaves its worktree after prepare is
// refused before launch.
func TestNodeSpawnSymlinkedTargetsAreNotOverlapped_t4578(t *testing.T) {
	root := t.TempDir()
	config := filepath.Join(t.TempDir(), "spawn.json")
	if err := os.WriteFile(config, []byte(`{"work_root":"`+root+`","workspace":"w","herdr_session":"w","enabled":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{spawnConfigPath: config}
	for _, dir := range []string{"job-a", "job-c"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Join(root, "job-a"), filepath.Join(root, "job-b")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../job-a", filepath.Join(root, "job-c", "live")); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		dir, lane, want string
	}{{"job-a", "", "job-a"}, {"job-b", "", ""}, {"job-c", "", "job-c"}, {"job-c", "live", ""}, {"job-a", "live", "job-a"}} {
		message := concurrencyMessage(1, tc.dir)
		if tc.lane != "" {
			message.Args = append(message.Args, "-L", tc.lane)
		}
		want := tc.want
		if want != "" {
			want = spawnOverlapKey(want, message.Args)
		}
		if got := client.spawnAdmissionKey(message); got != want {
			t.Errorf("dir=%s lane=%s key=%q, want %q", tc.dir, tc.lane, got, want)
		}
	}
	if !spawnLaneEscapes(filepath.Join(root, "job-c"), "live") || spawnLaneEscapes(filepath.Join(root, "job-a"), "live") || spawnLaneEscapes(filepath.Join(root, "job-a"), "") {
		t.Fatal("lane escape check is wrong")
	}
}

// A request without a checkout queued behind open dispatches drains the
// node: later checkouts do not refill the freed slots ahead of it.
func TestNodeSpawnHubDrainsForQueuedExclusiveRequest_t4578(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := slotNode(t, h, server, 2)
	tasks := map[string]hubSpawnTask{}
	for i, dir := range []string{"job-a", "job-b", "", "job-d"} {
		task := checkoutDispatchFixture(t, h, i+1, dir)
		tasks[dir] = task
		// Distinct ages fix the scan order.
		time.Sleep(10 * time.Millisecond) // fixture pacing
	}
	h.startSpawnCoordinator(t.Context())
	frames := make(chan string, 4)
	go func() {
		// Ends when the test closes the connection.
		for {
			_, payload, err := node.Read(context.Background())
			if err != nil {
				return
			}
			var frame struct {
				Type      string `json:"type"`
				RequestID string `json:"request_id"`
			}
			if json.Unmarshal(payload, &frame) == nil && frame.Type == "job.spawn" {
				frames <- frame.RequestID
			}
		}
	}()
	next := func(label string) string {
		select {
		case id := <-frames:
			return id
		case <-time.After(5 * time.Second):
			t.Fatalf("no frame: %s", label)
			return ""
		}
	}
	open := []string{next("first"), next("second")}
	slices.Sort(open)
	want := []string{tasks["job-a"].Node.RequestID, tasks["job-b"].Node.RequestID}
	slices.Sort(want)
	if !slices.Equal(open, want) {
		t.Fatalf("opened %v, want %v", open, want)
	}
	if err := wsjson.Write(t.Context(), node, nodeRefusal(tasks["job-a"].Node.RequestID, "spawn_disabled", false)); err != nil {
		t.Fatal(err)
	}
	// Window: with job-b still open, neither the exclusive request nor the
	// later checkout may start.
	select {
	case id := <-frames:
		t.Fatalf("hub refilled the drained slot with %s", id)
	case <-time.After(2500 * time.Millisecond):
	}
	if err := wsjson.Write(t.Context(), node, nodeRefusal(tasks["job-b"].Node.RequestID, "spawn_disabled", false)); err != nil {
		t.Fatal(err)
	}
	if got := next("after drain"); got != tasks[""].Node.RequestID {
		t.Fatalf("after the drain the hub sent %s, want the exclusive request %s", got, tasks[""].Node.RequestID)
	}
}

// Job ids that differ only in case share one jobs-inbox directory on a
// case-insensitive volume, so their spawns never land at once even in
// distinct worktrees; the hub applies the same rule.
func TestNodeSpawnSerializesCaseAliasedJobIDs_t4578(t *testing.T) {
	rig := newConcurrencyRig(t, 2)
	first, second := concurrencyMessage(1, "dir-1"), concurrencyMessage(2, "dir-2")
	first.Args, second.Args = []string{"--job", "job-Review-A"}, []string{"--job", "job-review-a"}
	results := rig.run(t, []hubOutboundMessage{first, second})
	select {
	case <-rig.started:
	case <-time.After(5 * time.Second):
		t.Fatal("neither spawn launched")
	}
	// Window: the case alias must not launch while the first is landing.
	select {
	case owner := <-rig.started:
		t.Fatalf("case-aliased job %s launched concurrently", owner)
	case <-time.After(150 * time.Millisecond):
	}
	close(rig.release)
	for range 2 {
		if result := nextSpawnResult(t, results); result.RC != 0 {
			t.Fatalf("spawn failed: %+v", result)
		}
	}
	// A legacy request without --job takes its job id from -l.
	if !spawnOverlapConflicts(spawnOverlapKey("dir-1", []string{"-l", "review-a"}), spawnOverlapKey("dir-2", []string{"-l", "review-b", "--job", "Review-A"})) {
		t.Fatal("a job id defaulted from -l was not in the overlap key")
	}
	if !spawnOverlapConflicts(spawnOverlapKey("dir-1", first.Args), spawnOverlapKey("dir-2", second.Args)) || spawnOverlapConflicts(spawnOverlapKey("dir-1", []string{"--job", "a"}), spawnOverlapKey("dir-2", []string{"--job", "b"})) {
		t.Fatal("overlap key conflict rule is wrong")
	}
}

// The lane is read as a flag, never from another flag's value: a --purpose
// of "-L" does not hide the real -L live.
func TestNodeSpawnLaneReadAsFlag_t4578(t *testing.T) {
	root := t.TempDir()
	config := filepath.Join(t.TempDir(), "spawn.json")
	if err := os.WriteFile(config, []byte(`{"work_root":"`+root+`","workspace":"w","herdr_session":"w","enabled":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"job-a", "job-c"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("../job-a", filepath.Join(root, "job-c", "live")); err != nil {
		t.Fatal(err)
	}
	message := concurrencyMessage(1, "job-c")
	message.Args = []string{"--job", "job-c", "--purpose", "-L", "-m", "sonnet", "-L", "live"}
	if got := (&HubClient{spawnConfigPath: config}).spawnAdmissionKey(message); got != "" {
		t.Fatalf("a lane hidden behind a flag value was not seen: key %q", got)
	}
	if lane := hubSpawnFlagArg(message.Args, "-L"); lane != "live" || !spawnLaneEscapes(filepath.Join(root, "job-c"), lane) {
		t.Fatalf("lane=%q", lane)
	}
}

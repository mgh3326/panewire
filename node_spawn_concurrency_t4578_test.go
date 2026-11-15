package panewire

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
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
	client := &HubClient{spawnConfigPath: t14SpawnConfig(t, t.TempDir()), spawnJournalDir: t.TempDir(), fleetEvents: (&fleetEventRecorder{}).write, spawnSlotsOverride: slots}
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
	client.spawnEngine = func(_ context.Context, cwd, brief, _, _ string, _ time.Time, args []string) (*hubSpawnOutput, fleet.Execution) {
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
		if string(landed) != string(owner) || string(delivered) != string(text) || string(text) != "brief "+string(owner) {
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

func TestNodeRunsNSpawnsAtOnceInDistinctWorktrees_t4578(t *testing.T) {
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

func TestNodeSerializesSpawnsForOneWorktree_t4578(t *testing.T) {
	const n = 3
	rig := newConcurrencyRig(t, n)
	// One dir under three spellings that fold to the same worktree key.
	var messages []hubOutboundMessage
	for i, dir := range []string{"job-x", "JOB-X", "job-x/."} {
		messages = append(messages, concurrencyMessage(i+1, dir))
	}
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

func TestNodeSpawnSlotsDefaultsAndAdvertisement_t4578(t *testing.T) {
	for machine, want := range map[string]int{"mbp-server": 3, "m1": 3, "rpi-5": 2, "oci-arm": 2} {
		if got := defaultNodeSpawnSlots(machine); got != want {
			t.Errorf("%s default slots=%d, want %d", machine, got, want)
		}
	}
	client := &HubClient{machineID: "rpi-5", spawnConfigPath: t14SpawnConfig(t, t.TempDir())}
	caps := client.helloCapabilities(hubSpawnCapabilities)
	if !slices.Contains(caps, hubCapabilitySpawnSlotsPrefix+"2") || cap(client.nodeSpawnGate()) != 2 {
		t.Fatalf("capabilities %v gate %d", caps, cap(client.nodeSpawnGate()))
	}
	if client.helloCapabilities(nil) != nil {
		t.Fatal("a hello without spawn capabilities advertised slots")
	}
	if got := hubNodeSpawnSlots(map[string]bool{"spawn_v2": true}); got != 1 {
		t.Fatalf("older node slots=%d, want 1", got)
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

func TestHubDispatchesUpToTheNodesAdvertisedSlots_t4578(t *testing.T) {
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
			for i := range 3 {
				boundDispatchFixture(t, h, i+1, "machine-a")
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
					if message, ok := parseHubOutbound(payload); ok && message.Type == "job.spawn" {
						frames <- message
					}
				}
			}()
			for i := range tc.want {
				select {
				case <-frames:
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
		})
	}
}

package panewire

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Per-node spawn concurrency (hk 4578). A node runs up to N spawns at once.
// The checkout-stability boundary that makes this safe:
//
//   - Only checkout spawns overlap. A checkout spawn executes inside its own
//     job worktree, <work_root>/<checkout dir> (a lane's -L subdirectory
//     included), and the dir is a validated job name. Admission holds that
//     name, case-folded, from before the executing journal claim until the
//     result is persisted, so checkout, workspace/tab creation, agent start,
//     brief delivery and landing proof (spawn.go fleet.start →
//     writeDeliveredBrief → deliver → model/task proof) never share a
//     directory with another spawn. The key is the name, not a resolved
//     path, so it does not move when spawn.json or a symlink changes.
//   - A spawn without a checkout (it runs in a cwd_map or default repo
//     directory, possibly a clone itself) is admitted alone on the node: no
//     other spawn runs while it does. A waiting exclusive spawn stops new
//     admissions so it cannot starve.
//   - Preparation of one clone (fetch, worktree add, update-ref) runs one at a
//     time inside this process (lockSpawnPrepare), taken only after admission.
//
// N is spawn.json's spawn_concurrency as of the last hello; unset it is 3, or 2
// on a host whose machine id names rpi or oci; an unreadable spawn.json gives 1. The
// node advertises N in hello; the hub dispatches more than one at a time only
// to a node that advertises it, and only checkout spawns into distinct dirs
// (decision 254: an older node or hub stays at one).

const (
	hubCapabilitySpawnSlotsPrefix = "spawn_slots_v1="
	nodeSpawnSlotsMax             = 8
)

// defaultNodeSpawnSlots is the per-host default when spawn.json is silent.
func defaultNodeSpawnSlots(machineID string) int {
	id := strings.ToLower(machineID)
	if strings.Contains(id, "rpi") || strings.Contains(id, "oci") {
		return 2
	}
	return 3
}

// nodeSpawnSlots is the N this node enforces: the value it advertised in its
// last hello, so the hub and the node never disagree about it. A change to
// spawn_concurrency takes effect at the next registration (a reconnect or a
// daemon restart). Before any hello it is read from spawn.json.
func (client *HubClient) nodeSpawnSlots() int {
	if client.spawnSlotsOverride != 0 {
		return min(client.spawnSlotsOverride, nodeSpawnSlotsMax)
	}
	if advertised := client.spawnSlotsAdvertised.Load(); advertised > 0 {
		return int(advertised)
	}
	return client.configuredSpawnSlots()
}

// configuredSpawnSlots reads the current spawn.json; a file that does not load
// fails closed to one.
func (client *HubClient) configuredSpawnSlots() int {
	config, _, err := client.resolveHubSpawnConfig()
	switch {
	case err != nil:
		return 1
	case config.SpawnConcurrency > 0:
		return min(config.SpawnConcurrency, nodeSpawnSlotsMax)
	}
	return defaultNodeSpawnSlots(client.machineID)
}

// helloCapabilities appends the slot count to a spawn-capable hello and makes
// it the count this node enforces.
func (client *HubClient) helloCapabilities(base []string) []string {
	if len(base) == 0 {
		return base
	}
	slots := client.configuredSpawnSlots()
	if client.spawnSlotsOverride != 0 {
		slots = client.nodeSpawnSlots()
	}
	client.spawnSlotsAdvertised.Store(int32(slots))
	client.nodeAdmission().wake()
	return append(append([]string(nil), base...), hubCapabilitySpawnSlotsPrefix+strconv.Itoa(slots))
}

// hubNodeSpawnSlots reads a node's advertised N; a node that advertises none
// (an older node) gets 1.
func hubNodeSpawnSlots(capabilities map[string]bool) int {
	slots := 1
	for capability := range capabilities {
		value, ok := strings.CutPrefix(capability, hubCapabilitySpawnSlotsPrefix)
		if !ok {
			continue
		}
		if n, err := strconv.Atoi(value); err == nil && n > slots {
			slots = min(n, nodeSpawnSlotsMax)
		}
	}
	return slots
}

// spawnAdmissionKey is the job dir a checkout spawn holds, or "" for a spawn
// the node admits alone. A job dir or lane that is a symlink may name another
// job's worktree, so such a spawn is admitted alone too; a lane that becomes
// a link only after prepare is refused before launch (spawnLaneEscapes).
func (client *HubClient) spawnAdmissionKey(message hubOutboundMessage) string {
	if message.Checkout == nil || message.Checkout.Dir == "" {
		return ""
	}
	root, err := client.nodeWorkRoot()
	if err != nil {
		return ""
	}
	dir := filepath.Join(root, message.Checkout.Dir)
	if spawnPathIsLink(dir) {
		return ""
	}
	if lane := hubSpawnArg(message.Args, "-L"); lane != "" && spawnPathIsLink(filepath.Join(dir, lane)) {
		return ""
	}
	return strings.ToLower(filepath.Clean(message.Checkout.Dir))
}

func spawnPathIsLink(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && info.Mode()&os.ModeSymlink != 0
}

// spawnLaneEscapes reports a -L lane under a prepared worktree that resolves
// outside it: the launch would run in another directory than the one held.
func spawnLaneEscapes(worktree, lane string) bool {
	if lane == "" {
		return false
	}
	root, err := filepath.EvalSymlinks(worktree)
	if err != nil {
		return true
	}
	target, err := filepath.EvalSymlinks(filepath.Join(worktree, lane))
	if err != nil {
		// Not there yet: the launcher creates it inside the worktree.
		return spawnPathIsLink(filepath.Join(worktree, lane))
	}
	return !nodePathWithin(root, target)
}

// hubSpawnOverlapKey is the hub's view of the same key.
func hubSpawnOverlapKey(request *hubSpawnRequest) string {
	if request == nil || request.Checkout == nil || request.Checkout.Dir == "" {
		return ""
	}
	return strings.ToLower(filepath.Clean(request.Checkout.Dir))
}

// nodeSpawnAdmission is the node's process-local spawn admission state.
type nodeSpawnAdmission struct {
	mu               sync.Mutex
	running          int
	exclusive        bool
	exclusiveWaiting int
	dirs             map[string]bool
	changed          chan struct{}
}

func (client *HubClient) nodeAdmission() *nodeSpawnAdmission {
	client.spawnAdmissionOnce.Do(func() {
		client.spawnAdmission = &nodeSpawnAdmission{dirs: map[string]bool{}, changed: make(chan struct{})}
	})
	return client.spawnAdmission
}

func (a *nodeSpawnAdmission) waitExclusive(delta int) {
	a.mu.Lock()
	a.exclusiveWaiting += delta
	a.wakeLocked()
	a.mu.Unlock()
}

func (a *nodeSpawnAdmission) wake() {
	a.mu.Lock()
	a.wakeLocked()
	a.mu.Unlock()
}

func (a *nodeSpawnAdmission) wakeLocked() {
	close(a.changed)
	a.changed = make(chan struct{})
}

// tryAdmit admits key ("" = alone) under limit, returning an idempotent
// release; otherwise it returns the channel closed at the next change.
func (a *nodeSpawnAdmission) tryAdmit(key string, limit int) (func(), <-chan struct{}) {
	a.mu.Lock()
	defer a.mu.Unlock()
	ok := !a.exclusive && a.running < max(limit, 1)
	if key == "" {
		ok = ok && a.running == 0
	} else {
		ok = ok && a.exclusiveWaiting == 0 && !a.dirs[key]
	}
	if !ok {
		return nil, a.changed
	}
	a.running++
	if key == "" {
		a.exclusive = true
	} else {
		a.dirs[key] = true
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.running--
			if key == "" {
				a.exclusive = false
			} else {
				delete(a.dirs, key)
			}
			a.wakeLocked()
		})
	}, nil
}

// spawnPrepareKey names the clone a checkout prepares in.
func spawnPrepareKey(checkout *hubSpawnCheckout) string {
	repo, ok := nodeOriginRepo(checkout.Origin)
	if !ok {
		repo = checkout.Origin
	}
	if i := strings.IndexByte(repo, '/'); i >= 0 {
		repo = repo[i+1:]
	}
	return "repo:" + strings.ToLower(repo)
}

// lockSpawnPrepare serializes preparation within one clone; false means the
// run ended while waiting.
func (client *HubClient) lockSpawnPrepare(ctx context.Context, checkout *hubSpawnCheckout) (func(), bool) {
	client.spawnLocksMu.Lock()
	if client.spawnLocks == nil {
		client.spawnLocks = map[string]chan struct{}{}
	}
	key := spawnPrepareKey(checkout)
	lock := client.spawnLocks[key]
	if lock == nil {
		lock = make(chan struct{}, 1)
		client.spawnLocks[key] = lock
	}
	client.spawnLocksMu.Unlock()
	select {
	case lock <- struct{}{}:
		return func() { <-lock }, true
	case <-ctx.Done():
		return nil, false
	}
}

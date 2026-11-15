package panewire

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
)

// Per-node spawn concurrency (hk 4578). A node runs up to N spawns at once;
// the checkout-stability boundary that makes this safe is:
//
//   - Whole-spawn directory hold: a spawn holds its execution directory from
//     before its executing journal claim until its result is persisted, so
//     checkout, workspace/tab creation, agent start, brief delivery and
//     landing proof (spawn.go fleet.start → writeDeliveredBrief → deliver →
//     model/task proof) all run with no other spawn touching that directory
//     (spawnHoldKeys).
//   - Prepare serialization: worktree preparation of one repository (fetch,
//     worktree add) runs one at a time inside this process, so two distinct
//     worktrees of one clone never race on the clone's refs and index locks,
//     and never report the cross-process git-op lock as busy to each other.
//
// Distinct worktrees therefore overlap only after their own preparation, each
// in its own directory. N is spawn.json's spawn_concurrency, or by default 3,
// and 2 on a host whose machine id names rpi or oci. The node advertises N in
// hello; a hub treats a node that does not as N=1 (decision 254), and an
// older hub never sends more than one at a time.

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

// nodeSpawnSlots is fixed at first use so the advertised N and the gate
// capacity never disagree within one process.
func (client *HubClient) nodeSpawnSlots() int {
	client.spawnSlotsOnce.Do(func() {
		slots := client.spawnSlotsOverride
		if slots == 0 {
			if config, _, err := client.resolveHubSpawnConfig(); err == nil {
				slots = config.SpawnConcurrency
			}
		}
		if slots <= 0 {
			slots = defaultNodeSpawnSlots(client.machineID)
		}
		client.spawnSlots = min(slots, nodeSpawnSlotsMax)
		client.spawnGate = make(chan struct{}, client.spawnSlots)
	})
	return client.spawnSlots
}

func (client *HubClient) nodeSpawnGate() chan struct{} {
	client.nodeSpawnSlots()
	return client.spawnGate
}

// helloCapabilities appends the advertised slot count to a spawn-capable
// hello.
func (client *HubClient) helloCapabilities(base []string) []string {
	if len(base) == 0 {
		return base
	}
	return append(append([]string(nil), base...), hubCapabilitySpawnSlotsPrefix+strconv.Itoa(client.nodeSpawnSlots()))
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

// spawnHoldKeys names what a spawn holds from before its executing claim
// until its result is persisted: the one directory it will execute in,
// resolved the way the run resolves it, so a cwd_map entry, a default repo dir
// and a checkout dir that name one directory share one key. A spawn that runs
// in a clone itself also holds that clone's prepare key, since another spawn's
// prepare moves the clone's refs. Keys are always taken in the returned order.
func (client *HubClient) spawnHoldKeys(message hubOutboundMessage) []string {
	config, defaults, err := client.resolveHubSpawnConfig()
	if err != nil {
		// The run refuses too; this key only keeps such copies apart.
		return []string{"unresolved:" + strings.ToLower(message.CWDKey)}
	}
	if message.Checkout != nil && config.WorkRoot != "" {
		return []string{"dir:" + spawnDirKey(filepath.Join(config.WorkRoot, message.Checkout.Dir))}
	}
	cwd, found := config.CWDMap[message.CWDKey]
	if !found && defaults {
		cwd, found = defaultRepoDir(config.WorkRoot, message.CWDKey)
	}
	if !found {
		return []string{"unresolved:" + strings.ToLower(message.CWDKey)}
	}
	keys := []string{"dir:" + spawnDirKey(cwd)}
	if config.WorkRoot != "" && spawnDirKey(filepath.Dir(cwd)) == spawnDirKey(config.WorkRoot) {
		keys = append(keys, "repo:"+strings.ToLower(filepath.Base(cwd)))
	}
	return keys
}

// spawnDirKey resolves symlinks in the deepest existing ancestor, so a dir
// not yet created keys the same as it will once it exists, and folds case for
// case-insensitive file systems.
func spawnDirKey(path string) string {
	path = filepath.Clean(path)
	var rest []string
	for {
		if resolved, err := filepath.EvalSymlinks(path); err == nil {
			path = resolved
			break
		}
		parent := filepath.Dir(path)
		if parent == path {
			break
		}
		rest = append([]string{filepath.Base(path)}, rest...)
		path = parent
	}
	return strings.ToLower(filepath.Join(append([]string{path}, rest...)...))
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

// spawnLock returns the one-slot channel for key; the map only grows by the
// set of worktree and repository names this node has spawned into.
func (client *HubClient) spawnLock(key string) chan struct{} {
	client.spawnLocksMu.Lock()
	defer client.spawnLocksMu.Unlock()
	if client.spawnLocks == nil {
		client.spawnLocks = map[string]chan struct{}{}
	}
	lock := client.spawnLocks[key]
	if lock == nil {
		lock = make(chan struct{}, 1)
		client.spawnLocks[key] = lock
	}
	return lock
}

// lockSpawnPrepare serializes preparation within one clone; false means the
// run ended while waiting.
func (client *HubClient) lockSpawnPrepare(ctx context.Context, checkout *hubSpawnCheckout) (func(), bool) {
	lock := client.spawnLock(spawnPrepareKey(checkout))
	select {
	case lock <- struct{}{}:
		return func() { <-lock }, true
	case <-ctx.Done():
		return nil, false
	}
}

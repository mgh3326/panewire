package panewire

import (
	"context"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Per-node spawn concurrency (hk 4578). A node runs up to N spawns at once.
// When an operator opts in, these rules decide what may overlap:
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
// Default-off (panewire-design decision on #613, hk 4578): N is 1 unless
// spawn.json sets spawn_concurrency above 1, as of the node's last hello; an
// unreadable spawn.json gives 1. Two steps are not proven safe for N>1 and are
// the boundary of this opt-in (hk 4821): herdr workspace create/resolve across
// processes and versions, and worktree identity between admission and
// prepare (docs/config.md). The node advertises N in hello only when it is
// above one; the hub dispatches more than one at a time only to such a node,
// and only checkout spawns into distinct dirs
// (decision 254: an older node or hub stays at one).

const (
	hubCapabilitySpawnSlotsPrefix = "spawn_slots_v1="
	nodeSpawnSlotsMax             = 8
)

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
	if client.spawnSlotsAtStart != 0 {
		return client.spawnSlotsAtStart
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
	return 1
}

// helloCapabilities makes the configured slot count the one this node
// enforces and, only when it is above one (an explicit opt-in), appends it to
// a spawn-capable hello; otherwise the hub keeps one-at-a-time dispatch.
func (client *HubClient) helloCapabilities(base []string) []string {
	if len(base) == 0 {
		return base
	}
	slots := client.configuredSpawnSlots()
	if client.spawnSlotsOverride != 0 || client.spawnSlotsAdvertised.Load() == 0 && client.spawnSlotsAtStart != 0 {
		slots = client.nodeSpawnSlots()
	}
	client.spawnSlotsAdvertised.Store(int32(slots))
	client.nodeAdmission().wake()
	if slots <= 1 {
		return base
	}
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

// spawnTarget is the checkout this node admitted, and everything prepare checks
// before its first git write (hk 6402). It is built at admission and handed to
// prepare in process; no wire message carries it, so no caller can name a root,
// a directory or a key (nodeWorktreePrepareArgs is the wire shape).
//
// Root is filled at the target's first prepare (pinWorkRoot, §3.5 of the
// design): the root a prepare pins is the one every path check in that run
// compares against, instead of a fresh read of spawn.json.
type spawnTarget struct {
	Identity   fleet.SpawnIdentity
	Dir        string
	Key        string
	Root       rootPin
	admission  *nodeSpawnAdmission
	requestID  string
	recordRoot func(context.Context, string) error
}

// rootPin is the work root a node prepares under: Configured is the cleaned
// `work_root` spawn.json named (it may be a link), Real is that path with every
// link resolved, and Dev/Ino identify the real directory. A pin is replaced
// when the admission gate is idle (hk 6403).
type rootPin struct {
	Configured string
	Real       string
	Dev        uint64
	Ino        uint64
}

// spawnTargetFor is the admission target of a spawn: nil when the spawn runs in
// a cwd_map or default repo directory (and so is admitted alone), the request's
// job dir and overlap key otherwise. The key is a pure function of the request
// — the same at hub and node — so a link at the job dir or the lane no longer
// changes admission (hk 6402); prepare refuses the link instead, at every N.
func (client *HubClient) spawnTargetFor(message hubOutboundMessage) *spawnTarget {
	if message.Checkout == nil || message.Checkout.Dir == "" {
		return nil
	}
	return &spawnTarget{
		Identity:  hubSpawnIdentity(message.Args),
		requestID: message.RequestID,
		Dir:       message.Checkout.Dir,
		Key:       spawnOverlapKey(message.Checkout.Dir, message.Args),
	}
}

// spawnOverlapKey holds the case-folded job dir and job id: two job ids that
// differ only in case share one jobs-inbox directory on a case-insensitive
// volume (brief-delivered.md, events), so they never land at once. Parts are
// joined by NUL; a key conflicts with another that shares any part.
func spawnOverlapKey(dir string, args []string) string {
	key := "dir:" + strings.ToLower(filepath.Clean(dir))
	// The effective job id, as the launcher reads it: --job, else the -l label.
	if job := hubSpawnIdentity(args).Job; job != "" {
		key += "\x00job:" + strings.ToLower(job)
	}
	return key
}

func spawnOverlapConflicts(a, b string) bool {
	for _, part := range strings.Split(a, "\x00") {
		if slices.Contains(strings.Split(b, "\x00"), part) {
			return true
		}
	}
	return false
}

// hubSpawnOverlapKey is the hub's view of the same key.
func hubSpawnOverlapKey(request *hubSpawnRequest) string {
	if request == nil || request.Checkout == nil || request.Checkout.Dir == "" {
		return ""
	}
	return spawnOverlapKey(request.Checkout.Dir, request.Args)
}

// nodeSpawnAdmission is the node's process-local spawn admission state.
type nodeSpawnAdmission struct {
	mu               sync.Mutex
	running          int
	exclusive        bool
	exclusiveWaiting int
	dirs             map[string]string
	changed          chan struct{}
	root             rootPin
	pinning          bool
	now              func() time.Time
	seeding          bool
	seedFailedUntil  time.Time
	seeded           map[string]seededSpawnHold
}

func (client *HubClient) nodeAdmission() *nodeSpawnAdmission {
	client.spawnAdmissionOnce.Do(func() {
		client.spawnAdmission = &nodeSpawnAdmission{dirs: map[string]string{}, changed: make(chan struct{}), now: client.nowUTC}
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
	release, changed, _, _ := a.tryAdmitTarget(key, limit, nil, nil)
	return release, changed
}

// tryAdmitTarget compares and binds the root under the same lock as the hold.
// Until the first prepare pins a root, another checkout cannot claim a slot.
func (a *nodeSpawnAdmission) tryAdmitTarget(key string, limit int, target *spawnTarget, readRoot func() (string, error)) (func(), <-chan struct{}, bool, time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.seeding {
		return nil, a.changed, false, 0
	}
	seedBlocked, recheck := a.seededAdmission(key)
	var resolvedPin rootPin
	if target != nil && (limit > 1 && a.root != (rootPin{}) || a.hasSeededRoot()) {
		// A duplicate of an admitted request must wait for its claim or
		// result even if the root changes in the meantime.
		if target.requestID != "" && a.dirs[key] == target.requestID {
			return nil, a.changed, false, recheck
		}
		configured, err := readRoot()
		if err != nil || (a.root != (rootPin{}) && configured != a.root.Configured) {
			return nil, nil, true, 0
		}
		current, code := pinWorkRoot(configured)
		if code != "" || (a.root != (rootPin{}) && current != a.root) || !a.matchesSeededRoot(current.Real) {
			return nil, nil, true, 0
		}
		resolvedPin = current
	}
	ok := !a.exclusive && !seedBlocked && a.running < max(limit, 1)
	if key == "" {
		ok = ok && a.running == 0
	} else {
		ok = ok && a.exclusiveWaiting == 0
		for held := range a.dirs {
			ok = ok && !spawnOverlapConflicts(held, key)
		}
	}
	if !ok || (target != nil && a.pinning) {
		return nil, a.changed, false, recheck
	}
	if target != nil {
		if resolvedPin != (rootPin{}) {
			a.root = resolvedPin
		}
		target.Root = a.root
		if a.root == (rootPin{}) {
			a.pinning = true
			target.admission = a
		}
	}
	a.running++
	if key == "" {
		a.exclusive = true
	} else {
		requestID := ""
		if target != nil {
			requestID = target.requestID
		}
		a.dirs[key] = requestID
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.running--
			a.clearIdleRoot()
			if key == "" {
				a.exclusive = false
			} else {
				delete(a.dirs, key)
			}
			a.wakeLocked()
		})
	}, nil, false, 0
}

// clearIdleRoot runs under mu after admission releases its last live or
// seeded root holder. A root verified for a seeded hold stays pinned between
// otherwise idle spawns until that hold ends.
func (a *nodeSpawnAdmission) clearIdleRoot() {
	if a.running == 0 && !a.hasSeededRoot() {
		a.root = rootPin{}
		a.pinning = false
	}
}

func (a *nodeSpawnAdmission) finishRootPin(pin rootPin) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.root = pin
	a.pinning = false
	a.wakeLocked()
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

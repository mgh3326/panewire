package panewire

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// creditHarness is the binary that spends every credit pool (pacelever's
// CreditHarness): a host without it never claims the account.
const creditHarness = "omp"

// Credit pools are the hand-recorded API credit balances the omp harness
// spends. No host measures them, so their presence has its own publisher: the
// node announces a pool when the harness is installed, the operator has not
// excluded the pool and the shared store carries a live manual credit entry for
// the pool's account. This is pacelever's maybeCreditPresence, the only
// publisher these pools had before pacelever retired (hk 3998, docs/quota.md
// §Credit pool presence).
type creditPresenceSource struct {
	// HostID is announced as the holder of the account; empty skips presence.
	HostID string
	// Now is the clock for freshness and stamps; nil is the wall clock.
	Now func() time.Time
	// SpawnConfigPath is the spawn.json whose handoffkeep_env names the store.
	SpawnConfigPath string
	// StoreTransport is the store's HTTP transport; nil is the default.
	StoreTransport http.RoundTripper
	// Installed reports whether a harness binary is installed on this host; nil
	// looks on PATH and in the fleet's install directories.
	Installed func(name string) bool
	// PresenceEvery is the minimum gap between announcements of one account;
	// zero is the shared default (10 min).
	PresenceEvery time.Duration
	// Pools are the credit pools; nil is fleet.CreditAccountPools().
	Pools []string
	// Account is pool's account key; nil reads this host's config.
	Account func(pool string) (string, bool)
	// Excluded reports the operator's exclude class for pool at now; nil reads
	// pacelever's pool policy.
	Excluded func(pool string, now time.Time) bool

	flowOnce sync.Once
	flow     *shared.Flow
}

func (s *creditPresenceSource) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

// installed is whether this host can spend a credit pool's balance. It looks
// where the fleet installs its tools, because a daemon's PATH can be narrower
// than an operator shell's (nodeReadinessDeps.find).
func (s *creditPresenceSource) installed() bool {
	if s.Installed != nil {
		return s.Installed(creditHarness)
	}
	_, ok := nodeReadinessDeps{}.find(creditHarness)
	return ok
}

func (s *creditPresenceSource) pools() []string {
	if s.Pools != nil {
		return s.Pools
	}
	return fleet.CreditAccountPools()
}

func (s *creditPresenceSource) account(pool string) (string, bool) {
	if s.Account != nil {
		return s.Account(pool)
	}
	return fleet.CreditAccountKey(pool)
}

func (s *creditPresenceSource) excluded(pool string, now time.Time) bool {
	if s.Excluded != nil {
		return s.Excluded(pool, now)
	}
	return fleet.CreditPoolExcluded(pool, now)
}

// class is the operator's class for pool, for the status line.
func (s *creditPresenceSource) class(pool string, now time.Time) string {
	if s.Excluded != nil {
		if s.Excluded(pool, now) {
			return "exclude"
		}
		return "spend"
	}
	return fleet.CreditPoolClass(pool, now)
}

func (s *creditPresenceSource) presenceFlow() *shared.Flow {
	s.flowOnce.Do(func() {
		s.flow = &shared.Flow{Now: s.Now, HostID: s.HostID, PresenceEvery: s.PresenceEvery}
	})
	return s.flow
}

// Publish announces presence for every credit pool this host holds, at most
// once per presence interval each. It is best effort and never changes a
// reading: a pool without the harness, excluded by the operator, with no
// account key, with no live credit entry or whose store read fails is skipped
// silently, as pacelever skipped it.
func (s *creditPresenceSource) Publish(ctx context.Context) {
	if s.HostID == "" || !s.installed() {
		return
	}
	store, err := quotaStoreClient(s.SpawnConfigPath, s.StoreTransport)
	if err != nil {
		return
	}
	now := s.now()
	for _, pool := range s.pools() {
		if ctx.Err() != nil {
			return
		}
		if s.excluded(pool, now) {
			continue
		}
		key, ok := s.account(pool)
		if !ok {
			continue
		}
		latest, err := store.Latest(ctx, key)
		if err != nil {
			continue
		}
		if fleet.CreditStateOf(latest, now) != fleet.CreditLive {
			continue
		}
		s.presenceFlow().Announce(ctx, store, key)
	}
}

// Status is pool's `quota status` line: the operator's class, the manual credit
// entry's state and this host's presence for the account, so an operator can
// see why a credit rung is or is not placeable here. ok is false when this host
// does not publish the pool (the harness is absent), so status lists a credit
// pool only where the node would announce it.
func (s *creditPresenceSource) Status(ctx context.Context, pool string) (quotaReading, bool) {
	reading, _, ok := s.statusWindow(ctx, pool)
	return reading, ok
}

// statusWindow is Status plus the newest manual credit entry of the account,
// which the text table prints as the pool's credit row. The window is zero
// when the store holds no well-formed credit entry.
func (s *creditPresenceSource) statusWindow(ctx context.Context, pool string) (quotaReading, fleet.CreditWindow, bool) {
	now := s.now()
	if !s.installed() {
		return quotaReading{}, fleet.CreditWindow{}, false
	}
	reading := quotaReading{Pool: pool, Source: "unavailable", Stale: true, Buckets: []quotaBucket{}, Class: s.class(pool, now), Credit: "unknown", Presence: "unknown"}
	key, ok := s.account(pool)
	if !ok {
		return reading, fleet.CreditWindow{}, true
	}
	_, reading.Account, _ = strings.Cut(key, ":")
	store, err := quotaStoreClient(s.SpawnConfigPath, s.StoreTransport)
	if err != nil {
		return reading, fleet.CreditWindow{}, true
	}
	latest, err := store.Latest(ctx, key)
	if err != nil {
		return reading, fleet.CreditWindow{}, true
	}
	state, recorded := fleet.CreditEntryOf(latest, now)
	reading.Credit, reading.Source = string(state), "manual"
	reading.Stale = state != fleet.CreditLive
	if !recorded.IsZero() {
		age := max(now.Sub(recorded).Seconds(), 0)
		reading.AgeSeconds = &age
	}
	if s.HostID != "" {
		if records, err := store.Presence(ctx); err == nil {
			reading.Presence = "absent"
			for _, record := range records {
				if record.HostID == s.HostID && record.AccountKey == key && !record.LastSeen.After(now.Add(time.Minute)) && now.Sub(record.LastSeen) <= fleet.HostPresenceTTL() {
					reading.Presence = "present"
				}
			}
		}
	}
	window, _ := fleet.CreditWindowOf(latest)
	return reading, window, true
}

// creditPresenceHostID is the id a credit presence record carries: the id
// placement reads the local host's records under (`[local].presence_id`, else
// the fleet machine id), else fallback. Publishing under any other id would
// leave the record invisible to placement and to `quota status`.
func creditPresenceHostID(fallback string) string {
	if id := fleet.HostPresenceID(); id != "" {
		return id
	}
	return fallback
}

// creditPresencePublisher is the node's credit presence publisher, built on
// first use from the client's clock, machine id, spawn.json and tool lookup. A
// fixture sets it first.
func (client *HubClient) creditPresencePublisher() *creditPresenceSource {
	client.creditPresenceMu.Lock()
	defer client.creditPresenceMu.Unlock()
	if client.creditPresence == nil {
		source := &creditPresenceSource{SpawnConfigPath: client.spawnConfigPath, Now: client.nowUTC,
			Installed: func(name string) bool { _, ok := client.readinessDeps.find(name); return ok }}
		if id := creditPresenceHostID(client.machineID); quotaPresenceHostPattern.MatchString(id) {
			source.HostID = id
		}
		client.creditPresence = source
	}
	return client.creditPresence
}

// quotaCLICreditSource builds `quota status`'s credit presence view: this
// host's spawn.json for the store and its presence id, the id placement reads
// the local host's records under. Tests replace it.
var quotaCLICreditSource = func() *creditPresenceSource {
	source := &creditPresenceSource{SpawnConfigPath: defaultHubSpawnConfigPath("")}
	id := creditPresenceHostID(fleetCensusMachineID(fleetCensusOptions{}))
	if quotaPresenceHostPattern.MatchString(id) {
		source.HostID = id
	}
	return source
}

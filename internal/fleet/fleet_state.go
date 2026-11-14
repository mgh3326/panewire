package fleet

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// Fleet state (panewire docs/fleet-state.md): nodes publish quota and load to
// the hub, which serves the latest values at GET /v1/state. Placement and the
// quota gate read that snapshot, and fall back to the direct reads (panewire
// place, the shared quota cache and an on-target measurement) only when the
// hub value is absent, stale or its publisher is dead. Every read records
// which source answered.

const (
	stateSourceHub    = "hub"
	stateSourceDirect = "direct"

	// Reasons a hub value was not used.
	stateOff           = "hub_state_off"
	stateUnavailable   = "hub_unavailable"
	stateAbsent        = "hub_absent"
	statePublisherDead = "publisher_dead"
	stateStale         = "hub_stale"
	stateIncomplete    = "hub_incomplete"

	// stateSnapshotMaxAge bounds how long one panewire run reuses a snapshot;
	// a placement that waited for a wake reads a new one.
	stateSnapshotMaxAge   = 15 * time.Second
	defaultPublisherTTL   = 90 * time.Second
	stateSnapshotTimeout  = 5 * time.Second
	stateSnapshotMaxBytes = 1 << 20
)

// StateUse records where one placement input came from.
type StateUse struct {
	Source string `json:"source"`
	Reason string `json:"reason,omitempty"`
}

// FleetStateReader reads the hub's fleet state snapshot. Tests replace it.
type FleetStateReader interface {
	Snapshot(ctx context.Context) (FleetSnapshot, error)
}

// FleetSnapshot is the subset of GET /v1/state panewire reads.
type FleetSnapshot struct {
	V       int                  `json:"v"`
	At      time.Time            `json:"at"`
	States  []FleetStateMessage  `json:"states"`
	Cooling map[string]time.Time `json:"pool_cooling,omitempty"`
}

// cools reports whether a candidate row's quota is held by a stop: a stop of
// its catalog pool holds every rung of that pool, and a stop of a reserved band
// key holds that band alone. Devin's bands hold apart, the way its cap and
// --exclude-pool keys read them: a paid weekly stop leaves the SWE-2 free rungs
// eligible and a free-band stop leaves the paid rungs eligible (hk 4582).
func (s *FleetSnapshot) cools(row CatalogRow, now time.Time) bool {
	if now.Before(s.Cooling[row.Pool]) {
		return true
	}
	if row.Pool != "devin" {
		return false
	}
	if IsFreeRung(row.Profile) {
		return now.Before(s.Cooling[devinFreeCapKey])
	}
	return now.Before(s.Cooling[devinPaidKey])
}

type FleetStateMessage struct {
	Host        string     `json:"host"`
	Kind        string     `json:"kind"`
	Key         string     `json:"key"`
	MeasuredAt  time.Time  `json:"measured_at"`
	ConfirmedAt *time.Time `json:"confirmed_at,omitempty"`
	Quota       *struct {
		AccountKey string             `json:"account_key"`
		Buckets    []accountBucket    `json:"buckets"`
		FreeBucket *shared.FreeBucket `json:"free_bucket,omitempty"`
	} `json:"quota,omitempty"`
	Load *struct {
		LoadRatio float64 `json:"load_ratio"`
		Score     float64 `json:"score"`
		Connected bool    `json:"connected"`
		SlotFull  bool    `json:"slot_full"`
		Reason    string  `json:"reason"`
		Source    string  `json:"source"`
		// Usable is the hub's own verdict for the host (placementUsable).
		Usable softBool `json:"usable,omitempty"`
		PlacementSignals
		Signals *hubSignals `json:"signals,omitempty"`
	} `json:"load,omitempty"`
	Publisher *struct {
		Connected bool      `json:"connected"`
		LastSeen  time.Time `json:"last_seen"`
	} `json:"publisher,omitempty"`
	Readiness *ReadinessState `json:"readiness,omitempty"`
}

// fleetStateLog is one run's record of the sources it used.
type fleetStateLog struct {
	snapshot  *FleetSnapshot
	err       error
	fetchedAt time.Time
	hosts     *StateUse
	quota     map[string]StateUse
	// slotFull names the hosts hubHosts left out because the hub reported
	// them slot-full. Only the placement shadow reads it (placement_shadow.go);
	// the live host list is unchanged.
	slotFull map[string]bool
}

// hubStateHTTP is the production reader: the operator credential from [hub],
// the same client rules as the hub spawn transport.
type hubStateHTTP struct{ a *App }

func (r hubStateHTTP) Snapshot(ctx context.Context) (FleetSnapshot, error) {
	c, e := newHubHTTP(r.a.Config.Hub)
	if e != nil {
		return FleetSnapshot{}, e
	}
	status, data, e := c.do(ctx, http.MethodGet, "/v1/state", nil, stateSnapshotTimeout)
	if e != nil {
		return FleetSnapshot{}, e
	}
	if status != http.StatusOK {
		return FleetSnapshot{}, classify(status, data)
	}
	var snap FleetSnapshot
	if len(data) > stateSnapshotMaxBytes || json.Unmarshal(data, &snap) != nil || snap.V != 1 || snap.At.IsZero() {
		return FleetSnapshot{}, &HubError{Kind: hubDown, Status: status}
	}
	return snap, nil
}

// fleetSnapshot answers the run's current snapshot, reading it at most once
// per stateSnapshotMaxAge. A failed read is remembered for the same window.
func (a *App) fleetSnapshot(ctx context.Context) (*FleetSnapshot, string) {
	if !a.fleetStateEnabled() {
		return nil, stateOff
	}
	if a.fleetLog == nil {
		a.fleetLog = &fleetStateLog{quota: map[string]StateUse{}}
	}
	log, now := a.fleetLog, a.Now()
	if log.fetchedAt.IsZero() || now.Sub(log.fetchedAt) >= stateSnapshotMaxAge {
		reader := a.State
		if reader == nil {
			reader = hubStateHTTP{a}
		}
		snap, e := reader.Snapshot(ctx)
		log.fetchedAt, log.err, log.snapshot = now, e, nil
		if e == nil {
			log.snapshot = &snap
		}
	}
	if log.snapshot == nil {
		return nil, stateUnavailable
	}
	return log.snapshot, ""
}

// fleetStateEnabled is [hub] state: on unless set to "off" (the rollback
// switch), and only with a hub URL configured.
func (a *App) fleetStateEnabled() bool {
	return !strings.EqualFold(str(a.Config.Hub, "state"), "off") && (a.State != nil || str(a.Config.Hub, "hub_url") != "")
}

func (a *App) publisherTTL() time.Duration {
	if n := integer(a.Config.Hub, "state_publisher_ttl_s", 0); n > 0 {
		return time.Duration(n) * time.Second
	}
	return defaultPublisherTTL
}

// publisherAlive applies the freshness rule's first half: the host's latest
// publisher value says connected and its last heartbeat is within the TTL of
// the snapshot time (the hub's clock, so panewire's skew does not matter).
func (a *App) publisherAlive(snap *FleetSnapshot, host string) bool {
	for _, m := range snap.States {
		if m.Kind == "publisher" && m.Host == host && m.Publisher != nil {
			return m.Publisher.Connected && snap.At.Sub(m.Publisher.LastSeen) <= a.publisherTTL()
		}
	}
	return false
}

// hubSharedAccount answers a key's reading from the hub when an alive
// publisher holds a fresh one, the freshest across publishers; otherwise the
// reason it cannot.
func (a *App) hubSharedAccount(ctx context.Context, key string) (SharedAccount, string) {
	snap, why := a.fleetSnapshot(ctx)
	if snap == nil {
		return SharedAccount{}, why
	}
	now, ttl := a.Now(), a.sharedFreshTTL(accountPool(key))
	reason := stateAbsent
	var best *FleetStateMessage
	for i, m := range snap.States {
		if m.Kind != "quota" || m.Key != key || m.Quota == nil || m.Quota.AccountKey != key {
			continue
		}
		if !a.publisherAlive(snap, m.Host) {
			if reason == stateAbsent {
				reason = statePublisherDead
			}
			continue
		}
		if age := now.Sub(m.validAt()); age < -time.Minute || age > ttl {
			reason = stateStale
			continue
		}
		if best == nil || m.validAt().After(best.validAt()) {
			best = &snap.States[i]
		}
	}
	if best == nil {
		return SharedAccount{}, reason
	}
	quota := withFree(accountPoolView(best.Quota.Buckets, now), best.Quota.FreeBucket, now)
	if quota == nil {
		return SharedAccount{}, stateStale
	}
	return SharedAccount{Key: key, Observed: true, Fresh: true, MeasuredAt: best.validAt(), Quota: quota}, ""
}

// validAt is when a quota reading was last known current: the later of its
// measurement and the shared store's re-confirmation of the same values, the
// rule the direct read uses (fleet.go sharedAccount).
func (m FleetStateMessage) validAt() time.Time {
	if m.ConfirmedAt != nil && m.ConfirmedAt.After(m.MeasuredAt) {
		return *m.ConfirmedAt
	}
	return m.MeasuredAt
}

// hubHosts answers the host list from the hub's load values. It is used only
// when every placement host is connected with an alive publisher and a fresh
// value: any other case falls back to `panewire place`, whose placement call
// is also what triggers the hub's wake-on-spill for a sleeping host.
func (a *App) hubHosts(ctx context.Context) ([]HostView, string) {
	// The slot-full names belong to one successful read of the host list: drop
	// the previous read's before trying another.
	if a.fleetLog != nil {
		a.fleetLog.slotFull = nil
	}
	snap, why := a.fleetSnapshot(ctx)
	if snap == nil {
		return nil, why
	}
	var out []HostView
	slotFull := map[string]bool{}
	for _, m := range snap.States {
		if m.Kind != "load" || m.Load == nil {
			continue
		}
		switch {
		case !m.Load.Connected || !a.publisherAlive(snap, m.Host):
			return nil, statePublisherDead
		case snap.At.Sub(m.MeasuredAt) > a.publisherTTL():
			return nil, stateStale
		case !validID(m.Host):
			return nil, stateIncomplete
		}
		if m.Load.SlotFull {
			// /v1/placement drops a slot-full machine from its candidates.
			slotFull[m.Host] = true
			continue
		}
		hv := HostView{Name: m.Host, Connected: true, Reason: m.Load.Reason, Signals: m.Load.PlacementSignals, Source: m.Load.Source}
		hv.Extra = HostExtra{Usable: m.Load.Usable.v, MeasuredAt: m.MeasuredAt}
		if m.Load.Signals != nil {
			hv.Signals = m.Load.Signals.PlacementSignals
			hv.Extra.CPUBusy1m, hv.Extra.SwapInLinux = m.Load.Signals.CPUBusy1m.v, m.Load.Signals.SwapInLinux.true()
		}
		score := m.Load.Score
		hv.Score = &score
		if m.Load.LoadRatio != 0 {
			// /v1/placement omits a zero load_ratio; keep that reading.
			ratio := m.Load.LoadRatio
			hv.LoadRatio = &ratio
		}
		out = append(out, hv)
	}
	// Kept even when every host is slot-full (out is nil and the live path
	// falls back to the direct read, which omits them too).
	a.fleetLog.slotFull = slotFull
	if out == nil {
		return nil, stateAbsent
	}
	sort.SliceStable(out, func(i, j int) bool { return *out[i].Score > *out[j].Score })
	return out, ""
}

// stateSource is the production PlacementSource: the hub's fleet state first,
// the direct reads of inner otherwise.
type stateSource struct {
	a     *App
	inner PlacementSource
}

func (s stateSource) Hosts(ctx context.Context, cwd string) ([]HostView, error) {
	views, why := s.a.hubHosts(ctx)
	use := StateUse{Source: stateSourceHub}
	var e error
	if why != "" {
		use = StateUse{Source: stateSourceDirect, Reason: why}
		views, e = s.inner.Hosts(ctx, cwd)
	}
	if s.a.fleetLog == nil {
		s.a.fleetLog = &fleetStateLog{quota: map[string]StateUse{}}
	}
	s.a.fleetLog.hosts = &use
	return views, e
}

func (s stateSource) Presence(ctx context.Context) ([]PresenceRecord, error) {
	return s.inner.Presence(ctx)
}

func (s stateSource) SharedAccount(ctx context.Context, key string) (SharedAccount, error) {
	acc, why := s.a.hubSharedAccount(ctx, key)
	use := StateUse{Source: stateSourceHub}
	var e error
	if why != "" {
		use = StateUse{Source: stateSourceDirect, Reason: why}
		acc, e = s.inner.SharedAccount(ctx, key)
	}
	acc.State = &use
	if s.a.fleetLog == nil {
		s.a.fleetLog = &fleetStateLog{quota: map[string]StateUse{}}
	}
	// The first read of a key decides how its gate was judged; a re-read
	// after a direct measurement must not overwrite that.
	if _, seen := s.a.fleetLog.quota[key]; !seen {
		s.a.fleetLog.quota[key] = use
	}
	return acc, e
}

// fleetStateRecord is the `state` object of the placement and selection
// records: the source of the host list and of each account's reading.
func (a *App) fleetStateRecord() map[string]any {
	if a.fleetLog == nil || (a.fleetLog.hosts == nil && len(a.fleetLog.quota) == 0) {
		return nil
	}
	rec := map[string]any{}
	if a.fleetLog.hosts != nil {
		rec["hosts"] = *a.fleetLog.hosts
	}
	if len(a.fleetLog.quota) > 0 {
		quota := map[string]StateUse{}
		for k, v := range a.fleetLog.quota {
			quota[k] = v
		}
		rec["quota"] = quota
	}
	return rec
}

// readinessStates answers each host's readiness from the hub's fleet state.
// It applies the freshness rule of the load value: the publisher is alive and
// the value is within the publisher TTL. A host whose value fails it is left
// out, which the filter reads as today's behaviour (no readiness drop).
func (a *App) readinessStates(ctx context.Context) map[string]ReadinessState {
	snap, _ := a.fleetSnapshot(ctx)
	if snap == nil {
		return nil
	}
	out := map[string]ReadinessState{}
	for _, m := range snap.States {
		if m.Kind != "readiness" || m.Readiness == nil || !a.publisherAlive(snap, m.Host) || snap.At.Sub(m.MeasuredAt) > a.publisherTTL() {
			continue
		}
		out[m.Host] = *m.Readiness
	}
	return out
}

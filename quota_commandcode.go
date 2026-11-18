package panewire

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/quota"
	"github.com/mgh3326/panewire/internal/quota/commandcode"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// The commandcode pool is read from omp (docs/quota.md §Native measurement:
// commandcode). omp may hold several Command Code accounts, so this pool alone
// has more than one account key per host: one `omp usage` call lists them all,
// and each is published, announced and judged under its own key
// `commandcode:<fp>`.

// commandcodeState is what the pool remembers between refreshes: the account
// fingerprints omp listed at its last good read and when (a hint for the status
// table and the login probe, never an input to a refresh), and the failure
// reasons already logged. pass serializes refreshes: a refresh's effects (the
// shared flow's presence, lease and publication) all derive from one omp
// snapshot, and two refreshes never interleave their effects.
type commandcodeState struct {
	pass    sync.Mutex
	mu      sync.Mutex
	known   []string
	knownAt time.Time
	warned  map[string]bool
}

// inventory is the accounts omp listed at the last good read, or nil when that
// read is older than MaxReportAge (or there was none or it failed).
func (st *commandcodeState) inventory(now time.Time) []string {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.knownAt.IsZero() || now.Sub(st.knownAt) > commandcode.MaxReportAge || now.Before(st.knownAt) {
		return nil
	}
	return slices.Clone(st.known)
}

func (st *commandcodeState) knownFPs() []string {
	st.mu.Lock()
	defer st.mu.Unlock()
	return slices.Clone(st.known)
}

// measureCommandcode lists the accounts omp holds. A failure is logged once per
// distinct reason until a read succeeds again, so a host whose omp output this
// build does not know says so without repeating every interval; "no omp, or no
// account" is a host with no Command Code login, not a fault, and is not logged.
func (s *nativeQuotaSource) measureCommandcode(ctx context.Context) ([]commandcode.Account, error) {
	config := s.Commandcode
	config.Now = s.Now
	accounts, err := commandcode.Measure(ctx, config)
	st := &s.commandcodeState
	st.mu.Lock()
	defer st.mu.Unlock()
	if err != nil {
		// Whatever the failure, the last list no longer vouches for any account.
		st.known, st.knownAt = nil, time.Time{}
		if s.Warn != nil && ctx.Err() == nil && quota.KindOf(err) != quota.ErrKindCredentials && !st.warned[err.Error()] {
			if st.warned == nil {
				st.warned = map[string]bool{}
			}
			st.warned[err.Error()] = true
			s.Warn("quota commandcode: " + err.Error() + "; the pool is not admitted from this host until omp's usage output is readable")
		}
		return nil, err
	}
	clear(st.warned)
	st.knownAt = s.now()
	st.known = st.known[:0]
	for _, account := range accounts {
		st.known = append(st.known, account.FP)
	}
	return accounts, nil
}

// commandcodeMeasurer is one account's observation as the shared flow's
// measurer, stamped with the time omp fetched it.
func commandcodeMeasurer(account commandcode.Account) shared.Measurer {
	return func(context.Context) (quota.Observation, error) {
		observed := account.Observation
		observed.AccountFP = account.FP
		return observed, nil
	}
}

// refreshCommandcode is RefreshAccounts for the commandcode pool: the current
// reading of every account omp holds.
//
// The invariant is that every reading returned, presence announced and
// observation published names an account the omp snapshot read by THIS pass
// vouches for (a fresh report of that account). So the pass reads omp first, once,
// and fails whole when it cannot: nothing is returned or announced from a cached
// list, and another host keeping a shared reading current says nothing about this
// host's login. Only then does each account of the snapshot take its own lease
// and publication through the shared flow (reusing a current shared reading
// where one exists, and announcing presence either way); the flow never calls
// omp and the set it works on is fixed. An account's store failure does not hide
// the others; the refresh fails when none has a reading.
func (s *nativeQuotaSource) refreshCommandcode(ctx context.Context) ([]shared.Reading, bool, error) {
	s.commandcodeState.pass.Lock()
	defer s.commandcodeState.pass.Unlock()
	store, storeErr := quotaStoreClient(s.SpawnConfigPath, s.StoreTransport)
	direct := errors.Is(storeErr, errQuotaStoreUnconfigured)
	if storeErr != nil && !direct {
		// Reading omp first or not, a configured store this node cannot use fails
		// the refresh without effects.
		return nil, false, quota.Fail(quota.ErrKindSharedUnavailable, "shared quota unavailable")
	}
	accounts, err := s.measureCommandcode(ctx)
	if err != nil {
		return nil, false, err
	}
	if !direct {
		s.flowOnce.Do(func() {
			s.flow = &shared.Flow{Now: s.Now, FreshTTL: s.refreshTTL(shared.OtherFreshTTL), HostID: s.HostID, Wait: s.Wait, Warn: quotaFlowWarn(s.Warn)}
		})
	}
	stale := quota.Fail(quota.ErrKindProbe, "omp usage report is stale")
	var (
		readings []shared.Reading
		lastErr  error
	)
	for _, account := range accounts {
		// An account's own report decides whether this host vouches for it: not
		// the shared reading, and not the time of the pass.
		witnessed := func() bool { return commandcode.Fresh(s.now(), account.Observation.MeasuredAt) }
		if !witnessed() {
			lastErr = stale
			continue
		}
		var reading shared.Reading
		var err error
		if direct {
			reading, err = s.measureDirect(ctx, commandcodeMeasurer(account))
		} else {
			reading, err = s.flow.Refresh(ctx, store, "commandcode:"+account.FP, func(ctx context.Context) (quota.Observation, error) {
				if !witnessed() {
					return quota.Observation{}, stale
				}
				return commandcodeMeasurer(account)(ctx)
			})
		}
		if err == nil && !witnessed() {
			// A lease wait outlived the report.
			err = stale
		}
		if err != nil {
			lastErr = err
			continue
		}
		// A reading published by a node that still measures through the API
		// carries a 30d bucket; the month is not read (decision 289).
		reading.Buckets = slices.DeleteFunc(slices.Clone(reading.Buckets), func(b shared.Bucket) bool { return !fleet.WindowRead("commandcode", b.Window) })
		readings = append(readings, reading)
	}
	if len(readings) == 0 {
		if lastErr == nil {
			lastErr = quota.Fail(quota.ErrKindAccountUnknown, "no account key")
		}
		return nil, true, lastErr
	}
	slices.SortFunc(readings, func(a, b shared.Reading) int { return strings.Compare(a.Key, b.Key) })
	return readings, true, nil
}

// storedCommandcode is storedAccounts for the commandcode pool: each account this
// host holds, read from the shared store without measuring.
func (s *nativeQuotaSource) storedCommandcode(ctx context.Context) ([]quotaReading, error) {
	store, err := quotaStoreClient(s.SpawnConfigPath, s.StoreTransport)
	if err != nil {
		return nil, quota.Fail(quota.ErrKindSharedUnavailable, "shared quota unavailable")
	}
	// The accounts are the ones the last good omp read listed while that read is
	// current, else the ones omp lists now: never the ones an old presence record
	// or an old list names (a removed account would stay "fresh" through another
	// host's readings).
	fps := s.commandcodeState.inventory(s.now())
	if len(fps) == 0 {
		accounts, err := s.measureCommandcode(ctx)
		if err != nil {
			return nil, err
		}
		for _, account := range accounts {
			fps = append(fps, account.FP)
		}
	}
	readings := make([]quotaReading, 0, len(fps))
	for _, fp := range fps {
		reading, err := s.storedReading(ctx, store, fp, shared.OtherFreshTTL)
		if err != nil {
			return nil, err
		}
		readings = append(readings, reading)
	}
	return readings, nil
}

// commandcodeMeasurableHere says whether this host lists a Command Code account
// omp holds as measurable (`[quota].measurable_accounts`): the login probe's gate.
// It reads the accounts the last good refresh listed (while current), so it asks omp nothing.
func (s *nativeQuotaSource) commandcodeMeasurableHere() bool {
	listed := fleet.HostMeasurableAccount
	if s.MeasurableAccount != nil {
		listed = s.MeasurableAccount
	}
	for _, fp := range s.commandcodeState.inventory(s.now()) {
		if listed("commandcode:" + fp) {
			return true
		}
	}
	return false
}

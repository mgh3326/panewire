package panewire

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"

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
// fingerprints omp listed at its last good read, so a refresh whose shared
// readings are all current asks omp nothing, and the failure reasons already
// logged.
type commandcodeState struct {
	mu     sync.Mutex
	known  []string
	warned map[string]bool
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
		if quota.KindOf(err) == quota.ErrKindCredentials {
			st.known = nil
		}
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
// reading of every account omp holds. With a shared store each account takes its
// own lease and the reading stays current in the store; the one `omp usage` call
// a refresh needs is made only when some account's shared reading is not current.
// An account that fails does not hide the others; the refresh fails when none
// has a reading.
func (s *nativeQuotaSource) refreshCommandcode(ctx context.Context) ([]shared.Reading, bool, error) {
	store, err := quotaStoreClient(s.SpawnConfigPath, s.StoreTransport)
	if errors.Is(err, errQuotaStoreUnconfigured) {
		accounts, err := s.measureCommandcode(ctx)
		if err != nil {
			return nil, false, err
		}
		var readings []shared.Reading
		for _, account := range accounts {
			reading, err := s.measureDirect(ctx, commandcodeMeasurer(account))
			if err != nil {
				return nil, true, err
			}
			readings = append(readings, reading)
		}
		return readings, true, nil
	}
	if err != nil {
		return nil, false, quota.Fail(quota.ErrKindSharedUnavailable, "shared quota unavailable")
	}
	s.flowOnce.Do(func() {
		s.flow = &shared.Flow{Now: s.Now, FreshTTL: s.refreshTTL(shared.OtherFreshTTL), HostID: s.HostID, Wait: s.Wait}
	})
	var (
		listed   bool
		accounts []commandcode.Account
		listErr  error
		measured bool
	)
	list := func(ctx context.Context) ([]commandcode.Account, error) {
		if !listed {
			listed = true
			accounts, listErr = s.measureCommandcode(ctx)
		}
		return accounts, listErr
	}
	fps := s.commandcodeState.knownFPs()
	if len(fps) == 0 {
		found, err := list(ctx)
		if err != nil {
			return nil, false, err
		}
		for _, account := range found {
			fps = append(fps, account.FP)
		}
	}
	var (
		readings []shared.Reading
		lastErr  error
		done     []string
	)
	refresh := func(fp string) {
		done = append(done, fp)
		reading, err := s.flow.Refresh(ctx, store, "commandcode:"+fp, func(ctx context.Context) (quota.Observation, error) {
			found, err := list(ctx)
			if err != nil {
				return quota.Observation{}, err
			}
			for _, account := range found {
				if account.FP == fp {
					measured = true
					return commandcodeMeasurer(account)(ctx)
				}
			}
			// omp no longer holds the account the shared reading was for.
			return quota.Observation{}, quota.Fail(quota.ErrKindAccountUnknown, "omp no longer holds the account")
		})
		if err != nil {
			lastErr = err
			return
		}
		// A reading published by a node that still measures through the API
		// carries a 30d bucket; the month is not read (decision 289).
		reading.Buckets = slices.DeleteFunc(slices.Clone(reading.Buckets), func(b shared.Bucket) bool { return !fleet.WindowRead("commandcode", b.Window) })
		readings = append(readings, reading)
	}
	for _, fp := range fps {
		refresh(fp)
	}
	// An account omp lists that this host did not know yet is refreshed in the same pass.
	for _, account := range accounts {
		if listErr == nil && !slices.Contains(done, account.FP) {
			refresh(account.FP)
		}
	}
	if len(readings) == 0 {
		if lastErr == nil {
			lastErr = quota.Fail(quota.ErrKindAccountUnknown, "no account key")
		}
		return nil, measured, lastErr
	}
	slices.SortFunc(readings, func(a, b shared.Reading) int { return strings.Compare(a.Key, b.Key) })
	return readings, measured, nil
}

// storedCommandcode is storedAccounts for the commandcode pool: each account this
// host is known to hold, read from the shared store without measuring. The
// accounts are the ones the node listed last, else the ones its presence records
// name, else the ones omp lists now.
func (s *nativeQuotaSource) storedCommandcode(ctx context.Context) ([]quotaReading, error) {
	store, err := quotaStoreClient(s.SpawnConfigPath, s.StoreTransport)
	if err != nil {
		return nil, quota.Fail(quota.ErrKindSharedUnavailable, "shared quota unavailable")
	}
	fps := s.commandcodeState.knownFPs()
	if len(fps) == 0 && s.HostID != "" {
		if records, err := store.Presence(ctx); err == nil {
			for _, record := range records {
				if fp, ok := strings.CutPrefix(record.AccountKey, "commandcode:"); ok && record.HostID == s.HostID && !slices.Contains(fps, fp) {
					fps = append(fps, fp)
				}
			}
			slices.Sort(fps)
		}
	}
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
// It reads the accounts the last refresh listed, so it asks omp nothing.
func (s *nativeQuotaSource) commandcodeMeasurableHere() bool {
	listed := fleet.HostMeasurableAccount
	if s.MeasurableAccount != nil {
		listed = s.MeasurableAccount
	}
	for _, fp := range s.commandcodeState.knownFPs() {
		if listed("commandcode:" + fp) {
			return true
		}
	}
	return false
}

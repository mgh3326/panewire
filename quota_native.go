package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/quota"
	"github.com/mgh3326/panewire/internal/quota/agy"
	"github.com/mgh3326/panewire/internal/quota/claude"
	"github.com/mgh3326/panewire/internal/quota/codex"
	"github.com/mgh3326/panewire/internal/quota/commandcode"
	"github.com/mgh3326/panewire/internal/quota/devin"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// The node measures the claude, codex, commandcode, devin and agy pools itself
// (docs/quota.md §Native measurement): each pool's own source (claude: the OAuth
// usage API then one `claude -p /usage`; codex: the app-server then the wham
// usage API; commandcode: the billing, subscription and usage-summary APIs;
// devin: the web quota endpoint then the `/usage` probe; agy: the running app's
// loopback server then the cloud quota summary) and the shared
// quota store's lease/publish flow. Every other pool still goes through
// pacelever until its own change.
//
// claude and codex are always native. commandcode, devin and agy have a switch (docs/quota.md
// §Native measurement: commandcode): it is native only once the host's
// `[quota].native_pools` lists it, and until then it is in shadow — see
// quota_shadow.go.

// isNativeQuotaPool says whether this host measures pool itself. The host
// config is read at call time, so flipping a switch needs no restart.
func isNativeQuotaPool(pool string) bool { return fleet.HostNativeQuotaPool(pool) }

// quotaPresenceHostPattern is the host id alphabet the store's presence
// records accept.
var quotaPresenceHostPattern = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)

// nativeQuotaSource measures one native pool and keeps its shared reading
// current. One value per pool serves a node, so a rate-limit backoff and the
// presence interval persist across the fleet-state loop and the quota.refresh
// op.
type nativeQuotaSource struct {
	// Pool is the pool measured: "claude", "codex", "commandcode", "devin" or
	// "agy".
	Pool string
	// Claude, Codex, Commandcode, Devin and Agy are the credential, HTTP and
	// process environment of a measurement of their pool.
	Claude      claude.Config
	Codex       codex.Config
	Commandcode commandcode.Config
	Devin       devin.Config
	Agy         agy.Config
	// AgyAccount is this host's configured agy account: the fingerprint and
	// whether `[quota].measurable_accounts` lets the host measure it. agy has no
	// login to derive a key from. Nil reads this host's fleet config.
	AgyAccount func() (fp string, measurable bool)
	// MeasurableAccount says whether this host lists an account key in
	// `[quota].measurable_accounts` (the login probe's gate for a pool whose
	// account comes from its login). Nil reads this host's fleet config.
	MeasurableAccount func(key string) bool
	// Warn reports a line to the node's log; nil is silent.
	Warn func(string)
	// SpawnConfigPath is the spawn.json whose handoffkeep_env names the store.
	SpawnConfigPath string
	// HostID is announced as the holder of the account; empty skips presence.
	HostID string
	// Now is the clock for freshness and stamps; nil is the wall clock.
	Now func() time.Time
	// StoreTransport is the store's HTTP transport; nil is the default.
	StoreTransport http.RoundTripper
	// Wait overrides the lease poll interval; fixtures only.
	Wait time.Duration

	flowOnce sync.Once
	flow     *shared.Flow
	// commandcodeAccounts remembers the account behind the login so the
	// fingerprint read before each refresh costs no request once known.
	commandcodeAccounts commandcode.IdentityCache
	// agyUnconfiguredOnce limits the "no agy account here" line to one.
	agyUnconfiguredOnce sync.Once
}

// errQuotaStoreUnconfigured says spawn.json names no handoffkeep_env, so there
// is no shared store: the measurement is taken directly and published to no
// one.
var errQuotaStoreUnconfigured = errors.New("no shared quota store configured")

// quotaStoreClient is the shared store client for spawn.json's handoffkeep_env.
// It reads only that one key, so a node whose spawn is disabled still has a
// store. Unconfigured is errQuotaStoreUnconfigured; any other error means a
// configured store this node cannot reach.
func quotaStoreClient(spawnConfigPath string, transport http.RoundTripper) (shared.Client, error) {
	info, err := os.Lstat(spawnConfigPath)
	if spawnConfigPath == "" || errors.Is(err, os.ErrNotExist) {
		return shared.Client{}, errQuotaStoreUnconfigured
	}
	if err != nil || !info.Mode().IsRegular() || info.Size() > 64<<10 {
		return shared.Client{}, errors.New("spawn.json unreadable")
	}
	contents, err := os.ReadFile(spawnConfigPath)
	if err != nil {
		return shared.Client{}, errors.New("spawn.json unreadable")
	}
	var config struct {
		HandoffkeepEnv string `json:"handoffkeep_env"`
	}
	if json.Unmarshal(contents, &config) != nil {
		return shared.Client{}, errors.New("spawn.json unreadable")
	}
	if config.HandoffkeepEnv == "" {
		return shared.Client{}, errQuotaStoreUnconfigured
	}
	env, err := loadHubHandoffkeepEnv(config.HandoffkeepEnv)
	if err != nil {
		return shared.Client{}, err
	}
	return shared.Client{BaseURL: env.URL, Auth: env.setAuth, HTTP: env.withoutCFRedirects(&http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }})}, nil
}

func (s *nativeQuotaSource) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

// poolMeasurement is what Refresh needs of a pool: how to measure it, the
// account fingerprint the login names before any request (or why the host has
// no login), and how long a shared reading stays current.
func (s *nativeQuotaSource) poolMeasurement() (measure shared.Measurer, accountFP func(context.Context) (string, error), freshTTL time.Duration) {
	switch s.Pool {
	case "commandcode":
		config := s.Commandcode
		config.Now = s.Now
		config.Identities = &s.commandcodeAccounts
		// The account is the one whoami names, so the fingerprint needs a
		// request the first time a login is seen (cached after that).
		return func(ctx context.Context) (quota.Observation, error) { return commandcode.Measure(ctx, config) },
			func(ctx context.Context) (string, error) { return commandcode.AccountFP(ctx, config) }, shared.OtherFreshTTL
	case "agy":
		return s.agyMeasurement()
	case "devin":
		config := s.Devin
		config.Now = s.Now
		return func(ctx context.Context) (quota.Observation, error) { return devin.Measure(ctx, config) },
			func(context.Context) (string, error) {
				// Only a host with a devin login measures; any other host's
				// reason is its missing credentials, never a lease wait.
				if !devin.HasLogin(config) {
					return "", quota.Fail(quota.ErrKindCredentials, "credentials unavailable")
				}
				return devin.AccountFP(config), nil
			}, shared.OtherFreshTTL
	case "codex":
		config := s.Codex
		config.Now = s.Now
		return func(ctx context.Context) (quota.Observation, error) { return codex.Measure(ctx, config) },
			func(context.Context) (string, error) {
				// A logged-out host reports its credentials, not a missing
				// account: the login rule reads the kind.
				if !codex.HasLogin(config) {
					return "", quota.Fail(quota.ErrKindCredentials, "credentials unavailable")
				}
				return codex.AccountFP(config), nil
			}, shared.OtherFreshTTL
	default:
		config := s.Claude
		config.Now = s.Now
		return func(ctx context.Context) (quota.Observation, error) { return claude.Measure(ctx, config) },
			func(context.Context) (string, error) { return claude.AccountFP(config), nil }, shared.DefaultFreshTTL
	}
}

// agyAccount is this host's agy account fingerprint and permission to measure
// it.
func (s *nativeQuotaSource) agyAccount() (string, bool) {
	if s.AgyAccount != nil {
		return s.AgyAccount()
	}
	return fleet.HostQuotaAccount("agy")
}

// agyMeasurable says whether this host holds an agy account it may measure:
// a configured fingerprint that `[quota].measurable_accounts` lists. A host
// with none has a configuration gap, not a login problem (hk 3783).
func (s *nativeQuotaSource) agyMeasurable() bool {
	fp, measurable := s.agyAccount()
	return fp != "" && measurable
}

// agyMeasurement is agy's measurement. Its account key is configured, not
// derived (pacelever's rule): a host with no `[quota.accounts]` agy
// fingerprint, or one `[quota].measurable_accounts` does not list, does not
// measure agy — it logs one line and reports its credentials, as a host with no
// login does. It never guesses a key.
func (s *nativeQuotaSource) agyMeasurement() (shared.Measurer, func(context.Context) (string, error), time.Duration) {
	unavailable := func() error {
		s.agyUnconfiguredOnce.Do(func() {
			if s.Warn != nil {
				s.Warn("quota agy: this host has no measurable agy account (set [quota.accounts].agy and list agy:<fingerprint> in [quota].measurable_accounts); not measuring agy")
			}
		})
		return quota.Fail(quota.ErrKindCredentials, "credentials unavailable")
	}
	config := func() (agy.Config, error) {
		fp, measurable := s.agyAccount()
		if fp == "" || !measurable {
			return agy.Config{}, unavailable()
		}
		config := s.Agy
		config.Now, config.Account = s.Now, fp
		return config, nil
	}
	return func(ctx context.Context) (quota.Observation, error) {
			cfg, err := config()
			if err != nil {
				return quota.Observation{}, err
			}
			return agy.Measure(ctx, cfg)
		}, func(context.Context) (string, error) {
			cfg, err := config()
			return cfg.Account, err
		}, shared.OtherFreshTTL
}

// Refresh answers the pool's current reading: the shared one when fresh, else
// a measurement published under the shared lease. With no shared store
// configured it measures directly. The error is a *quota.Error.
func (s *nativeQuotaSource) Refresh(ctx context.Context) (shared.Reading, error) {
	reading, _, err := s.RefreshWithMeasurement(ctx)
	return reading, err
}

// RefreshWithMeasurement reports whether this call measured successfully with
// this host's credential. A shared fresh reading returns measured=false.
func (s *nativeQuotaSource) RefreshWithMeasurement(ctx context.Context) (shared.Reading, bool, error) {
	measure, accountFP, freshTTL := s.poolMeasurement()
	measured := false
	ownMeasure := func(ctx context.Context) (quota.Observation, error) {
		observation, err := measure(ctx)
		if err == nil {
			measured = true
		}
		return observation, err
	}
	store, err := quotaStoreClient(s.SpawnConfigPath, s.StoreTransport)
	if errors.Is(err, errQuotaStoreUnconfigured) {
		reading, err := s.measureDirect(ctx, ownMeasure)
		return reading, measured, err
	}
	if err != nil {
		return shared.Reading{}, false, quota.Fail(quota.ErrKindSharedUnavailable, "shared quota unavailable")
	}
	fp, err := accountFP(ctx)
	if err != nil {
		return shared.Reading{}, false, err
	}
	if fp == "" {
		return shared.Reading{}, false, quota.Fail(quota.ErrKindAccountUnknown, "no account key")
	}
	s.flowOnce.Do(func() {
		s.flow = &shared.Flow{Now: s.Now, FreshTTL: freshTTL, HostID: s.HostID, Wait: s.Wait}
		if s.Pool == "devin" {
			config := s.Devin
			config.Now = s.Now
			s.flow.LocalFree = devin.LocalFree(config)
			s.flow.MaxWait = devinLeaseWait
		}
	})
	reading, err := s.flow.Refresh(ctx, store, s.Pool+":"+fp, ownMeasure)
	return reading, measured, err
}

// nativeStatusPools are the pools panewire can measure itself, in the order
// `panewire quota status` lists them when pacelever is absent; a pool with a
// switch is listed only once the host's native_pools names it.
var nativeStatusPools = []string{"claude", "codex", "commandcode", "devin", "agy"}

// stored is the pool's reading as the shared store holds it for this host's
// account, read without measuring: no lease, no publication, no presence.
// `panewire quota status` reports it when pacelever is absent. The error is a
// *quota.Error.
func (s *nativeQuotaSource) stored(ctx context.Context) (quotaReading, error) {
	_, accountFP, freshTTL := s.poolMeasurement()
	store, err := quotaStoreClient(s.SpawnConfigPath, s.StoreTransport)
	if err != nil {
		return quotaReading{}, quota.Fail(quota.ErrKindSharedUnavailable, "shared quota unavailable")
	}
	fp, err := accountFP(ctx)
	if err != nil {
		return quotaReading{}, err
	}
	if fp == "" {
		return quotaReading{}, quota.Fail(quota.ErrKindAccountUnknown, "no account key")
	}
	latest, err := store.Latest(ctx, s.Pool+":"+fp)
	if err != nil {
		return quotaReading{}, quota.Fail(quota.ErrKindSharedUnavailable, "shared quota unavailable")
	}
	reading := quotaReading{Pool: s.Pool, Account: fp, Source: quota.SourceHandoffkeep, Stale: true, Buckets: []quotaBucket{}}
	o := latest.Observation
	if o == nil || o.Kind != shared.KindAuto || o.MeasuredAt.IsZero() {
		return reading, nil
	}
	age := max(s.now().Sub(shared.ValidAt(o)).Seconds(), 0)
	reading.AgeSeconds, reading.Stale = &age, age > freshTTL.Seconds()
	for _, b := range o.Buckets {
		used := b.UsedPct
		bucket := quotaBucket{Window: b.Window, UsedPct: &used}
		if b.ResetsAt != nil {
			bucket.ResetsAt = b.ResetsAt.UTC().Format(time.RFC3339Nano)
		}
		bucket.Scope.Kind, bucket.Scope.Name = b.Scope, b.Model
		reading.Buckets = append(reading.Buckets, bucket)
	}
	return reading, nil
}

// devinLeaseWait bounds how long a devin refresh waits for another measurement
// of the account: past it the refresh gives up with shared_pending instead of
// holding a spawn path for two lease windows.
var devinLeaseWait = 15 * time.Second

// measureOnly is one measurement that touches no shared store: no lease, no
// publication, no presence. Shadow mode uses it to compare against pacelever
// without changing what other hosts read.
func (s *nativeQuotaSource) measureOnly(ctx context.Context) (shared.Reading, error) {
	measure, _, _ := s.poolMeasurement()
	return s.measureDirect(ctx, measure)
}

// measureDirect is the unshared measurement: nothing is published, and the
// reading carries the account key only when the login names one.
func (s *nativeQuotaSource) measureDirect(ctx context.Context, measure shared.Measurer) (shared.Reading, error) {
	observed, err := measure(ctx)
	if err != nil {
		return shared.Reading{}, err
	}
	at := s.now()
	stored, err := shared.Convert(observed, at)
	if err != nil {
		return shared.Reading{}, err
	}
	reading := shared.Reading{Source: observed.Source, MeasuredAt: at, Plan: observed.Plan, Buckets: stored.Buckets}
	if observed.AccountFP != "" {
		reading.Key = s.Pool + ":" + observed.AccountFP
	}
	return reading, nil
}

// quotaErrorKind is err's quota error kind, or fleetQuotaErrorKindUnreadable
// for a failure that is not one.
func quotaErrorKind(err error) string {
	if kind := quota.KindOf(err); fleetQuotaErrorKindPattern.MatchString(kind) {
		return kind
	}
	return fleetQuotaErrorKindUnreadable
}

// nativeQuotaSource is the node's own measurement of pool, built on first use
// from the client's clock, machine id and spawn.json. A fixture sets it first.
func (client *HubClient) nativeQuotaSource(pool string) *nativeQuotaSource {
	client.nativeQuotaMu.Lock()
	defer client.nativeQuotaMu.Unlock()
	source := client.nativeQuota[pool]
	if source == nil {
		source = &nativeQuotaSource{Pool: pool, SpawnConfigPath: client.spawnConfigPath, Now: client.nowUTC, Warn: client.warnMessage}
		if quotaPresenceHostPattern.MatchString(client.machineID) {
			source.HostID = client.machineID
		}
		if client.nativeQuota == nil {
			client.nativeQuota = map[string]*nativeQuotaSource{}
		}
		client.nativeQuota[pool] = source
	}
	return source
}

// quotaCLISource builds the CLI's measurement of pool: this host's spawn.json
// for the store, and its node id for presence. Tests replace it.
var quotaCLISource = func(pool string) *nativeQuotaSource {
	source := &nativeQuotaSource{Pool: pool, SpawnConfigPath: defaultHubSpawnConfigPath("")}
	if id := fleetCensusMachineID(fleetCensusOptions{}); quotaPresenceHostPattern.MatchString(id) {
		source.HostID = id
	}
	return source
}

// runNativeQuotaRefresh is `panewire quota refresh <pool>` for a native pool:
// it measures through the same source the node daemon uses and prints the
// reading.
func runNativeQuotaRefresh(ctx context.Context, pool string, out, errOut io.Writer) int {
	source := quotaCLISource(pool)
	ctx, cancel := context.WithTimeout(ctx, fleetQuotaTimeout)
	defer cancel()
	reading, err := source.Refresh(ctx)
	if err != nil {
		_, _ = errOut.Write([]byte("quota refresh: " + quotaErrorKind(err) + "\n"))
		if ctx.Err() != nil {
			return ExitTimeout
		}
		return ExitInternal
	}
	_, account, _ := strings.Cut(reading.Key, ":")
	validAt := reading.MeasuredAt
	if reading.ConfirmedAt != nil && reading.ConfirmedAt.After(validAt) {
		validAt = *reading.ConfirmedAt
	}
	age := max(source.now().Sub(validAt).Seconds(), 0)
	result := quotaReading{Pool: pool, Account: account, Source: reading.Source, AgeSeconds: &age}
	for _, b := range reading.Buckets {
		used := b.UsedPct
		bucket := quotaBucket{Window: b.Window, UsedPct: &used}
		if b.ResetsAt != nil {
			bucket.ResetsAt = b.ResetsAt.UTC().Format(time.RFC3339Nano)
		}
		bucket.Scope.Kind, bucket.Scope.Name = b.Scope, b.Model
		result.Buckets = append(result.Buckets, bucket)
	}
	if result.Buckets == nil {
		result.Buckets = []quotaBucket{}
	}
	_ = json.NewEncoder(out).Encode(result)
	return 0
}

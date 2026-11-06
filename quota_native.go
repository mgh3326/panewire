package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
	"github.com/mgh3326/panewire/internal/quota/claude"
	"github.com/mgh3326/panewire/internal/quota/codex"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// The node measures the claude and codex pools itself (docs/quota.md §Native
// measurement): each pool's own source (claude: the OAuth usage API then one
// `claude -p /usage`; codex: the app-server then the wham usage API) and the
// shared quota store's lease/publish flow. Every other pool still goes through
// pacelever until its own change.

// quotaNativePools are the pools measured in this process.
var quotaNativePools = []string{"claude", "codex"}

func isNativeQuotaPool(pool string) bool { return slices.Contains(quotaNativePools, pool) }

// quotaPresenceHostPattern is the host id alphabet the store's presence
// records accept.
var quotaPresenceHostPattern = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)

// nativeQuotaSource measures one native pool and keeps its shared reading
// current. One value per pool serves a node, so a rate-limit backoff and the
// presence interval persist across the fleet-state loop and the quota.refresh
// op.
type nativeQuotaSource struct {
	// Pool is the pool measured: "claude" or "codex".
	Pool string
	// Claude and Codex are the credential, HTTP and process environment of a
	// measurement of their pool.
	Claude claude.Config
	Codex  codex.Config
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
func (s *nativeQuotaSource) poolMeasurement() (measure shared.Measurer, accountFP func() (string, error), freshTTL time.Duration) {
	switch s.Pool {
	case "codex":
		config := s.Codex
		config.Now = s.Now
		return func(ctx context.Context) (quota.Observation, error) { return codex.Measure(ctx, config) },
			func() (string, error) {
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
			func() (string, error) { return claude.AccountFP(config), nil }, shared.DefaultFreshTTL
	}
}

// Refresh answers the pool's current reading: the shared one when fresh, else
// a measurement published under the shared lease. With no shared store
// configured it measures directly. The error is a *quota.Error.
func (s *nativeQuotaSource) Refresh(ctx context.Context) (shared.Reading, error) {
	measure, accountFP, freshTTL := s.poolMeasurement()
	store, err := quotaStoreClient(s.SpawnConfigPath, s.StoreTransport)
	if errors.Is(err, errQuotaStoreUnconfigured) {
		return s.measureDirect(ctx, measure)
	}
	if err != nil {
		return shared.Reading{}, quota.Fail(quota.ErrKindSharedUnavailable, "shared quota unavailable")
	}
	fp, err := accountFP()
	if err != nil {
		return shared.Reading{}, err
	}
	if fp == "" {
		return shared.Reading{}, quota.Fail(quota.ErrKindAccountUnknown, "no account key")
	}
	s.flowOnce.Do(func() {
		s.flow = &shared.Flow{Now: s.Now, FreshTTL: freshTTL, HostID: s.HostID, Wait: s.Wait}
	})
	return s.flow.Refresh(ctx, store, s.Pool+":"+fp, measure)
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
		source = &nativeQuotaSource{Pool: pool, SpawnConfigPath: client.spawnConfigPath, Now: client.nowUTC}
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

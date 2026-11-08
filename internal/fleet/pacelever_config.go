package fleet

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

// The native launch lookup loads pacelever's config.toml as `pacelever policy
// launch` loads it before it resolves anything (v0.3.28 config.go
// LoadConfig), and refuses the file, exit 2, exactly where pacelever refuses
// it. The wire types below are pacelever's, field for field, because a
// value's TOML type is part of the rule: a string where pacelever decodes a
// number is refused there.
//
// Only the native launch lookup validates this way; the gate and the credit
// pool views keep parseAdmissionPolicy (admission_policy.go).

type paceleverPoolPolicy struct {
	Exploration        paceleverExplorationPolicy `toml:"exploration"`
	Class              string                     `toml:"class"`
	Until              string                     `toml:"until"`
	Note               string                     `toml:"note"`
	Boost              float64                    `toml:"boost"`
	Cutoff             float64                    `toml:"cutoff"`
	Subscribed         *bool                      `toml:"subscribed"`
	CapacityWeight     float64                    `toml:"capacity_weight"`
	PriceUSD           float64                    `toml:"price_usd"`
	Plan               string                     `toml:"plan"`
	FusionBillingFP    string                     `toml:"fusion_billing_fp"`
	InflightReservePct map[string]float64         `toml:"inflight_reserve_pct"`
	CreditMaxAgeS      float64                    `toml:"credit_max_age_s"`
	OnExhaust          string                     `toml:"on_exhaust"`
	Bin                string                     `toml:"bin"`
	OrgID              string                     `toml:"org_id"`
}

type paceleverExplorationPolicy struct {
	Fraction     float64  `toml:"fraction"`
	Enabled      *bool    `toml:"enabled"`
	ExperimentID string   `toml:"experiment_id"`
	Seed         string   `toml:"seed"`
	ModelOrder   []string `toml:"model_order"`
}

// paceleverBenchConfig is pacelever's [bench]. Of its settings only
// catalog_ttl_s and catalog_stale_max_s bear on the launch catalog's
// freshness (bench.go ReadRows); cache_ttl_s is the TTL of the other bench
// scopes, and backend and the plaintext opt-ins choose whether a remote
// catalog is read at all.
type paceleverBenchConfig struct {
	Backend                  string  `toml:"backend"`
	CacheTTLS                float64 `toml:"cache_ttl_s"`
	CatalogTTLS              float64 `toml:"catalog_ttl_s"`
	CatalogStaleMaxS         float64 `toml:"catalog_stale_max_s"`
	AllowPlaintextURL        bool    `toml:"allow_plaintext_url"`
	AllowPlaintextCatalog    bool    `toml:"allow_plaintext_catalog"`
	AllowPlaintextReps       bool    `toml:"allow_plaintext_reps"`
	AllowPlaintextQuotaShare bool    `toml:"allow_plaintext_quota_share"`
}

type paceleverQuotaConfig struct {
	URL                string             `toml:"url"`
	CredentialEnv      string             `toml:"credential_env"`
	HostID             string             `toml:"host_id"`
	NodeEnv            string             `toml:"node_env"`
	FreshTTLS          float64            `toml:"fresh_ttl_s"`
	PoolFreshTTLS      map[string]float64 `toml:"pool_fresh_ttl_s"`
	LeaseTTLS          int                `toml:"lease_ttl_s"`
	WaitS              float64            `toml:"wait_s"`
	PresenceEveryS     float64            `toml:"presence_every_s"`
	Accounts           map[string]string  `toml:"accounts"`
	MeasurableAccounts []string           `toml:"measurable_accounts"`
}

// parsePaceleverConfig is pacelever v0.3.28's LoadConfig over the file's
// bytes; it returns the file's [bench]. Errors withhold the file's values, as
// pacelever's do.
func parsePaceleverConfig(data []byte) (paceleverBenchConfig, error) {
	// config.go:133 the file is TOML.
	var raw map[string]any
	if err := toml.Unmarshal(data, &raw); err != nil {
		return paceleverBenchConfig{}, errors.New("invalid TOML configuration (values withheld)")
	}
	// config.go:146 the sections decode into pacelever's types.
	var w struct {
		Quota    paceleverQuotaConfig      `toml:"quota"`
		Pools    map[string]map[string]any `toml:"pools"`
		Profiles map[string]struct {
			Subscribed *bool `toml:"subscribed"`
		} `toml:"profiles"`
		Bench    paceleverBenchConfig `toml:"bench"`
		Settings map[string]float64   `toml:"settings"`
	}
	if err := toml.Unmarshal(data, &w); err != nil {
		return paceleverBenchConfig{}, errors.New("invalid configuration types (values withheld)")
	}
	// config.go:150 the shared quota settings (shared_quota.go:32).
	if err := validatePaceleverQuota(w.Quota); err != nil {
		return paceleverBenchConfig{}, err
	}
	for name, v := range w.Pools {
		// config.go:157 a pool pacelever does not know is ignored.
		if !slices.Contains(admissionPolicyPools, name) {
			continue
		}
		if err := validatePaceleverPool(name, v); err != nil {
			return paceleverBenchConfig{}, err
		}
	}
	return w.Bench, nil
}

// validatePaceleverPool is LoadConfig's check of one known pool's table
// (config.go:161-188).
func validatePaceleverPool(name string, v map[string]any) error {
	// An unquoted TOML date is read as its text.
	if until, ok := v["until"]; ok {
		v["until"] = fmt.Sprint(until)
	}
	b, err := toml.Marshal(v)
	if err != nil {
		return fmt.Errorf("invalid pool %s", name)
	}
	var p paceleverPoolPolicy
	if err := toml.Unmarshal(b, &p); err != nil {
		return fmt.Errorf("invalid pool %s types (values withheld)", name)
	}
	if p.Class != "" && p.Class != "spend" && p.Class != "preserve" && p.Class != "exclude" {
		return fmt.Errorf("invalid pools.%s.class", name)
	}
	if err := validatePaceleverExploration(p.Exploration); err != nil {
		return fmt.Errorf("invalid pools.%s.exploration: %w", name, err)
	}
	if p.Cutoff < 0 || p.Cutoff > 100 {
		return fmt.Errorf("invalid pools.%s.cutoff", name)
	}
	for window, n := range p.InflightReservePct {
		if n < 0 || n > 100 {
			return fmt.Errorf("invalid pools.%s.inflight_reserve_pct.%s", name, window)
		}
	}
	if p.CreditMaxAgeS < 0 || math.IsNaN(p.CreditMaxAgeS) || math.IsInf(p.CreditMaxAgeS, 0) || p.CreditMaxAgeS > 1e9 {
		return fmt.Errorf("invalid pools.%s.credit_max_age_s", name)
	}
	return nil
}

// validatePaceleverExploration is pacelever's ExplorationPolicy.Validate
// (exploration.go:23).
func validatePaceleverExploration(p paceleverExplorationPolicy) error {
	if math.IsNaN(p.Fraction) || math.IsInf(p.Fraction, 0) || p.Fraction < 0 || p.Fraction > 1 {
		return errors.New("exploration fraction must be in 0..1")
	}
	active := p.Fraction > 0 && (p.Enabled == nil || *p.Enabled)
	if active && (p.ExperimentID == "" || len(p.ExperimentID) > 128 || strings.ContainsAny(p.ExperimentID, "\x00\r\n\t ")) {
		return errors.New("active exploration requires a single-token experiment_id of at most 128 characters")
	}
	if len(p.Seed) > 256 || strings.ContainsAny(p.Seed, "\x00\r\n") {
		return errors.New("invalid exploration seed")
	}
	seen := map[string]bool{}
	for _, profile := range p.ModelOrder {
		if profile == "" || seen[profile] || strings.ContainsAny(profile, "\x00\r\n\t ") {
			return errors.New("exploration model_order requires distinct profile names")
		}
		seen[profile] = true
	}
	return nil
}

var (
	paceleverAccountKey = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}:[0-9a-f]{16}$`)
	paceleverSafeHost   = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
)

// validatePaceleverQuota is pacelever's QuotaConfig.Validate
// (shared_quota.go:32). A host_id passes when pacelever's safeHost (share.go:18)
// keeps it as it is: its characters and at most 64 of them.
func validatePaceleverQuota(q paceleverQuotaConfig) error {
	bad := func(v float64) bool { return math.IsNaN(v) || math.IsInf(v, 0) }
	if bad(q.FreshTTLS) || bad(q.WaitS) || bad(q.PresenceEveryS) || q.FreshTTLS < 0 || q.FreshTTLS > 86400 || q.LeaseTTLS < 0 || q.LeaseTTLS > 120 || q.WaitS < 0 || q.WaitS > 10 || q.PresenceEveryS < 0 || q.PresenceEveryS > 86400 {
		return errors.New("invalid shared quota TTLs")
	}
	if q.HostID != "" && (!paceleverSafeHost.MatchString(q.HostID) || len(q.HostID) > 64) {
		return errors.New("invalid shared quota host_id")
	}
	for pool, ttl := range q.PoolFreshTTLS {
		if !slices.Contains(admissionPolicyPools, pool) || bad(ttl) || ttl <= 0 || ttl > 86400 {
			return errors.New("invalid shared quota TTLs")
		}
	}
	for pool, fp := range q.Accounts {
		if !slices.Contains(admissionPolicyPools, pool) || !accountFP.MatchString(fp) {
			return errors.New("invalid shared quota account fingerprint")
		}
	}
	for _, key := range q.MeasurableAccounts {
		if !paceleverAccountKey.MatchString(key) {
			return errors.New("invalid measurable account key")
		}
	}
	return nil
}

// launchPolicy is what the native launch lookup reads from pacelever's
// config.toml: the pool policy the gate reads, and the catalog freshness
// pacelever's policy launch honours, from one read of a file validated as
// pacelever validates it.
type launchPolicy struct {
	admissionPolicy
	Freshness CatalogFreshness
}

// loadLaunchPolicy reads pacelever's config.toml for the launch lookup. A
// missing file is the default policy; an unreadable file, or one pacelever's
// LoadConfig refuses, is an error.
func loadLaunchPolicy(getenv func(string) string, home string) (launchPolicy, error) {
	data, found, err := readPaceleverConfig(getenv, home)
	if err != nil || !found {
		return launchPolicy{}, err
	}
	bench, err := parsePaceleverConfig(data)
	if err != nil {
		return launchPolicy{}, err
	}
	policy, err := parseAdmissionPolicy(data)
	if err != nil {
		return launchPolicy{}, err
	}
	return launchPolicy{admissionPolicy: policy, Freshness: CatalogFreshness{TTLS: bench.CatalogTTLS, StaleMaxS: bench.CatalogStaleMaxS}}, nil
}

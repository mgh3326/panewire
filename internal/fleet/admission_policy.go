package fleet

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/configpath"
	"github.com/pelletier/go-toml/v2"
)

// admissionPoolPolicy is one pool's operator policy from panewire's config
// file: its class (spend, preserve or exclude, until a date), the cutoff,
// subscription and ranking weights.
type admissionPoolPolicy struct {
	Class, Until       string
	Boost, Cutoff      float64
	CapacityWeight     float64
	Subscribed         *bool
	FusionBillingFP    string
	InflightReservePct map[string]float64
}

// admissionPolicy is the policy the gate judges with. It is read from
// panewire's config.toml.
type admissionPolicy struct {
	Pools    map[string]admissionPoolPolicy
	Profiles map[string]*bool
	Settings map[string]float64
	// Accounts contains the account fingerprint this host is pinned to per pool.
	Accounts map[string]string
}

// admissionPolicyPools are the pools pacelever's config accepts; a table for
// any other pool is ignored, as pacelever ignores it.
var admissionPolicyPools = []string{"claude", "codex", "grok", "kimi", "devin", "kiro", "agy", "clinepass", "commandcode", "anthropic-api", "openai-api"}

// readPanewireConfig checks panewire's path before the pacelever and scopefuel
// paths. Only a missing file falls through; an unreadable file fails closed.
func readPanewireConfig(getenv func(string) string, home string) (data []byte, found bool, err error) {
	paths := configpath.AdmissionCandidates(getenv, home, "config.toml")
	_, variable, explicit := configpath.Explicit(getenv)
	for _, path := range paths {
		data, err = os.ReadFile(path)
		if errors.Is(err, fs.ErrNotExist) {
			if explicit {
				return nil, false, fmt.Errorf("%s %q does not exist", variable, path)
			}
			continue
		}
		if err != nil {
			if explicit {
				return nil, false, fmt.Errorf("cannot read %s %q", variable, path)
			}
			return nil, false, fmt.Errorf("cannot read panewire configuration %q", path)
		}
		return data, true, nil
	}
	return nil, false, nil
}

// loadAdmissionPolicy reads the policy. A missing file is the default policy;
// an unreadable or invalid one is an error, never a guessed policy.
func loadAdmissionPolicy(getenv func(string) string, home string) (admissionPolicy, error) {
	data, found, err := readPanewireConfig(getenv, home)
	if err != nil || !found {
		return admissionPolicy{}, err
	}
	return parseAdmissionPolicy(data)
}

func parseAdmissionPolicy(data []byte) (admissionPolicy, error) {
	var raw struct {
		Pools    map[string]map[string]any `toml:"pools"`
		Profiles map[string]struct {
			Subscribed *bool `toml:"subscribed"`
		} `toml:"profiles"`
		Settings map[string]float64 `toml:"settings"`
		Quota    struct {
			Accounts map[string]string `toml:"accounts"`
		} `toml:"quota"`
	}
	if err := toml.Unmarshal(data, &raw); err != nil {
		return admissionPolicy{}, fmt.Errorf("invalid panewire configuration (values withheld)")
	}
	p := admissionPolicy{Pools: map[string]admissionPoolPolicy{}, Profiles: map[string]*bool{}, Settings: raw.Settings, Accounts: map[string]string{}}
	for pool, fp := range raw.Quota.Accounts {
		if validID(pool) && accountFP.MatchString(fp) {
			p.Accounts[pool] = fp
		}
	}
	for name, profile := range raw.Profiles {
		p.Profiles[name] = profile.Subscribed
	}
	for name, table := range raw.Pools {
		if !slices.Contains(admissionPolicyPools, name) {
			continue
		}
		pool, err := admissionPoolFrom(table)
		if err != nil {
			return admissionPolicy{}, fmt.Errorf("invalid pacelever pools.%s: %v", name, err)
		}
		p.Pools[name] = pool
	}
	return p, nil
}

func admissionPoolFrom(table map[string]any) (admissionPoolPolicy, error) {
	var p admissionPoolPolicy
	number := func(key string) (float64, error) {
		v, ok := table[key]
		if !ok {
			return 0, nil
		}
		n, err := admissionPoolNumber(v)
		if err != nil {
			return 0, fmt.Errorf("%s must be a number", key)
		}
		return n, nil
	}
	var err error
	if p.Boost, err = number("boost"); err != nil {
		return p, err
	}
	if p.Cutoff, err = number("cutoff"); err != nil || p.Cutoff < 0 || p.Cutoff > 100 {
		return p, fmt.Errorf("cutoff must be 0..100")
	}
	if p.CapacityWeight, err = number("capacity_weight"); err != nil {
		return p, err
	}
	if v, ok := table["class"]; ok {
		class, isString := v.(string)
		if !isString || class != "" && class != "spend" && class != "preserve" && class != "exclude" {
			return p, fmt.Errorf("class must be spend, preserve or exclude")
		}
		p.Class = class
	}
	if v, ok := table["until"]; ok {
		// An unquoted TOML date is as valid as a quoted one.
		p.Until = fmt.Sprint(v)
	}
	if v, ok := table["subscribed"]; ok {
		b, isBool := v.(bool)
		if !isBool {
			return p, fmt.Errorf("subscribed must be a boolean")
		}
		p.Subscribed = &b
	}
	if v, ok := table["fusion_billing_fp"]; ok {
		fp, isString := v.(string)
		if !isString {
			return p, fmt.Errorf("fusion_billing_fp must be a string")
		}
		p.FusionBillingFP = fp
	}
	if v, ok := table["inflight_reserve_pct"]; ok {
		windows, isMap := v.(map[string]any)
		if !isMap {
			return p, fmt.Errorf("inflight_reserve_pct must map windows to percentages")
		}
		p.InflightReservePct = map[string]float64{}
		for window, n := range windows {
			reserve, err := admissionPoolNumber(n)
			if err != nil || reserve < 0 || reserve > 100 {
				return p, fmt.Errorf("inflight_reserve_pct.%s must be 0..100", window)
			}
			p.InflightReservePct[window] = reserve
		}
	}
	return p, nil
}

func admissionPoolNumber(v any) (float64, error) {
	switch n := v.(type) {
	case int64:
		return float64(n), nil
	case float64:
		return n, nil
	}
	return 0, fmt.Errorf("not a number")
}

// pool is the pool's effective policy at now: spend unless a class is set and
// its until date has not passed, cutoff 99 and capacity weight 1 by default.
func (p admissionPolicy) pool(name string, now time.Time) admissionPoolPolicy {
	pool := p.Pools[name]
	if pool.Class == "" {
		pool.Class = "spend"
	}
	if pool.Until != "" {
		until, err := time.Parse("2006-01-02", strings.Split(pool.Until, "T")[0])
		if err != nil || now.UTC().Format("2006-01-02") > until.Format("2006-01-02") {
			pool.Class, pool.Boost = "spend", 0
		}
	}
	if pool.Cutoff == 0 {
		pool.Cutoff = 99
	}
	if pool.CapacityWeight <= 0 {
		pool.CapacityWeight = 1
	}
	return pool
}

// subscribed: a profile setting wins over its pool's; both default to on.
func (p admissionPolicy) subscribed(profile, pool string) bool {
	if s := p.Profiles[profile]; s != nil {
		return *s
	}
	if s := p.Pools[pool].Subscribed; s != nil {
		return *s
	}
	return true
}

// paceThreshold is the 7-day pace multiple that promotes other pools'
// alternatives.
func (p admissionPolicy) paceThreshold() float64 {
	for _, k := range []string{"altlane_pace_multiple", "altlane_pace_threshold"} {
		if n := p.Settings[k]; n > 0 {
			return n
		}
	}
	return 1.2
}

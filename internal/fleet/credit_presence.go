package fleet

import (
	"io"
	"os"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// A credit pool has no login to derive an account from and nothing to measure:
// the operator records its balance by hand and the omp harness spends it. A
// host holds the account when the harness is installed and the shared store
// carries a live credit entry for the pool's configured key (pacelever's
// maybeCreditPresence, shared_quota.go@c7894c6:359-400). The node publishes
// presence on that rule so selection can bind a credit rung to the host that
// spends the credit (hk 3998).

// CreditState is the state of an account's operator-recorded credit entry.
type CreditState string

const (
	// CreditLive is an entry for window "credit" that has not expired.
	CreditLive CreditState = "live"
	// CreditExpired is a credit entry whose expiry has passed.
	CreditExpired CreditState = "expired"
	// CreditAbsent is a store with no well-formed credit entry.
	CreditAbsent CreditState = "absent"
)

// CreditAccountPools are the pools whose balance the operator records by hand
// and the omp harness spends: pacelever's CreditPools.
func CreditAccountPools() []string { return creditAccountPools }

// CreditEntryOf is the newest manual credit window of an account: the state at
// now and when the operator recorded it. A zero recorded means the store has
// no well-formed credit entry. It reads the same shape pacelever's
// sharedManual accepted.
func CreditEntryOf(latest shared.Latest, now time.Time) (state CreditState, recorded time.Time) {
	live, found := false, false
	for _, m := range admissionManualFrom(latest.Manual) {
		if m.Window != "credit" {
			continue
		}
		expiry, err := time.Parse(time.RFC3339Nano, m.ExpiresAt)
		if err != nil {
			continue
		}
		measured, err := time.Parse(time.RFC3339Nano, m.MeasuredAt)
		if err != nil {
			continue
		}
		found = true
		if measured.After(recorded) {
			recorded = measured
		}
		if expiry.After(now) {
			live = true
		}
	}
	switch {
	case live:
		return CreditLive, recorded
	case found:
		return CreditExpired, recorded
	}
	return CreditAbsent, recorded
}

// CreditStateOf is CreditEntryOf's state alone.
func CreditStateOf(latest shared.Latest, now time.Time) CreditState {
	state, _ := CreditEntryOf(latest, now)
	return state
}

// CreditAccountKey is the key this host publishes a credit pool's presence
// under and reads its manual entry from: panewire's `[quota].accounts`
// fingerprint for the pool (the one `panewire manual set` writes), else
// pacelever's `[quota.accounts]` (the file pacelever read), so a key published
// in the pacelever era still names the same account. Both are read at call
// time; a missing or invalid fingerprint has no key, and the host never
// guesses one.
func CreditAccountKey(pool string) (string, bool) {
	if config, err := LoadConfig(configPath(io.Discard)); err == nil {
		if fp := quotaAccounts(config.Quota)[pool]; fp != "" {
			return pool + ":" + fp, true
		}
	}
	home, _ := os.UserHomeDir()
	policy, err := loadAdmissionPolicy(os.Getenv, home)
	if err != nil {
		return "", false
	}
	if fp := policy.Accounts[pool]; fp != "" {
		return pool + ":" + fp, true
	}
	return "", false
}

// CreditPoolClass is pool's operator class at now from pacelever's pool
// policy: spend unless the table names spend, preserve or exclude, and spend
// when the policy is unreadable (the gate refuses that separately).
func CreditPoolClass(pool string, now time.Time) string {
	home, _ := os.UserHomeDir()
	policy, err := loadAdmissionPolicy(os.Getenv, home)
	if err != nil {
		return "spend"
	}
	return policy.pool(pool, now).Class
}

// CreditPoolExcluded reports whether the operator's pool class excludes pool
// at now: the class the gate refuses a credit rung on. An unreadable policy is
// not an exclusion (the gate refuses on it separately).
func CreditPoolExcluded(pool string, now time.Time) bool {
	return CreditPoolClass(pool, now) == "exclude"
}

// HostPresenceTTL is `[quota].presence_ttl_s` from this host's fleet config,
// else the 24 h default placement reads presence with.
func HostPresenceTTL() time.Duration {
	config, err := LoadConfig(configPath(io.Discard))
	if err != nil {
		return 24 * time.Hour
	}
	if n := integer(config.Quota, "presence_ttl_s", 0); n > 0 {
		return time.Duration(n) * time.Second
	}
	return 24 * time.Hour
}

// HostPresenceID is the id this host's presence records carry, as placement
// reads the local host: `[local].presence_id`, else `[local].machine_id` or
// `[hub].machine_id`. Empty means the fleet config names none.
func HostPresenceID() string {
	config, err := LoadConfig(configPath(io.Discard))
	if err != nil {
		return ""
	}
	if id := str(config.Local, "presence_id"); id != "" {
		return id
	}
	for _, m := range []map[string]any{config.Local, config.Hub} {
		if id := str(m, "machine_id"); id != "" {
			return id
		}
	}
	return ""
}

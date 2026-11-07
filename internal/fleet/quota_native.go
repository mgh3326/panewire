package fleet

import (
	"fmt"
	"io"
	"slices"
	"strings"
)

// Which pools panewire measures itself (docs/quota.md §Native measurement).
// claude and codex always are. A pool with a switch — commandcode, devin and agy — is
// measured natively only once the host's fleet config lists it in
// `[quota].native_pools`; until then it is in shadow: spawns keep running
// pacelever for it while the node compares its own measurement against
// pacelever's.

// quotaSwitchedPools are the pools that have a native/shadow switch.
var quotaSwitchedPools = []string{"commandcode", "devin", "agy"}

// QuotaPoolHasSwitch reports whether pool is measured natively only when the
// host's `[quota].native_pools` lists it.
func QuotaPoolHasSwitch(pool string) bool { return slices.Contains(quotaSwitchedPools, pool) }

// IsNativeQuotaPool says whether panewire measures pool itself (`panewire quota
// refresh <pool>`) given the host's `[quota]` table; every other pool is
// refreshed by pacelever. A pool with a switch is native only when listed.
func IsNativeQuotaPool(pool string, quota map[string]any) bool {
	switch {
	case pool == "claude" || pool == "codex":
		return true
	case QuotaPoolHasSwitch(pool):
		listed, _ := quota["native_pools"].([]any)
		return slices.Contains(listed, any(pool))
	}
	return false
}

// HostNativeQuotaPool is IsNativeQuotaPool for this host's own fleet config,
// read now so that flipping the switch needs no restart. A missing, unreadable
// or invalid config leaves a switched pool in shadow.
func HostNativeQuotaPool(pool string) bool {
	if !QuotaPoolHasSwitch(pool) {
		return IsNativeQuotaPool(pool, nil)
	}
	config, err := LoadConfig(configPath(io.Discard))
	if err != nil {
		return false
	}
	return IsNativeQuotaPool(pool, config.Quota)
}

func (a *App) nativeQuotaPool(pool string) bool { return IsNativeQuotaPool(pool, a.Config.Quota) }

// validateNativePools checks `[quota].native_pools`: a list of pools that have
// a switch.
func validateNativePools(quota map[string]any) error {
	value, ok := quota["native_pools"]
	if !ok {
		return nil
	}
	list, isList := value.([]any)
	if !isList {
		return fail(exitConfig, "quota.native_pools must be a list of pool names")
	}
	for _, entry := range list {
		pool, isString := entry.(string)
		if !isString || !QuotaPoolHasSwitch(pool) {
			return fail(exitConfig, "%s", fmt.Sprintf("quota.native_pools lists only pools with a switch: %v", quotaSwitchedPools))
		}
	}
	return nil
}

// HostQuotaAccount is this host's configured shared account for a pool that has
// no login to derive one from (agy): the `[quota.accounts]` fingerprint, the
// account key it names, and whether `[quota].measurable_accounts` lets this
// host measure that key. It reads the host's own fleet config now, so a change
// needs no restart. A missing, unreadable or invalid config, or no
// fingerprint, has no key: the host does not guess one.
func HostQuotaAccount(pool string) (fp string, measurable bool) {
	config, err := LoadConfig(configPath(io.Discard))
	if err != nil {
		return "", false
	}
	return quotaAccount(config.Quota, pool)
}

func quotaAccount(quota map[string]any, pool string) (string, bool) {
	accounts, _ := quota["accounts"].(map[string]any)
	fp := str(accounts, pool)
	if !accountFP.MatchString(fp) {
		return "", false
	}
	listed, _ := quota["measurable_accounts"].([]any)
	return fp, slices.Contains(listed, any(pool+":"+fp))
}

// validateMeasurableAccounts checks `[quota].measurable_accounts`: a list of
// `pool:<16-hex>` account keys this host may measure.
func validateMeasurableAccounts(quota map[string]any) error {
	value, ok := quota["measurable_accounts"]
	if !ok {
		return nil
	}
	list, isList := value.([]any)
	if !isList {
		return fail(exitConfig, "quota.measurable_accounts must be a list of pool:fingerprint account keys")
	}
	for _, entry := range list {
		key, isString := entry.(string)
		pool, fp, found := strings.Cut(key, ":")
		if !isString || !found || !validID(pool) || !accountFP.MatchString(fp) {
			return fail(exitConfig, "quota.measurable_accounts entries must be pool:<16-hex fingerprint>")
		}
	}
	return nil
}

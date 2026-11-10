package fleet

import (
	"io"
	"slices"
	"strings"
	"time"
)

// Native quota sources are the providers implemented in internal/quota.
var nativeQuotaPools = []string{"claude", "codex", "commandcode", "devin", "agy"}

// IsNativeQuotaPool says whether panewire measures pool itself (`panewire quota
// refresh <pool>`); other pools have no automatic measurement source.
func IsNativeQuotaPool(pool string, _ map[string]any) bool {
	return slices.Contains(nativeQuotaPools, pool)
}

func HostNativeQuotaPool(pool string) bool { return IsNativeQuotaPool(pool, nil) }

func (a *App) nativeQuotaPool(pool string) bool { return IsNativeQuotaPool(pool, a.Config.Quota) }

func validateNativePools(map[string]any) error { return nil }

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

// HostMeasurableAccount says whether this host's `[quota].measurable_accounts`
// lists key (`pool:<16-hex>`). It reads the host's own fleet config now; a
// missing or invalid config lists nothing.
func HostMeasurableAccount(key string) bool {
	config, err := LoadConfig(configPath(io.Discard))
	if err != nil {
		return false
	}
	listed, _ := config.Quota["measurable_accounts"].([]any)
	return slices.Contains(listed, any(key))
}

// HostLoginProbeInterval is `[quota].login_probe_interval`: how often the node
// measures a pool with this host's own credential even when the shared reading
// is fresh (docs/fleet-state.md, "Login probe"). Zero, an unset key or an
// unreadable config is disabled. It is read at each use, so a change needs no
// restart.
func HostLoginProbeInterval() time.Duration {
	config, err := LoadConfig(configPath(io.Discard))
	if err != nil {
		return 0
	}
	interval, _ := loginProbeInterval(config.Quota)
	return interval
}

func loginProbeInterval(quota map[string]any) (time.Duration, error) {
	value, ok := quota["login_probe_interval"]
	if !ok {
		return 0, nil
	}
	text, isString := value.(string)
	interval, err := time.ParseDuration(text)
	if !isString || err != nil || interval < 0 {
		return 0, fail(exitConfig, "quota.login_probe_interval must be a non-negative duration such as \"30m\"")
	}
	return interval, nil
}

func validateLoginProbeInterval(quota map[string]any) error {
	_, err := loginProbeInterval(quota)
	return err
}

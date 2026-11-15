package fleet

import (
	"fmt"
	"io"
	"maps"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

// Config accepts the fleet hosts.toml surface. Capability booleans remain
// data so new harnesses do not require a configuration parser change.
type Config struct {
	HostOrder []string                  `toml:"-"`
	Local     map[string]any            `toml:"local"`
	Hub       map[string]any            `toml:"hub"`
	Hosts     map[string]map[string]any `toml:"hosts"`
	Artifacts map[string]any            `toml:"artifacts"`
	Placement map[string]any            `toml:"placement"`
	Quota     map[string]any            `toml:"quota"`
	Sweep     map[string]any            `toml:"sweep"`
	// Reap holds [reap] job_dir_ttl, the node's opt-in to the job dir TTL
	// (docs/job-dir-ttl.md).
	Reap map[string]any `toml:"reap"`
	// ignoredTables names top-level tables this release does not read; New
	// warns about each, so a config from a newer or older release still loads.
	ignoredTables []string
}

func expand(p string) string {
	if p == "~" {
		h, _ := os.UserHomeDir()
		return h
	}
	if strings.HasPrefix(p, "~/") {
		h, _ := os.UserHomeDir()
		return filepath.Join(h, p[2:])
	}
	return p
}

// configPath resolves the fleet hosts config. FLEET_HOSTS_CONFIG wins, then
// XDG_CONFIG_HOME|~/.config/fleet/hosts.toml. For one release the retired
// wrk env var and path still load, each with a one-line migration warning.
func configPath(errw io.Writer) string {
	p := os.Getenv("XDG_CONFIG_HOME")
	if p == "" {
		p = expand("~/.config")
	}
	next := filepath.Join(p, "fleet", "hosts.toml")
	if v := os.Getenv("FLEET_HOSTS_CONFIG"); v != "" {
		return v
	}
	if v := os.Getenv("WRK_HOSTS_CONFIG"); v != "" {
		if _, e := os.Stat(next); os.IsNotExist(e) {
			fmt.Fprintf(errw, "panewire: warning: WRK_HOSTS_CONFIG is retired; move the hosts config to %s or set FLEET_HOSTS_CONFIG, then unset WRK_HOSTS_CONFIG\n", next)
			return v
		}
		// The fleet config already exists: a stale export must not keep
		// overriding it for the whole deprecation release.
		fmt.Fprintf(errw, "panewire: warning: WRK_HOSTS_CONFIG is retired and ignored; the fleet hosts config is at %s, unset WRK_HOSTS_CONFIG\n", next)
	}
	old := filepath.Join(p, "wrk", "hosts.toml")
	if _, e := os.Stat(next); os.IsNotExist(e) {
		if _, e = os.Stat(old); e == nil {
			fmt.Fprintf(errw, "panewire: warning: hosts config still at %s; move it to %s\n", old, next)
			return old
		} else if !os.IsNotExist(e) {
			// An unreadable retired path (e.g. EACCES) must not silently
			// fall through to an empty fleet config: returning it lets
			// LoadConfig surface the read error.
			return old
		}
	}
	return next
}

// warnRetiredQuotaKeys prints one migration line per retired [quota] key the
// loaded config still sets. It names the key, never its value.
func warnRetiredQuotaKeys(c Config, errw io.Writer) {
	for _, key := range []string{"gate", "launch", "native_pools"} {
		if _, ok := c.Quota[key]; ok {
			fmt.Fprintf(errw, "panewire: 경고: [quota] %s 설정은 폐기되었습니다; panewire는 native quota 동작을 사용합니다\n", key)
		}
	}
	if str(c.Quota, "credential_env") != "" {
		fmt.Fprintln(errw, "panewire: warning: [quota] credential_env is retired; panewire reads HANDOFFKEEP_TOKEN_SVC_PANEWIRE or ~/.config/panewire/hk-svc.env")
	}
}

// warnIgnoredHostsTables names each top-level key of the hosts config that
// this release does not read. The file still loads; the key is ignored.
func warnIgnoredHostsTables(c Config, path string, errw io.Writer) {
	for _, key := range c.ignoredTables {
		fmt.Fprintf(errw, "panewire: warning: unknown hosts key [%s] in %s is not read by this release; ignored\n", key, path)
	}
}

// ReportUserIdle reads [local] report_user_idle from the node's hosts config. The
// node reads it on each heartbeat, so an edit applies at the next one. A
// missing or unreadable config means off: the node reports no user input.
func ReportUserIdle() bool {
	c, err := LoadConfig(configPath(io.Discard))
	return err == nil && boolean(c.Local, "report_user_idle", false)
}

func LoadConfig(path string) (Config, error) {
	c := Config{}
	b, e := os.ReadFile(path)
	if os.IsNotExist(e) {
		return c, nil
	}
	if e != nil {
		return c, fail(exitConfig, "hosts config: %v", e)
	}
	if strings.Contains(string(b), "\ufeff") {
		return c, fail(exitConfig, "hosts config contains a BOM")
	}
	var raw map[string]any
	if e = toml.Unmarshal(b, &raw); e != nil {
		return c, fail(exitConfig, "hosts config: %v", e)
	}
	for _, k := range slices.Sorted(maps.Keys(raw)) {
		if k != "local" && k != "hub" && k != "hosts" && k != "artifacts" && k != "placement" && k != "quota" && k != "sweep" && k != "reap" {
			c.ignoredTables = append(c.ignoredTables, k)
		}
	}
	if e = toml.Unmarshal(b, &c); e != nil {
		return c, fail(exitConfig, "hosts config: %v", e)
	}
	for _, m := range regexp.MustCompile(`(?m)^\s*\[hosts\.([A-Za-z0-9._-]+)\]\s*(?:#.*)?$`).FindAllStringSubmatch(string(b), -1) {
		c.HostOrder = append(c.HostOrder, m[1])
	}
	for _, k := range []string{"spawn", "cmd_dsflash41"} {
		if v, ok := c.Local[k]; ok {
			if _, ok = v.(bool); !ok {
				return c, fail(exitConfig, "local.%s must be a boolean", k)
			}
		}
	}
	for _, k := range []string{"max_active", "shell_ready_timeout_ms", "brief_submit_timeout_ms", "brief_submit_retry_ms", "fleet_usage_alert_gb"} {
		if v, ok := c.Local[k]; ok {
			if n, ok := v.(int64); !ok || n < 0 {
				return c, fail(exitConfig, "local.%s must be a nonnegative integer", k)
			}
		}
	}
	if v, ok := c.Hub["quota_gate"]; ok {
		if v != "local" || str(c.Hub, "hub_token_env") != "" {
			return c, fail(exitConfig, "hub.quota_gate accepts only local and contradicts hub_token_env")
		}
	}
	if v, ok := c.Hub["state"]; ok && v != "on" && v != "off" {
		return c, fail(exitConfig, "hub.state accepts only on or off")
	}
	if v, ok := c.Sweep["remove_worktrees"]; ok {
		if _, ok := v.(bool); !ok {
			return c, fail(exitConfig, "sweep.remove_worktrees must be a boolean")
		}
	}
	if v, ok := c.Reap["job_dir_ttl"]; ok {
		if text, isText := v.(string); !isText || jobDirTTLValue(text) == 0 {
			return c, fail(exitConfig, "reap.job_dir_ttl must be a duration of at least %s, such as \"72h\" or \"3d\"", MinJobDirTTL)
		}
	}
	if v, ok := c.Hub["state_publisher_ttl_s"]; ok {
		if n, ok := v.(int64); !ok || n < 1 || n > 3600 {
			return c, fail(exitConfig, "hub.state_publisher_ttl_s must be 1..3600")
		}
	}
	if str(c.Hub, "hub_token_env") != "" && str(c.Hub, "hub_url") == "" {
		return c, fail(exitConfig, "hub_token_env requires hub_url")
	}
	for _, key := range []string{"session_machine_ids", "session_lanes_token_envs"} {
		if value, exists := c.Hub[key]; exists {
			mapping, ok := value.(map[string]any)
			if !ok {
				return c, fail(exitConfig, "hub.%s must be a session table", key)
			}
			for session, value := range mapping {
				text, ok := value.(string)
				if !validID(session) || !ok || strings.TrimSpace(text) == "" || key == "session_machine_ids" && !validID(text) {
					return c, fail(exitConfig, "hub.%s has an invalid session entry", key)
				}
			}
		}
	}
	for name, values := range c.Hosts {
		if raw, ok := values["aliases"]; ok {
			aliases, ok := raw.([]any)
			if !ok {
				return c, fail(exitConfig, "hosts.%s.aliases must be a list; set aliases = [\"short-name\"]", name)
			}
			for _, v := range aliases {
				alias, ok := v.(string)
				if !ok || !validID(alias) {
					return c, fail(exitConfig, "hosts.%s.aliases entries must be safe host names; use letters, digits, dot, underscore or hyphen", name)
				}
			}
		}
	}
	for _, name := range slices.Sorted(maps.Keys(c.Hosts)) {
		h := c.Hosts[name]
		if !validID(name) {
			return c, fail(exitConfig, "invalid host name")
		}
		if v := str(h, "ssh"); v != "" && !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9@._-]*$`).MatchString(v) {
			return c, fail(exitConfig, "SSH target must be an SSH config alias")
		}
		if v := str(h, "via"); v != "" && v != "ssh" && v != "hub" {
			return c, fail(exitConfig, "hosts.%s.via must be ssh or hub", name)
		}
		if _, ok := h["hub_wait"]; ok {
			if n, isInt := h["hub_wait"].(int64); !isInt || n < 1 || n > 300 {
				return c, fail(exitConfig, "hosts.%s.hub_wait must be 1..300 seconds", name)
			}
		}
		if v := str(h, "brief_doc_prefix"); v != "" && !validDocPrefix(v) {
			return c, fail(exitConfig, "hosts.%s.brief_doc_prefix must be a key prefix ending in /", name)
		}
		if e := validTags(h["tags"], "hosts."+name+".tags"); e != nil {
			return c, e
		}
		if e := validHostFit(h, "hosts."+name); e != nil {
			return c, e
		}
		if v := str(h, "presence_id"); v != "" && !validID(v) {
			return c, fail(exitConfig, "hosts.%s.presence_id must be a machine ID", name)
		}
	}
	if e := validTags(c.Local["tags"], "local.tags"); e != nil {
		return c, e
	}
	if e := validHostFit(c.Local, "local"); e != nil {
		return c, e
	}
	if v := str(c.Local, "presence_id"); v != "" && !validID(v) {
		return c, fail(exitConfig, "local.presence_id must be a machine ID")
	}
	if v, ok := c.Placement["load_threshold"]; ok {
		if n, ok := v.(float64); !ok || n < 0 {
			if m, isInt := v.(int64); !isInt || m < 0 {
				return c, fail(exitConfig, "placement.load_threshold must be a nonnegative number")
			}
		}
	}
	if e := validShadowPolicy(c.Placement); e != nil {
		return c, e
	}
	for _, k := range []string{"wake_after_s", "presence_ttl_s", "fresh_ttl_s"} {
		for _, m := range []map[string]any{c.Placement, c.Quota} {
			if v, ok := m[k]; ok {
				if n, ok := v.(int64); !ok || n < 0 {
					return c, fail(exitConfig, "%s must be a nonnegative integer", k)
				}
			}
		}
	}
	if v, ok := c.Quota["pool_fresh_ttl_s"]; ok {
		m, isMap := v.(map[string]any)
		if !isMap {
			return c, fail(exitConfig, "quota.pool_fresh_ttl_s must map pool names to nonnegative integers")
		}
		for pool, n := range m {
			if i, isInt := n.(int64); !validID(pool) || !isInt || i < 0 {
				return c, fail(exitConfig, "quota.pool_fresh_ttl_s.%s must be a nonnegative integer", pool)
			}
		}
	}
	if err := validateNativePools(c.Quota); err != nil {
		return c, err
	}
	if err := validatePoolMaxConcurrent(c.Quota); err != nil {
		return c, err
	}
	if err := validateMeasurableAccounts(c.Quota); err != nil {
		return c, err
	}
	if err := validateLoginProbeInterval(c.Quota); err != nil {
		return c, err
	}
	if err := validateGateMode(c.Quota); err != nil {
		return c, err
	}
	if err := validateLaunchMode(c.Quota); err != nil {
		return c, err
	}
	if v, ok := c.Quota["credit_total_usd"]; ok {
		m, isMap := v.(map[string]any)
		if !isMap {
			return c, fail(exitConfig, "quota.credit_total_usd must map pool names to positive numbers")
		}
		for _, pool := range slices.Sorted(maps.Keys(m)) {
			n, isNum := number(m, pool)
			if !validID(pool) || !isNum || n <= 0 || !finite(n) {
				return c, fail(exitConfig, "quota.credit_total_usd.%s must be a positive number", pool)
			}
		}
	}
	if v := str(c.Quota, "url"); v != "" {
		u, e := url.Parse(v)
		if e != nil || u.Scheme != "https" && (u.Scheme != "http" || !isLoopbackHost(u.Hostname())) {
			return c, fail(exitConfig, "quota.url requires HTTPS")
		}
	}
	// credential_env is retired (hk 3570): the bearer is always the
	// svc-panewire credential (internal/hkcred). An old file still parses,
	// and New warns once (warnRetiredQuotaKeys).
	if v := str(c.Quota, "credential_env"); v != "" && !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(v) {
		return c, fail(exitConfig, "quota.credential_env must name an environment variable")
	}
	if accounts, ok := c.Quota["accounts"].(map[string]any); ok {
		for _, pool := range slices.Sorted(maps.Keys(accounts)) {
			fp := accounts[pool]
			if !validID(pool) {
				return c, fail(exitConfig, "invalid quota account pool name")
			}
			if s, ok := fp.(string); !ok || !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(s) {
				return c, fail(exitConfig, "quota.accounts.%s must be a 16-hex account fingerprint", pool)
			}
		}
	}
	if v, ok := c.Local["max_load_ratio"]; ok {
		switch n := v.(type) {
		case int64:
			if n < 0 {
				return c, typeError("local.max_load_ratio")
			}
		case float64:
			if n < 0 {
				return c, typeError("local.max_load_ratio")
			}
		default:
			return c, fail(exitConfig, "local.max_load_ratio must be a nonnegative number")
		}
	}
	if v, ok := c.Local["shell_ready_timeout_ms"]; ok {
		n := v.(int64)
		if n < 30000 || n > 300000 {
			return c, fail(exitConfig, "shell_ready_timeout_ms must be 30000..300000")
		}
	}
	submitTimeout := integer(c.Local, "brief_submit_timeout_ms", defaultBriefSubmitTimeoutMS)
	if submitTimeout < 30000 || submitTimeout > 600000 {
		return c, fail(exitConfig, "brief_submit_timeout_ms must be 30000..600000")
	}
	if retry := integer(c.Local, "brief_submit_retry_ms", defaultBriefSubmitRetryMS); retry < 1000 || retry >= submitTimeout {
		return c, fail(exitConfig, "brief_submit_retry_ms must be at least 1000 and below brief_submit_timeout_ms")
	}
	return c, nil
}
func str(m map[string]any, k string) string { v, _ := m[k].(string); return v }
func boolean(m map[string]any, k string, def bool) bool {
	v, ok := m[k].(bool)
	if !ok {
		return def
	}
	return v
}
func integer(m map[string]any, k string, def int64) int64 {
	v, ok := m[k].(int64)
	if !ok {
		return def
	}
	return v
}
func stringsValue(m map[string]any, k string) []string {
	var out []string
	switch v := m[k].(type) {
	case []any:
		for _, x := range v {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
	case string:
		out = strings.Split(v, ",")
	}
	return out
}
func (c Config) hubArgs(role string) []string {
	var a []string
	if v := str(c.Hub, "hub_url"); v != "" {
		a = append(a, "--hub-url", v)
	}
	token := str(c.Hub, "lanes_token_env")
	if token == "" && role == "builder" {
		token = str(c.Hub, "operator_token_env")
	}
	if token == "" {
		token = str(c.Hub, "hub_token_env")
	}
	if token != "" {
		a = append(a, "--hub-token-env", token)
	}
	if v := str(c.Hub, "hub_cf_env"); v != "" {
		a = append(a, "--hub-cf-env", v)
	}
	return a
}

var tagRule = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

func isLoopbackHost(h string) bool {
	if h == "localhost" {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}

// isTailnetHost reports a literal Tailscale CGNAT address (100.64.0.0/10).
// Plain HTTP to it is allowed only with the explicit [quota]
// allow_plaintext_tailnet opt-in, mirroring pacelever's
// allow_plaintext_quota_share: the tailnet already encrypts the hop.
func isTailnetHost(h string) bool {
	ip := net.ParseIP(h)
	_, cgnat, _ := net.ParseCIDR("100.64.0.0/10")
	return ip != nil && ip.To4() != nil && cgnat.Contains(ip)
}

func validTags(v any, key string) error {
	if v == nil {
		return nil
	}
	for _, t := range stringsValue(map[string]any{"v": v}, "v") {
		if !tagRule.MatchString(t) {
			return fail(exitConfig, "%s contains an invalid tag %q", key, t)
		}
	}
	return nil
}
func typeError(key string) error          { return fail(exitConfig, "%s has an invalid type", key) }
func unmarshalTOML(b []byte, v any) error { return toml.Unmarshal(b, v) }
func (a *App) controlHubArgs(operator bool) []string {
	token := str(a.Config.Hub, "hub_token_env")
	if operator {
		token = firstNonempty(str(a.Config.Hub, "operator_token_env"), token)
	}
	args := []string{"--hub-url", str(a.Config.Hub, "hub_url"), "--hub-token-env", token}
	if v := str(a.Config.Hub, "hub_cf_env"); v != "" {
		args = append(args, "--hub-cf-env", v)
	}
	return args
}

// validDocPrefix accepts an hk document key prefix: path segments of key
// characters, none empty, "." or "..", ending in "/".
func validDocPrefix(v string) bool {
	segs, ok := strings.CutSuffix(v, "/")
	if !ok || segs == "" || len(v) > 100 {
		return false
	}
	for _, seg := range strings.Split(segs, "/") {
		if seg == "" || seg == "." || seg == ".." || !regexp.MustCompile(`^[A-Za-z0-9._-]+$`).MatchString(seg) {
			return false
		}
	}
	return true
}

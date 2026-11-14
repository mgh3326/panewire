package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	neturl "net/url"
	"os"
	"slices"
	"strings"
	"time"
)

// Remote-spawn preflight. Six independent conditions each needed their own fix
// before a remote spawn worked; this reports all of them for one candidate
// host, each with a named reason, so one read-only look replaces six rounds of
// trial and error:
//
//	hub_acceptance     the hub lists the host, it is connected and accepting
//	quota_observation  the shared quota observation of the host's account is
//	                   observed and no older than fresh_ttl
//	quota_transport    the remote [quota] setting suits the hk URL scheme
//	hub_load           the hub reports a load reading and no memory pressure
//	review_config      ~/.config/mergewake/config.yaml exists on the host
//	version_skew       panewire and panewire on the host match this machine
//
// The preflight only reads: hub placement, hk presence and shared observation,
// and one SSH probe per host that tests two files, reads the [quota] opt-in
// from the remote hosts.toml and asks the two binaries for their versions. It
// never prints a URL, a host address or a file's contents.
const (
	checkHubAcceptance    = "hub_acceptance"
	checkQuotaObservation = "quota_observation"
	checkQuotaTransport   = "quota_transport"
	checkHubLoad          = "hub_load"
	checkReviewConfig     = "review_config"
	checkVersionSkew      = "version_skew"
)

type preflightCheck struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Reason string `json:"reason,omitempty"`
	Detail string `json:"detail,omitempty"`
	// Skipped marks a check that needs the SSH probe when none was made. It
	// counts as ok: nothing was found wrong, nothing was looked at.
	Skipped bool `json:"skipped,omitempty"`
}

type hostPreflight struct {
	Host   string           `json:"host"`
	OK     bool             `json:"ok"`
	Checks []preflightCheck `json:"checks"`
}

func (h hostPreflight) hasSkipped() bool {
	for _, c := range h.Checks {
		if c.Skipped {
			return true
		}
	}
	return false
}

// reasons lists every failed check as name:reason.
func (h hostPreflight) reasons() []string {
	out := []string{}
	for _, c := range h.Checks {
		if !c.OK {
			out = append(out, c.Name+":"+c.Reason)
		}
	}
	return out
}

// record is the compact form kept in the placement record: the verdict and the
// reasons, plus each failed check's detail.
func (h hostPreflight) record() map[string]any {
	failed := []any{}
	for _, c := range h.Checks {
		if !c.OK {
			row := map[string]any{"check": c.Name, "reason": c.Reason}
			if c.Detail != "" {
				row["detail"] = c.Detail
			}
			failed = append(failed, row)
		}
	}
	return map[string]any{"ok": h.OK, "reasons": h.reasons(), "failed": failed}
}

// preflightBudget bounds the whole command. The hub and hk reads and one SSH
// probe per host run in sequence, and a probe whose connection stalls after
// connecting would otherwise hold the runner's per-call limit once per
// remaining host — the intermittent >120s stall seen on mbp (hk 3598). The
// command is a diagnostic; it must answer inside the wait its callers give
// it. A var only so a test can shrink it.
var preflightBudget = 90 * time.Second

func pass(name, detail string) preflightCheck {
	return preflightCheck{Name: name, OK: true, Detail: detail}
}
func refuse(name, reason, detail string) preflightCheck {
	return preflightCheck{Name: name, Reason: reason, Detail: detail}
}

// preflightEnv is what a preflight reads once for all hosts: the hub host list
// (nil when unreadable), host presence and this machine's versions.
type preflightEnv struct {
	hub         map[string]HostView
	presence    []PresenceRecord
	presenceErr bool
	versions    *localVersions
}

type localVersions struct{ panewire string }

// newPreflightEnv reads the hub and presence now.
func (a *App) newPreflightEnv(ctx context.Context) *preflightEnv {
	cwd, _ := os.Getwd()
	hub, _ := a.hostViews(ctx, cwd)
	return a.newPreflightEnvWith(ctx, hub)
}

func (a *App) newPreflightEnvWith(ctx context.Context, hub map[string]HostView) *preflightEnv {
	e := &preflightEnv{hub: hub}
	if a.Source == nil {
		e.presenceErr = true
		return e
	}
	records, err := a.Source.Presence(ctx)
	e.presence, e.presenceErr = records, err != nil
	return e
}

// preflightEnvFrom reuses the reads a placement plan already made.
func (a *App) preflightEnvFrom(ctx context.Context, p *placePlan) *preflightEnv {
	if p.pool == "" {
		// Presence is only read for a plan with a quota pool.
		return a.newPreflightEnvWith(ctx, p.hub)
	}
	return &preflightEnv{hub: p.hub, presence: p.presence, presenceErr: p.presenceErr}
}

func (a *App) localVersions(ctx context.Context, e *preflightEnv) localVersions {
	if e.versions == nil {
		v := localVersions{panewire: a.Version}
		e.versions = &v
	}
	return *e.versions
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return strings.TrimSpace(line)
}

// remoteHostNames lists the configured remote hosts in placement order.
func (a *App) remoteHostNames() []string {
	names := slices.Clone(a.Config.HostOrder)
	var extra []string
	for name := range a.Config.Hosts {
		if !slices.Contains(names, name) {
			extra = append(extra, name)
		}
	}
	slices.Sort(extra)
	return append(names, extra...)
}

// preflightHost runs the six checks for one remote host. probe false skips the
// SSH probe (three checks) for a host that must not be contacted. pool restricts the
// shared-quota check to one pool; empty checks every pool the host's presence
// names.
func (a *App) preflightHost(ctx context.Context, e *preflightEnv, name string, cfg map[string]any, pool string, probe bool) hostPreflight {
	h := hostPreflight{Host: name}
	view, listed := e.hub[name]
	rp := a.probeRemote(ctx, name, cfg, probe)
	h.Checks = []preflightCheck{
		a.hubAcceptance(e, view, listed),
		a.quotaObservation(ctx, e, name, cfg, pool),
		a.quotaTransport(rp),
		a.hubLoad(e, view, listed),
		a.reviewConfig(rp),
		a.versionSkew(ctx, e, rp),
	}
	h.OK = true
	for _, c := range h.Checks {
		h.OK = h.OK && c.OK
	}
	return h
}

func (a *App) hubAcceptance(e *preflightEnv, v HostView, listed bool) preflightCheck {
	switch {
	case e.hub == nil:
		return refuse(checkHubAcceptance, "hub_unavailable", "the hub host list could not be read")
	case !listed:
		return refuse(checkHubAcceptance, "not_listed", "the hub does not list this host")
	case strings.Contains(v.Reason, "not_accepting"):
		return refuse(checkHubAcceptance, "not_accepting", "hub reason: "+v.Reason)
	case reasonHasToken(v.Reason, "lanes_full"):
		return refuse(checkHubAcceptance, "lanes_full", "hub reason: "+v.Reason)
	case !v.Connected:
		return refuse(checkHubAcceptance, "disconnected", "hub reason: "+firstNonempty(v.Reason, "none"))
	}
	return pass(checkHubAcceptance, "accepting; hub reason: "+firstNonempty(v.Reason, "none"))
}

func (a *App) hubLoad(e *preflightEnv, v HostView, listed bool) preflightCheck {
	reason := strings.ToLower(v.Reason)
	switch {
	case e.hub == nil || !listed:
		return refuse(checkHubLoad, "load_unknown", "no hub reading for this host")
	case strings.Contains(reason, "memory") || strings.Contains(reason, "swap") || strings.Contains(reason, "pressure"):
		return refuse(checkHubLoad, "memory_pressure", "hub reason: "+v.Reason)
	case v.LoadRatio == nil:
		return refuse(checkHubLoad, "load_unknown", "the hub metrics source has no reading for this host")
	}
	if t := a.loadThreshold(); *v.LoadRatio >= t {
		return refuse(checkHubLoad, "load_over_threshold", fmt.Sprintf("load_ratio=%.2f threshold=%.2f", *v.LoadRatio, t))
	}
	return pass(checkHubLoad, fmt.Sprintf("load_ratio=%.2f", *v.LoadRatio))
}

// quotaObservation checks, per pool the host's presence names, that the shared
// observation of the host's account exists and is younger than fresh_ttl. A
// stale one is what made remote candidates flap: the gate then has to measure
// the target before it can admit it.
func (a *App) quotaObservation(ctx context.Context, e *preflightEnv, host string, cfg map[string]any, pool string) preflightCheck {
	if e.presenceErr {
		return refuse(checkQuotaObservation, "presence_unavailable", "host presence could not be read")
	}
	id := firstNonempty(str(cfg, "presence_id"), host)
	now, ttl := a.Now(), a.placementTTL("presence_ttl_s", 86400)
	pools := []string{pool}
	if pool == "" {
		pools = nil
		for _, rec := range e.presence {
			if p, _, ok := strings.Cut(rec.AccountKey, ":"); ok && rec.HostID == id && validID(p) && !slices.Contains(pools, p) {
				pools = append(pools, p)
			}
		}
		slices.Sort(pools)
	}
	if len(pools) == 0 {
		return refuse(checkQuotaObservation, "no_usable_presence", "no account presence recorded for this host")
	}
	var reasons, details []string
	for _, p := range pools {
		key := hostAccount(e.presence, p, id, now, ttl)
		var acc SharedAccount
		if key != "" && a.Source != nil {
			var err error
			if acc, err = a.Source.SharedAccount(ctx, key); err != nil {
				acc = SharedAccount{Key: key, Degraded: true}
			}
		} else if key != "" {
			acc = SharedAccount{Key: key, Degraded: true}
		}
		age := "unknown"
		if !acc.MeasuredAt.IsZero() {
			age = now.Sub(acc.MeasuredAt).Round(time.Second).String()
		}
		if r := sharedReason(key, acc); r != "" {
			reasons = append(reasons, p+":"+r)
		}
		details = append(details, fmt.Sprintf("%s age=%s fresh_ttl=%s", p, age, a.sharedFreshTTL(p)))
	}
	if len(reasons) > 0 {
		return refuse(checkQuotaObservation, strings.Join(reasons, ","), strings.Join(details, "; "))
	}
	return pass(checkQuotaObservation, strings.Join(details, "; "))
}

// remoteProbe is what one SSH probe learned about a host.
type remoteProbe struct {
	skipped           bool   // no probe was asked for
	failure           string // non-empty: the probe did not run
	review, plaintext string
	panewireVer       string
	havePanewire      bool
}

// probeScript prints key=value lines. It tests two paths and reads one boolean;
// it never prints a file's contents.
const probeScript = `if [ -f "$HOME/.config/mergewake/config.yaml" ]; then echo review_config=present; else echo review_config=absent; fi
cfg="${FLEET_HOSTS_CONFIG:-${XDG_CONFIG_HOME:-$HOME/.config}/fleet/hosts.toml}"
if [ -f "$cfg" ]; then awk '/^[[:space:]]*\[/{s=$0; gsub(/[[:space:]]/,"",s); q=(s=="[quota]")} q && /^[[:space:]]*allow_plaintext_tailnet[[:space:]]*=[[:space:]]*true/{f=1} END{print "plaintext_tailnet=" (f ? "true" : "false")}' "$cfg"; else echo plaintext_tailnet=noconfig; fi
echo "panewire=$(panewire --version 2>/dev/null | head -n 1)"`

func (a *App) probeRemote(ctx context.Context, name string, cfg map[string]any, probe bool) remoteProbe {
	if !probe {
		return remoteProbe{skipped: true}
	}
	if str(cfg, "via") == "hub" {
		return remoteProbe{failure: "via_hub_unsupported"}
	}
	target := firstNonempty(str(cfg, "ssh"), name)
	if strings.HasPrefix(target, "-") || strings.ContainsAny(target, "\r\n") {
		return remoteProbe{failure: "invalid_ssh_target"}
	}
	r := a.sshRun(ctx, cfg, name, probeScript)
	switch r.Code {
	case 0:
	case exitUnreachable:
		return remoteProbe{failure: "unreachable"}
	default:
		return remoteProbe{failure: fmt.Sprintf("probe_failed_rc%d", r.Code)}
	}
	var p remoteProbe
	for _, line := range strings.Split(r.Out, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			continue
		}
		switch k {
		case "review_config":
			p.review = v
		case "plaintext_tailnet":
			p.plaintext = v
		case "panewire":
			p.panewireVer, p.havePanewire = strings.TrimSpace(v), strings.TrimSpace(v) != ""
		}
	}
	return p
}

func (p remoteProbe) refuse(name string) (preflightCheck, bool) {
	if p.skipped {
		return preflightCheck{Name: name, OK: true, Skipped: true, Detail: "not probed: the host was not contacted"}, true
	}
	if p.failure != "" {
		return refuse(name, p.failure, "the SSH probe did not run"), true
	}
	return preflightCheck{}, false
}

// quotaTransport: the remote panewire reads the shared quota cache itself, so a
// plain-http hk URL on the tailnet works there only with [quota]
// allow_plaintext_tailnet = true in the remote hosts.toml.
func (a *App) quotaTransport(p remoteProbe) preflightCheck {
	u := a.quotaURL()
	if u == "" {
		return refuse(checkQuotaTransport, "hk_url_unset", "no hk URL is configured on this machine to compare against")
	}
	parsed, err := neturl.Parse(u)
	if err != nil || parsed.Host == "" {
		return refuse(checkQuotaTransport, "hk_url_invalid", "")
	}
	switch {
	case parsed.Scheme == "https":
		return pass(checkQuotaTransport, "scheme=https; no plaintext opt-in needed")
	case parsed.Scheme != "http":
		return refuse(checkQuotaTransport, "hk_url_invalid", "scheme="+parsed.Scheme)
	case isLoopbackHost(parsed.Hostname()):
		return pass(checkQuotaTransport, "scheme=http loopback; no plaintext opt-in needed")
	case !isTailnetHost(parsed.Hostname()):
		return refuse(checkQuotaTransport, "plaintext_non_tailnet", "scheme=http to a non-tailnet host is refused by every panewire")
	}
	if r, bad := p.refuse(checkQuotaTransport); bad {
		return r
	}
	switch p.plaintext {
	case "true":
		return pass(checkQuotaTransport, "scheme=http tailnet; remote [quota] allow_plaintext_tailnet=true")
	case "noconfig":
		return refuse(checkQuotaTransport, "remote_config_missing", "scheme=http tailnet; the remote hosts.toml does not exist")
	}
	return refuse(checkQuotaTransport, "plaintext_tailnet_not_allowed", "scheme=http tailnet; remote [quota] allow_plaintext_tailnet is not true")
}

func (a *App) reviewConfig(p remoteProbe) preflightCheck {
	if r, bad := p.refuse(checkReviewConfig); bad {
		return r
	}
	if p.review == "present" {
		return pass(checkReviewConfig, "~/.config/mergewake/config.yaml exists")
	}
	return refuse(checkReviewConfig, "absent", "~/.config/mergewake/config.yaml is missing on the host")
}

func (a *App) versionSkew(ctx context.Context, e *preflightEnv, p remoteProbe) preflightCheck {
	if r, bad := p.refuse(checkVersionSkew); bad {
		return r
	}
	local := a.localVersions(ctx, e)
	var reasons, details []string
	compare := func(tool, here, there string, have bool) {
		switch {
		case !have:
			reasons = append(reasons, tool+"_missing")
			details = append(details, tool+" reports no version on the host")
		case here == "":
			reasons = append(reasons, tool+"_local_unknown")
			details = append(details, tool+" version unknown on this machine")
		case here != there:
			reasons = append(reasons, tool+"_skew")
			details = append(details, fmt.Sprintf("%s local=%q remote=%q", tool, here, there))
		default:
			details = append(details, fmt.Sprintf("%s=%q", tool, there))
		}
	}
	compare("panewire", local.panewire, p.panewireVer, p.havePanewire)
	if len(reasons) > 0 {
		return refuse(checkVersionSkew, strings.Join(reasons, ","), strings.Join(details, "; "))
	}
	return pass(checkVersionSkew, strings.Join(details, "; "))
}

// attachPreflight records the preflight of a candidate the placement refused,
// so the placement record says why the host is not usable.
func (a *App) attachPreflight(ctx context.Context, e *preflightEnv, c *placeCandidate, pool string, probe bool) {
	if c.local || (c.preflight != nil && (!probe || !c.preflight.hasSkipped())) {
		// Already explained, and a repeat would only probe the host again.
		return
	}
	h := a.preflightHost(ctx, e, c.name, c.cfg, pool, probe)
	c.preflight = &h
}

// printPreflightSummary renders the one-line verdict of a host.
func printPreflightSummary(w io.Writer, h hostPreflight) {
	if h.OK {
		fmt.Fprintf(w, "preflight host=%s ok=true\n", h.Host)
		return
	}
	fmt.Fprintf(w, "preflight host=%s ok=false reasons=%s\n", h.Host, strings.Join(h.reasons(), ","))
}

func (a *App) printPreflightChecks(h hostPreflight) {
	printPreflightSummary(a.Out, h)
	for _, c := range h.Checks {
		state := "ok"
		if c.Skipped {
			state = "skipped"
		}
		if !c.OK {
			state = "refused reason=" + c.Reason
		}
		line := fmt.Sprintf("  check %s %s", c.Name, state)
		if c.Detail != "" {
			line += " detail=" + c.Detail
		}
		fmt.Fprintln(a.Out, line)
	}
}

// preflightCmd is `panewire preflight [--host H] [--pool P] [--json]`: the
// remote-spawn preflight alone. Read-only; exit 0 when every checked host is
// good, exitPreflight (with the reasons on stdout and in the error) otherwise.
func (a *App) preflightCmd(ctx context.Context, args []string) error {
	ctx, cancel := context.WithTimeout(ctx, preflightBudget)
	defer cancel()
	var host, pool string
	var asJSON bool
	f := flags("preflight")
	f.StringVar(&host, "host", "", "check one configured remote host")
	f.StringVar(&pool, "pool", "", "restrict the shared-quota check to one pool")
	f.BoolVar(&asJSON, "json", false, "print the full result as JSON")
	if e := f.Parse(args); e != nil {
		return fail(exitUsage, "%v", e)
	}
	if f.NArg() > 0 {
		return fail(exitUsage, "preflight takes no positional arguments")
	}
	if pool != "" && !validID(pool) {
		return fail(exitUsage, "invalid pool %q", pool)
	}
	names := a.remoteHostNames()
	if host != "" {
		if _, ok := a.Config.Hosts[host]; !ok {
			return fail(exitUsage, "preflight needs a configured remote host; %q is not one", host)
		}
		names = []string{host}
	}
	if len(names) == 0 {
		return fail(exitConfig, "no remote hosts are configured")
	}
	env := a.newPreflightEnv(ctx)
	var results []hostPreflight
	var bad []string
	for _, name := range names {
		h := a.preflightHost(ctx, env, name, a.Config.Hosts[name], pool, true)
		results = append(results, h)
		if !h.OK {
			bad = append(bad, fmt.Sprintf("%s (%s)", name, strings.Join(h.reasons(), ",")))
		}
	}
	if asJSON {
		b, _ := json.MarshalIndent(map[string]any{"mode": "preflight", "ok": len(bad) == 0, "hosts": results}, "", "  ")
		fmt.Fprintln(a.Out, string(b))
	} else {
		for _, h := range results {
			a.printPreflightChecks(h)
		}
	}
	if len(bad) > 0 {
		return fail(exitPreflight, "preflight refused: %s", strings.Join(bad, "; "))
	}
	return nil
}

// preflightNotes summarises the preflight of every refused candidate for an
// error message, because a spawn that fails records no placement.
// triedNotes names every host this placement asked and how it answered.
func (p *placePlan) triedNotes() string {
	var parts []string
	for _, c := range p.candidates {
		if c.outcome != "" {
			parts = append(parts, c.name+" "+c.outcome)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return " (tried: " + strings.Join(parts, "; ") + ")"
}

func (p *placePlan) preflightNotes() string {
	var parts []string
	for _, c := range p.candidates {
		if c.preflight != nil && !c.preflight.OK {
			parts = append(parts, c.name+" "+strings.Join(c.preflight.reasons(), ","))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return " (preflight: " + strings.Join(parts, "; ") + ")"
}

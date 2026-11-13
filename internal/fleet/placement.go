package fleet

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	neturl "net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/handoffkeep/client"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// HostView is one machine's connectivity and load as reported by the hub's
// host list. LoadRatio is load5/ncpu; a nil ratio means the hub could not
// measure it. A wakeable machine absent from the list is treated as asleep;
// other unlisted machines are treated as awake and proven by the spawn leg.
type HostView struct {
	Name      string
	Connected bool
	LoadRatio *float64
	// Score is the hub's placement score: free capacity (the product of the
	// cpu, mem, io, load and jobs terms) times the host's power, less the
	// claims inside the spawn bridge. Higher is better; nil when the hub did
	// not send one.
	Score   *float64
	Reason  string
	Signals PlacementSignals
	Source  string
	// Decided marks the host the hub's placement named, after its all_loaded
	// and hysteresis rules; DecisionReason is why ("" for the policy-order
	// winner, "challenger", "all_loaded"). Ranking prefers it within a bucket,
	// so a rival has to beat the winner by the hub's margin, not by a point.
	Decided        bool
	DecisionReason string
	// SlotFull marks a host the hub reported slot-full and left out of its
	// candidates: absent from the list, it is still not a place to spawn, and
	// is not the same as a host the hub did not report (see plan).
	SlotFull bool
	// Extra is what only the shadow comparison reads (placement_shadow.go);
	// no live decision looks at it.
	Extra HostExtra
}

// PresenceRecord is a pacelever host-presence row: which host measured which
// shared-quota account key and when.
type PresenceRecord struct {
	HostID     string    `json:"host_id"`
	AccountKey string    `json:"account_key"`
	LastSeen   time.Time `json:"last_seen"`
}

// SharedAccount is the shared quota cache state of one account key. A degraded
// read counts as unknown, never as an absent account.
type SharedAccount struct {
	Key        string
	Observed   bool
	Fresh      bool
	Degraded   bool
	MeasuredAt time.Time
	Quota      *PoolView
	// State names the source that answered: the hub's fleet state or the
	// direct shared-cache read (fleet_state.go). Nil for a direct source.
	State *StateUse
}

// PlacementSource supplies the external reads a placement decision needs:
// the hub's host list, pacelever's host-presence records and the shared quota
// cache. A read error degrades the corresponding inputs to "unknown".
type PlacementSource interface {
	Hosts(ctx context.Context, cwd string) ([]HostView, error)
	Presence(ctx context.Context) ([]PresenceRecord, error)
	SharedAccount(ctx context.Context, key string) (SharedAccount, error)
}

// WakeRequest describes one wake attempt on an asleep host.
type WakeRequest struct {
	HoldMinutes int
	WaitSeconds int
	Reason      string
}

// Waker wakes an asleep host. No production implementation remains (the
// `panewire node wake` command was removed); tests substitute a fake, and
// an unset Wake reports "no waker configured".
type Waker interface {
	Wake(ctx context.Context, host string, req WakeRequest) error
}

const (
	loginPresent = "present"
	loginUnknown = "unknown"
)

var (
	tagPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
	accountFP  = regexp.MustCompile(`^[0-9a-f]{16}$`)
	// cloneURL whitelists git clone transports: https, ssh and git schemes
	// plus scp-style user@host:path. ext/fd/file and other helper transports
	// are refused because they can execute local commands.
	cloneURL = regexp.MustCompile(`^(https|ssh|git)://[^\s]+$|^[A-Za-z0-9._-]+@[A-Za-z0-9._-]+:[^\s]+$`)
)

// credentialedURL reports whether a clone URL embeds credentials: an https
// userinfo (token-as-username or basic auth) or any scheme carrying a
// password. A bare ssh user (git@host) is not a credential.
func credentialedURL(url string) bool {
	u, e := neturl.Parse(url)
	if e != nil || u.User == nil {
		return false
	}
	if u.Scheme == "https" {
		return true
	}
	_, hasPass := u.User.Password()
	return hasPass
}

func parseTags(raw string) ([]string, error) {
	var out []string
	for _, t := range strings.Split(raw, ",") {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if !tagPattern.MatchString(t) {
			return nil, fail(exitUsage, "invalid tag %q", t)
		}
		if !slices.Contains(out, t) {
			out = append(out, t)
		}
	}
	return out, nil
}

// parseHosts reads the comma-separated --exclude-host list.
func parseHosts(raw string) ([]string, error) {
	var out []string
	for _, h := range strings.Split(raw, ",") {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		if !validID(h) {
			return nil, fail(exitUsage, "invalid excluded host %q", h)
		}
		if !slices.Contains(out, h) {
			out = append(out, h)
		}
	}
	return out, nil
}

// parsePools reads the comma-separated --exclude-pool list.
func parsePools(raw string) ([]string, error) {
	var out []string
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if !validID(p) {
			return nil, fail(exitUsage, "invalid excluded pool %q", p)
		}
		if !slices.Contains(out, p) {
			out = append(out, p)
		}
	}
	return out, nil
}

func exitCode(e error) int {
	var x *ExitError
	if errors.As(e, &x) {
		return x.Code
	}
	return 0
}

func number(m map[string]any, k string) (float64, bool) {
	switch v := m[k].(type) {
	case int64:
		return float64(v), true
	case float64:
		return v, true
	}
	return 0, false
}

type placeCandidate struct {
	name     string
	cfg      map[string]any
	local    bool
	tags     []string
	order    int
	eligible bool
	missing  []string
	// missingCap is the seat capability key this host does not carry. The
	// flag lives in the local copy of hosts.toml, so a candidate without it
	// can only refuse the spawn — offering it would burn a delegation round
	// and its rc70 refusal would stop the whole round early.
	missingCap string
	// roleRefusal names a role or grade the host's roles/grades lists do
	// not accept (host_fit.go).
	roleRefusal string
	awake       bool
	wakeable    bool
	excluded    string
	login       string
	judgment    *quotaJudgment
	load        *float64
	score       *float64
	signals     PlacementSignals
	source      string
	// decided and decision carry the hub's decision: the host it named and why.
	decided  bool
	decision string
	over     bool
	outcome  string
	// preflight is the remote-spawn preflight of a candidate the placement
	// refused; it names why the host was not usable.
	preflight *hostPreflight
}

type placePlan struct {
	tags       []string
	profile    string
	pool       string
	repo       string
	waited     time.Duration
	threshold  float64
	at         time.Time
	candidates []placeCandidate
	ordered    []*placeCandidate
	sleepers   []*placeCandidate
	wake       map[string]any
	degraded   []string
	// hostsDegraded marks a failed hub host-list read: unlisted wakeable
	// machines are then treated as awake so SSH proves them, and no wake
	// reason may fire off a missing reading.
	hostsDegraded bool
	// hub, presence and presenceErr keep the reads the plan was built from so
	// a refused candidate's preflight does not ask the hub or hk again.
	hub         map[string]HostView
	presence    []PresenceRecord
	presenceErr bool
}

type candResult struct {
	ok          bool
	stop        error
	unreachable bool
	// quotaRefused marks a candidate the quota gate refused before any SSH.
	quotaRefused bool
	rc           int
	note         string
	res          Result
}

func (a *App) loadThreshold() float64 {
	switch n := a.Config.Placement["load_threshold"].(type) {
	case int64:
		return float64(n)
	case float64:
		return n
	}
	return 1.0
}

// sharedFreshTTL is the freshness window for one pool's shared quota reading:
// quota.pool_fresh_ttl_s[pool], then quota.fresh_ttl_s, then 900 s for claude
// and 300 s for every other pool. It is pacelever's rule (shared_quota.go
// freshTTL), so the status view and the gate agree on a reading
// (handoff/2026-11-05/pacelever-design-local-gate-stale-ack §4).
func (a *App) sharedFreshTTL(pool string) time.Duration {
	if m, ok := a.Config.Quota["pool_fresh_ttl_s"].(map[string]any); ok {
		if n := integer(m, pool, 0); n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	if pool == "claude" {
		return a.placementTTL("fresh_ttl_s", 900)
	}
	return a.placementTTL("fresh_ttl_s", 300)
}

// accountPool is the pool part of an account key ("claude:<fp>" -> "claude").
func accountPool(key string) string {
	pool, _, _ := strings.Cut(key, ":")
	return pool
}

func (a *App) placementTTL(key string, def int64) time.Duration {
	if n := integer(a.Config.Quota, key, 0); n > 0 {
		return time.Duration(n) * time.Second
	}
	if key == "fresh_ttl_s" && def == 300 {
		return shared.OtherFreshTTL
	}
	return time.Duration(def) * time.Second
}

func (a *App) loadRatio(ctx context.Context) (float64, error) {
	var text string
	if runtime.GOOS == "linux" {
		b, e := os.ReadFile("/proc/loadavg")
		if e != nil {
			return 0, e
		}
		text = string(b)
	} else {
		r := a.call(ctx, "sysctl", "-n", "vm.loadavg")
		if e := check(r, "load measurement"); e != nil {
			return 0, e
		}
		text = r.Out
	}
	fields := strings.Fields(strings.Trim(text, "{} \n"))
	if len(fields) < 2 {
		return 0, fail(exitConfig, "host load unavailable")
	}
	load, e := strconv.ParseFloat(fields[1], 64)
	return load / float64(runtime.NumCPU()), e
}

func (a *App) localLoad(ctx context.Context) *float64 {
	if a.LoadRatio == nil {
		return nil
	}
	r, e := a.LoadRatio(ctx)
	if e != nil || r < 0 {
		return nil
	}
	return &r
}

func (a *App) hostViews(ctx context.Context, cwd string) (map[string]HostView, []string) {
	if a.Source == nil {
		return nil, []string{"hosts_unavailable"}
	}
	views, e := a.Source.Hosts(ctx, cwd)
	if e != nil {
		return nil, []string{"hosts_degraded"}
	}
	m := map[string]HostView{}
	for _, v := range views {
		m[v.Name] = v
	}
	return m, nil
}

// neededAccounts resolves which shared account keys a quota pool needs:
// explicitly configured fingerprints, or the keys presence records carry for
// the pool.
func neededAccounts(quota map[string]any, pool string, presence []PresenceRecord) []string {
	var out []string
	if pool == "" {
		return out
	}
	if accounts, ok := quota["accounts"].(map[string]any); ok {
		if fp := str(accounts, pool); accountFP.MatchString(fp) {
			return []string{pool + ":" + fp}
		}
	}
	for _, rec := range presence {
		fp, ok := strings.CutPrefix(rec.AccountKey, pool+":")
		if ok && accountFP.MatchString(fp) && !slices.Contains(out, rec.AccountKey) {
			out = append(out, rec.AccountKey)
		}
	}
	return out
}

// hostAccount selects the most recent fresh presence for this host and pool.
// Equal-time conflicting keys are ambiguous; an older login must never replace
// the current account merely because its quota happens to be available.
func hostAccount(presence []PresenceRecord, pool, host string, now time.Time, ttl time.Duration) string {
	var newest time.Time
	key := ""
	ambiguous := false
	for _, rec := range presence {
		fp, ok := strings.CutPrefix(rec.AccountKey, pool+":")
		age := now.Sub(rec.LastSeen)
		if pool == "" || rec.HostID != host || !ok || !accountFP.MatchString(fp) || age < -time.Minute || age > ttl {
			continue
		}
		if rec.LastSeen.After(newest) {
			newest, key, ambiguous = rec.LastSeen, rec.AccountKey, false
		} else if rec.LastSeen.Equal(newest) && rec.AccountKey != key {
			ambiguous = true
		}
	}
	if ambiguous {
		return ""
	}
	return key
}

func (a *App) remoteAccount(ctx context.Context, pool, host string, cfg map[string]any) (string, SharedAccount) {
	if a.Source == nil || pool == "" {
		return "", SharedAccount{}
	}
	presence, err := a.Source.Presence(ctx)
	if err != nil {
		return "", SharedAccount{}
	}
	key := hostAccount(presence, pool, firstNonempty(str(cfg, "presence_id"), host), a.Now(), a.placementTTL("presence_ttl_s", 86400))
	if key == "" {
		return "", SharedAccount{}
	}
	acc, err := a.Source.SharedAccount(ctx, key)
	if err != nil {
		acc = SharedAccount{Key: key, Degraded: true}
	}
	return key, acc
}

// candidateLogin classes a host's evidence for holding the needed account.
// "present" requires a fresh presence record corroborated by a readable
// shared-cache observation; everything else is "unknown" — a degraded or
// missing reading is never treated as absent.
func (a *App) candidateLogin(presence []PresenceRecord, accounts map[string]SharedAccount, keys []string, hostID string, now time.Time, ttl time.Duration) string {
	if len(keys) == 0 {
		return ""
	}
	for _, rec := range presence {
		if rec.HostID != hostID || !slices.Contains(keys, rec.AccountKey) {
			continue
		}
		age := now.Sub(rec.LastSeen)
		if age < -time.Minute || age > ttl {
			continue
		}
		// Login evidence needs a readable shared observation, not a fresh one:
		// quota freshness is the quota gate's job (it re-measures a stale
		// account). Requiring Fresh here made every host "no_presence" once
		// nothing polled the shared store between spawns.
		acc := accounts[rec.AccountKey]
		mAge := now.Sub(acc.MeasuredAt)
		if acc.Observed && !acc.Degraded && (acc.Fresh || mAge >= -time.Minute && mAge <= ttl) {
			return loginPresent
		}
	}
	return loginUnknown
}

func (a *App) plan(ctx context.Context, s Spawn, pool string, waited time.Duration) (*placePlan, error) {
	required, e := parseTags(s.Tags)
	if e != nil {
		return nil, e
	}
	excluded, e := parseHosts(s.ExcludeHosts)
	if e != nil {
		return nil, e
	}
	threshold := a.loadThreshold()
	hub, degraded := a.hostViews(ctx, s.CWD)
	var presence []PresenceRecord
	presencesDegraded := pool != "" && a.Source == nil
	if pool != "" && a.Source != nil {
		if records, e := a.Source.Presence(ctx); e != nil {
			presencesDegraded = true
		} else {
			presence = records
		}
	}
	// Remote accounts come from each host's presence, independently of the
	// spawner's configured pool fingerprint.
	keys := neededAccounts(nil, pool, presence)
	accounts := map[string]SharedAccount{}
	if a.Source != nil {
		for _, k := range keys {
			acc, e := a.Source.SharedAccount(ctx, k)
			if e != nil {
				acc = SharedAccount{Key: k, Degraded: true}
			}
			accounts[k] = acc
		}
	}
	if presencesDegraded {
		degraded = append(degraded, "presence_degraded")
	}
	if len(keys) > 0 {
		known := false
		for _, acc := range accounts {
			if acc.Observed && !acc.Degraded {
				known = true
			}
		}
		if !known {
			degraded = append(degraded, "quota_cache_degraded")
		}
	}
	now := a.Now()
	presTTL := a.placementTTL("presence_ttl_s", 86400)
	enum := []placeCandidate{}
	machine := a.machine()
	local := placeCandidate{name: machine, cfg: a.Config.Local, local: true, awake: true, tags: hostCaps(a.Config.Local)}
	if !boolean(a.Config.Local, "spawn", true) {
		local.excluded = "spawn_disabled"
	}
	local.load = a.localLoad(ctx)
	if hv, reported := hub[machine]; reported {
		local.score = hv.Score
		local.signals = hv.Signals
		local.source = hv.Source
	}
	maxRatio, hasGuard := number(a.Config.Local, "max_load_ratio")
	if local.load != nil && hasGuard && *local.load >= maxRatio {
		local.over = true
	}
	if local.load == nil && hasGuard {
		// A broken load probe must not silently disable the pressure
		// guard; treat local as pressured like the old fail-closed path.
		local.over = true
		degraded = append(degraded, "local_load_degraded")
	}
	if _, ok := a.Config.Local["max_active"]; ok {
		agents, e := a.agents(ctx)
		if e != nil {
			return nil, e
		}
		count := int64(0)
		for _, agent := range agents {
			if liveStatus(agent.Status) {
				count++
			}
		}
		if count >= integer(a.Config.Local, "max_active", 0) {
			local.over = true
		}
	}
	local.login = a.candidateLogin(presence, accounts, neededAccounts(a.Config.Quota, pool, presence), firstNonempty(str(a.Config.Local, "presence_id"), machine), now, presTTL)
	if presencesDegraded && local.login == "" {
		local.login = loginUnknown
	}
	enum = append(enum, local)
	for i, name := range a.remoteHostNames() {
		h := a.Config.Hosts[name]
		c := placeCandidate{name: name, cfg: h, order: i + 1, tags: hostCaps(h), wakeable: str(h, "wake") == "panewire"}
		if a.fleetLog != nil && a.fleetLog.slotFull[name] || hub[name].SlotFull {
			// The hub left the host out because its task slots are full.
			// Unlisted does not mean unknown here: never fall back to
			// proving it by spawning.
			c.excluded = "task_slots_full"
		}
		if hv, reported := hub[name]; reported {
			switch {
			case c.excluded != "":
			case strings.Contains(hv.Reason, "not_accepting"):
				c.excluded = "not_accepting"
			case !hv.Connected:
			default:
				c.awake = true
			}
			c.load = hv.LoadRatio
			c.score = hv.Score
			c.signals = hv.Signals
			c.source = hv.Source
			c.decided, c.decision = hv.Decided, hv.DecisionReason
		} else {
			c.awake = !c.wakeable || hub == nil
		}
		c.login = a.candidateLogin(presence, accounts, keys, firstNonempty(str(h, "presence_id"), name), now, presTTL)
		if presencesDegraded && c.login == "" {
			c.login = loginUnknown
		}
		enum = append(enum, c)
	}
	p := &placePlan{tags: required, profile: s.Profile, pool: pool, waited: waited, threshold: threshold, at: now, degraded: degraded, hostsDegraded: hub == nil, hub: hub, presence: presence, presenceErr: presencesDegraded}
	needCap := ""
	if seat, ok := a.Policy.Profiles[s.Profile]; ok && seat.Capability != "standard" {
		needCap = seat.Capability
	}
	// A host the readiness filter dropped for this harness is not a fallback
	// either: selection chose among ready pairs, and placement must not walk
	// past it to one that is not.
	view := a.newReadinessView(ctx, s.CWD)
	kind := a.Policy.Profiles[s.Profile].Kind
	for i := range enum {
		c := &enum[i]
		notReady := view.drop(c.name, c.cfg, kind)
		if c.excluded == "" && s.AccountKey != "" && hostAccount(presence, pool, firstNonempty(str(c.cfg, "presence_id"), c.name), now, presTTL) != s.AccountKey {
			c.excluded = "different_account"
		}
		if c.excluded == "" && slices.Contains(excluded, c.name) {
			c.excluded = "excluded_host"
		}
		c.missing = missingCaps(c.cfg, required)
		if needCap != "" && !boolean(c.cfg, needCap, false) {
			c.missingCap = needCap
		}
		c.roleRefusal = roleRefusal(c.cfg, s.Role, s.TaskGrade)
		c.eligible = len(c.missing) == 0 && c.missingCap == "" && c.roleRefusal == ""
		if !c.over && c.load != nil && *c.load >= threshold {
			c.over = true
		}
		switch {
		case c.excluded != "":
			c.outcome = c.excluded
		case c.missingCap != "":
			c.outcome = "missing_capability:" + c.missingCap
		case c.roleRefusal != "":
			c.outcome = c.roleRefusal
		case notReady != "":
			c.outcome = notReady
		case !c.eligible:
			c.outcome = "missing_tags:" + strings.Join(c.missing, ",")
		case c.awake:
			p.ordered = append(p.ordered, c)
		case c.wakeable:
			p.sleepers = append(p.sleepers, c)
		default:
			c.outcome = "disconnected"
		}
	}
	p.candidates = enum
	sortPlaceCandidates(p.ordered, threshold, s.SelHost)
	return p, nil
}

// freshReadGeneration starts a new generation of the hub reads a decision
// ranks on: the fleet snapshot, success or failure, is read again, so nothing
// taken before the decision's gate (selection, an earlier decision) stands in
// for this decision's own read.
func (a *App) freshReadGeneration() {
	if a.fleetLog != nil {
		a.fleetLog.fetchedAt, a.fleetLog.snapshot, a.fleetLog.err = time.Time{}, nil, nil
	}
}

// carryTried keeps, on the new plan, what the attempts on tried hosts proved
// about them, as the old plan recorded it: the outcome, the quota judgment,
// the preflight and the reading the attempt was made on. The rest of the new
// plan is this decision's own read.
func (np *placePlan) carryTried(old *placePlan, tried map[string]bool) {
	for i := range np.candidates {
		c := &np.candidates[i]
		if !tried[c.name] {
			continue
		}
		prev := old.candidate(c.name)
		if prev == nil {
			continue
		}
		c.outcome, c.judgment, c.preflight = prev.outcome, prev.judgment, prev.preflight
		c.score, c.signals, c.load, c.source = prev.score, prev.signals, prev.load, prev.source
		c.decided, c.decision, c.over = prev.decided, prev.decision, prev.over
	}
	np.repo, np.wake = old.repo, old.wake
}

// sortPlaceCandidates orders awake candidates: load bucket, the host-first
// selection, login evidence, the hub's decision, then score (or load when a
// candidate has no score). plan sorts with it, and a refusal re-sorts the
// candidates still to try on fresh hub scores.
func sortPlaceCandidates(ordered []*placeCandidate, threshold float64, selHost string) {
	// One metric for the whole list: mixing score and load per pair would make
	// the comparator intransitive when only some hosts carry a hub score.
	byScore := len(ordered) > 0
	for _, c := range ordered {
		if c.score == nil {
			byScore = false
		}
	}
	slices.SortStableFunc(ordered, func(x, y *placeCandidate) int {
		if bx, by := placeBucket(*x, threshold), placeBucket(*y, threshold); bx != by {
			return bx - by
		}
		// Host-first selection already chose this machine for the account;
		// the load bucket still wins so a pressured host stays a last resort.
		if px, py := x.name == selHost, y.name == selHost; px != py {
			if px {
				return -1
			}
			return 1
		}
		if rx, ry := loginRank(x.login), loginRank(y.login); rx != ry {
			return rx - ry
		}
		// The hub's decision already applies its hysteresis margin and its
		// all-hosts-loaded rule to the scores below; within a bucket it ranks
		// first, so a rival that scores a little higher does not take over.
		if x.decided != y.decided {
			if x.decided {
				return -1
			}
			return 1
		}
		// The hub score already folds load and host speed together, so a
		// fast machine at moderate load beats a slow idle one. Raw load is
		// the fallback when any candidate has no score.
		if byScore {
			if c := cmp.Compare(*y.score, *x.score); c != 0 {
				return c
			}
		} else if c := cmp.Compare(loadOrInf(x.load), loadOrInf(y.load)); c != 0 {
			return c
		}
		return x.order - y.order
	})
}

// loginRank orders login evidence: positive evidence and "no account needed"
// rank equally ahead of unknown evidence. It is symmetric so the comparator
// above stays a strict weak ordering.
func loginRank(login string) int {
	if login == loginUnknown {
		return 1
	}
	return 0
}

func loadOrInf(l *float64) float64 {
	if l == nil {
		return 1e9
	}
	return *l
}

// placeBucket orders awake candidates: hosts with positive login evidence and
// a measured load under the threshold first, then unknown-login hosts under
// the threshold, then candidates with no usable load reading, and only when
// nothing else qualifies any host at or above the load threshold.
func placeBucket(c placeCandidate, threshold float64) int {
	if c.over {
		return 4
	}
	under := c.load != nil && *c.load < threshold
	if c.login != loginUnknown {
		if under {
			return 0
		}
		return 2
	}
	if under {
		return 1
	}
	return 3
}

// record renders the placement decision written to the job record: every
// evaluated input (tags, pool, repo, wait), the per-candidate classification
// and the chosen host.
func (p *placePlan) record(chosen string) map[string]any {
	candidates := []any{}
	for _, c := range p.candidates {
		row := map[string]any{"host": c.name, "eligible": c.eligible, "awake": c.awake, "login": c.login, "over_threshold": c.over}
		if c.wakeable {
			row["wakeable"] = true
		}
		if c.local {
			row["local"] = true
		}
		if c.load != nil {
			row["load_ratio"] = *c.load
		}
		if c.score != nil {
			row["score"] = *c.score
		}
		if len(c.missing) > 0 {
			row["missing_tags"] = c.missing
		}
		if c.outcome != "" {
			row["outcome"] = c.outcome
		}
		if c.judgment != nil {
			row["quota_gate"] = *c.judgment
		}
		if c.preflight != nil {
			row["preflight"] = c.preflight.record()
		}
		if c.name == chosen {
			row["chosen"] = true
		}
		candidates = append(candidates, row)
	}
	rec := map[string]any{"source": "policy", "tags": p.tags, "profile": p.profile, "waited_s": int64(p.waited.Seconds()), "load_threshold": p.threshold, "candidates": candidates, "chose": chosen, "at": p.at.Format(time.RFC3339Nano)}
	if p.pool != "" {
		rec["pool"] = p.pool
	}
	if p.repo != "" {
		rec["repo"] = p.repo
	}
	if len(p.degraded) > 0 {
		rec["degraded"] = p.degraded
	}
	if p.wake != nil {
		rec["wake"] = p.wake
	}
	rec["ranking"] = p.ranking()
	return rec
}

// allOver reports whether every awake ordered candidate sits at or above
// the load threshold — the precondition for the waited-long-enough wake rule.
func (p *placePlan) allOver() bool {
	if len(p.ordered) == 0 {
		return false
	}
	for _, c := range p.ordered {
		if !c.over {
			return false
		}
	}
	return true
}

func (p *placePlan) candidate(name string) *placeCandidate {
	for i := range p.candidates {
		if p.candidates[i].name == name {
			return &p.candidates[i]
		}
	}
	return nil
}

// wakeTargets applies the policy's wake rules once the awake round is over:
// a required capability no awake host has, every awake candidate over the
// load threshold after the job has waited long enough, the needed account
// measured only on an asleep host, or an operator request. Wakeable hosts
// that proved unreachable during the round join the target list whenever a
// policy reason fires; on their own they are not a wake reason.
func (a *App) wakeTargets(s Spawn, p *placePlan, unreachable []string) ([]string, []string) {
	var reasons, targets []string
	add := func(name string) {
		if !slices.Contains(targets, name) {
			targets = append(targets, name)
		}
	}
	if len(p.ordered) == 0 && len(p.sleepers) > 0 {
		reasons = append(reasons, "capability_absent_awake")
		for _, c := range p.sleepers {
			add(c.name)
		}
	}
	if p.waited >= a.wakeAfter() && p.allOver() {
		reasons = append(reasons, "all_awake_over_threshold")
		for _, c := range p.sleepers {
			add(c.name)
		}
	}
	if s.OperatorRequest != "" || s.Purpose == "predeploy" {
		reason := "operator_requested"
		if s.OperatorRequest == "" {
			reason = "predeploy_slow_suite"
		}
		for _, c := range p.sleepers {
			add(c.name)
		}
		if len(targets) > 0 {
			reasons = append(reasons, reason)
		}
	}
	// The needed account is logged in only on an asleep host when fresh
	// presence exists there and no eligible awake host shows it. Degraded
	// presence reads make this unprovable, so they never wake.
	presentOnAwake := false
	for _, c := range p.ordered {
		if c.login == loginPresent {
			presentOnAwake = true
		}
	}
	presentSleepers := []string{}
	for _, c := range p.sleepers {
		if c.login == loginPresent {
			presentSleepers = append(presentSleepers, c.name)
		}
	}
	if !presentOnAwake && len(presentSleepers) > 0 {
		reasons = append(reasons, "account_only_on")
		merged := slices.Clone(presentSleepers)
		for _, t := range targets {
			if !slices.Contains(merged, t) {
				merged = append(merged, t)
			}
		}
		targets = merged
	}
	if len(reasons) > 0 {
		for _, name := range unreachable {
			add(name)
		}
	}
	return targets, reasons
}

func (a *App) wakeAfter() time.Duration {
	if n := integer(a.Config.Placement, "wake_after_s", 0); n > 0 {
		return time.Duration(n) * time.Second
	}
	return 10 * time.Minute
}

// waitForNode polls the hub host list and an SSH probe until the woken node
// reports connected, then lets the caller place the job.
func (a *App) waitForNode(ctx context.Context, host string, h map[string]any, waitS int) error {
	if waitS <= 0 {
		waitS = 90
	}
	deadline := a.Now().Add(time.Duration(waitS) * time.Second)
	target := str(h, "ssh")
	if target == "" {
		target = host
	}
	for {
		if a.Source != nil {
			if views, e := a.Source.Hosts(ctx, ""); e == nil {
				for _, v := range views {
					if v.Name == host && v.Connected {
						return nil
					}
				}
			}
		}
		// A hub host is proven by its hub connection alone; ssh never runs.
		if str(h, "via") != "hub" {
			if r := a.call(ctx, "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "--", target, "true"); r.Code == 0 {
				return nil
			}
		}
		if !a.Now().Before(deadline) {
			return fail(exitPlacement, "node %s did not connect after wake", host)
		}
		if e := a.Sleep(ctx, 5*time.Second); e != nil {
			return e
		}
	}
}

type repoResolution struct {
	sourceDir string // caller-side evidence path, never transmitted or executed by the hub
	name      string
	url       string
	rel       string
	// wt, branch and head describe a linked worktree checkout: name is then
	// the canonical clone and wt the worktree directory beside it. wt is
	// empty for a canonical checkout.
	wt     string
	branch string
	head   string
	// localRoot is the directory holding the local checkout; root is the
	// remote work root chosen by remoteDir ("" means ~/work).
	localRoot string
	root      string
}

// localRepo resolves the job checkout: the repo name from the top-level
// basename, the origin URL used to clone it on a target, and the cwd's path
// inside the checkout.
func (a *App) localRepo(ctx context.Context, cwd string) *repoResolution {
	if a.Repository != nil {
		r := a.Repository
		repo := &repoResolution{name: r.Name, url: r.Origin, rel: r.Relative, wt: r.Worktree, localRoot: r.LocalRoot}
		if r.Checkout != nil {
			repo.branch, repo.head = r.Checkout.Branch, r.Checkout.Sha
		}
		return repo
	}

	r := a.call(ctx, "git", "-C", cwd, "rev-parse", "--show-toplevel")
	top := strings.TrimSpace(r.Out)
	if r.Code != 0 || top == "" {
		return nil
	}
	name := filepath.Base(top)
	if !validID(name) {
		return nil
	}
	wt := ""
	if g := a.call(ctx, "git", "-C", cwd, "rev-parse", "--path-format=absolute", "--git-dir", "--git-common-dir"); g.Code == 0 {
		// A linked worktree has its own git dir apart from the common one,
		// which is the canonical checkout's .git. The clone on a target is
		// named after the canonical checkout, not the worktree.
		if dirs := strings.Fields(g.Out); len(dirs) == 2 && dirs[0] != dirs[1] && filepath.Base(dirs[1]) == ".git" {
			if canon := filepath.Dir(dirs[1]); validID(filepath.Base(canon)) {
				wt, name = name, filepath.Base(canon)
			}
		}
	}
	// --show-prefix comes from git itself, so symlinked checkout roots
	// (/var vs /private/var) still map onto the right subdirectory.
	rel := ""
	if g := a.call(ctx, "git", "-C", cwd, "rev-parse", "--show-prefix"); g.Code == 0 {
		rel = strings.TrimSuffix(strings.TrimSpace(g.Out), "/")
	}
	url := ""
	if g := a.call(ctx, "git", "-C", cwd, "remote", "get-url", "origin"); g.Code == 0 {
		url = strings.TrimSpace(g.Out)
		if !cloneURL.MatchString(url) || credentialedURL(url) {
			url = ""
		}
	}
	repo := &repoResolution{name: name, url: url, rel: rel, wt: wt, localRoot: filepath.Dir(top)}
	if wt != "" {
		if g := a.call(ctx, "git", "-C", cwd, "branch", "--show-current"); g.Code == 0 {
			repo.branch = strings.TrimSpace(g.Out)
		}
		if g := a.call(ctx, "git", "-C", cwd, "rev-parse", "HEAD"); g.Code == 0 {
			repo.head = strings.TrimSpace(g.Out)
		}
	}
	return repo
}

// carriesCheckout reports whether the spawn carries a linked-worktree
// checkout the target node can clone (node_worktree.go). A canonical checkout
// does not: it launches in a cwd_map path.
func (a *App) carriesCheckout(repo *repoResolution) bool {
	if a.Repository != nil && a.Repository.Checkout != nil {
		return true
	}
	return repo != nil && repo.wt != ""
}

// remotePlan is where a job runs on a target and what must exist there
// first: the repo clone under root, and for a linked local worktree the
// matching worktree beside it.
type remotePlan struct {
	cwd      string
	repo     *repoResolution // clone to ensure; nil when nothing is prepared
	worktree string          // remote worktree directory to prepare; "" for none
}

// remoteDir maps the job cwd onto the target. An explicit cwd_map entry stays
// authoritative for the path; otherwise the repo convention applies: the
// host's work root (derived from its cwd_map entries, else $HOME/work) holds
// the clone and, for a linked local worktree, the worktree next to it.
func remoteDir(h map[string]any, cwd string, repo *repoResolution) (remotePlan, error) {
	if mapped := mappedCWD(h["cwd_map"], cwd); mapped != "" {
		if repo == nil || repo.wt == "" {
			return remotePlan{cwd: mapped}, nil
		}
		wtDir := strings.TrimSuffix(mapped, "/"+repo.rel)
		clone := *repo
		clone.root = path.Dir(wtDir)
		return remotePlan{cwd: mapped, repo: &clone, worktree: wtDir}, nil
	}
	if repo == nil {
		return remotePlan{}, nil
	}
	root, err := remoteWorkRoot(h, repo.localRoot)
	if err != nil {
		return remotePlan{}, err
	}
	clone := *repo
	clone.root = root
	dir := root + "/" + repo.name
	plan := remotePlan{repo: &clone}
	if repo.wt != "" {
		dir = root + "/" + repo.wt
		plan.worktree = dir
	}
	if repo.rel != "" {
		dir += "/" + repo.rel
	}
	plan.cwd = dir
	return plan, nil
}

// remoteWorkRoot derives the target's work root from its cwd_map entries that
// map a checkout living directly under the local work root, so a host with
// explicit entries keeps its layout for paths they do not cover. Without
// such entries it is ~/work.
func remoteWorkRoot(h map[string]any, localRoot string) (string, error) {
	mapping, _ := h["cwd_map"].(map[string]any)
	roots := map[string]bool{}
	for from, v := range mapping {
		to, ok := v.(string)
		if !ok || (!strings.HasPrefix(to, "/") && !strings.HasPrefix(to, "~/")) {
			continue
		}
		from, to = path.Clean(from), path.Clean(to)
		switch {
		case from == localRoot:
			roots[to] = true
		case path.Dir(from) == localRoot:
			roots[path.Dir(to)] = true
		}
	}
	switch len(roots) {
	case 0:
		return "~/work", nil
	case 1:
		for r := range roots {
			return r, nil
		}
	}
	return "", fail(exitConfig, "cwd_map entries disagree on the remote work root; add an explicit entry for this path")
}

// remotePath renders a remote path for an ssh command line: a leading ~ is
// left to the remote $HOME, everything else is single-quoted.
func remotePath(p string) string {
	if p == "~" {
		return `"$HOME"`
	}
	if rest, ok := strings.CutPrefix(p, "~/"); ok {
		return `"$HOME"` + shellQuote("/"+rest)
	}
	return shellQuote(p)
}

func (a *App) sshRun(ctx context.Context, h map[string]any, host, script string, opts ...string) Result {
	target := str(h, "ssh")
	if target == "" {
		target = host
	}
	args := append([]string{"-o", "BatchMode=yes", "-o", "ConnectTimeout=5"}, opts...)
	return a.call(ctx, "ssh", append(args, "--", target, script)...)
}

func (a *App) ensureRepo(ctx context.Context, h map[string]any, host string, repo *repoResolution) error {
	root := firstNonempty(repo.root, "~/work")
	dir := remotePath(root + "/" + repo.name)
	r := a.sshRun(ctx, h, host, "test -d "+dir)
	if r.Code == 0 {
		if repo.url == "" {
			return nil
		}
		o := a.sshRun(ctx, h, host, "git -C "+dir+" remote get-url origin")
		if o.Code == exitUnreachable {
			return fail(exitUnreachable, "repo origin check unreachable")
		}
		if got := strings.TrimSpace(o.Out); o.Code != 0 || got != repo.url {
			return fail(exitConfig, "repo %s on %s has an origin that does not match the local checkout", repo.name, host)
		}
		return nil
	}
	if r.Code == exitUnreachable {
		return fail(exitUnreachable, "repo check unreachable")
	}
	if repo.url == "" {
		return fail(exitConfig, "repo %s missing on %s and no origin URL to clone", repo.name, host)
	}
	r = a.sshRun(ctx, h, host, "mkdir -p "+remotePath(root)+" && git clone -- "+shellQuote(repo.url)+" "+dir, "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=4")
	if r.Code == exitUnreachable {
		return fail(exitUnreachable, "repo clone unreachable")
	}
	if e := check(r, "repo clone"); e != nil {
		return e
	}
	return nil
}

func (a *App) spawnRemote(ctx context.Context, s Spawn, brief string, c *placeCandidate, repo *repoResolution, forced bool) candResult {
	h := c.cfg
	if s.ExecutionLane == "mock" {
		if forced {
			return candResult{stop: fail(exitMockRefused, "remote mock spawns refused")}
		}
		return candResult{rc: exitMockRefused, note: "mock_lane_remote_refused"}
	}
	if str(h, "via") == "hub" {
		return a.spawnHub(ctx, s, brief, c, repo, forced)
	}
	plan, e := remoteDir(h, s.CWD, repo)
	if e != nil {
		return candResult{stop: e}
	}
	cwd := plan.cwd
	if cwd == "" {
		return candResult{note: "repo_unmappable"}
	}
	if s.DryRun {
		payload, e := a.prepareBriefForHost(s, c.name, brief)
		if e != nil {
			return candResult{stop: e}
		}
		fmt.Fprintf(a.Out, "dry-run host=%s profile=%s role=%s job=%s\n%s\n", c.name, s.Profile, s.Role, s.Job, payload)
		return candResult{ok: true}
	}
	if plan.repo != nil {
		e := a.ensureRepo(ctx, h, c.name, plan.repo)
		if e == nil && plan.worktree != "" {
			e = a.ensureWorktree(ctx, h, c.name, plan.repo, plan.worktree)
		}
		if e != nil {
			if exitCode(e) == exitUnreachable {
				return candResult{unreachable: true, note: "repo:" + e.Error()}
			}
			if plan.worktree != "" {
				// A job from a linked worktree must run on that branch: fail
				// closed rather than try another host or fall back to local.
				return candResult{stop: e}
			}
			return candResult{rc: exitCode(e), note: "repo:" + e.Error()}
		}
	}
	target := str(h, "ssh")
	if target == "" {
		target = c.name
	}
	if strings.HasPrefix(target, "-") || strings.ContainsAny(target, "\r\n") {
		return candResult{stop: fail(exitConfig, "invalid SSH target")}
	}
	remote := s
	remote.ParentJob = ""
	remote.CWD = cwd
	remote.Host = "local"
	remote.Workspace = str(h, "workspace")
	remote.Prompt = ""
	remote.Tags = ""
	remote.Delegated = true
	remote.Wait = false
	if remote.AccountKey != "" {
		remote.FreshTTLS = int64(a.sharedFreshTTL(accountPool(remote.AccountKey)) / time.Second)
	}
	if s.Report != "" {
		if !within(a.Jobs, s.Report) {
			return candResult{stop: fail(exitUsage, "remote report outside jobs root")}
		}
		rel, e := filepath.Rel(a.Jobs, s.Report)
		if e != nil {
			return candResult{stop: e}
		}
		remote.Report = ""
		remote.ReportRelative = rel
	}
	envelope := struct {
		Spawn Spawn
		Brief string
	}{remote, brief}
	b, e := encode(envelope)
	if e != nil {
		return candResult{stop: e}
	}
	session := str(h, "herdr_session")
	if session == "" {
		session = "worker"
	}
	if s.placement != nil {
		s.placement.contactedHost = c.name
	}
	r := a.Runner.Run(ctx, Command{Name: "ssh", Args: []string{"-o", "BatchMode=yes", "--", target, "env HERDR_SESSION=" + shellQuote(session) + " panewire _spawn-stdin"}, Input: string(b)})
	s.placement.remoteReceipt(c.name, r.Out)
	fmt.Fprint(a.Err, r.Err)
	if strings.Contains(r.Out, "OK pane=") {
		fmt.Fprint(a.Out, strings.TrimRight(r.Out, "\n")+" host="+c.name+"\n")
		if e := check(r, "remote spawn"); e != nil {
			return candResult{stop: e, res: r}
		}
		return candResult{ok: true, res: r}
	}
	if r.Code == 0 {
		return candResult{stop: fail(exitStateConflict, "remote spawn returned no receipt")}
	}
	if r.Code == exitUnreachable {
		return candResult{unreachable: true, rc: r.Code, note: "unreachable", res: r}
	}
	if !forced && r.Code == exitConfig && strings.Contains(r.Err+r.Out, "WORKSPACE_UNCONFIGURED:") {
		return candResult{rc: r.Code, note: "workspace_unconfigured", res: r}
	}
	if forced || slices.Contains([]int{2, 70, 74, 75, 76, 79, 82, exitTrustPrompt}, r.Code) {
		return candResult{stop: check(r, "remote spawn"), res: r}
	}
	return candResult{rc: r.Code, note: fmt.Sprintf("refused rc=%d", r.Code), res: r}
}

func (a *App) place(ctx context.Context, s Spawn, brief string) (handled bool, rec map[string]any, err error) {
	defer func() { a.placementEvent(s, rec, err) }()
	if s.Host != "auto" {
		return a.placeForced(ctx, s, brief)
	}
	return a.placeAuto(ctx, s, brief)
}

// requestRefusal is the gate's refusal of the spawn's request that no quota
// reading can lift (see requestGate), decided once for the chosen rung, or nil.
func (a *App) requestRefusal(ctx context.Context, s Spawn) error {
	gate, ok := a.Quota.(requestGate)
	if !ok {
		return nil
	}
	qr, ok := a.quotaRequest(s)
	if !ok {
		return nil
	}
	return gate.RequestRefusal(ctx, qr)
}

func (a *App) placeAuto(ctx context.Context, s Spawn, brief string) (bool, map[string]any, error) {
	task, _, e := a.taskPrecheck(ctx, s)
	if e != nil {
		return true, nil, e
	}
	// A request the gate refuses whatever any host's reading says fails here
	// with the gate's own reason, before a host is judged or contacted.
	if e := a.requestRefusal(ctx, s); e != nil {
		return true, nil, e
	}
	// One decision at a time through read-rank-count: the next decision must
	// see the job this one counts (hk 4448). The gate is held from the host
	// read to PlacementTry, never through a wake or a launch, and taken again
	// before each later candidate is ranked. release is safe to call twice.
	release := func() {}
	takeGate := func() error {
		release()
		if a.PlacementGate == nil || s.DryRun {
			return nil
		}
		gateRelease, e := a.PlacementGate(ctx)
		if e != nil {
			return fail(exitPlacement, "placement decision gate: %v", e)
		}
		release = sync.OnceFunc(gateRelease)
		return nil
	}
	if e := takeGate(); e != nil {
		return true, nil, e
	}
	defer func() { release() }()
	waited := taskWaited(task, a.Now())
	pool := a.quotaPool(ctx, s)
	s.TaskGrade = s.Grade
	if m := taskMarker.FindStringSubmatch(task.Title); s.TaskGrade == "" && m != nil {
		s.TaskGrade = strings.ToUpper(m[1])
	}
	if topRoutingRole(s.RoutingRole) {
		s.TaskGrade = ""
	}
	// Every decision builds its plan from reads taken inside the gate: the
	// first, each fallback after a refusal, and the one after a wake. The new
	// plan replaces the old; only what the attempts proved about the hosts
	// already tried is carried over (carryTried).
	var p *placePlan
	tried := map[string]bool{}
	var preEnv *preflightEnv
	replan := func() error {
		a.freshReadGeneration()
		if a.PlacementGate != nil && !s.DryRun {
			// Host-first selection named SelHost before the gate, on reads
			// other decisions have since counted jobs against; the account it
			// chose still limits the hosts, and the host is ranked on the
			// scores this decision reads.
			s.SelHost = ""
		}
		np, e := a.plan(ctx, s, pool, waited)
		if e != nil {
			return e
		}
		if p != nil {
			np.carryTried(p, tried)
		}
		p, preEnv = np, nil
		if s.placement != nil {
			s.placement.plan = p
		}
		return nil
	}
	if e := replan(); e != nil {
		return true, nil, e
	}
	var repo *repoResolution
	repoDone := false
	resolveRepo := func() *repoResolution {
		if !repoDone {
			repoDone = true
			repo = a.localRepo(ctx, s.CWD)
			if repo != nil {
				p.repo = repo.name
			}
		}
		return repo
	}
	var pendingFallback string
	var fallbacks []map[string]any
	spawnOne := func(c *placeCandidate) (res candResult) {
		defer func() {
			if res.stop != nil && res.note == "" {
				res.note = exitReason(exitCode(res.stop))
			}
			if !res.ok && res.stop == nil && s.placement != nil {
				if s.placement.started {
					res.note = "launch_failed"
				}
				s.placement.started = false
				s.placement.execution = placementExecution{}
			}
		}()
		if a.PlacementTry != nil && !s.DryRun {
			a.PlacementTry(c.name)
		}
		release()
		qr, ok := a.quotaRequest(s)
		if !ok {
			return candResult{rc: exitConfig, note: "quota_request_unavailable"}
		}
		if s.DryRun {
			if err := a.checkDryRunAdmission(ctx, s, c.cfg); err != nil {
				return candResult{stop: err}
			}
		}
		key, acc := a.remoteAccount(ctx, pool, c.name, c.cfg)
		j, err := a.judgeHost(ctx, qr, pool, c.name, false, key, acc)
		c.judgment = &j
		if err != nil {
			if j.Reason == hubDown {
				return candResult{rc: exitCode(err), note: hubDown}
			}
			return candResult{rc: exitCode(err), note: hostGateNote(j, err), quotaRefused: true}
		}
		if pendingFallback != "" {
			fallbacks = append(fallbacks, map[string]any{"from_profile": pendingFallback, "reason": "harness_version", "rc": 70, "to_profile": s.Profile, "host": c.name})
			if s.Sel == nil {
				s.Sel = map[string]any{}
			}
			s.Sel["launch_fallbacks"] = fallbacks
			if err := hkCommentTask(ctx, s.Task, fmt.Sprintf("Launch fallback: from-profile=%s rc=70 reason=harness_version to-profile=%s host=%s", pendingFallback, s.Profile, c.name)); err != nil {
				a.warn("launch fallback task comment failed: %v", err)
			}
			pendingFallback = ""
		}
		return a.spawnRemote(ctx, s, brief, c, resolveRepo(), false)
	}
	spawn := func(c *placeCandidate) candResult {
		for {
			res := spawnOne(c)
			if res.note != "hub_launch_proof_failed" {
				return res
			}
			if !s.autoSelected || s.Delegated {
				return res
			}
			previous := s.Profile
			pendingFallback = previous
			seat, ok := a.Policy.Profiles[previous]
			if !ok || slices.Contains(s.failedHarnesses, seat.Kind) {
				return res
			}
			s.failedHarnesses = append(s.failedHarnesses, seat.Kind)
			candidate := s
			candidate.Host, candidate.Effort = c.name, s.requestedEffort
			sel, err := a.selectProfile(ctx, candidate)
			if err != nil || sel == nil || sel.chosen == nil {
				return res
			}
			s.Profile, s.AccountKey, s.RoutingRole, s.SelHost, s.route = sel.chosen.seatName, sel.chosen.accountKey, sel.req.route.role, sel.chosen.host, sel.req.route
			s.Effort = firstNonempty(sel.chosen.row.Effort, s.requestedEffort)
			if s.E6Arm != "" && !ValidE6Arm(s.E6Arm, sel.chosen.seat.Canon) {
				s.E6Arm = ""
			}
			pool = a.quotaPool(ctx, s)
			s.Sel = sel.record()
		}
	}
	emit := func(chosen string) map[string]any {
		a.stampSelection(&s, chosen)
		rec := p.record(chosen)
		if len(fallbacks) > 0 {
			rec["profile"] = s.Profile
			rec["pool"] = pool
		}
		a.attachSelection(rec, s)
		if !s.DryRun {
			if e := a.event(ctx, s.Job, "job.placement", rec); e != nil {
				a.warn("placement decision record failed: %v", e)
			}
		}
		return rec
	}
	var unreachable []string
	var deferredLocal *placeCandidate
	var localRefusal error
	// A refused remote candidate gets its preflight, so the placement record
	// says which of the known remote-spawn conditions the host fails. The
	// hub and presence reads come from the plan.
	// A host the quota gate refused was never contacted over SSH, and the
	// preflight keeps it that way: it reads the hub and hk only.
	explain := func(c *placeCandidate, res candResult) {
		if preEnv == nil {
			preEnv = a.preflightEnvFrom(ctx, p)
		}
		a.attachPreflight(ctx, preEnv, c, pool, !res.quotaRefused)
	}
	admitLocal := func(c *placeCandidate) bool {
		if a.Quota == nil {
			return true
		}
		if qr, ok := a.quotaRequest(s); ok {
			if _, err := a.Quota.Launch(ctx, qr); err != nil {
				c.outcome, localRefusal = "admission_refused", err
				return false
			}
			j, err := a.judgeHost(ctx, qr, pool, c.name, true, "", SharedAccount{})
			c.judgment = &j
			if err != nil {
				c.outcome, localRefusal = firstNonempty(gateCodeNote(err), "quota_refused"), err
				return false
			}
		}
		return true
	}
	for first := true; ; first = false {
		if !first {
			// A refusal moves on. The next candidate comes from a new plan
			// read under the gate, so the fallback sees the jobs other
			// decisions counted and every host's current eligibility.
			if e := takeGate(); e != nil {
				return true, nil, e
			}
			if e := replan(); e != nil {
				return true, nil, e
			}
		}
		var c *placeCandidate
		for _, candidate := range p.ordered {
			if !tried[candidate.name] {
				c = candidate
				break
			}
		}
		if c == nil {
			break
		}
		tried[c.name] = true
		if c.local {
			// An over-threshold local is a last resort: when the job has
			// waited long enough and every awake candidate is saturated the
			// policy wakes a sleeper before committing here.
			if c.over && len(p.sleepers) > 0 && p.waited >= a.wakeAfter() && p.allOver() {
				c.outcome = "deferred_over_threshold"
				deferredLocal = c
				continue
			}
			// Verify the catalog admits this launch so a refusal can move
			// on to the next candidate instead of committing to local.
			if !admitLocal(c) {
				continue
			}
			rec := p.record(c.name)
			a.stampSelection(&s, c.name)
			a.attachSelection(rec, s)
			return false, rec, nil
		}
		res := spawn(c)
		c.outcome = res.note
		if res.ok {
			return true, emit(c.name), nil
		}
		if res.stop != nil {
			return true, nil, res.stop
		}
		explain(c, res)
		if res.unreachable && c.wakeable {
			unreachable = append(unreachable, c.name)
		}
	}
	targets, reasons := a.wakeTargets(s, p, unreachable)
	lastNote := ""
	// A wake waits for a node for up to wake_wait; no other decision waits
	// behind it.
	release()
	for _, w := range targets {
		c := p.candidate(w)
		if c == nil {
			// Wake targets must come from this plan so every gate attempt has
			// a candidate in the placement record.
			lastNote = "wake target missing from placement plan"
			continue
		}
		p.wake = map[string]any{"host": w, "reasons": reasons, "ok": false}
		h := c.cfg
		req := WakeRequest{HoldMinutes: int(integer(h, "wake_hold", 10)), WaitSeconds: int(integer(h, "wake_wait", 90)), Reason: "panewire placement: " + strings.Join(reasons, ",")}
		var wakeErr error
		switch {
		case s.DryRun:
			fmt.Fprintf(a.Out, "dry-run wake host=%s reasons=%s\n", w, strings.Join(reasons, ","))
		case a.Wake != nil:
			wakeErr = a.Wake.Wake(ctx, w, req)
		default:
			wakeErr = fail(exitPlacement, "no waker configured")
		}
		if wakeErr == nil && !s.DryRun {
			wakeErr = a.waitForNode(ctx, w, h, req.WaitSeconds)
		}
		if wakeErr != nil {
			p.wake["error"] = wakeErr.Error()
			lastNote = wakeErr.Error()
			continue
		}
		p.wake["ok"] = true
		// The woken host is an input to a new decision, not a foregone one:
		// read again under the gate and count it only if it is still a place
		// this job may go.
		if e := takeGate(); e != nil {
			return true, nil, e
		}
		woke := p.wake
		delete(tried, w)
		if e := replan(); e != nil {
			return true, nil, e
		}
		p.wake = woke
		c = p.candidate(w)
		if c == nil || !slices.Contains(p.ordered, c) {
			release()
			lastNote = "wake target no longer eligible"
			if c != nil && c.outcome != "" {
				lastNote = "wake target " + c.outcome
			}
			continue
		}
		tried[w] = true
		res := spawn(c)
		c.outcome = res.note
		if res.ok {
			return true, emit(w), nil
		}
		if res.stop != nil {
			return true, nil, res.stop
		}
		explain(c, res)
		lastNote = res.note
	}
	if deferredLocal != nil && admitLocal(deferredLocal) {
		rec := p.record(deferredLocal.name)
		a.attachSelection(rec, s)
		return false, rec, nil
	}
	notes := p.triedNotes() + p.preflightNotes()
	if len(targets) > 0 {
		if lastNote != "" {
			return true, nil, fail(exitPlacement, "wake target did not accept spawn: %s%s", lastNote, notes)
		}
		return true, nil, fail(exitPlacement, "wake target did not accept spawn%s", notes)
	}
	if !boolean(a.Config.Local, "spawn", true) {
		return true, nil, fail(exitLocalDisabled, "local spawns are disabled on this host; no remote candidate completed%s", notes)
	}
	if localRefusal != nil {
		return true, nil, localRefusal
	}
	return true, nil, fail(exitPlacement, "no placement candidate accepted spawn; no local fallback%s", notes)
}

func (a *App) placeForced(ctx context.Context, s Spawn, brief string) (bool, map[string]any, error) {
	tags, _ := parseTags(s.Tags)
	rec := map[string]any{"source": "forced", "tags": tags, "profile": s.Profile, "host": s.Host}
	a.attachSelection(rec, s)
	if s.Host == "local" || s.Host == a.machine() {
		return false, rec, nil
	}
	h, ok := a.Config.Hosts[s.Host]
	if !ok {
		return true, nil, fail(exitConfig, "placement selected unconfigured host %s", s.Host)
	}
	if seat, ok := a.Policy.Profiles[s.Profile]; ok && seat.Capability != "standard" && !boolean(h, seat.Capability, false) {
		return true, nil, fail(exitConfig, "host %s lacks capability %s", s.Host, seat.Capability)
	}
	task, _, e := a.taskPrecheck(ctx, s)
	if e != nil {
		return true, nil, e
	}
	s.TaskGrade = s.Grade
	if s.TaskGrade == "" {
		if m := taskMarker.FindStringSubmatch(task.Title); m != nil {
			s.TaskGrade = strings.ToUpper(m[1])
		}
	}
	if topRoutingRole(s.RoutingRole) {
		s.TaskGrade = ""
	}
	emit := func() {
		if s.DryRun {
			return
		}
		if e := a.event(ctx, s.Job, "job.placement", rec); e != nil {
			a.warn("placement decision record failed: %v", e)
		}
	}
	c := placeCandidate{name: s.Host, cfg: h, wakeable: str(h, "wake") == "panewire", awake: true, eligible: true}
	// A pinned host is decided too; later auto decisions must see its job.
	// The count goes in through the gate, so it never lands between another
	// decision's read and its own count.
	if a.PlacementTry != nil && !s.DryRun {
		release := func() {}
		if a.PlacementGate != nil {
			gateRelease, e := a.PlacementGate(ctx)
			if e != nil {
				return true, nil, fail(exitPlacement, "placement decision gate: %v", e)
			}
			release = gateRelease
		}
		a.PlacementTry(s.Host)
		release()
	}
	qr, ok := a.quotaRequest(s)
	if !ok {
		return true, nil, fail(exitConfig, "quota_request_unavailable")
	}
	// Without the pool no presence can name the host's account: refuse
	// with the catalog's reason (an unplaced rung, say), not as absence.
	pool, e := a.launchPool(ctx, s)
	if e != nil {
		code := exitCode(e)
		if code == 0 {
			code = exitConfig
		}
		return true, nil, fail(code, "quota pool for %s@%s unresolved: %v", s.Profile, qr.Effort, e)
	}
	spawn := func() candResult {
		if s.DryRun {
			if err := a.checkDryRunAdmission(ctx, s, c.cfg); err != nil {
				return candResult{stop: err}
			}
		}
		key, acc := a.remoteAccount(ctx, pool, c.name, c.cfg)
		if s.AccountKey != "" && key != s.AccountKey {
			return candResult{stop: fail(exitAuthUnavailable, "AUTH_UNAVAILABLE: target no longer holds the selected account")}
		}
		j, err := a.judgeHost(ctx, qr, pool, c.name, false, key, acc)
		rec["quota_gate"] = j
		if err != nil {
			return candResult{stop: err}
		}
		return a.spawnRemote(ctx, s, brief, &c, a.localRepo(ctx, s.CWD), true)
	}
	res := spawn()
	if res.ok {
		emit()
		return true, rec, nil
	}
	if res.stop != nil {
		return true, nil, res.stop
	}
	if res.note == "repo_unmappable" {
		return true, nil, fail(exitConfig, "host %s cannot map the job cwd: no cwd_map override and no resolvable repo", s.Host)
	}
	if res.unreachable && c.wakeable {
		req := WakeRequest{HoldMinutes: int(integer(h, "wake_hold", 10)), WaitSeconds: int(integer(h, "wake_wait", 90)), Reason: "panewire: explicit host unreachable"}
		if a.Wake == nil {
			return true, nil, fail(exitPlacement, "host wake unavailable")
		}
		if e := a.Wake.Wake(ctx, s.Host, req); e != nil {
			return true, nil, fail(exitPlacement, "host wake failed: %v", e)
		}
		if e := a.waitForNode(ctx, s.Host, h, req.WaitSeconds); e != nil {
			return true, nil, e
		}
		res = spawn()
		if res.ok {
			emit()
			return true, rec, nil
		}
		if res.stop != nil {
			return true, nil, res.stop
		}
		if res.res.Code != 0 {
			return true, nil, check(res.res, "remote spawn")
		}
		if res.rc != 0 {
			return true, nil, fail(res.rc, "host %s answered its wake but the spawn still failed: %s", s.Host, res.note)
		}
		return true, nil, fail(exitPlacement, "host %s answered its wake but the spawn still failed: %s", s.Host, res.note)
	}
	if res.res.Code != 0 {
		return true, nil, check(res.res, "remote spawn")
	}
	if res.rc != 0 {
		return true, nil, fail(res.rc, "host %s refused the spawn: %s", s.Host, res.note)
	}
	return true, nil, fail(exitPlacement, "host %s refused the spawn: %s", s.Host, res.note)
}

// quotaPool resolves which shared-quota pool the job's profile draws on, so
// placement can prefer hosts that measured the account. A catalog failure is
// not a placement failure: the spawn-time gate on the chosen host is the
// final admission check.
func (a *App) quotaRequest(s Spawn) (QuotaRequest, bool) {
	seat, ok := a.Policy.Profiles[s.Profile]
	if !ok {
		return QuotaRequest{}, false
	}
	effort := s.Effort
	if effort == "" {
		effort = seat.Effort
	}
	role := s.Role
	if topRoutingRole(s.RoutingRole) {
		role = s.RoutingRole
	}
	return QuotaRequest{Profile: seat.Canon, GateProfile: seat.gateName(), Effort: effort, Role: role, OperatorRequest: s.OperatorRequest, RequestedBy: s.RequestedBy, Purpose: s.Purpose, AccountKey: s.AccountKey, E6Arm: a.e6Arm(s.E6Arm)}, true
}

func (a *App) quotaPool(ctx context.Context, s Spawn) string {
	pool, _ := a.launchPool(ctx, s)
	return pool
}

// launchPool is quotaPool with the reason a pool is unknown: the selected
// account's pool, else the catalog's pool for the launch rung.
func (a *App) launchPool(ctx context.Context, s Spawn) (string, error) {
	if s.AccountKey != "" {
		pool, _, _ := strings.Cut(s.AccountKey, ":")
		if validID(pool) {
			return pool, nil
		}
	}
	if a.Quota == nil {
		return "", fail(exitConfig, "quota catalog unavailable")
	}
	qr, ok := a.quotaRequest(s)
	if !ok {
		return "", fail(exitConfig, "missing seat policy for %s", s.Profile)
	}
	l, e := a.Quota.Launch(ctx, qr)
	if e != nil {
		return "", e
	}
	return l.Pool, nil
}

func taskWaited(t client.Task, now time.Time) time.Duration {
	if t.CreatedAt.IsZero() {
		return 0
	}
	if d := now.Sub(t.CreatedAt); d > 0 {
		return d
	}
	return 0
}

func parseHubHosts(raw string) ([]HostView, error) {
	var p struct {
		Decision   *string           `json:"decision"`
		Reason     string            `json:"reason"`
		Source     string            `json:"source"`
		Candidates []json.RawMessage `json:"candidates"`
	}
	if e := json.Unmarshal([]byte(raw), &p); e != nil {
		return nil, fail(exitConfig, "invalid placement response")
	}
	var out []HostView
	for _, v := range p.Candidates {
		var c struct {
			Machine, Host, Target, Reason string
			Connected                     *bool
			// The hub sends snake_case; encoding/json folds case but not
			// underscores, so the field needs its tag.
			LoadRatio *float64 `json:"load_ratio"`
			Score     *float64 `json:"score"`
			PlacementSignals
			Signals *hubSignals `json:"signals"`
			// The node's memory telemetry is on the candidate already; only
			// the shadow comparison reads it.
			MemoryFreePct softFloat `json:"memory_free_pct"`
			SwapUsedMB    softFloat `json:"swap_used_mb"`
			SwapInPerS    softFloat `json:"swap_in_per_s"`
			Pressure      softFloat `json:"pressure"`
		}
		var name string
		if json.Unmarshal(v, &name) != nil {
			if json.Unmarshal(v, &c) != nil {
				return nil, fail(exitConfig, "invalid placement candidate")
			}
			name = c.Machine
			if name == "" {
				name = c.Host
			}
			if name == "" {
				name = c.Target
			}
		}
		if !validID(name) {
			continue
		}
		hv := HostView{Name: name, Connected: true, LoadRatio: c.LoadRatio, Score: c.Score, Reason: c.Reason, Signals: c.PlacementSignals, Source: p.Source}
		if p.Decision != nil && *p.Decision == name {
			hv.Decided, hv.DecisionReason = true, p.Reason
		}
		hv.Extra = HostExtra{MemFreePct: c.MemoryFreePct.v, SwapUsedMB: c.SwapUsedMB.v, SwapInPerS: c.SwapInPerS.v, Pressure: c.Pressure.v}
		if c.Signals != nil {
			hv.Signals = c.Signals.PlacementSignals
			hv.Extra.CPUBusy1m, hv.Extra.SwapInLinux = c.Signals.CPUBusy1m.v, c.Signals.SwapInLinux.true()
		}
		if c.Connected != nil {
			hv.Connected = *c.Connected
		}
		out = append(out, hv)
	}
	return out, nil
}

func mappedCWD(raw any, cwd string) string {
	mapping, ok := raw.(map[string]any)
	if !ok {
		return ""
	}
	best, mapped := "", ""
	for from, v := range mapping {
		to, ok := v.(string)
		if !ok {
			continue
		}
		if cwd == from || strings.HasPrefix(cwd, from+"/") || strings.HasPrefix(cwd, from+".") {
			if len(from) > len(best) {
				best = from
				mapped = to + strings.TrimPrefix(cwd, from)
			}
		}
	}
	return mapped
}
func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
func (a *App) spawnStdin(ctx context.Context) error {
	b, e := io.ReadAll(io.LimitReader(os.Stdin, 4<<20))
	if e != nil {
		return e
	}
	var env struct {
		Spawn Spawn
		Brief string
	}
	if e = decode(b, &env); e != nil {
		return e
	}
	if env.Spawn.Host != "local" {
		return fail(exitUsage, "delegation must target local")
	}
	if e = a.adoptFreshTTL(env.Spawn.FreshTTLS); e != nil {
		return e
	}
	f, e := os.CreateTemp("", "panewire-brief-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if _, e = f.WriteString(env.Brief); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	env.Spawn.Prompt = f.Name()
	env.Spawn.Delegated = true
	return a.Spawn(ctx, env.Spawn)
}

// adoptFreshTTL makes this delegated spawn judge shared-quota freshness with
// the placing host's window (seconds; 0 keeps this host's own). Every reading
// here (hub state, direct hk read, the selected-account recheck) then agrees
// with the plan that chose this host.
func (a *App) adoptFreshTTL(seconds int64) error {
	if seconds == 0 {
		return nil
	}
	if seconds < 0 || seconds > 86400 {
		return fail(exitUsage, "delegated fresh_ttl_s %d out of range", seconds)
	}
	quota := maps.Clone(a.Config.Quota)
	if quota == nil {
		quota = map[string]any{}
	}
	quota["fresh_ttl_s"] = seconds
	// The placing host already resolved its per-pool window into seconds.
	delete(quota, "pool_fresh_ttl_s")
	a.Config.Quota = quota
	return nil
}
func (a *App) prepareBriefForHost(s Spawn, host, body string) (string, error) {
	copy := *a
	copy.Config = a.Config
	copy.Config.Local = map[string]any{}
	for k, v := range a.Config.Hosts[host] {
		copy.Config.Local[k] = v
	}
	copy.Config.Local["machine_id"] = host
	seat := a.Policy.Profiles[s.Profile]
	return copy.prepareBrief(s, seat, Launch{Effort: firstNonempty(s.Effort, seat.Effort)}, body)
}

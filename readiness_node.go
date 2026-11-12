package panewire

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/hkcred"
)

// Node side of readiness (docs/placement.md, "Readiness"). The node probes
// what it can launch, keeps the result in a cache, refreshes it on a slow
// interval and after every failed launch, and sends it on the heartbeat to a
// hub that advertised hubFeatureReadiness. The report holds names, versions
// and booleans: no credential, no path, no file contents and no screen text.

const (
	// nodeReadinessInterval is the slow refresh of the cache.
	nodeReadinessInterval = 5 * time.Minute
	// nodeReadinessResend re-sends an unchanged report so the hub's value
	// stays inside the consumers' freshness window.
	nodeReadinessResend = 45 * time.Second
	// nodeReadinessProbeTimeout bounds one probe command.
	nodeReadinessProbeTimeout = 5 * time.Second
	nodeReadinessMaxRepos     = 256
	// nodeDialogTTL is how long a launch's first-run dialog stays reported. A
	// pair the dialog dropped is not launched again, so nothing else could
	// clear it once the host owner has dealt with it; after the TTL the pair
	// gets one more launch, and a launch that stops at the dialog again
	// reports it for another TTL.
	nodeDialogTTL = 30 * time.Minute

	hubSpawnDefaultWorkspace = "workers"
	hubSpawnDefaultSession   = "default"
)

var nodeVersionToken = regexp.MustCompile(`[0-9]+(?:\.[0-9]+)+`)

// nodeHarnessBinaries maps a harness kind to the executable that launches it.
var nodeHarnessBinaries = map[string]string{"claude": "claude", "codex": "codex", "omp": "omp", "agy": "agy", "devin": "devin"}

// nodeHerdrIntegrations maps a harness kind to its name in
// `herdr integration status`.
var nodeHerdrIntegrations = map[string]string{"claude": "claude", "codex": "codex", "omp": "omp", "agy": "antigravity-cli", "devin": "devin"}

var nodeToolchains = map[string][]string{"go": {"go", "version"}, "node": {"node", "--version"}, "bun": {"bun", "--version"}}

// nodeReadinessDeps are the probe's external reads. The zero value runs the
// real ones; tests substitute fakes.
type nodeReadinessDeps struct {
	lookPath func(name string) (string, error)
	run      func(ctx context.Context, name string, args ...string) ([]byte, error)
	listDir  func(dir string) []string
	// envReadable answers whether the handoffkeep env file is present and
	// valid; only the answer leaves the node.
	envReadable func(path string) bool
}

func (d nodeReadinessDeps) find(name string) (string, bool) {
	look := d.lookPath
	if look == nil {
		look = exec.LookPath
	}
	if path, err := look(name); err == nil {
		return path, true
	}
	// The daemon's PATH can be narrower than the PATH a herdr pane launches
	// with; look where the fleet installs tools before reporting one missing.
	if d.lookPath != nil {
		return "", false
	}
	home, _ := os.UserHomeDir()
	for _, dir := range []string{filepath.Join(home, ".local", "bin"), filepath.Join(home, ".bun", "bin"), filepath.Join(home, ".npm-global", "bin"), filepath.Join(home, ".cargo", "bin"), filepath.Join(home, "go", "bin"), "/usr/local/bin", "/opt/homebrew/bin", "/usr/local/go/bin"} {
		candidate := filepath.Join(dir, name)
		if info, err := os.Stat(candidate); err == nil && info.Mode().IsRegular() && info.Mode()&0o111 != 0 {
			return candidate, true
		}
	}
	return "", false
}

func (d nodeReadinessDeps) output(ctx context.Context, name string, args ...string) string {
	run := d.run
	if run == nil {
		run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
			return exec.CommandContext(ctx, name, args...).Output()
		}
	}
	bounded, cancel := context.WithTimeout(ctx, nodeReadinessProbeTimeout)
	defer cancel()
	out, err := run(bounded, name, args...)
	if err != nil {
		return ""
	}
	return string(out)
}

// versionToken keeps the first dotted number of a command's output, never the
// output itself.
func versionToken(output string) string {
	return nodeVersionToken.FindString(output)
}

// parseHerdrIntegrations reads `herdr integration status`: the names whose
// line says "current". The line's path is dropped.
func parseHerdrIntegrations(output string) map[string]bool {
	current := map[string]bool{}
	for _, line := range strings.Split(output, "\n") {
		name, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		current[strings.TrimSpace(name)] = strings.HasPrefix(strings.TrimSpace(rest), "current")
	}
	return current
}

// nodeReadinessState is the cache and the facts launches teach the node.
type nodeReadinessState struct {
	mu         sync.Mutex
	cached     *fleet.Readiness
	cachedAt   time.Time
	dirty      bool
	refreshing bool
	// dialogs is the first-run dialog each kind's last launch stopped at.
	dialogs map[string]dialogMark
	// accounts maps a pool to its last pacelever refresh outcome. A pool not
	// in it is not measured here, or its login is unknown.
	accounts map[string]quotaAccountNote
	// probedAt, probeMissing and loginScreens are the host's own login
	// evidence the quota loop's refresh cannot give (quota_login_probe.go): when
	// each pool was last probed, the pools whose last probe failed with a login
	// error, and the kinds whose last rc=76 launch showed the login screen.
	probedAt     map[string]time.Time
	probeMissing map[string]bool
	loginScreens map[string]time.Time
	// sentPeer, sentAt and sentCheckedAt mark what the current connection has
	// been sent.
	sentPeer      *hubClientConnection
	sentAt        time.Time
	sentCheckedAt time.Time
}

type dialogMark struct {
	marker string
	at     time.Time
}

// quotaAccountNote is a pool's last refresh outcome: key "" means pacelever
// named a login failure (Login missing); a key is the account a successful
// refresh read at at.
type quotaAccountNote struct {
	key string
	at  time.Time
}

// noteQuotaAccount records a refresh that read accountKey, or "" for one that
// failed with a login error kind.
func (client *HubClient) noteQuotaAccount(pool, accountKey string) {
	at := client.nowUTC()
	client.readiness.mu.Lock()
	defer client.readiness.mu.Unlock()
	if client.readiness.accounts == nil {
		client.readiness.accounts = map[string]quotaAccountNote{}
	}
	client.readiness.accounts[pool] = quotaAccountNote{key: accountKey, at: at}
}

// noteOwnQuotaMeasurement clears only a login-screen mark after this host's
// own credential completed a successful measurement. Shared readings do not
// call it.
func (client *HubClient) noteOwnQuotaMeasurement(pool string) {
	client.readiness.mu.Lock()
	if _, marked := client.readiness.loginScreens[pool]; marked {
		delete(client.readiness.loginScreens, pool)
		client.readiness.dirty = true
	}
	client.readiness.mu.Unlock()
}

// noteQuotaUnknown records a refresh that failed for a reason other than the
// login (timeout, transient error, excluded pool, unreadable status). The
// rule: a present account stays reported while its last successful refresh
// is younger than fleetQuotaRepublish; otherwise, and always after a login
// failure, the pool's note is cleared and its Login reads unknown.
func (client *HubClient) noteQuotaUnknown(pool string) {
	now := client.nowUTC()
	client.readiness.mu.Lock()
	defer client.readiness.mu.Unlock()
	if note, ok := client.readiness.accounts[pool]; ok && (note.key == "" || now.Sub(note.at) >= fleetQuotaRepublish) {
		delete(client.readiness.accounts, pool)
	}
}

// classifyLaunchDialog names the first-run dialog a failed launch stopped at,
// from the refusal markers the spawn engine writes (internal/fleet/herdr.go,
// omp.go). It reads only those markers, never a screen.
func classifyLaunchDialog(kind, tail string) string {
	switch {
	case kind == "codex" && strings.Contains(tail, "HOOKS_REVIEW_REQUIRED"):
		return fleet.DialogCodexHooksReview
	case kind == "codex" && strings.Contains(tail, "WORKSPACE_TRUST_REQUIRED"):
		return fleet.DialogCodexTrust
	case kind == "omp" && strings.Contains(tail, "OMP_SETUP_WIZARD"):
		return fleet.DialogOmpSetupWizard
	case kind == "claude" && strings.Contains(tail, "WORKSPACE_TRUST_REQUIRED"):
		return fleet.DialogClaudeTrust
	}
	return ""
}

// noteLaunchResult teaches the cache what a launch showed: a launch that got
// past the dialogs clears the kind's login evidence, a login-screen capture
// sets it, and every failure asks for a refresh of the whole report. The hub
// result may be translated after this call so the node marks the original rc=76.
func (client *HubClient) noteLaunchResult(message hubOutboundMessage, result hubSpawnResult) {
	kind := fleet.ProfileKind(hubSpawnArg(message.Args, "-m"))
	// The capture is read before the lock. rc=76 with the harness's login
	// screen on it is a login missing at once, with no two-failure threshold.
	loginScreen := false
	if result.RC == fleet.ExitSubmitUnconfirmed && kind != "" {
		job := result.JobID
		if job == "" {
			job = hubSpawnArg(message.Args, "--job")
		}
		loginScreen = client.launchShowedLoginScreen(kind, job)
	}
	client.readiness.mu.Lock()
	defer client.readiness.mu.Unlock()
	if result.RC != 0 {
		client.readiness.dirty = true
	}
	if kind == "" {
		return
	}
	if loginScreen {
		if client.readiness.loginScreens == nil {
			client.readiness.loginScreens = map[string]time.Time{}
		}
		if !loginScreenFresh(client.readiness.loginScreens, kind, client.nowUTC()) {
			client.warnMessage("launch of " + kind + " ended rc=76 at its login screen; harness login missing")
		}
		client.readiness.loginScreens[kind] = client.nowUTC()
	} else if result.RC == 0 {
		// A launch that got through shows this host's login works: it clears the
		// host's own login evidence for the kind, and the report is re-sent.
		_, marked := client.readiness.loginScreens[kind]
		if marked || client.readiness.probeMissing[kind] {
			delete(client.readiness.loginScreens, kind)
			delete(client.readiness.probeMissing, kind)
			client.readiness.dirty = true
		}
	}
	if client.readiness.dialogs == nil {
		client.readiness.dialogs = map[string]dialogMark{}
	}
	switch dialog := classifyLaunchDialog(kind, result.StdoutTail); {
	case result.RC == 0:
		if _, had := client.readiness.dialogs[kind]; had {
			delete(client.readiness.dialogs, kind)
			client.readiness.dirty = true
		}
	case dialog != "":
		client.readiness.dialogs[kind] = dialogMark{marker: dialog, at: client.nowUTC()}
	}
}

// resolveHubSpawnConfig reads spawn.json. A missing file at the default path
// is not an error: the node runs on defaults (work root ~/work, the workers
// workspace, the default herdr session, and panewire's svc-panewire hk
// credential resolved as hkcred.Load does: the HANDOFFKEEP_URL +
// HANDOFFKEEP_TOKEN_SVC_PANEWIRE environment pair, else hkcred.PathIn
// (PANEWIRE_HK_ENV, else ~/.config/panewire/hk-svc.env) with the svc key
// only; never the hk CLI's config.env, hk 3570). A file the operator named
// explicitly must exist. defaults reports which one ran.
func (client *HubClient) resolveHubSpawnConfig() (config hubSpawnConfig, defaults bool, err error) {
	config, err = loadHubSpawnConfig(client.spawnConfigPath)
	var configErr *hubSpawnConfigError
	if err == nil || !client.spawnConfigDefault || !asConfigError(err, &configErr) || configErr.Rule != "config_missing" {
		return config, false, err
	}
	home, homeErr := os.UserHomeDir()
	if homeErr != nil {
		return hubSpawnConfig{}, false, err
	}
	return hubSpawnConfig{Enabled: true, Workspace: hubSpawnDefaultWorkspace, HERDRSession: hubSpawnDefaultSession, WorkRoot: filepath.Join(home, "work"), HandoffkeepEnv: hkcred.PathIn(home), HandoffkeepEnvDefault: true}, true, nil
}

func asConfigError(err error, target **hubSpawnConfigError) bool {
	configErr, ok := err.(*hubSpawnConfigError)
	if ok {
		*target = configErr
	}
	return ok
}

// defaultRepoDir resolves a repo key to <work_root>/<key> on a node running on
// defaults, when that is a real directory. A symlink is never followed.
func defaultRepoDir(workRoot, key string) (string, bool) {
	if workRoot == "" || !hubSpawnCWDKeyPattern.MatchString(key) {
		return "", false
	}
	path := filepath.Join(workRoot, key)
	if info, err := os.Lstat(path); err == nil && info.IsDir() {
		return path, true
	}
	// Keys are lower case; a checkout may keep its remote's spelling
	// (ParkingArcade). Resolve only when exactly one directory matches.
	entries, err := os.ReadDir(workRoot)
	if err != nil {
		return "", false
	}
	match := ""
	for _, entry := range entries {
		if fleet.RepoKey(entry.Name()) != key {
			continue
		}
		if match != "" {
			return "", false
		}
		match = entry.Name()
	}
	if match == "" {
		return "", false
	}
	path = filepath.Join(workRoot, match)
	if info, err := os.Lstat(path); err != nil || !info.IsDir() {
		return "", false
	}
	return path, true
}

// collectReadiness probes the node. It never fails: a probe that cannot run
// reports the thing as absent.
func (client *HubClient) collectReadiness(ctx context.Context) *fleet.Readiness {
	deps := client.readinessDeps
	out := &fleet.Readiness{
		PanewireVersion: fleet.BuildVersion,
		CheckedAt:       client.nowUTC(),
		Harnesses:       map[string]fleet.HarnessReadiness{},
		Repos:           []string{},
		Toolchains:      map[string]string{},
	}
	integrations := parseHerdrIntegrations(deps.output(ctx, "herdr", "integration", "status"))
	client.readiness.mu.Lock()
	dialogs, accounts := cloneStringMap(client.readiness.dialogs), cloneStringMap(client.readiness.accounts)
	probeMissing, loginScreens := cloneStringMap(client.readiness.probeMissing), cloneStringMap(client.readiness.loginScreens)
	client.readiness.mu.Unlock()
	for _, kind := range fleet.ReadinessKinds {
		h := fleet.HarnessReadiness{Login: fleet.ReadinessLoginUnknown}
		if mark, ok := dialogs[kind]; ok && out.CheckedAt.Sub(mark.at) < nodeDialogTTL {
			h.Dialog = mark.marker
		}
		if kind == "cmd" {
			h.Installed, h.HerdrIntegration = true, true
			out.Harnesses[kind] = h
			continue
		}
		if path, ok := deps.find(nodeHarnessBinaries[kind]); ok {
			h.Installed = true
			h.Version = versionToken(deps.output(ctx, path, "--version"))
		}
		h.HerdrIntegration = integrations[nodeHerdrIntegrations[kind]]
		note, measured := accounts[kind]
		measurable := false
		if _, marked := loginScreens[kind]; marked {
			measurable = client.nativeQuotaSource(kind).measurableHere(ctx)
		}
		switch {
		case probeMissing[kind] || loginScreenActive(loginScreens, kind, out.CheckedAt, measurable) || measured && note.key == "":
			// The loop's last refresh, a login probe and a launch that stopped at
			// the login screen each say the login is missing; a refresh that read
			// a shared reading says nothing against them.
			h.Login = fleet.ReadinessLoginMissing
		case measured:
			h.Login, h.Account = fleet.ReadinessLoginPresent, note.key
		}
		out.Harnesses[kind] = h
	}
	for tool, argv := range nodeToolchains {
		if path, ok := deps.find(argv[0]); ok {
			out.Toolchains[tool] = versionToken(deps.output(ctx, path, argv[1:]...))
		}
	}
	config, defaults, err := client.resolveHubSpawnConfig()
	out.Config.Source = fleet.ReadinessConfigFile
	if defaults {
		out.Config.Source = fleet.ReadinessConfigDefault
	}
	if err != nil {
		// A refused spawn.json cannot launch anything: no repos, no brief.
		return out
	}
	out.Config.Workspace = fleet.ReadinessConfigName(config.Workspace, fleet.ReadinessDefaultWorkspace)
	out.Config.HerdrSession = fleet.ReadinessConfigName(config.HERDRSession, fleet.ReadinessDefaultSession)
	if info, err := os.Stat(config.WorkRoot); config.WorkRoot != "" && err == nil && info.IsDir() {
		out.Config.WorkRoot = true
	}
	for key := range config.CWDMap {
		out.Repos = append(out.Repos, key)
	}
	if defaults && out.Config.WorkRoot {
		list := deps.listDir
		if list == nil {
			list = func(dir string) []string {
				entries, _ := os.ReadDir(dir)
				var names []string
				for _, entry := range entries {
					names = append(names, entry.Name())
				}
				return names
			}
		}
		for _, name := range list(config.WorkRoot) {
			key := fleet.RepoKey(name)
			if _, ok := defaultRepoDir(config.WorkRoot, key); ok {
				out.Repos = append(out.Repos, key)
			}
		}
	}
	slices.Sort(out.Repos)
	out.Repos = slices.Compact(out.Repos)
	if len(out.Repos) > nodeReadinessMaxRepos {
		out.Repos = out.Repos[:nodeReadinessMaxRepos]
	}
	if deps.envReadable != nil {
		out.Brief.HandoffkeepEnv = config.HandoffkeepEnv != "" && deps.envReadable(config.HandoffkeepEnv)
	} else {
		_, envErr := config.handoffkeepEnv()
		out.Brief.HandoffkeepEnv = envErr == nil
	}
	return out
}

func cloneStringMap[V any](in map[string]V) map[string]V {
	out := make(map[string]V, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// refreshReadiness probes now and stores the result.
func (client *HubClient) refreshReadiness(ctx context.Context) {
	collect := client.readinessCollector
	if collect == nil {
		collect = client.collectReadiness
	}
	report := collect(ctx)
	client.readiness.mu.Lock()
	client.readiness.cached, client.readiness.cachedAt, client.readiness.dirty, client.readiness.refreshing = report, client.nowUTC(), false, false
	client.readiness.mu.Unlock()
}

// readinessForHeartbeat answers the report to send on this heartbeat, or nil.
// It never probes inline: a probe runs commands and must not hold up the
// heartbeat, so a due refresh starts in the background and the cached report
// is sent meanwhile. Nothing is sent to a hub that did not advertise
// hubFeatureReadiness, and an unchanged report is re-sent only every
// nodeReadinessResend.
func (client *HubClient) readinessForHeartbeat(ctx context.Context) *fleet.Readiness {
	peer := client.signalsPeer.Load()
	if peer == nil || !peer.acceptsReadiness.Load() {
		return nil
	}
	now := client.nowUTC()
	state := &client.readiness
	state.mu.Lock()
	defer state.mu.Unlock()
	if (state.cached == nil || state.dirty || now.Sub(state.cachedAt) >= nodeReadinessInterval) && !state.refreshing && ctx.Err() == nil {
		state.refreshing = true
		go client.refreshReadiness(context.WithoutCancel(ctx))
	}
	if state.cached == nil {
		return nil
	}
	if state.sentPeer == peer && state.sentCheckedAt.Equal(state.cached.CheckedAt) && now.Sub(state.sentAt) < nodeReadinessResend {
		return nil
	}
	state.sentPeer, state.sentAt, state.sentCheckedAt = peer, now, state.cached.CheckedAt
	report := *state.cached
	return &report
}

// readinessJSON is the report as the hub reads it; tests use it to check
// what a report may contain.
func readinessJSON(report *fleet.Readiness) string {
	encoded, _ := json.Marshal(report)
	return string(encoded)
}

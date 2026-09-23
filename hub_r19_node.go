package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

var hubRequestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,96}$`)

const (
	hubUpdateRedirectLimit = 3
	hubQuotaOutputLimit    = 16 << 10
	// A quota report crosses the wire as a JSON string inside a message the hub
	// reads under hubMaxMessageBytes. JSON escaping can more than double the raw
	// stdout size ("" and \n cost two bytes each, control bytes six), so the raw
	// cap alone can still produce a message the hub refuses. The bound that
	// matters is therefore the encoded one, and it is kept below
	// hubMaxMessageBytes so the surrounding envelope still fits.
	hubQuotaEncodedLimit = 24 << 10
	// hubUpdateBackupsKept bounds the rollback copies left beside the
	// executable; each one is a full binary.
	hubUpdateBackupsKept = 2
)

func validHubRequestID(value string) bool { return hubRequestIDPattern.MatchString(value) }
func validHubSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// hubUpdateDefaultRepository is the only GitHub repository whose release
// assets a node installs unless PANEWIRE_UPDATE_REPO names another one. Binding
// the repository, not just the host, is what keeps a leaked operator token from
// pointing every node at an arbitrary GitHub release.
const hubUpdateDefaultRepository = "mgh3326/panewire"

// hubUpdateRepositoryDisabled is what an invalid PANEWIRE_UPDATE_REPO becomes:
// it fails validHubUpdateRepository, so no URL can match it.
const hubUpdateRepositoryDisabled = "disabled"

var (
	hubUpdateRepositoryPattern = regexp.MustCompile(`^[A-Za-z0-9-]{1,39}/[A-Za-z0-9._-]{1,100}$`)
	hubUpdateTagPattern        = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	// The release asset name carries the stamped version, so the version the
	// operator publishes, the file the node downloads, and the string the
	// smoke run must print are one value.
	hubUpdateAssetPattern = regexp.MustCompile(`^panewire_([A-Za-z0-9._-]{1,64})_(darwin|linux)_(amd64|arm64)$`)
)

// hubUpdateRepositoryFromEnv reads PANEWIRE_UPDATE_REPO. An unset variable
// selects the default repository; a malformed one disables updates (an empty
// repository matches no URL) rather than silently falling back.
func hubUpdateRepositoryFromEnv(lookup func(string) (string, bool)) (string, bool) {
	value, set := lookup("PANEWIRE_UPDATE_REPO")
	if !set || value == "" {
		return hubUpdateDefaultRepository, true
	}
	if !validHubUpdateRepository(value) {
		return "", false
	}
	return value, true
}

func validHubUpdateRepository(value string) bool {
	if !hubUpdateRepositoryPattern.MatchString(value) {
		return false
	}
	_, name, _ := strings.Cut(value, "/")
	return name != "." && name != ".."
}

// hubReleaseAsset is a parsed pinned release download URL.
type hubReleaseAsset struct {
	Tag     string
	Version string
	OS      string
	Arch    string
}

// parseHubReleaseURL accepts exactly
// https://github.com/<repository>/releases/download/<tag>/panewire_<version>_<os>_<arch>.
// The path is compared segment by segment on the decoded form, and any
// percent-encoding, dot segment, query, fragment, userinfo, or non-443 port is
// refused, so no spelling of another repository's path can pass.
func parseHubReleaseURL(value, repository string) (hubReleaseAsset, bool) {
	if !validHubUpdateRepository(repository) || strings.ContainsAny(value, "%\\#") {
		return hubReleaseAsset{}, false
	}
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || (u.Port() != "" && u.Port() != "443") {
		return hubReleaseAsset{}, false
	}
	if strings.ToLower(u.Hostname()) != "github.com" {
		return hubReleaseAsset{}, false
	}
	owner, name, _ := strings.Cut(repository, "/")
	segments := strings.Split(u.Path, "/")
	if len(segments) != 7 || segments[0] != "" || segments[1] != owner || segments[2] != name || segments[3] != "releases" || segments[4] != "download" {
		return hubReleaseAsset{}, false
	}
	tag := segments[5]
	if !hubUpdateTagPattern.MatchString(tag) || tag == "." || tag == ".." {
		return hubReleaseAsset{}, false
	}
	match := hubUpdateAssetPattern.FindStringSubmatch(segments[6])
	if match == nil || !hubVersionPattern.MatchString(match[1]) {
		return hubReleaseAsset{}, false
	}
	return hubReleaseAsset{Tag: tag, Version: match[1], OS: match[2], Arch: match[3]}, true
}

// validHubUpdateURL is the pinned-release check every published or received
// update URL must pass before any download starts.
func validHubUpdateURL(value, repository string) bool {
	_, ok := parseHubReleaseURL(value, repository)
	return ok
}

// validHubUpdateURLForVersion additionally requires the asset name to carry
// the published version.
func validHubUpdateURLForVersion(value, repository, version string) bool {
	asset, ok := parseHubReleaseURL(value, repository)
	return ok && asset.Version == version
}

// hubUpdateAssetHosts are GitHub's release-asset stores. A download may reach
// them only as a redirect from a pinned release URL; they are never a valid
// starting point. release-assets.githubusercontent.com is where github.com
// release downloads redirect today (observed 2026-09-23); objects is the older
// store.
var hubUpdateAssetHosts = map[string]bool{
	"objects.githubusercontent.com":        true,
	"release-assets.githubusercontent.com": true,
}

func validHubUpdateRedirect(target *url.URL, repository string) bool {
	if target.Scheme != "https" || target.User != nil || target.Opaque != "" || (target.Port() != "" && target.Port() != "443") {
		return false
	}
	if hubUpdateAssetHosts[strings.ToLower(target.Hostname())] {
		// GitHub's signed release-asset redirect carries query parameters.
		return true
	}
	return validHubUpdateURL(target.String(), repository)
}

func hubUpdateClient(client *http.Client, repository string) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	copy := *client
	copy.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) > hubUpdateRedirectLimit || !validHubUpdateRedirect(request.URL, repository) {
			return errors.New("update redirect rejected")
		}
		previous := via[len(via)-1].URL
		if previous.Scheme != "https" || request.URL.Scheme != "https" {
			return errors.New("update redirect rejected")
		}
		return nil
	}
	return &copy
}

// hubUpdateRequest is one update.available instruction as the node applies it.
type hubUpdateRequest struct {
	URL        string
	SHA256     string
	Version    string
	Repository string
}

var errHubUpdateSmoke = errors.New("update rejected(smoke)")

const hubUpdateSmokeOutputLimit = 256

// hubUpdateSmokeTimeout bounds the pre-rename `version` run (a variable only so
// fixtures can exercise the timeout without waiting five seconds).
var hubUpdateSmokeTimeout = 5 * time.Second

// applyHubUpdate downloads to a sibling temporary file, verifies it before
// touching the executable, then leaves a timestamped rollback copy. It is
// intentionally fail-closed: every error before the final rename leaves the
// running executable unchanged. Before the rename the candidate must run
// `version` and print exactly the published version (the smoke run), and the
// rollback state the next daemon start judges is written.
func applyHubUpdate(ctx context.Context, httpClient *http.Client, executable string, update hubUpdateRequest) error {
	if !validHubUpdateURLForVersion(update.URL, update.Repository, update.Version) || !validHubSHA256(update.SHA256) {
		return errors.New("update rejected")
	}
	if executable == "" {
		var err error
		executable, err = os.Executable()
		if err != nil {
			return errors.New("update unavailable")
		}
	}
	info, err := os.Stat(executable)
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("update unavailable")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, update.URL, nil)
	if err != nil {
		return errors.New("update unavailable")
	}
	response, err := hubUpdateClient(httpClient, update.Repository).Do(request)
	if err != nil || response.StatusCode != http.StatusOK {
		if response != nil {
			response.Body.Close()
		}
		return errors.New("update unavailable")
	}
	defer response.Body.Close()
	temporary, err := os.CreateTemp(filepath.Dir(executable), ".panewire-update-")
	if err != nil {
		return errors.New("update unavailable")
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	hash := sha256.New()
	if _, err = io.Copy(io.MultiWriter(temporary, hash), io.LimitReader(response.Body, 128<<20)); err != nil || temporary.Close() != nil {
		return errors.New("update unavailable")
	}
	if hex.EncodeToString(hash.Sum(nil)) != strings.ToLower(update.SHA256) {
		return errors.New("update checksum mismatch")
	}
	if err = os.Chmod(temporaryName, info.Mode().Perm()|0o100); err != nil {
		return errors.New("update unavailable")
	}
	// The smoke run precedes the rollback copy: a candidate that cannot report
	// its version costs neither the executable nor a .bak.
	if err = smokeHubUpdate(ctx, temporaryName, update.Version); err != nil {
		return errHubUpdateSmoke
	}
	backup := executable + ".bak-" + time.Now().UTC().Format("20060102T150405Z")
	// Keep the old inode available before atomically replacing its pathname.
	// Unlike rename(executable, backup), this never creates a crash window in
	// which the executable pathname is absent.
	if err = linkOrCopyHubExecutable(executable, backup, info.Mode()); err != nil {
		return errors.New("update unavailable")
	}
	// The probation record is written before the rename so a new binary that
	// dies before it can write anything is still judged at its next start.
	hubUpdateStateMu.Lock()
	err = writeHubUpdateState(executable, hubUpdateState{Version: update.Version, Backup: filepath.Base(backup)})
	if err == nil {
		if err = os.Rename(temporaryName, executable); err != nil {
			_ = os.Remove(hubUpdateStatePath(executable))
		}
	}
	hubUpdateStateMu.Unlock()
	if err != nil {
		return errors.New("update unavailable")
	}
	pruneHubUpdateBackups(executable, hubUpdateBackupsKept)
	return nil
}

// smokeHubUpdate runs `<candidate> version` with no inherited environment (no
// hub or Cloudflare credential reaches it) under a hard deadline and requires
// exit 0 and exactly the expected version on stdout.
func smokeHubUpdate(ctx context.Context, candidate, expectedVersion string) error {
	runContext, cancel := context.WithTimeout(ctx, hubUpdateSmokeTimeout)
	defer cancel()
	var (
		cmd    *exec.Cmd
		stdout io.ReadCloser
		err    error
	)
	// A child forked elsewhere in this process can briefly hold the candidate's
	// just-closed write descriptor, which makes exec fail with ETXTBSY. That is
	// a property of this process, not of the candidate, so it is retried with a
	// fresh command.
	for attempt := 0; ; attempt++ {
		cmd = hubUpdateSmokeCommand(runContext, candidate)
		if stdout, err = cmd.StdoutPipe(); err != nil {
			return errHubUpdateSmoke
		}
		err = cmd.Start()
		if err == nil || !errors.Is(err, syscall.ETXTBSY) || attempt == 4 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		return errHubUpdateSmoke
	}
	out, readErr := io.ReadAll(io.LimitReader(stdout, hubUpdateSmokeOutputLimit+1))
	if len(out) > hubUpdateSmokeOutputLimit {
		_ = cmd.Cancel()
	}
	waitErr := cmd.Wait()
	if runContext.Err() != nil || readErr != nil || waitErr != nil || len(out) > hubUpdateSmokeOutputLimit {
		return errHubUpdateSmoke
	}
	if strings.TrimSpace(string(out)) != expectedVersion {
		return errHubUpdateSmoke
	}
	return nil
}

func hubUpdateSmokeCommand(ctx context.Context, candidate string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, candidate, "version")
	cmd.Env = []string{}
	cmd.Dir = filepath.Dir(candidate)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = time.Second
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	return cmd
}

// pruneHubUpdateBackups keeps the newest rollback copies beside the executable
// and removes the rest. Without it every successful update leaves another full
// binary in the install directory forever. Pruning runs after the replacement
// has succeeded, so a failed update never costs a rollback copy, and a failure
// to remove a stale copy is deliberately not an update failure.
func pruneHubUpdateBackups(executable string, keep int) {
	if keep < 1 {
		keep = 1
	}
	directory := filepath.Dir(executable)
	prefix := filepath.Base(executable) + ".bak-"
	entries, err := os.ReadDir(directory)
	if err != nil {
		return
	}
	backups := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), prefix) {
			backups = append(backups, entry.Name())
		}
	}
	if len(backups) <= keep {
		return
	}
	// The suffix is a fixed-width UTC timestamp, so lexical order is age order.
	sort.Strings(backups)
	for _, stale := range backups[:len(backups)-keep] {
		_ = os.Remove(filepath.Join(directory, stale))
	}
}

func linkOrCopyHubExecutable(source, destination string, mode os.FileMode) error {
	if err := os.Link(source, destination); err == nil {
		return nil
	}
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode.Perm())
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	syncErr := out.Sync()
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// beginHubUpdate admits one self-update at a time. Concurrent update.available
// instructions would otherwise race two applyHubUpdate calls against the same
// executable: each rename is atomic so the binary is never corrupt, but the
// surviving version becomes whichever download finished last and every loser
// still leaves a rollback copy behind. The caller replies update.busy when this
// returns false.
func (client *HubClient) beginHubUpdate() bool {
	client.updateMu.Lock()
	defer client.updateMu.Unlock()
	if client.updateInFlight {
		return false
	}
	client.updateInFlight = true
	return true
}

func (client *HubClient) endHubUpdate() {
	client.updateMu.Lock()
	client.updateInFlight = false
	client.updateMu.Unlock()
}

func (client *HubClient) handleHubUpdate(ctx context.Context, peer *hubClientConnection, message hubOutboundMessage) {
	defer client.endHubUpdate()
	if hubUpdateExcludedMachine(client.machineID) {
		// A node that shares its executable with the hub never replaces it.
		client.warn("hub update rejected")
		return
	}
	updateContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := applyHubUpdate(updateContext, client.updateHTTPClient, client.executablePath, hubUpdateRequest{URL: message.URL, SHA256: message.SHA256, Version: message.Version, Repository: client.updateRepository})
	if errors.Is(err, errHubUpdateSmoke) {
		client.warn("hub update rejected(smoke)")
		if peer != nil {
			_ = peer.write(ctx, struct {
				Type    string `json:"type"`
				Reason  string `json:"reason"`
				Version string `json:"version"`
			}{Type: "update.rejected", Reason: "smoke", Version: message.Version})
		}
		return
	}
	if err != nil {
		client.warn("hub update rejected")
		return
	}
	client.restart()
}

// hubUpdateExcludedMachines never self-update: the NCP root node runs the same
// executable file as the hub service, so replacing it would redeploy the hub
// (BLOCK-3). The comparison folds case and surrounding space so no spelling of
// the identifier escapes it.
var hubUpdateExcludedMachines = []string{"ncp"}

func hubUpdateExcludedMachine(machine string) bool {
	folded := strings.ToLower(strings.TrimSpace(machine))
	for _, excluded := range hubUpdateExcludedMachines {
		if folded == excluded {
			return true
		}
	}
	return false
}

// hubUpdateRollbackAfter is how many consecutive starts of a newly installed
// version may die early without a hub hello before the next start restores
// the rollback copy.
const hubUpdateRollbackAfter = 3

// hubUpdateProbationWindow is AC4's "died right after start" bound. A start
// of the version under probation that is still alive this long after it began
// was not an early death, so it ends the consecutive-failure streak even if
// the hub was unreachable. (A variable only so fixtures need not wait 60s.)
var hubUpdateProbationWindow = 60 * time.Second

// hubUpdateStateMu serializes this process's reads and writes of the
// probation record (startup, the window timer, and the hello confirmation).
var hubUpdateStateMu sync.Mutex

// hubUpdateState is the probation record applyHubUpdate leaves beside the
// executable. Starts counts daemon starts of Version that have not (yet)
// reached a hub hello.
type hubUpdateState struct {
	Version             string `json:"version"`
	Backup              string `json:"backup"`
	Starts              int    `json:"starts"`
	RollbackUnavailable bool   `json:"rollback_unavailable,omitempty"`
}

func hubUpdateStatePath(executable string) string { return executable + ".update-state" }

func readHubUpdateState(executable string) (hubUpdateState, bool) {
	raw, err := os.ReadFile(hubUpdateStatePath(executable))
	if err != nil || len(raw) > 4096 {
		return hubUpdateState{}, false
	}
	var state hubUpdateState
	if json.Unmarshal(raw, &state) != nil {
		return hubUpdateState{}, false
	}
	return state, true
}

func writeHubUpdateState(executable string, state hubUpdateState) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(executable), ".panewire-update-state-")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if _, err = temporary.Write(raw); err != nil {
		temporary.Close()
		return err
	}
	if err = temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err = temporary.Close(); err != nil {
		return err
	}
	return os.Rename(name, hubUpdateStatePath(executable))
}

// hubUpdateStartup is the rollback judgment. It runs first in `panewire
// daemon`, before flag parsing or any schema guard, because a process that
// dies cannot restore itself: only the next start can. It reports true when it
// restored the rollback copy; the caller then exits so launchd/systemd start
// the restored executable. When it counts this start it also arms the
// probation window; the returned stop cancels it and must run when the daemon
// exits, so a process that ends inside the window stays counted.
func hubUpdateStartup(executable, version string, warn func(string)) (bool, func()) {
	if executable == "" {
		var err error
		if executable, err = os.Executable(); err != nil {
			return false, func() {}
		}
	}
	hubUpdateStateMu.Lock()
	defer hubUpdateStateMu.Unlock()
	rolledBack, counted := judgeHubUpdateStartup(executable, version, warn)
	if !counted {
		return rolledBack, func() {}
	}
	timer := time.AfterFunc(hubUpdateProbationWindow, func() { survivedHubUpdateWindow(executable, version) })
	return false, func() { timer.Stop() }
}

// survivedHubUpdateWindow resets the consecutive early-death count once this
// start outlived the probation window. Probation itself continues until a
// hello, so a later run of early deaths still rolls back.
func survivedHubUpdateWindow(executable, version string) {
	hubUpdateStateMu.Lock()
	defer hubUpdateStateMu.Unlock()
	state, found := readHubUpdateState(executable)
	if !found || state.Version != version || state.RollbackUnavailable || state.Starts == 0 {
		return
	}
	state.Starts = 0
	_ = writeHubUpdateState(executable, state)
}

// judgeHubUpdateStartup reports whether it rolled back and whether it counted
// this start as one more probation start.
func judgeHubUpdateStartup(executable, version string, warn func(string)) (bool, bool) {
	state, found := readHubUpdateState(executable)
	if !found {
		if _, err := os.Stat(hubUpdateStatePath(executable)); err == nil {
			// An unreadable record cannot be judged; drop it rather than let it
			// block every later update.
			_ = os.Remove(hubUpdateStatePath(executable))
		}
		return false, false
	}
	if state.Version != version {
		// The running executable is not the version under probation: the
		// rename never happened, or someone replaced the file by hand.
		_ = os.Remove(hubUpdateStatePath(executable))
		return false, false
	}
	if state.RollbackUnavailable {
		return false, false
	}
	if state.Starts < hubUpdateRollbackAfter {
		state.Starts++
		if err := writeHubUpdateState(executable, state); err != nil {
			warn("hub update probation was not recorded")
			return false, false
		}
		return false, true
	}
	if err := restoreHubUpdateBackup(executable, state.Backup); err != nil {
		warn("hub update rollback unavailable")
		state.RollbackUnavailable = true
		_ = writeHubUpdateState(executable, state)
		return false, false
	}
	_ = os.Remove(hubUpdateStatePath(executable))
	warn("hub update rolled back to " + state.Backup)
	return true, false
}

// restoreHubUpdateBackup atomically puts the recorded rollback copy back at
// the executable path. The copy itself is kept.
func restoreHubUpdateBackup(executable, backup string) error {
	prefix := filepath.Base(executable) + ".bak-"
	if backup == "" || backup != filepath.Base(backup) || !strings.HasPrefix(backup, prefix) {
		return errors.New("invalid rollback copy")
	}
	source := filepath.Join(filepath.Dir(executable), backup)
	info, err := os.Lstat(source)
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("rollback copy unavailable")
	}
	temporary, err := os.CreateTemp(filepath.Dir(executable), ".panewire-rollback-")
	if err != nil {
		return err
	}
	name := temporary.Name()
	temporary.Close()
	defer os.Remove(name)
	if err = os.Remove(name); err != nil {
		return err
	}
	if err = linkOrCopyHubExecutable(source, name, info.Mode()); err != nil {
		return err
	}
	return os.Rename(name, executable)
}

// confirmHubUpdate ends probation once this process has said hello to a hub:
// the installed version started, reached the hub, and is not a crash loop.
func (client *HubClient) confirmHubUpdate() {
	executable := client.executablePath
	if executable == "" {
		var err error
		if executable, err = os.Executable(); err != nil {
			return
		}
	}
	hubUpdateStateMu.Lock()
	defer hubUpdateStateMu.Unlock()
	if state, found := readHubUpdateState(executable); found && state.Version == client.version {
		_ = os.Remove(hubUpdateStatePath(executable))
	}
}

func (client *HubClient) handleHubQuota(ctx context.Context, peer *hubClientConnection, message hubOutboundMessage) {
	command, err := exec.LookPath("scopefuel")
	if errors.Is(err, exec.ErrNotFound) {
		err = errors.New("executable file not found")
	}
	payload := ""
	if err == nil {
		runContext, cancel := context.WithTimeout(ctx, 15*time.Second)
		out, runErr := runHubScopefuel(runContext, command)
		cancel()
		if runErr != nil {
			err = runErr
		} else {
			payload = string(out)
		}
	}
	response := struct {
		Type      string `json:"type"`
		RequestID string `json:"request_id"`
		Payload   string `json:"payload,omitempty"`
		Error     string `json:"error,omitempty"`
	}{Type: "quota.report", RequestID: message.RequestID, Payload: payload}
	if err != nil {
		switch err.Error() {
		case "output_too_large", "timeout":
			response.Error = err.Error()
		case "executable file not found":
			response.Error = "unsupported"
		default:
			response.Error = "scopefuel failed"
		}
	}
	_ = peer.write(ctx, response)
}

// scopefuel documents HOME, CODEX_HOME, and CLAUDE_CONFIG_DIR as credential
// locations.  PATH finds its helpers; USER and LANG retain ordinary CLI
// behavior.  Nothing else from the daemon (notably hub/CF credentials) is
// inherited by this fixed command.
func hubScopefuelEnvironment(environment []string) []string {
	allowed := map[string]bool{
		"PATH": true, "HOME": true, "USER": true, "LANG": true,
		"CODEX_HOME": true, "CLAUDE_CONFIG_DIR": true,
	}
	return filterHubEnvironment(environment, allowed)
}

func filterHubEnvironment(environment []string, allowed map[string]bool) []string {
	filtered := make([]string, 0, len(allowed))
	for _, entry := range environment {
		name, _, found := strings.Cut(entry, "=")
		if found && allowed[name] {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func runHubScopefuel(ctx context.Context, command string) ([]byte, error) {
	return runHubScopefuelWithEnvironment(ctx, command, hubScopefuelEnvironment(os.Environ()))
}

func runHubScopefuelWithEnvironment(ctx context.Context, command string, environment []string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, command, "--json")
	cmd.Env = environment
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = time.Second
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.New("scopefuel failed")
	}
	if err := cmd.Start(); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return nil, errors.New("executable file not found")
		}
		return nil, errors.New("scopefuel failed")
	}
	out, readErr := io.ReadAll(io.LimitReader(stdout, hubQuotaOutputLimit+1))
	if len(out) > hubQuotaOutputLimit {
		_ = cmd.Cancel()
		_ = cmd.Wait()
		return nil, errors.New("output_too_large")
	}
	waitErr := cmd.Wait()
	if ctx.Err() == context.DeadlineExceeded {
		return nil, errors.New("timeout")
	}
	if readErr != nil || waitErr != nil {
		return nil, errors.New("scopefuel failed")
	}
	if hubQuotaEncodedSize(out) > hubQuotaEncodedLimit {
		return nil, errors.New("output_too_large")
	}
	return out, nil
}

// hubQuotaEncodedSize is what carrying payload as a JSON string actually costs
// on the wire, quotes and every escape included.
func hubQuotaEncodedSize(payload []byte) int {
	encoded, err := json.Marshal(string(payload))
	if err != nil {
		return hubQuotaEncodedLimit + 1
	}
	return len(encoded)
}

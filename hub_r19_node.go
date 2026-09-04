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
func validHubUpdateURL(value string) bool {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" || (u.Port() != "" && u.Port() != "443") {
		return false
	}
	// Release downloads start at github.com and GitHub redirects the asset to
	// objects.githubusercontent.com.  No other host is a release authority.
	switch strings.ToLower(u.Hostname()) {
	case "github.com":
		return u.RawQuery == ""
	case "objects.githubusercontent.com":
		// GitHub's signed release-asset redirect carries query parameters.
		return true
	default:
		return false
	}
}

func hubUpdateClient(client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	copy := *client
	copy.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) > hubUpdateRedirectLimit || !validHubUpdateURL(request.URL.String()) {
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

// applyHubUpdate downloads to a sibling temporary file, verifies it before
// touching the executable, then leaves a timestamped rollback copy. It is
// intentionally fail-closed: every error before the final rename leaves the
// running executable unchanged.
func applyHubUpdate(ctx context.Context, httpClient *http.Client, executable, assetURL, expectedSHA string) error {
	if !validHubUpdateURL(assetURL) || !validHubSHA256(expectedSHA) {
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
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, assetURL, nil)
	if err != nil {
		return errors.New("update unavailable")
	}
	response, err := hubUpdateClient(httpClient).Do(request)
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
	if hex.EncodeToString(hash.Sum(nil)) != strings.ToLower(expectedSHA) {
		return errors.New("update checksum mismatch")
	}
	if err = os.Chmod(temporaryName, info.Mode().Perm()); err != nil {
		return errors.New("update unavailable")
	}
	backup := executable + ".bak-" + time.Now().UTC().Format("20060102T150405Z")
	// Keep the old inode available before atomically replacing its pathname.
	// Unlike rename(executable, backup), this never creates a crash window in
	// which the executable pathname is absent.
	if err = linkOrCopyHubExecutable(executable, backup, info.Mode()); err != nil {
		return errors.New("update unavailable")
	}
	if err = os.Rename(temporaryName, executable); err != nil {
		return errors.New("update unavailable")
	}
	pruneHubUpdateBackups(executable, hubUpdateBackupsKept)
	return nil
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

func (client *HubClient) handleHubUpdate(message hubOutboundMessage) {
	defer client.endHubUpdate()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := applyHubUpdate(ctx, client.updateHTTPClient, client.executablePath, message.URL, message.SHA256); err != nil {
		client.warn("hub update rejected")
		return
	}
	client.restart()
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
	cmd := exec.CommandContext(ctx, command, "--json")
	cmd.Env = hubScopefuelEnvironment(os.Environ())
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

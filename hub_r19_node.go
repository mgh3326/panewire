package panewire

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"syscall"
	"time"
)

var hubRequestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,96}$`)

const (
	hubQuotaOutputLimit = 16 << 10
	// A quota report crosses the wire as a JSON string inside a message the hub
	// reads under hubMaxMessageBytes. JSON escaping can more than double the raw
	// stdout size ("" and \n cost two bytes each, control bytes six), so the raw
	// cap alone can still produce a message the hub refuses. The bound that
	// matters is therefore the encoded one, and it is kept below
	// hubMaxMessageBytes so the surrounding envelope still fits.
	hubQuotaEncodedLimit = 24 << 10
)

func validHubRequestID(value string) bool { return hubRequestIDPattern.MatchString(value) }
func validHubSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
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

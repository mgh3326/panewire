package claude

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

const (
	cliTimeout    = 15 * time.Second
	cliOutputCap  = 1 << 20
	cliKillDelay  = time.Second
	cliTempPrefix = "panewire-claude-usage-"
)

// fetchCLI is the single CLI fallback: `claude -p /usage` in a fresh private
// directory, never the caller's repository. A failure to run is probe_error;
// an unrecognised panel is parse_error.
func (c Config) fetchCLI(ctx context.Context) (quota.Observation, error) {
	timeout := c.CLITimeout
	if timeout <= 0 {
		timeout = cliTimeout
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	dir, err := os.MkdirTemp("", cliTempPrefix)
	if err != nil {
		return quota.Observation{}, quota.Fail(quota.ErrKindProbe, "usage CLI directory unavailable")
	}
	defer os.RemoveAll(dir)
	run := c.RunCLI
	if run == nil {
		run = runUsageCommand
	}
	binary := c.Binary
	if binary == "" {
		binary = "claude"
	}
	configDir := c.ConfigDir
	text, err := run(probeCtx, binary, dir, configDir)
	if err != nil {
		return quota.Observation{}, quota.Fail(quota.ErrKindProbe, "usage CLI unavailable")
	}
	return ParseUsageScreen(text, c.now())
}

// runUsageCommand runs the real claude binary. The environment is inherited
// (the CLI needs HOME and PATH) with CLAUDE_CONFIG_DIR set from the account
// selector; the token is not passed, and stderr is discarded.
func runUsageCommand(ctx context.Context, binary, dir, configDir string) (string, error) {
	path, err := exec.LookPath(binary)
	if err != nil {
		return "", fmt.Errorf("claude executable unavailable")
	}
	cmd := exec.CommandContext(ctx, path, "-p", "/usage", "--allowed-tools", "")
	cmd.Dir = dir
	cmd.Env = os.Environ()
	if configDir != "" {
		cmd.Env = append(cmd.Env, "CLAUDE_CONFIG_DIR="+configDir)
	}
	out := &boundedBuffer{}
	cmd.Stdout = out
	cmd.Stdin = nil
	cmd.WaitDelay = cliKillDelay
	killGroupOnCancel(cmd)
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("claude usage command failed")
	}
	return out.String(), nil
}

// boundedBuffer keeps at most cliOutputCap bytes and discards the rest.
type boundedBuffer struct{ buf bytes.Buffer }

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if left := cliOutputCap - b.buf.Len(); left > 0 {
		b.buf.Write(p[:min(len(p), left)])
	}
	return len(p), nil
}

func (b *boundedBuffer) String() string { return b.buf.String() }

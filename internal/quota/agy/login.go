package agy

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/configpath"
)

// fileToken is the CLI's OAuth access token from its login file, in
// pacelever's two locations. An unreadable, oversized or malformed file has
// none.
func (c Config) fileToken() string {
	for _, path := range []string{
		filepath.Join(c.home(), ".gemini", "antigravity-cli", "antigravity-oauth-token"),
		filepath.Join(c.base("XDG_CONFIG_HOME", ".config"), "antigravity-cli", "antigravity-oauth-token"),
	} {
		var file struct {
			Token struct {
				AccessToken string `json:"access_token"`
			} `json:"token"`
		}
		if data, err := c.readFile(path); err == nil && json.Unmarshal(data, &file) == nil {
			if token := strings.TrimSpace(file.Token.AccessToken); token != "" {
				return token
			}
		}
	}
	return ""
}

func (c Config) readFile(path string) ([]byte, error) {
	read := c.ReadFile
	if read == nil {
		read = os.ReadFile
	}
	data, err := read(path)
	switch {
	case err != nil:
		return nil, err
	case len(data) > authFileLimit:
		return nil, errors.New("file too large")
	}
	return data, nil
}

// secretToken is the OS keyring copy of the login: the keychain item on macOS,
// the secret service item on Linux, "" elsewhere and when the item is absent,
// locked or unreadable. The read is bounded at 10 s and never prompts.
func (c Config) secretToken(ctx context.Context) string {
	read := c.secretReader()
	if read == nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := read(ctx)
	if err != nil {
		return ""
	}
	return envelopeToken(strings.TrimSpace(out))
}

// secretReader is the platform's keyring read: the keychain on macOS, the
// secret service on Linux, none elsewhere.
func (c Config) secretReader() func(context.Context) (string, error) {
	switch c.goos() {
	case "darwin":
		if c.Keychain != nil {
			return c.Keychain
		}
		return securityKeychain
	case "linux":
		if c.SecretService != nil {
			return c.SecretService
		}
		return secretServiceItem
	}
	return nil
}

// securityKeychain reads the item's secret through `security`. The service and
// account names are fixed and hold no secret.
func securityKeychain(ctx context.Context) (string, error) {
	out, err := exec.CommandContext(ctx, "security", "find-generic-password", "-s", "gemini", "-a", "antigravity", "-w").Output()
	return string(out), err
}

// envelopeToken unwraps the go-keyring envelope and reads $.token.access_token
// from the enclosed JSON.
func envelopeToken(raw string) string {
	if s := strings.TrimPrefix(raw, "go-keyring-base64:"); s != raw {
		decoded, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return ""
		}
		raw = string(decoded)
	} else if s := strings.TrimPrefix(raw, "go-keyring-encoded:"); s != raw {
		decoded, err := hex.DecodeString(s)
		if err != nil {
			return ""
		}
		raw = string(decoded)
	}
	var data struct {
		Token struct {
			AccessToken string `json:"access_token"`
		} `json:"token"`
	}
	if json.Unmarshal([]byte(raw), &data) != nil {
		return ""
	}
	return strings.TrimSpace(data.Token.AccessToken)
}

// wakeLockPath keeps local quota refreshes single-flight. It selects an
// existing legacy lock path only when no panewire cache path exists.
func (c Config) wakeLockPath() string {
	if c.WakeLock != "" {
		return c.WakeLock
	}
	paths := configpath.CacheCandidates(c.env, c.home(), "agy-wake.lock")
	if c.env("PANEWIRE_CACHE") != "" {
		return paths[0]
	}
	if c.env("PACELEVER_CACHE") != "" && len(paths) > 1 {
		return paths[1]
	}
	if _, err := os.Stat(paths[0]); errors.Is(err, os.ErrNotExist) && len(paths) > 1 {
		if _, legacyErr := os.Stat(paths[1]); legacyErr == nil {
			return paths[1]
		}
	}
	return paths[0]
}

func (c Config) locked(fn func() error) error {
	if c.Locked != nil {
		return c.Locked(c.wakeLockPath(), fn)
	}
	return withFileLock(c.wakeLockPath(), fn)
}

func (c Config) wake(ctx context.Context, binary string) error {
	if c.Wake != nil {
		return c.Wake(ctx, binary)
	}
	ctx, cancel := context.WithTimeout(ctx, wakeTimeout)
	defer cancel()
	// The CLI output may carry account details, so it is discarded.
	cmd := exec.CommandContext(ctx, binary, "models")
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	return cmd.Run()
}

// refresh re-arms a refused token under the lock shared with pacelever. A token
// another holder just refreshed is reused instead of waking the CLI again. It
// returns the token to retry with and whether it came from the OS keyring.
func (c Config) refresh(ctx context.Context, binary, token string, usedSecret bool) (string, bool, error) {
	// Re-read whichever store supplied the token: the wake or a concurrent
	// refresh may have replaced it in either one.
	latest := func() (string, bool) {
		if fresh := c.fileToken(); fresh != "" {
			return fresh, false
		}
		if usedSecret {
			if fresh := c.secretToken(ctx); fresh != "" {
				return fresh, true
			}
		}
		return token, usedSecret
	}
	next, nextSecret := token, usedSecret
	err := c.locked(func() error {
		if fresh, secret := latest(); fresh != token {
			next, nextSecret = fresh, secret
			return nil
		}
		if err := c.wake(ctx, binary); err != nil {
			return err
		}
		next, nextSecret = latest()
		return nil
	})
	return next, nextSecret, err
}

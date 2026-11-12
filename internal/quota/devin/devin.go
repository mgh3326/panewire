// Package devin measures the devin pool's quota the way pacelever does: the
// read-only web quota endpoint first, devin's own `/usage` panel through a
// pseudo-terminal if that fails, the same buckets, the same error kinds and the
// same account fingerprint.
//
// Only a host with a devin login measures. The API key proves a login exists;
// it is never read into a request, hashed, logged or returned. The web source
// uses DEVIN_BEARER_TOKEN from the environment and the organization named by
// pacelever's config, as pacelever does, and nothing is written back.
package devin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"

	"github.com/mgh3326/panewire/internal/configpath"
	"github.com/mgh3326/panewire/internal/quota"
)

const (
	authFileLimit = 1 << 20
	// ProbeTimeout is how long the `/usage` probe may take, as pacelever's.
	ProbeTimeout = 35 * time.Second
)

// Doer is the HTTP client the web request goes through.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// Config is everything a measurement reads from its environment. The zero
// value measures this host's login with the real filesystem, devin binary,
// pseudo-terminal and wall clock; tests replace the seams.
type Config struct {
	HTTP Doer // nil: 20 s timeout, redirects refused
	// Endpoint is the web origin; empty is https://app.devin.ai.
	Endpoint string
	// Now is the clock for relative reset times; nil is the wall clock.
	Now func() time.Time
	// Home is the home directory; empty is os.UserHomeDir.
	Home string
	// LookupEnv reads the environment; nil is os.LookupEnv.
	LookupEnv func(string) (string, bool)
	// ReadFile reads the login, identity and settings files; nil is os.ReadFile.
	ReadFile func(string) ([]byte, error)
	// ReplaceFile publishes a cache file atomically; nil uses the durable helper.
	ReplaceFile func(string, string, []byte, os.FileMode) error
	// Binary is the configured devin executable, tried after the
	// PANEWIRE_DEVIN_BIN override and the settings file's path, before PATH.
	Binary string
	// LookPath finds devin on PATH; nil is exec.LookPath.
	LookPath func(string) (string, error)
	// Hostname and UID name this host and user for the free-limit record's
	// key; nil are os.Hostname and os.Getuid.
	Hostname func() (string, error)
	UID      func() int
	// Clock paces the probe; nil is the wall clock. A frozen clock never
	// expires a probe, so tests drive a fake one.
	Clock Clock
	// ProbeTimeout bounds one probe; zero is ProbeTimeout.
	ProbeTimeout time.Duration
	// StartProbe starts the devin TUI on a pseudo-terminal; nil starts the real
	// binary. Closing the terminal ends and reaps the process.
	StartProbe func(ctx context.Context, binary, dir string, args []string) (Terminal, error)
}

func (c Config) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now().UTC()
}

func (c Config) home() string {
	if c.Home != "" {
		return c.Home
	}
	home, _ := os.UserHomeDir()
	return home
}

func (c Config) lookupEnv(name string) (string, bool) {
	if c.LookupEnv != nil {
		return c.LookupEnv(name)
	}
	return os.LookupEnv(name)
}

func (c Config) env(name string) string {
	value, _ := c.lookupEnv(name)
	return strings.TrimSpace(value)
}

func (c Config) readFile(path string) []byte {
	data, err := c.readFileErr(path)
	if err != nil {
		return nil
	}
	return data
}

// readFileErr reads a small file, saying why when it cannot: an absent file is
// fs.ErrNotExist, so a caller can tell it from a present one it cannot use.
func (c Config) readFileErr(path string) ([]byte, error) {
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

// base is an XDG root: the variable when set, else a directory under home.
func (c Config) base(variable, fallback string) string {
	if dir := c.env(variable); dir != "" {
		return dir
	}
	return filepath.Join(c.home(), fallback)
}

func (c Config) dataBase() string   { return c.base("XDG_DATA_HOME", ".local/share") }
func (c Config) configBase() string { return c.base("XDG_CONFIG_HOME", ".config") }
func (c Config) cacheBase() string  { return c.base("XDG_CACHE_HOME", ".cache") }

// Fingerprint is hex(sha256("devin-account|"+org))[:16]; no organization has
// none. It is pacelever's accountFP("devin", org).
func Fingerprint(org string) string {
	if org == "" {
		return ""
	}
	sum := sha256.Sum256([]byte("devin-account|" + org))
	return hex.EncodeToString(sum[:8])
}

// HasLogin reports whether this host has a devin login: the CLI's credentials
// file holds a non-blank API key. Only such a host measures.
func HasLogin(cfg Config) bool {
	var credentials struct {
		APIKey string `toml:"windsurf_api_key"`
	}
	body := cfg.readFile(filepath.Join(cfg.dataBase(), "devin", "credentials.toml"))
	return body != nil && toml.Unmarshal(body, &credentials) == nil && strings.TrimSpace(credentials.APIKey) != ""
}

// AccountFP is this host's devin account fingerprint, "" without a login or
// without a selected organization. The organization is the non-secret
// identity; a selection left behind after logout is not a login.
func AccountFP(cfg Config) string {
	if !HasLogin(cfg) {
		return ""
	}
	var config struct {
		Devin struct {
			OrgID any `json:"org_id"`
		} `json:"devin"`
	}
	if !decodeJSONObject(cfg.readFile(filepath.Join(cfg.configBase(), "devin", "config.json")), &config) {
		return ""
	}
	org, _ := config.Devin.OrgID.(string)
	return Fingerprint(strings.TrimSpace(org))
}

// settings are the two values pacelever's config.toml sets for the devin pool.
type settings struct{ orgID, bin string }

func (c Config) settings() (settings, error) {
	paths := configpath.Candidates(c.env, c.home(), "config.toml")
	_, variable, explicit := configpath.Explicit(c.env)
	var file struct {
		Pools struct {
			Devin struct {
				OrgID string `toml:"org_id"`
				Bin   string `toml:"bin"`
			} `toml:"devin"`
		} `toml:"pools"`
	}
	for _, path := range paths {
		body, err := c.readFileErr(path)
		if errors.Is(err, os.ErrNotExist) {
			if explicit {
				return settings{}, fmt.Errorf("%s %q does not exist", variable, path)
			}
			continue
		}
		if err != nil {
			return settings{}, fmt.Errorf("cannot read %s %q: %w", configPathLabel(variable, explicit), path, err)
		}
		if err := toml.Unmarshal(body, &file); err != nil {
			return settings{}, fmt.Errorf("invalid %s %q: %w", configPathLabel(variable, explicit), path, err)
		}
		return settings{orgID: strings.TrimSpace(file.Pools.Devin.OrgID), bin: strings.TrimSpace(file.Pools.Devin.Bin)}, nil
	}
	return settings{}, nil
}

func configPathLabel(variable string, explicit bool) string {
	if explicit {
		return variable
	}
	return "Devin config"
}

// binary is the devin executable: PANEWIRE_DEVIN_BIN (PACELEVER_DEVIN_BIN
// fallback), the settings file's
// path, the configured one, then devin on PATH.
func (c Config) binary(s settings) string {
	explicit := c.env("PANEWIRE_DEVIN_BIN")
	if explicit == "" {
		explicit = c.env("PACELEVER_DEVIN_BIN")
	}
	if explicit == "" {
		explicit = s.bin
	}
	if explicit == "" {
		explicit = c.Binary
	}
	if explicit != "" {
		if rest, ok := strings.CutPrefix(explicit, "~/"); ok {
			return filepath.Join(c.home(), rest)
		}
		return explicit
	}
	look := c.LookPath
	if look == nil {
		look = exec.LookPath
	}
	if path, err := look("devin"); err == nil {
		return path
	}
	return "devin"
}

// Measure reads the devin quota: the web endpoint when DEVIN_BEARER_TOKEN and
// an organization are set, then the `/usage` probe. A web failure falls through
// to the probe silently, as pacelever's does. Without a login nothing is run or
// sent. The error is a *quota.Error.
func Measure(ctx context.Context, cfg Config) (quota.Observation, error) {
	if !HasLogin(cfg) {
		return quota.Observation{}, quota.Fail(quota.ErrKindCredentials, "credentials unavailable")
	}
	login := AccountFP(cfg)
	s, err := cfg.settings()
	if err != nil {
		return quota.Observation{}, quota.Fail(quota.ErrKindCredentials, err.Error())
	}
	if observed, err := cfg.fetchWeb(ctx, cfg.env("DEVIN_BEARER_TOKEN"), s.orgID); err == nil {
		// The web source selects its organization independently of the CLI
		// login, so its measurement carries the organization it measured; the
		// shared flow refuses it when that is not the login's account.
		observed.AccountFP = Fingerprint(s.orgID)
		return observed, nil
	}
	observed, err := cfg.fetchProbe(ctx, cfg.binary(s))
	if err != nil {
		return quota.Observation{}, err
	}
	observed.AccountFP = login
	return observed, nil
}

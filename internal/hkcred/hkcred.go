// Package hkcred resolves panewire's own handoffkeep service credential, the
// svc-panewire client id, for every panewire hk HTTP path that is not handed
// an explicit file (hk 3526 §4, hk 3570). The contract is in docs/config.md
// ("handoffkeep service credential"):
//
//  1. HANDOFFKEEP_URL and HANDOFFKEEP_TOKEN_SVC_PANEWIRE both set in the
//     environment (with the optional CF Access pair) win.
//  2. Otherwise the panewire-owned mode-0600 file ~/.config/panewire/hk-svc.env
//     supplies the same keys; PANEWIRE_HK_ENV overrides the path.
//  3. The hk CLI's ~/.config/handoffkeep/config.env and HANDOFFKEEP_TOKEN are
//     never read here. An explicit file given by flag reads its token through
//     ExplicitFileToken instead.
package hkcred

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/mgh3326/panewire/internal/envfile"
)

// The keys of the svc-panewire credential, in the environment and in the file.
const (
	URLKey      = "HANDOFFKEEP_URL"
	TokenKey    = "HANDOFFKEEP_TOKEN_SVC_PANEWIRE"
	CFIDKey     = "HANDOFFKEEP_CF_ACCESS_CLIENT_ID"
	CFSecretKey = "HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET"
	// PathEnv names a file to read in place of the default path.
	PathEnv = "PANEWIRE_HK_ENV"
)

// explicitFileLegacyTokenKey is the token key an explicit file written before
// hk 3570 carries. Only ExplicitFileToken reads it.
const explicitFileLegacyTokenKey = "HANDOFFKEEP_TOKEN"

// Credentials is one resolved hk endpoint and bearer, with the optional
// Cloudflare Access service-token pair (both or neither).
type Credentials struct {
	URL, Token, CFID, CFSecret string
}

// UnconfiguredError reports why no usable credential was found. Its text
// names keys and the file path, never a value.
type UnconfiguredError struct {
	// Path is the file consulted; empty when the home directory is unknown.
	Path string
	// Missing lists the keys the chosen source lacks. With Absent it lists
	// the environment keys that were not both set.
	Missing []string
	// Absent is true when the file does not exist.
	Absent bool
	// Unreadable is true when the file exists but is not a regular mode-0600
	// env file.
	Unreadable bool
	// Source is "environment" or "file".
	Source string
}

func (e *UnconfiguredError) Error() string {
	prefix := "hk service credential unconfigured: "
	switch {
	case e.Path == "" && e.Source == "file":
		return prefix + "no home directory for ~/.config/panewire/hk-svc.env; set " + URLKey + " and " + TokenKey + " or " + PathEnv
	case e.Absent:
		return prefix + "set " + URLKey + " and " + TokenKey + ", or create " + e.Path
	case e.Unreadable:
		return prefix + e.Path + " must be a regular mode-0600 env file"
	case e.Source == "environment":
		return prefix + "environment lacks " + strings.Join(e.Missing, ", ")
	default:
		return prefix + e.Path + " lacks " + strings.Join(e.Missing, ", ")
	}
}

// Path is the svc-panewire credential file: PANEWIRE_HK_ENV, else
// ~/.config/panewire/hk-svc.env.
func Path() (string, error) {
	if p := strings.TrimSpace(os.Getenv(PathEnv)); p != "" {
		return p, nil
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "", errors.New("no home directory")
	}
	return PathIn(home), nil
}

// PathIn is Path for a known home directory.
func PathIn(home string) string {
	if p := strings.TrimSpace(os.Getenv(PathEnv)); p != "" {
		return p
	}
	return filepath.Join(home, ".config", "panewire", "hk-svc.env")
}

// Load resolves the svc-panewire credential. On error it answers the zero
// Credentials, so no caller can take a token from a source it refused; a
// caller that needs only the endpoint uses URL.
func Load() (Credentials, error) {
	creds, err := resolve()
	if err != nil {
		return Credentials{}, err
	}
	return creds, nil
}

// URL is the hk endpoint of the source Load would choose, even when that
// source lacks its token or half a CF pair: a transport preflight compares
// endpoints, not credentials. Empty when there is none.
func URL() string {
	creds, _ := resolve()
	return creds.URL
}

// resolve is Load before the error rule: with an error it still answers what
// the chosen source carried.
func resolve() (Credentials, error) {
	env := func(key string) string { return strings.TrimSpace(os.Getenv(key)) }
	if url, token := env(URLKey), env(TokenKey); url != "" && token != "" {
		creds := Credentials{URL: url, Token: token, CFID: env(CFIDKey), CFSecret: env(CFSecretKey)}
		if missing := creds.missing(); len(missing) > 0 {
			return creds, &UnconfiguredError{Source: "environment", Missing: missing}
		}
		return creds, nil
	}
	path, err := Path()
	if err != nil {
		return Credentials{}, &UnconfiguredError{Source: "file"}
	}
	if _, err := os.Lstat(path); errors.Is(err, fs.ErrNotExist) {
		return Credentials{}, &UnconfiguredError{Source: "file", Path: path, Absent: true}
	}
	values, err := envfile.Load(path)
	if err != nil {
		return Credentials{}, &UnconfiguredError{Source: "file", Path: path, Unreadable: true}
	}
	creds := Credentials{URL: values[URLKey], Token: values[TokenKey], CFID: values[CFIDKey], CFSecret: values[CFSecretKey]}
	if missing := creds.missing(); len(missing) > 0 {
		return creds, &UnconfiguredError{Source: "file", Path: path, Missing: missing}
	}
	return creds, nil
}

// missing names the absent required keys and the absent half of a CF pair.
func (c Credentials) missing() []string {
	var out []string
	if c.URL == "" {
		out = append(out, URLKey)
	}
	if c.Token == "" {
		out = append(out, TokenKey)
	}
	switch {
	case c.CFID != "" && c.CFSecret == "":
		out = append(out, CFSecretKey)
	case c.CFID == "" && c.CFSecret != "":
		out = append(out, CFIDKey)
	}
	return out
}

// ExplicitFileToken is the bearer of a file the operator named explicitly
// (a hub flag, spawn.json handoffkeep_env):
// HANDOFFKEEP_TOKEN_SVC_PANEWIRE first, else the file's HANDOFFKEEP_TOKEN so
// files written before hk 3570 keep working.
func ExplicitFileToken(values map[string]string) string {
	if token := values[TokenKey]; token != "" {
		return token
	}
	return values[explicitFileLegacyTokenKey]
}

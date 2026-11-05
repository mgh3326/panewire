package claude

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"golang.org/x/text/unicode/norm"
)

// locations says where this login lives. Only locations: no token is read
// here.
type locations struct {
	credential string // <dir>/.credentials.json
	identity   string // .claude.json naming the account
	service    string // macOS keychain service name
	// shareable is false when the credential store is steered away from the
	// config dir, so the identity file does not name the credential's account.
	shareable bool
}

// locations resolves the credential, identity and keychain service from the
// selector, the environment and the home directory.
func (c Config) locations() locations {
	configDir := c.ConfigDir
	if configDir == "" {
		configDir, _ = c.lookupEnv("CLAUDE_CONFIG_DIR")
	}
	secure, hasSecure := c.lookupEnv("CLAUDE_SECURESTORAGE_CONFIG_DIR")
	dir := configDir
	if hasSecure {
		dir = secure
	}
	home := c.home()
	if dir == "" {
		dir = filepath.Join(home, ".claude")
	}
	loc := locations{credential: filepath.Join(dir, ".credentials.json"), identity: filepath.Join(home, ".claude.json")}
	if configDir != "" {
		loc.identity = filepath.Join(configDir, ".claude.json")
	}
	contextName := configDir
	if hasSecure {
		contextName = secure
	}
	loc.service = "Claude Code-credentials"
	if contextName != "" {
		sum := sha256.Sum256([]byte(norm.NFC.String(contextName)))
		loc.service += "-" + hex.EncodeToString(sum[:4])
	}
	loc.shareable = !hasSecure || secure == configDir
	return loc
}

// oauth is the claudeAiOauth block of the credential file and, on darwin, the
// keychain item, the unexpired and then later-expiring one preferred.
func (c Config) oauth(ctx context.Context, loc locations) map[string]any {
	oauth := object(c.readObject(loc.credential)["claudeAiOauth"])
	goos := c.GOOS
	if goos == "" {
		goos = runtime.GOOS
	}
	if goos != "darwin" {
		return oauth
	}
	keychain := c.Keychain
	if keychain == nil {
		keychain = securityKeychain
	}
	out, err := keychain(ctx, loc.service)
	if err != nil {
		return oauth
	}
	data := decodeObject(out)
	return chooseOAuth(oauth, object(data["claudeAiOauth"]), c.now())
}

// securityKeychain reads a keychain item's secret through `security`. The
// service name is the only argument that varies and holds no secret.
func securityKeychain(ctx context.Context, service string) ([]byte, error) {
	return exec.CommandContext(ctx, "security", "find-generic-password", "-s", service, "-w").Output()
}

// chooseOAuth picks between the file and keychain logins: the one with a
// token, then the unexpired one, then the later expiry.
func chooseOAuth(file, keychain map[string]any, now time.Time) map[string]any {
	if str(file["accessToken"]) == "" {
		return keychain
	}
	if str(keychain["accessToken"]) == "" {
		return file
	}
	f, fok := finiteNumber(file["expiresAt"])
	k, kok := finiteNumber(keychain["expiresAt"])
	expiredF := fok && f <= float64(now.UnixMilli())
	expiredK := kok && k <= float64(now.UnixMilli())
	if expiredF != expiredK {
		if expiredF {
			return keychain
		}
		return file
	}
	if fok && kok && k > f {
		return keychain
	}
	return file
}

// accountFP is the fingerprint of the account recorded in an identity file:
// hex(sha256("claude-account|"+accountUuid))[:16]. An unrecorded account is "".
func (c Config) accountFP(identityPath string) string {
	uuid := str(object(c.readObject(identityPath)["oauthAccount"])["accountUuid"])
	if uuid == "" {
		return ""
	}
	sum := sha256.Sum256([]byte("claude-account|" + uuid))
	return hex.EncodeToString(sum[:8])
}

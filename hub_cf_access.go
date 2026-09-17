package panewire

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"math/big"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

// hubCFAccessCertsTTL bounds how long a fetched Access key set is trusted
// before a refresh; an unknown key id always forces one refresh attempt.
const hubCFAccessCertsTTL = time.Hour

var hubCFAccessTeamPattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]{0,62}[A-Za-z0-9])?$`)

// hubCFAccessVerifier authenticates browser requests by the signed assertion
// Cloudflare Access attaches to requests that cleared an Access application
// (Cf-Access-Jwt-Assertion). Signature, audience, and expiry are all checked;
// the unsigned identity headers (Cf-Access-Authenticated-User-Email and
// friends) are never consulted — any client can set those.
type hubCFAccessVerifier struct {
	certsURL string
	aud      string
	client   *http.Client
	now      func() time.Time
	logger   *slog.Logger
	mu       sync.Mutex
	keys     map[string]*rsa.PublicKey
	fetched  time.Time
}

func newHubCFAccessVerifier(team, aud, certsURL string, client *http.Client, now func() time.Time, logger *slog.Logger) (*hubCFAccessVerifier, error) {
	if !hubCFAccessTeamPattern.MatchString(team) || aud == "" || len(aud) > 256 {
		return nil, errors.New("hub Cloudflare Access configuration is invalid")
	}
	if certsURL == "" {
		certsURL = "https://" + team + ".cloudflareaccess.com/cdn-cgi/access/certs"
	}
	if !validHandoffkeepBaseURL(certsURL) {
		return nil, errors.New("hub Cloudflare Access configuration is invalid")
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &hubCFAccessVerifier{certsURL: certsURL, aud: aud, client: client, now: now, logger: logger, keys: map[string]*rsa.PublicKey{}}, nil
}

// verify reports whether the request carries a Cf-Access-Jwt-Assertion whose
// RS256 signature verifies against the Access team's published keys, whose
// audience is the configured application AUD, and which is currently valid.
func (v *hubCFAccessVerifier) verify(request *http.Request) bool {
	token := strings.TrimSpace(request.Header.Get("Cf-Access-Jwt-Assertion"))
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return false
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if json.Unmarshal(headerJSON, &header) != nil || header.Alg != "RS256" || header.Kid == "" {
		return false
	}
	key := v.keyFor(header.Kid)
	if key == nil {
		return false
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return false
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil {
		return false
	}
	claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	var claims struct {
		Aud json.RawMessage `json:"aud"`
		Exp int64           `json:"exp"`
		Nbf int64           `json:"nbf"`
	}
	if json.Unmarshal(claimsJSON, &claims) != nil {
		return false
	}
	now := v.now().Unix()
	if claims.Exp == 0 || claims.Exp <= now || (claims.Nbf != 0 && claims.Nbf > now) {
		return false
	}
	return cfAccessAudienceMatches(claims.Aud, v.aud)
}

func cfAccessAudienceMatches(raw json.RawMessage, want string) bool {
	var single string
	if json.Unmarshal(raw, &single) == nil {
		return single == want
	}
	var list []string
	if json.Unmarshal(raw, &list) != nil {
		return false
	}
	for _, candidate := range list {
		if candidate == want {
			return true
		}
	}
	return false
}

func (v *hubCFAccessVerifier) keyFor(kid string) *rsa.PublicKey {
	v.mu.Lock()
	defer v.mu.Unlock()
	if key, ok := v.keys[kid]; ok && v.now().Sub(v.fetched) < hubCFAccessCertsTTL {
		return key
	}
	if err := v.refreshLocked(); err != nil {
		v.logger.Warn("Cloudflare Access key refresh failed")
		return nil
	}
	return v.keys[kid]
}

// refreshLocked replaces the cached key set from the team's certs endpoint.
// The response is bounded and the client timeout-bounded; a failure keeps the
// previous cache rather than clearing it.
func (v *hubCFAccessVerifier) refreshLocked() error {
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, v.certsURL, nil)
	if err != nil {
		return err
	}
	response, err := v.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("certs endpoint rejected")
	}
	var document struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if json.NewDecoder(http.MaxBytesReader(nil, response.Body, 1<<20)).Decode(&document) != nil {
		return errors.New("certs document unreadable")
	}
	keys := make(map[string]*rsa.PublicKey, len(document.Keys))
	for _, jwk := range document.Keys {
		if jwk.Kty != "RSA" || jwk.Kid == "" {
			continue
		}
		modulus, err := base64.RawURLEncoding.DecodeString(jwk.N)
		if err != nil {
			continue
		}
		exponentBytes, err := base64.RawURLEncoding.DecodeString(jwk.E)
		if err != nil || len(exponentBytes) == 0 || len(exponentBytes) > 8 {
			continue
		}
		exponent := new(big.Int).SetBytes(exponentBytes).Int64()
		if exponent < 3 || exponent > int64(^uint32(0)>>1) {
			continue
		}
		keys[jwk.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(modulus), E: int(exponent)}
	}
	if len(keys) == 0 {
		return errors.New("certs document carried no RSA keys")
	}
	v.keys = keys
	v.fetched = v.now()
	return nil
}

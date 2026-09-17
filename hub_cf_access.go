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
	"sync/atomic"
	"time"
)

// hubCFAccessCertsTTL bounds how long a fetched Access key set is trusted
// before it is considered stale.
const hubCFAccessCertsTTL = time.Hour

// hubCFAccessRefreshInterval is the proactive re-fetch cadence; it stays
// well under the TTL so the cache normally refreshes before it expires.
const hubCFAccessRefreshInterval = 30 * time.Minute

// hubCFAccessKickMinInterval rate-limits request-driven refresh kicks: a
// flood of unauthenticated requests with an expired cache can schedule at
// most one certs fetch per interval.
const hubCFAccessKickMinInterval = 2 * time.Second

// hubCFAccessMaxTokenLifetime caps how long a single assertion may live:
// a token whose exp sits far past its iat is rejected no matter how valid
// the signature is. Cloudflare Access sessions cap at one month, so the
// bound rejects forged-lifetime tokens without touching legitimate ones.
const hubCFAccessMaxTokenLifetime = 31 * 24 * time.Hour

var hubCFAccessTeamPattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]{0,62}[A-Za-z0-9])?$`)

// hubCFAccessVerifier authenticates browser requests by the signed assertion
// Cloudflare Access attaches to requests that cleared an Access application
// (Cf-Access-Jwt-Assertion). Signature, audience, and expiry are all checked;
// the unsigned identity headers (Cf-Access-Authenticated-User-Email and
// friends) are never consulted — any client can set those.
type hubCFAccessVerifier struct {
	certsURL string
	aud      string
	issuer   string
	client   *http.Client
	now      func() time.Time
	logger   *slog.Logger
	mu       sync.RWMutex
	keys     map[string]*rsa.PublicKey
	fetched  time.Time
	// kick (capacity 1) asks the background loop to refresh the key set.
	// The request path never fetches itself: with no usable cached key a
	// request is rejected (fail closed) while the refresh runs behind it.
	kick     chan struct{}
	loopOnce sync.Once
	// lastKick throttles request-driven kicks to hubCFAccessKickMinInterval,
	// so unauthenticated request volume cannot drive fetch volume.
	lastKick atomic.Int64
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
	verifier := &hubCFAccessVerifier{certsURL: certsURL, aud: aud, issuer: "https://" + team + ".cloudflareaccess.com", client: client, now: now, logger: logger, keys: map[string]*rsa.PublicKey{}, kick: make(chan struct{}, 1)}
	verifier.kickRefresh()
	return verifier, nil
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
		Iat int64           `json:"iat"`
		Iss string          `json:"iss"`
	}
	if json.Unmarshal(claimsJSON, &claims) != nil {
		return false
	}
	now := v.now().Unix()
	if claims.Exp == 0 || claims.Exp <= now || (claims.Nbf != 0 && claims.Nbf > now) {
		return false
	}
	// An assertion naming a different team's issuer is not ours, and a token
	// whose issue-to-expiry span exceeds the platform maximum is rejected no
	// matter how far out its exp sits.
	if claims.Iss != "" && claims.Iss != v.issuer {
		return false
	}
	if claims.Iat == 0 || claims.Exp-claims.Iat > int64(hubCFAccessMaxTokenLifetime/time.Second) {
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

// keyFor serves only the cached key set — it never touches the network. A
// missing or stale cache kicks a background refresh and reports failure, so
// a certs outage or a slow certs endpoint can never block a request (and a
// request is never let through unverified: fail closed).
func (v *hubCFAccessVerifier) keyFor(kid string) *rsa.PublicKey {
	v.mu.RLock()
	key, fresh := v.keys[kid], v.now().Sub(v.fetched) < hubCFAccessCertsTTL
	v.mu.RUnlock()
	if fresh && key != nil {
		return key
	}
	if !fresh {
		// Only an absent or expired cache may schedule a fetch. The kid
		// arrives inside an unverified token, so an unknown kid on a fresh
		// key set must not spend network — otherwise every unauthenticated
		// caller holds a knob that stalls legitimate requests.
		v.kickRefresh()
	}
	return nil
}

// kickRefresh asks the background loop to re-fetch the certs document. It is
// non-blocking — kicks collapse while a fetch is in flight — and rate-
// limited, so request volume cannot drive fetch volume.
func (v *hubCFAccessVerifier) kickRefresh() {
	now := v.now().UnixNano()
	for {
		last := v.lastKick.Load()
		if last != 0 && now-last < int64(hubCFAccessKickMinInterval) {
			return
		}
		if v.lastKick.CompareAndSwap(last, now) {
			break
		}
	}
	v.loopOnce.Do(func() { go v.refreshLoop() })
	select {
	case v.kick <- struct{}{}:
	default:
	}
}

// refreshLoop owns every certs fetch: a periodic proactive refresh plus
// on-demand kicks from keyFor for an absent or expired cache.
func (v *hubCFAccessVerifier) refreshLoop() {
	for {
		select {
		case <-v.kick:
		case <-time.After(hubCFAccessRefreshInterval):
		}
		if err := v.refresh(); err != nil {
			v.logger.Warn("Cloudflare Access key refresh failed", "error", err)
		}
	}
}

// refresh replaces the cached key set from the team's certs endpoint. The
// fetch happens with no lock held; the cache is swapped only on success, so
// a failed refresh keeps the previous keys.
func (v *hubCFAccessVerifier) refresh() error {
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
	v.mu.Lock()
	v.keys = keys
	v.fetched = v.now()
	v.mu.Unlock()
	return nil
}

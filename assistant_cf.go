package panewire

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

// assistantCFVerifier is the inbound Cloudflare Access gate for the
// assistant binary. It mirrors the hub verifier's RS256/aud/exp/iss checks
// but adds what the hub deliberately lacks: identity binding. A verified
// assertion is only accepted when its common_name — the Access service
// token's identity — is on the configured allowlist, so "any token that
// cleared the Access app" is not "the assistant". Email claims grant
// nothing here: this surface is for the service identity only (design Q6).
type assistantCFVerifier struct {
	certsURL string
	aud      string
	issuer   string
	allowed  map[string]struct{}
	subjects map[string]struct{}
	client   *http.Client

	mu          sync.Mutex
	keys        map[string]*rsa.PublicKey
	fetchedAt   time.Time
	lastUnknown time.Time

	// now, when set, is the clock for Access-user exp/nbf. Production
	// leaves it nil. Tests freeze it so a one-second boundary cannot
	// flake. It does not add leeway.
	now func() time.Time
}

const (
	assistantCFCertsTTL          = 10 * time.Minute
	assistantCFUnknownKidBackoff = time.Minute
)

func newAssistantCFVerifier(team, aud, certsURL string, serviceNames []string, client *http.Client) (*assistantCFVerifier, error) {
	if !hubCFAccessTeamPattern.MatchString(team) || aud == "" || len(aud) > 256 {
		return nil, configError("cf access configuration is invalid")
	}
	allowed := make(map[string]struct{}, len(serviceNames))
	for _, name := range serviceNames {
		name = strings.ToLower(strings.TrimSpace(name))
		if name != "" {
			allowed[name] = struct{}{}
		}
	}
	if len(allowed) == 0 {
		return nil, configError("cf access service name allowlist is empty")
	}
	if certsURL == "" {
		certsURL = "https://" + team + ".cloudflareaccess.com/cdn-cgi/access/certs"
	}
	if !validHandoffkeepBaseURL(certsURL) {
		return nil, configError("cf access certs url invalid")
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	v := &assistantCFVerifier{
		certsURL: certsURL,
		aud:      aud,
		issuer:   "https://" + team + ".cloudflareaccess.com",
		allowed:  allowed,
		client:   client,
		keys:     map[string]*rsa.PublicKey{},
	}
	return v, nil
}

// authenticate verifies the assertion and returns the bound identity
// "service:<common_name>". It deliberately reveals nothing about the failure.
func (v *assistantCFVerifier) authenticate(r *http.Request) (string, bool) {
	token := strings.TrimSpace(r.Header.Get("Cf-Access-Jwt-Assertion"))
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", false
	}
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", false
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if json.Unmarshal(headerJSON, &header) != nil || header.Alg != "RS256" || header.Kid == "" {
		return "", false
	}
	key := v.keyFor(r.Context(), header.Kid)
	if key == nil {
		return "", false
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", false
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil {
		return "", false
	}
	claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", false
	}
	var claims struct {
		Aud        json.RawMessage `json:"aud"`
		Exp        int64           `json:"exp"`
		Nbf        int64           `json:"nbf"`
		Iat        int64           `json:"iat"`
		Iss        string          `json:"iss"`
		CommonName string          `json:"common_name"`
	}
	if json.Unmarshal(claimsJSON, &claims) != nil {
		return "", false
	}
	now := time.Now().Unix()
	if claims.Exp == 0 || claims.Exp <= now || (claims.Nbf != 0 && claims.Nbf > now) {
		return "", false
	}
	// Service mode only. An empty iss is still accepted here so existing
	// service-token assertions keep working. Access-user mode does not use
	// this function; authenticateAccessUser requires a non-empty iss.
	if claims.Iss != "" && claims.Iss != v.issuer {
		return "", false
	}
	if claims.Iat == 0 || claims.Exp-claims.Iat > int64(hubCFAccessMaxTokenLifetime/time.Second) {
		return "", false
	}
	if !cfAccessAudienceMatches(claims.Aud, v.aud) {
		return "", false
	}
	// Identity binding: the signature being right is not enough — the
	// service identity named by common_name must be on the allowlist.
	name := strings.ToLower(strings.TrimSpace(claims.CommonName))
	if name == "" {
		return "", false
	}
	if _, ok := v.allowed[name]; !ok {
		return "", false
	}
	return "service:" + name, true
}

// keyFor serves the cached key set and refreshes synchronously on a cold or
// stale cache, rate-limited for unknown kids so forged headers cannot drive
// fetch volume. Mirrors handoffkeep's cfaccess.Verifier policy.
func (v *assistantCFVerifier) keyFor(ctx context.Context, kid string) *rsa.PublicKey {
	v.mu.Lock()
	key := v.keys[kid]
	fresh := v.keys != nil && time.Since(v.fetchedAt) < assistantCFCertsTTL
	refreshUnknown := key == nil && (v.lastUnknown.IsZero() || time.Since(v.lastUnknown) >= assistantCFUnknownKidBackoff)
	if refreshUnknown {
		v.lastUnknown = time.Now()
	}
	v.mu.Unlock()
	if key != nil && fresh {
		return key
	}
	if key == nil && !refreshUnknown {
		return nil
	}
	if err := v.refresh(ctx); err != nil {
		// A fresh cache can still validate a known key when a refresh fails.
		v.mu.Lock()
		cached, cacheFresh := v.keys[kid], v.keys != nil && time.Since(v.fetchedAt) < assistantCFCertsTTL
		v.mu.Unlock()
		if cached != nil && cacheFresh {
			return cached
		}
		return nil
	}
	v.mu.Lock()
	key = v.keys[kid]
	v.mu.Unlock()
	return key
}

func (v *assistantCFVerifier) refresh(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, v.certsURL, nil)
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
	v.fetchedAt = time.Now()
	v.mu.Unlock()
	return nil
}

// newAssistantAccessUserVerifier builds the Access-user gate. The JWKS URL
// is required (no team default) and subjects is the allowlist of stable
// subs. Email is not a key. Service-token common_name values are not
// consulted.
func newAssistantAccessUserVerifier(team, aud, certsURL string, subjects []string, client *http.Client) (*assistantCFVerifier, error) {
	if !hubCFAccessTeamPattern.MatchString(team) || aud == "" || len(aud) > 256 {
		return nil, configError("cf access configuration is invalid")
	}
	if certsURL == "" || !validHandoffkeepBaseURL(certsURL) {
		return nil, configError("access-user mode requires jwks url")
	}
	allowedSubs := make(map[string]struct{}, len(subjects))
	for _, sub := range subjects {
		sub = strings.TrimSpace(sub)
		if sub != "" {
			allowedSubs[sub] = struct{}{}
		}
	}
	if len(allowedSubs) == 0 {
		return nil, configError("access-user mode requires subject allowlist")
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &assistantCFVerifier{
		certsURL: certsURL,
		aud:      aud,
		issuer:   "https://" + team + ".cloudflareaccess.com",
		subjects: allowedSubs,
		client:   client,
		keys:     map[string]*rsa.PublicKey{},
	}, nil
}

// keyLookup resolves a kid and distinguishes a fresh JWKS that simply
// lacks the kid from a fetch that produced nothing usable. Both deny.
func (v *assistantCFVerifier) keyLookup(ctx context.Context, kid string) (*rsa.PublicKey, string) {
	key := v.keyFor(ctx, kid)
	if key != nil {
		return key, ""
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	fresh := len(v.keys) > 0 && time.Since(v.fetchedAt) < assistantCFCertsTTL
	if !fresh {
		return nil, "jwks_unavailable"
	}
	return nil, "unknown_kid"
}

// accessUserNow is the Access-user exp/nbf instant in unix seconds.
// The comparison itself has no leeway: exp == now is expired, and
// nbf == now+1 is not yet valid.
func (v *assistantCFVerifier) accessUserNow() int64 {
	if v.now != nil {
		return v.now().Unix()
	}
	return time.Now().Unix()
}

// authenticateAccessUser verifies a user Cf-Access-Jwt-Assertion. The
// returned reason is empty on success. It is an audit token only — the
// HTTP response stays the generic unauthorized error. Caller-supplied
// identity headers are never read. The allowlist key is sub, never email.
// An empty sub is the service-token shape and is rejected even when
// common_name is set. exp/nbf use the same strict comparison as service
// mode (no added leeway).
func (v *assistantCFVerifier) authenticateAccessUser(r *http.Request) (assistantPrincipal, string) {
	principal := assistantPrincipal{Mode: assistantAuthModeAccessUser}
	token := strings.TrimSpace(r.Header.Get("Cf-Access-Jwt-Assertion"))
	if token == "" {
		return principal, "assertion_missing"
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return principal, "assertion_malformed"
	}
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return principal, "assertion_malformed"
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if json.Unmarshal(headerJSON, &header) != nil || header.Alg != "RS256" || header.Kid == "" {
		return principal, "assertion_malformed"
	}
	key, keyReason := v.keyLookup(r.Context(), header.Kid)
	if key == nil {
		return principal, keyReason
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return principal, "assertion_malformed"
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil {
		return principal, "bad_signature"
	}
	claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return principal, "assertion_malformed"
	}
	var claims struct {
		Aud        json.RawMessage `json:"aud"`
		Exp        int64           `json:"exp"`
		Nbf        int64           `json:"nbf"`
		Iat        int64           `json:"iat"`
		Iss        string          `json:"iss"`
		Sub        string          `json:"sub"`
		Email      string          `json:"email"`
		CommonName string          `json:"common_name"`
	}
	if json.Unmarshal(claimsJSON, &claims) != nil {
		return principal, "assertion_malformed"
	}
	// Every aud value, not only the one that matched. Same string-or-list
	// grammar as cfAccessAudienceMatches. Recorded on the audit line even
	// when a later check rejects the token.
	principal.Audiences = cfAccessAudiences(claims.Aud)
	now := v.accessUserNow()
	if claims.Exp == 0 || claims.Exp <= now {
		return principal, "expired"
	}
	if claims.Nbf != 0 && claims.Nbf > now {
		return principal, "not_yet_valid"
	}
	// Issuer is required. An empty iss is not "unspecified, so skip" —
	// that hole is service mode only (authenticate, above).
	if claims.Iss == "" {
		return principal, "iss_missing"
	}
	if claims.Iss != v.issuer {
		principal.Issuer = claims.Iss
		return principal, "iss_mismatch"
	}
	principal.Issuer = claims.Iss
	if claims.Iat == 0 || claims.Exp-claims.Iat > int64(hubCFAccessMaxTokenLifetime/time.Second) {
		return principal, "lifetime"
	}
	// Presented subject and email are recorded before the aud and allowlist
	// checks so a rejection still shows who the token named. They do not
	// authorize the request.
	sub := strings.TrimSpace(claims.Sub)
	principal.Subject = sub
	principal.Email = strings.TrimSpace(claims.Email)
	if !cfAccessAudienceMatches(claims.Aud, v.aud) {
		return principal, "aud_mismatch"
	}
	if sub == "" {
		return principal, "service_token"
	}
	if _, ok := v.subjects[sub]; !ok {
		return principal, "subject_not_allowlisted"
	}
	principal.Label = "access-user|" + claims.Iss + "|" + sub
	return principal, ""
}

// cfAccessAudiences returns every aud value using the same grammar as
// cfAccessAudienceMatches: a JSON string is one value, a JSON array is
// every element. Anything else is an empty list.
func cfAccessAudiences(raw json.RawMessage) []string {
	if len(bytes.TrimSpace(raw)) == 0 {
		return []string{}
	}
	var single string
	if json.Unmarshal(raw, &single) == nil {
		return []string{single}
	}
	var list []string
	if json.Unmarshal(raw, &list) != nil || list == nil {
		return []string{}
	}
	return list
}

package panewire

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// cfAccessTestAUD is the Access application audience the fixtures sign for.
const cfAccessTestAUD = "cf-access-test-aud"

// cfAccessTestSigningKey is one RSA key shared by every test that needs a signed
// Cf-Access-Jwt-Assertion; the matching public key is served by cfAccessTestCerts.
var cfAccessTestSigningKey = sync.OnceValue(func() *rsa.PrivateKey {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return key
})

// cfAccessTestCerts serves the Access certs document shape for the test key.
func cfAccessTestCerts(t *testing.T, key *rsa.PrivateKey) *httptest.Server {
	t.Helper()
	jwk := map[string]any{
		"kty": "RSA", "kid": "k1", "alg": "RS256", "use": "sig",
		"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]any{jwk}})
	}))
	t.Cleanup(server.Close)
	return server
}

// cfAccessSignJWTClaims builds a synthetic Cf-Access-Jwt-Assertion over arbitrary
// claims: the same shape and signature Cloudflare emits, so the verifier
// exercises its real code path.
func cfAccessSignJWTClaims(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	headerJSON, err := json.Marshal(map[string]any{"alg": "RS256", "kid": kid, "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	header := base64.RawURLEncoding.EncodeToString(headerJSON)
	payloadJSON, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	payload := base64.RawURLEncoding.EncodeToString(payloadJSON)
	digest := sha256.Sum256([]byte(header + "." + payload))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return header + "." + payload + "." + base64.RawURLEncoding.EncodeToString(signature)
}

// cfAccessSignJWT builds a well-formed assertion: correct audience, sane expiry,
// and the iat/sub/email claims a real Cloudflare token carries.
func cfAccessSignJWT(t *testing.T, key *rsa.PrivateKey, aud string, exp time.Time) string {
	t.Helper()
	return cfAccessSignJWTClaims(t, key, "k1", map[string]any{"aud": []string{aud}, "exp": exp.Unix(), "iat": time.Now().Unix(), "sub": "op@example.test", "email": "op@example.test"})
}

// cfAccessTestToken is a valid assertion for cfAccessTestAUD, good for an hour.
func cfAccessTestToken(t *testing.T) string {
	t.Helper()
	return cfAccessSignJWT(t, cfAccessTestSigningKey(), cfAccessTestAUD, time.Now().Add(time.Hour))
}

// cfAccessWarmKeys synchronously fills the verifier cache so a test never
// races the background refresh the verifier starts on construction.
func cfAccessWarmKeys(t *testing.T, hub *HubServer) {
	t.Helper()
	if hub.cfAccess != nil {
		if err := hub.cfAccess.refresh(); err != nil {
			t.Fatalf("warm access keys: %v", err)
		}
	}
}

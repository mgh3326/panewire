package panewire

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const (
	accessUserIssuer = "https://team.cloudflareaccess.com"
	accessUserSub    = "user-sub-1"
	accessUserEmail  = "operator@example.test"
)

func accessUserBaseClaims(now time.Time) map[string]any {
	return map[string]any{
		"aud":   []string{assistantTestAUD},
		"exp":   now.Add(time.Hour).Unix(),
		"iat":   now.Unix(),
		"iss":   accessUserIssuer,
		"sub":   accessUserSub,
		"email": accessUserEmail,
	}
}

func accessUserServer(t *testing.T, hk *fakeAssistantHK, subjects []string, mutate func(*assistantConfig)) (*assistantServer, *bytes.Buffer) {
	t.Helper()
	certs := chatTestCerts(t, chatTestSigningKey())
	return accessUserServerWithCerts(t, hk, certs.URL, subjects, mutate)
}

func accessUserServerWithCerts(t *testing.T, hk *fakeAssistantHK, certsURL string, subjects []string, mutate func(*assistantConfig)) (*assistantServer, *bytes.Buffer) {
	t.Helper()
	srv, logs := assistantTestServer(t, hk, func(cfg *assistantConfig) {
		cfg.AuthMode = assistantAuthModeAccessUser
		cfg.CFTeam = "team"
		cfg.CFAUD = assistantTestAUD
		cfg.CFCertsURL = certsURL
		cfg.CFSubjects = subjects
		if mutate != nil {
			mutate(cfg)
		}
	})
	return srv, logs
}

func accessUserPost(srv *assistantServer, method, path, jwt, bearer string, extra http.Header) *httptest.ResponseRecorder {
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"pending_list","arguments":{}}}`
	if path != "/mcp" || method != http.MethodPost {
		body = ""
	}
	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	var req *http.Request
	if reader != nil {
		req = httptest.NewRequest(method, path, reader)
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	if jwt != "" {
		req.Header.Set("Cf-Access-Jwt-Assertion", jwt)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	for key, values := range extra {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	writer := httptest.NewRecorder()
	srv.ServeHTTP(writer, req)
	return writer
}

func assertGenericUnauthorized(t *testing.T, writer *httptest.ResponseRecorder) {
	t.Helper()
	if writer.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%q, want 401", writer.Code, writer.Body.String())
	}
	if writer.Body.String() != `{"error":"unauthorized"}` {
		t.Fatalf("body=%q, want generic unauthorized", writer.Body.String())
	}
}

func auditWithResult(t *testing.T, logs *bytes.Buffer, result string) map[string]any {
	t.Helper()
	var found map[string]any
	for _, line := range strings.Split(logs.String(), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var decoded map[string]any
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatalf("audit line not json: %v %s", err, line)
		}
		if decoded["result"] == result {
			if found != nil {
				t.Fatalf("duplicate audit result %q in %s", result, logs.String())
			}
			found = decoded
		}
	}
	if found == nil {
		t.Fatalf("audit result %q missing in %s", result, logs.String())
	}
	return found
}

func assertNoHK(t *testing.T, hk *fakeAssistantHK) {
	t.Helper()
	if calls := hk.requestLog(); len(calls) != 0 {
		t.Fatalf("rejected request reached hk: %v", calls)
	}
}

// AC1: each bad assertion is a distinct audit reason and a generic 401.
// A valid user assertion is allowed and reaches the tool.
func TestAccessUserAssertionOutcomes(t *testing.T) {
	now := time.Now()
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	type tokenSpec struct {
		reason string
		kid    string
		key    *rsa.PrivateKey
		claims map[string]any
	}
	base := func() map[string]any { return accessUserBaseClaims(now) }
	specs := []tokenSpec{
		{reason: "", kid: "k1", claims: base()},
		{reason: "iss_mismatch", kid: "k1", claims: func() map[string]any {
			c := base()
			c["iss"] = "https://other.cloudflareaccess.com"
			return c
		}()},
		{reason: "iss_missing", kid: "k1", claims: func() map[string]any {
			c := base()
			delete(c, "iss")
			return c
		}()},
		{reason: "aud_mismatch", kid: "k1", claims: func() map[string]any {
			c := base()
			c["aud"] = []string{"some-other-aud"}
			return c
		}()},
		{reason: "expired", kid: "k1", claims: func() map[string]any {
			c := base()
			c["iat"] = now.Add(-2 * time.Hour).Unix()
			c["exp"] = now.Add(-time.Hour).Unix()
			return c
		}()},
		{reason: "not_yet_valid", kid: "k1", claims: func() map[string]any {
			c := base()
			c["nbf"] = now.Add(time.Hour).Unix()
			c["exp"] = now.Add(2 * time.Hour).Unix()
			return c
		}()},
		{reason: "bad_signature", kid: "k1", key: otherKey, claims: base()},
		{reason: "unknown_kid", kid: "k-missing", claims: base()},
		{reason: "subject_not_allowlisted", kid: "k1", claims: func() map[string]any {
			c := base()
			c["sub"] = "intruder-sub"
			return c
		}()},
		{reason: "service_token", kid: "k1", claims: func() map[string]any {
			c := base()
			c["sub"] = ""
			c["common_name"] = "berry.svc"
			return c
		}()},
	}
	for _, spec := range specs {
		spec := spec
		name := spec.reason
		if name == "" {
			name = "allowed"
		}
		t.Run(name, func(t *testing.T) {
			hk := newFakeAssistantHK(t)
			srv, logs := accessUserServer(t, hk, []string{accessUserSub}, nil)
			key := spec.key
			if key == nil {
				key = chatTestSigningKey()
			}
			jwt := chatSignJWTClaims(t, key, spec.kid, spec.claims)
			writer := accessUserPost(srv, http.MethodPost, "/mcp", jwt, "", nil)
			if spec.reason == "" {
				if writer.Code != http.StatusOK {
					t.Fatalf("valid assertion status=%d body=%s", writer.Code, writer.Body.String())
				}
				if strings.Contains(writer.Body.String(), jwt) {
					t.Fatal("tool output contains the assertion")
				}
				line := auditWithResult(t, logs, "ok")
				if line["auth_mode"] != assistantAuthModeAccessUser || line["iss"] != accessUserIssuer || line["sub"] != accessUserSub || line["issuer"] != accessUserIssuer || line["email"] != accessUserEmail {
					t.Fatalf("principal audit=%v", line)
				}
				assertAudiences(t, line, []string{assistantTestAUD})
				assertStructuredPrincipal(t, line, []string{assistantTestAUD})
				if identity, _ := line["identity"].(string); identity != "access-user|"+accessUserIssuer+"|"+accessUserSub || strings.Contains(identity, accessUserEmail) {
					t.Fatalf("identity key=%q", identity)
				}
				if len(hk.requestLog()) == 0 {
					t.Fatal("allowed tool call did not reach hk")
				}
				return
			}
			assertGenericUnauthorized(t, writer)
			if strings.Contains(writer.Body.String(), spec.reason) {
				t.Fatalf("reason %s leaked to the caller", spec.reason)
			}
			auditWithResult(t, logs, spec.reason)
			assertNoHK(t, hk)
			if strings.Contains(logs.String(), jwt) {
				t.Fatal("audit log contains the assertion")
			}
		})
	}
}

// Mutant (a): an empty iss must not be accepted again in Access-user mode.
func TestMutantAccessUserEmptyIssRejected(t *testing.T) {
	now := time.Now()
	for _, name := range []string{"omitted", "empty"} {
		t.Run(name, func(t *testing.T) {
			hk := newFakeAssistantHK(t)
			srv, logs := accessUserServer(t, hk, []string{accessUserSub}, nil)
			claims := accessUserBaseClaims(now)
			if name == "omitted" {
				delete(claims, "iss")
			} else {
				claims["iss"] = ""
			}
			jwt := chatSignJWTClaims(t, chatTestSigningKey(), "k1", claims)
			writer := accessUserPost(srv, http.MethodPost, "/mcp", jwt, "", nil)
			assertGenericUnauthorized(t, writer)
			line := auditWithResult(t, logs, "iss_missing")
			if line["result"] == "iss_mismatch" {
				t.Fatal("empty iss collapsed into iss_mismatch")
			}
			assertNoHK(t, hk)
		})
	}
}

// Mutant (b): a valid bearer must not authorize Access-user mode.
func TestMutantAccessUserNoBearerFallback(t *testing.T) {
	now := time.Now()
	hk := newFakeAssistantHK(t)
	srv, logs := accessUserServer(t, hk, []string{accessUserSub}, nil)
	valid := chatSignJWTClaims(t, chatTestSigningKey(), "k1", accessUserBaseClaims(now))

	missing := accessUserPost(srv, http.MethodPost, "/mcp", "", assistantTestBerryToken, nil)
	assertGenericUnauthorized(t, missing)
	auditWithResult(t, logs, "assertion_missing")
	assertNoHK(t, hk)

	logs.Reset()
	bad := accessUserPost(srv, http.MethodPost, "/mcp", valid+"x", assistantTestBerryToken, nil)
	assertGenericUnauthorized(t, bad)
	if strings.Contains(logs.String(), `"result":"ok"`) {
		t.Fatal("invalid assertion accepted because the bearer was valid")
	}
	assertNoHK(t, hk)

	logs.Reset()
	noBearer := accessUserPost(srv, http.MethodPost, "/mcp", valid, "", nil)
	if noBearer.Code != http.StatusOK {
		t.Fatalf("valid assertion without bearer status=%d body=%s", noBearer.Code, noBearer.Body.String())
	}

	logs.Reset()
	wrongBearer := accessUserPost(srv, http.MethodPost, "/mcp", valid, "not-the-bearer", nil)
	if wrongBearer.Code != http.StatusOK {
		t.Fatalf("valid assertion with an ignored bearer status=%d", wrongBearer.Code)
	}
}

// Mutant (c): email must not be the principal key.
func TestMutantAccessUserEmailIsNotPrincipal(t *testing.T) {
	now := time.Now()
	hk := newFakeAssistantHK(t)
	// The allowlist holds the sub only. The email is a different string.
	srv, logs := accessUserServer(t, hk, []string{accessUserSub}, nil)
	claims := accessUserBaseClaims(now)
	claims["email"] = "not-on-the-list@example.test"
	jwt := chatSignJWTClaims(t, chatTestSigningKey(), "k1", claims)
	writer := accessUserPost(srv, http.MethodPost, "/mcp", jwt, "", nil)
	if writer.Code != http.StatusOK {
		t.Fatalf("sub allowlisted, foreign email status=%d body=%s", writer.Code, writer.Body.String())
	}
	line := auditWithResult(t, logs, "ok")
	identity, _ := line["identity"].(string)
	if strings.Contains(identity, "not-on-the-list@example.test") || line["email"] != "not-on-the-list@example.test" || line["sub"] != accessUserSub {
		t.Fatalf("email leaked into the key or display was dropped: %v", line)
	}

	// The allowlist holds the email string and not the sub. That must not pass.
	hk2 := newFakeAssistantHK(t)
	srv2, logs2 := accessUserServer(t, hk2, []string{accessUserEmail}, nil)
	emailKey := accessUserBaseClaims(now)
	emailKey["sub"] = "some-other-sub"
	emailKey["email"] = accessUserEmail
	writer = accessUserPost(srv2, http.MethodPost, "/mcp", chatSignJWTClaims(t, chatTestSigningKey(), "k1", emailKey), "", nil)
	assertGenericUnauthorized(t, writer)
	auditWithResult(t, logs2, "subject_not_allowlisted")
	assertNoHK(t, hk2)

	// Same email, two subs: only the allowlisted sub passes.
	hk3 := newFakeAssistantHK(t)
	srv3, _ := accessUserServer(t, hk3, []string{"sub-a"}, nil)
	shared := accessUserBaseClaims(now)
	shared["email"] = "shared@example.test"
	shared["sub"] = "sub-b"
	denied := accessUserPost(srv3, http.MethodPost, "/mcp", chatSignJWTClaims(t, chatTestSigningKey(), "k1", shared), "", nil)
	assertGenericUnauthorized(t, denied)
	shared["sub"] = "sub-a"
	allowed := accessUserPost(srv3, http.MethodPost, "/mcp", chatSignJWTClaims(t, chatTestSigningKey(), "k1", shared), "", nil)
	if allowed.Code != http.StatusOK {
		t.Fatalf("allowlisted sub with a shared email status=%d", allowed.Code)
	}
}

// Mutant (d): service-token claims (empty sub, common_name set) are rejected
// even when that common_name is on a service-name list or the subject list.
func TestMutantAccessUserServiceTokenRejected(t *testing.T) {
	now := time.Now()
	hk := newFakeAssistantHK(t)
	srv, logs := accessUserServer(t, hk, []string{"berry.svc", accessUserSub}, func(cfg *assistantConfig) {
		cfg.CFServiceNames = []string{"berry.svc"}
	})
	claims := accessUserBaseClaims(now)
	claims["sub"] = ""
	claims["common_name"] = "berry.svc"
	claims["email"] = "berry.svc"
	writer := accessUserPost(srv, http.MethodPost, "/mcp", chatSignJWTClaims(t, chatTestSigningKey(), "k1", claims), "", nil)
	assertGenericUnauthorized(t, writer)
	auditWithResult(t, logs, "service_token")
	assertNoHK(t, hk)

	// A real user sub still passes when common_name is also present.
	logs.Reset()
	user := accessUserBaseClaims(now)
	user["common_name"] = "berry.svc"
	ok := accessUserPost(srv, http.MethodPost, "/mcp", chatSignJWTClaims(t, chatTestSigningKey(), "k1", user), "", nil)
	if ok.Code != http.StatusOK {
		t.Fatalf("user sub with an extra common_name status=%d body=%s", ok.Code, ok.Body.String())
	}
	line := auditWithResult(t, logs, "ok")
	if line["sub"] != accessUserSub {
		t.Fatalf("common_name replaced the principal: %v", line)
	}
}

// Mutant (e): removing the AUD check must not let a foreign audience through.
func TestMutantAccessUserAUDRequired(t *testing.T) {
	now := time.Now()
	hk := newFakeAssistantHK(t)
	srv, logs := accessUserServer(t, hk, []string{accessUserSub}, nil)
	claims := accessUserBaseClaims(now)
	claims["aud"] = assistantTestAUD + "-other"
	writer := accessUserPost(srv, http.MethodPost, "/mcp", chatSignJWTClaims(t, chatTestSigningKey(), "k1", claims), "", nil)
	assertGenericUnauthorized(t, writer)
	auditWithResult(t, logs, "aud_mismatch")
	assertNoHK(t, hk)

	logs.Reset()
	claims["aud"] = []string{"nope", assistantTestAUD}
	listed := accessUserPost(srv, http.MethodPost, "/mcp", chatSignJWTClaims(t, chatTestSigningKey(), "k1", claims), "", nil)
	if listed.Code != http.StatusOK {
		t.Fatalf("aud list containing the assistant aud status=%d", listed.Code)
	}
}

// AC3: bearer mode and service mode still require the bearer.
func TestBearerModeStillRequiresBearer(t *testing.T) {
	hk := newFakeAssistantHK(t)
	srv, _ := assistantTestServer(t, hk, func(cfg *assistantConfig) {
		cfg.AuthMode = assistantAuthModeBearer
	})
	missing := accessUserPost(srv, http.MethodPost, "/mcp", "", "", nil)
	assertGenericUnauthorized(t, missing)
	withAssertion := accessUserPost(srv, http.MethodPost, "/mcp", "not-a-jwt", "", nil)
	assertGenericUnauthorized(t, withAssertion)
	assertNoHK(t, hk)
	ok := accessUserPost(srv, http.MethodPost, "/mcp", "", assistantTestBerryToken, nil)
	if ok.Code != http.StatusOK {
		t.Fatalf("bearer mode with the bearer status=%d body=%s", ok.Code, ok.Body.String())
	}
}

func TestServiceModeStillRequiresBearer(t *testing.T) {
	hk := newFakeAssistantHK(t)
	certs := chatTestCerts(t, chatTestSigningKey())
	srv, logs := assistantTestServer(t, hk, func(cfg *assistantConfig) {
		cfg.AuthMode = assistantAuthModeService
		cfg.CFTeam = "team"
		cfg.CFAUD = assistantTestAUD
		cfg.CFCertsURL = certs.URL
		cfg.CFServiceNames = []string{"berry.svc"}
	})
	now := time.Now()
	// No iss: service mode still accepts that, as it does today.
	claims := map[string]any{
		"aud": []string{assistantTestAUD}, "exp": now.Add(time.Hour).Unix(), "iat": now.Unix(),
		"common_name": "berry.svc",
	}
	jwt := chatSignJWTClaims(t, chatTestSigningKey(), "k1", claims)
	noBearer := accessUserPost(srv, http.MethodPost, "/mcp", jwt, "", nil)
	assertGenericUnauthorized(t, noBearer)
	assertNoHK(t, hk)
	bearerOnly := accessUserPost(srv, http.MethodPost, "/mcp", "", assistantTestBerryToken, nil)
	assertGenericUnauthorized(t, bearerOnly)
	ok := accessUserPost(srv, http.MethodPost, "/mcp", jwt, assistantTestBerryToken, nil)
	if ok.Code != http.StatusOK {
		t.Fatalf("service mode status=%d body=%s", ok.Code, ok.Body.String())
	}
	if !strings.Contains(logs.String(), "service:berry.svc") {
		t.Fatalf("service identity missing from audit: %s", logs.String())
	}
}

// AC4: every missing Access-user field, and an unknown mode, refuses startup.
func TestAccessUserConfigRefuses(t *testing.T) {
	dir := t.TempDir()
	tokenFile := writeMode0600(t, dir, "berry.token", assistantTestBerryToken)
	targetsFile := writeMode0600(t, dir, "targets.json", assistantTestTargets)
	base := "HANDOFFKEEP_URL=http://127.0.0.1:8080\nHANDOFFKEEP_TOKEN=" + assistantTestHKToken + "\n" +
		"PANEWIRE_ASSISTANT_TARGETS_FILE=" + targetsFile + "\n"
	withToken := base + "PANEWIRE_ASSISTANT_TOKEN_FILE=" + tokenFile + "\n"
	full := withToken + "PANEWIRE_ASSISTANT_AUTH_MODE=access-user\n" +
		"PANEWIRE_ASSISTANT_CF_TEAM=team\nPANEWIRE_ASSISTANT_CF_AUD=aud-1\n" +
		"PANEWIRE_ASSISTANT_CF_CERTS_URL=http://127.0.0.1:9/certs\n" +
		"PANEWIRE_ASSISTANT_CF_SUBJECTS=user-sub-1\n"
	write := func(name, body string) string {
		t.Helper()
		return writeMode0600(t, dir, name, body)
	}
	cfg, err := loadAssistantConfig(write("full.env", full))
	if err != nil {
		t.Fatalf("complete access-user config refused: %v", err)
	}
	if cfg.AuthMode != assistantAuthModeAccessUser || len(cfg.CFSubjects) != 1 || cfg.CFSubjects[0] != accessUserSub {
		t.Fatalf("parsed config mode=%q subjects=%v", cfg.AuthMode, cfg.CFSubjects)
	}
	// The bearer file is not required in this mode.
	if _, err := loadAssistantConfig(write("no-token.env", strings.Replace(full, "PANEWIRE_ASSISTANT_TOKEN_FILE="+tokenFile+"\n", "", 1))); err != nil {
		t.Fatalf("access-user without a bearer file refused: %v", err)
	}
	cases := []struct {
		name, body, want string
	}{
		{"unknown", withToken + "PANEWIRE_ASSISTANT_AUTH_MODE=oauth\n", "auth mode unknown"},
		{"mixed-case", withToken + "PANEWIRE_ASSISTANT_AUTH_MODE=Access-user\n", "auth mode unknown"},
		{"team", strings.Replace(full, "PANEWIRE_ASSISTANT_CF_TEAM=team\n", "", 1), "access-user mode requires team domain"},
		{"aud", strings.Replace(full, "PANEWIRE_ASSISTANT_CF_AUD=aud-1\n", "", 1), "access-user mode requires aud"},
		{"jwks", strings.Replace(full, "PANEWIRE_ASSISTANT_CF_CERTS_URL=http://127.0.0.1:9/certs\n", "", 1), "access-user mode requires jwks url"},
		{"subjects", strings.Replace(full, "PANEWIRE_ASSISTANT_CF_SUBJECTS=user-sub-1\n", "PANEWIRE_ASSISTANT_CF_SUBJECTS=,,\n", 1), "access-user mode requires subject allowlist"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadAssistantConfig(write(tc.name+".env", tc.body))
			if err == nil || err.Error() != tc.want {
				t.Fatalf("err=%v, want %q", err, tc.want)
			}
		})
	}
}

// AC5: planted bearer, assertion header, claim, and JWKS body never surface.
func TestAccessUserPlantedValuesNeverSurface(t *testing.T) {
	const (
		plantBearer = "PLANT-BEARER-1470-9f3c"
		plantJKU    = "PLANT-JKU-1470-9f3c"
		plantJWKS   = "PLANT-JWKS-1470-9f3c"
		plantNonce  = "PLANT-NONCE-1470-9f3c"
	)
	key := chatTestSigningKey()
	certs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		jwk := map[string]any{
			"kty": "RSA", "kid": "k1", "alg": "RS256", "use": "sig",
			"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(bigIntBytes(int64(key.E))),
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{jwk}, "plant": plantJWKS})
	}))
	t.Cleanup(certs.Close)
	hk := newFakeAssistantHK(t)
	srv, logs := accessUserServerWithCerts(t, hk, certs.URL, []string{accessUserSub}, nil)
	claims := accessUserBaseClaims(time.Now())
	claims["identity_nonce"] = plantNonce
	jwt := signJWTWithHeader(t, key, map[string]any{"alg": "RS256", "kid": "k1", "typ": "JWT", "jku": plantJKU}, claims)
	writer := accessUserPost(srv, http.MethodPost, "/mcp", jwt, plantBearer, nil)
	if writer.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	blob := logs.String() + "\n" + writer.Body.String()
	for _, plant := range []string{plantBearer, plantJKU, plantJWKS, plantNonce, jwt} {
		if strings.Contains(blob, plant) {
			t.Fatalf("planted value %q surfaced", plant)
		}
	}
}

// Caller-supplied identity headers do not change the verified principal.
func TestAccessUserCallerHeadersDoNotSetPrincipal(t *testing.T) {
	const (
		headerEmail = "HEADER-EMAIL-1470-not-the-operator"
		headerSub   = "HEADER-SUB-1470-not-allowlisted"
	)
	hk := newFakeAssistantHK(t)
	srv, logs := accessUserServer(t, hk, []string{accessUserSub}, nil)
	jwt := chatSignJWTClaims(t, chatTestSigningKey(), "k1", accessUserBaseClaims(time.Now()))
	extra := http.Header{}
	extra.Set("Cf-Access-Authenticated-User-Email", headerEmail)
	extra.Set("Cf-Access-Authenticated-User-Id", headerSub)
	extra.Set("X-Auth-User", headerSub)
	writer := accessUserPost(srv, http.MethodPost, "/mcp", jwt, "", extra)
	if writer.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	line := auditWithResult(t, logs, "ok")
	if line["sub"] != accessUserSub || line["email"] != accessUserEmail || line["iss"] != accessUserIssuer {
		t.Fatalf("header overrode principal: %v", line)
	}
	blob := logs.String() + "\n" + writer.Body.String()
	if strings.Contains(blob, headerEmail) || strings.Contains(blob, headerSub) {
		t.Fatal("caller header value entered the audit or the tool output")
	}
}

// A JWKS fetch failure denies. The response body is not logged.
func TestAccessUserJWKSFailureDenies(t *testing.T) {
	const plant = "PLANT-JWKS-BODY-1470-should-not-leak"
	certs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(plant))
	}))
	t.Cleanup(certs.Close)
	hk := newFakeAssistantHK(t)
	srv, logs := accessUserServerWithCerts(t, hk, certs.URL, []string{accessUserSub}, nil)
	jwt := chatSignJWTClaims(t, chatTestSigningKey(), "k1", accessUserBaseClaims(time.Now()))
	writer := accessUserPost(srv, http.MethodPost, "/mcp", jwt, "", nil)
	assertGenericUnauthorized(t, writer)
	auditWithResult(t, logs, "jwks_unavailable")
	assertNoHK(t, hk)
	if strings.Contains(logs.String()+writer.Body.String(), plant) || strings.Contains(logs.String(), jwt) {
		t.Fatal("JWKS failure leaked a body or the assertion")
	}
}

// No route reaches a tool without the Access-user check.
func TestAccessUserNoPathBypasses(t *testing.T) {
	hk := newFakeAssistantHK(t)
	srv, _ := accessUserServer(t, hk, []string{accessUserSub}, nil)
	for _, tc := range []struct {
		method, path string
	}{
		{http.MethodPost, "/mcp"},
		{http.MethodGet, "/mcp"},
		{http.MethodDelete, "/mcp"},
		{http.MethodGet, "/healthz"},
		{http.MethodPost, "/"},
		{http.MethodPost, "/v1/tasks"},
	} {
		writer := accessUserPost(srv, tc.method, tc.path, "", "", nil)
		assertGenericUnauthorized(t, writer)
	}
	assertNoHK(t, hk)
	jwt := chatSignJWTClaims(t, chatTestSigningKey(), "k1", accessUserBaseClaims(time.Now()))
	authedWrongPath := accessUserPost(srv, http.MethodGet, "/healthz", jwt, "", nil)
	if authedWrongPath.Code != http.StatusNotFound {
		t.Fatalf("authed healthz status=%d, want 404", authedWrongPath.Code)
	}
	assertNoHK(t, hk)
}

// AC5: a successful read audits the structured principal. Access-user
// writes do not run, so the principal is not posted to hk.
func TestAccessUserPrincipalAuditedNotSentToHK(t *testing.T) {
	hk := newFakeAssistantHK(t)
	hk.failNonGET = true
	hub := newFakeAssistantHub(t)
	certs := chatTestCerts(t, chatTestSigningKey())
	srv, logs := assistantWriteServer(t, hk, hub, assistantWritesAll, func(cfg *assistantConfig) {
		cfg.AuthMode = assistantAuthModeAccessUser
		cfg.CFTeam = "team"
		cfg.CFAUD = assistantTestAUD
		cfg.CFCertsURL = certs.URL
		cfg.CFSubjects = []string{accessUserSub}
	})
	jwt := chatSignJWTClaims(t, chatTestSigningKey(), "k1", accessUserBaseClaims(time.Now()))
	read := accessUserTool(t, srv, jwt, "pending_list", map[string]any{})
	if read.isError || read.httpCode != http.StatusOK {
		t.Fatalf("pending_list: code=%d err=%v text=%s", read.httpCode, read.isError, read.text)
	}
	line := auditWithResult(t, logs, "ok")
	if line["auth_mode"] != assistantAuthModeAccessUser || line["issuer"] != accessUserIssuer || line["iss"] != accessUserIssuer || line["sub"] != accessUserSub || line["email"] != accessUserEmail {
		t.Fatalf("read audit principal=%v", line)
	}
	assertAudiences(t, line, []string{assistantTestAUD})
	assertStructuredPrincipal(t, line, []string{assistantTestAUD})
	if line["subject"] == accessUserSub {
		t.Fatal("tool subject field was overwritten by the principal subject")
	}
	for _, call := range hk.requestLog() {
		if strings.Contains(call, accessUserIssuer) || strings.Contains(call, accessUserSub) || strings.Contains(call, accessUserEmail) {
			t.Fatalf("principal on hk request: %s", call)
		}
	}
	if len(hk.postedBodies("POST /v1/assistant/decisions/resolve")) != 0 || len(hk.postedBodies("POST /v1/chat/messages")) != 0 {
		t.Fatal("access-user write posted to hk")
	}
	if hub.postCount() != 0 {
		t.Fatalf("hub posts=%d", hub.postCount())
	}
	for _, tool := range []string{"answer_decision", "answer_question", "deliver"} {
		got := accessUserTool(t, srv, jwt, tool, map[string]any{})
		if !got.isError || !strings.Contains(got.text, `"error":"writes_blocked_access_user"`) {
			t.Fatalf("%s: code=%d err=%v text=%s", tool, got.httpCode, got.isError, got.text)
		}
	}
	if len(hk.postedBodies("POST /v1/assistant/decisions/resolve")) != 0 || hub.postCount() != 0 {
		t.Fatal("blocked write reached hk or hub")
	}
}

func assertStructuredPrincipal(t *testing.T, line map[string]any, audiences []string) {
	t.Helper()
	principal, ok := line["principal"].(map[string]any)
	if !ok {
		t.Fatalf("principal=%T %v", line["principal"], line["principal"])
	}
	if principal["mode"] != assistantAuthModeAccessUser || principal["issuer"] != accessUserIssuer || principal["subject"] != accessUserSub || principal["email"] != accessUserEmail {
		t.Fatalf("structured principal=%v", principal)
	}
	assertAudiences(t, principal, audiences)
}

func assertAudiences(t *testing.T, line map[string]any, want []string) {
	t.Helper()
	raw, ok := line["audiences"].([]any)
	if !ok || len(raw) != len(want) {
		t.Fatalf("audiences=%v want %v", line["audiences"], want)
	}
	for i := range want {
		got, _ := raw[i].(string)
		if got != want[i] {
			t.Fatalf("audiences=%v want %v", raw, want)
		}
	}
}

func accessUserRPC(srv *assistantServer, jwt, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body))
	if jwt != "" {
		req.Header.Set("Cf-Access-Jwt-Assertion", jwt)
	}
	writer := httptest.NewRecorder()
	srv.ServeHTTP(writer, req)
	return writer
}

func accessUserListedTools(t *testing.T, srv *assistantServer, jwt string) []string {
	t.Helper()
	writer := accessUserRPC(srv, jwt, `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`)
	if writer.Code != http.StatusOK {
		t.Fatalf("tools/list status=%d body=%s", writer.Code, writer.Body.String())
	}
	var response struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(writer.Body.Bytes(), &response); err != nil {
		t.Fatalf("tools/list: %v body=%s", err, writer.Body.String())
	}
	names := make([]string, 0, len(response.Result.Tools))
	for _, tool := range response.Result.Tools {
		names = append(names, tool.Name)
	}
	return names
}

// Audiences on the principal are every aud value from the token. A second
// value is kept beside the one that matched. A JSON string aud is one value.
func TestAccessUserAudiencesKeepEveryValue(t *testing.T) {
	now := time.Now()
	hk := newFakeAssistantHK(t)
	srv, logs := accessUserServer(t, hk, []string{accessUserSub}, nil)
	claims := accessUserBaseClaims(now)
	claims["aud"] = []string{"extra-aud-value", assistantTestAUD}
	jwt := chatSignJWTClaims(t, chatTestSigningKey(), "k1", claims)
	writer := accessUserPost(srv, http.MethodPost, "/mcp", jwt, "", nil)
	if writer.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	line := auditWithResult(t, logs, "ok")
	assertAudiences(t, line, []string{"extra-aud-value", assistantTestAUD})
	assertStructuredPrincipal(t, line, []string{"extra-aud-value", assistantTestAUD})
	if identity, _ := line["identity"].(string); strings.Contains(identity, "extra-aud-value") || strings.Contains(identity, accessUserEmail) {
		t.Fatalf("identity key absorbed audience or email: %q", identity)
	}

	logs.Reset()
	claims["aud"] = assistantTestAUD
	jwt = chatSignJWTClaims(t, chatTestSigningKey(), "k1", claims)
	writer = accessUserPost(srv, http.MethodPost, "/mcp", jwt, "", nil)
	if writer.Code != http.StatusOK {
		t.Fatalf("string aud status=%d body=%s", writer.Code, writer.Body.String())
	}
	stringLine := auditWithResult(t, logs, "ok")
	assertAudiences(t, stringLine, []string{assistantTestAUD})
	assertStructuredPrincipal(t, stringLine, []string{assistantTestAUD})

	logs.Reset()
	claims["aud"] = []string{"other-a", "other-b"}
	jwt = chatSignJWTClaims(t, chatTestSigningKey(), "k1", claims)
	writer = accessUserPost(srv, http.MethodPost, "/mcp", jwt, "", nil)
	assertGenericUnauthorized(t, writer)
	mismatch := auditWithResult(t, logs, "aud_mismatch")
	assertAudiences(t, mismatch, []string{"other-a", "other-b"})
	assertStructuredPrincipal(t, mismatch, []string{"other-a", "other-b"})
}

// A valid assertion alone reaches initialize and a read tool. No bearer
// and no extra identity header is required.
func TestAccessUserMCPHandshake(t *testing.T) {
	hk := newFakeAssistantHK(t)
	hub := newFakeAssistantHub(t)
	certs := chatTestCerts(t, chatTestSigningKey())
	srv, _ := assistantWriteServer(t, hk, hub, assistantWritesAll, func(cfg *assistantConfig) {
		cfg.AuthMode = assistantAuthModeAccessUser
		cfg.CFTeam = "team"
		cfg.CFAUD = assistantTestAUD
		cfg.CFCertsURL = certs.URL
		cfg.CFSubjects = []string{accessUserSub}
	})
	jwt := chatSignJWTClaims(t, chatTestSigningKey(), "k1", accessUserBaseClaims(time.Now()))
	init := accessUserRPC(srv, jwt, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}`)
	if init.Code != http.StatusOK {
		t.Fatalf("initialize status=%d body=%s", init.Code, init.Body.String())
	}
	if strings.Contains(init.Body.String(), "Write tools enabled") || strings.Contains(init.Body.String(), "answer_decision") || strings.Contains(init.Body.String(), "answer_question") {
		t.Fatalf("initialize advertised a write tool: %s", init.Body.String())
	}
	if !strings.Contains(init.Body.String(), "pending_list") {
		t.Fatalf("initialize omitted read tools: %s", init.Body.String())
	}
	want := []string{"targets", "pending_list", "pending_detail", "progress", "poll"}
	got := accessUserListedTools(t, srv, jwt)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("tools/list=%v, want %v", got, want)
	}
	read := accessUserTool(t, srv, jwt, "targets", map[string]any{})
	if read.isError || read.httpCode != http.StatusOK {
		t.Fatalf("targets: code=%d err=%v text=%s", read.httpCode, read.isError, read.text)
	}
	if len(hk.requestLog()) != 0 {
		t.Fatalf("targets reached hk: %v", hk.requestLog())
	}
}

// AC8 and mutant (f). The legal writes value `all` is on. Literal `1` is
// not a flag value and is refused by the existing loader test. With the
// block removed, write tools are listed and these calls reach hk or hub.
func TestMutantAccessUserWriteBlockRemoved(t *testing.T) {
	hkHits := 0
	hkBomb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hkHits++
		t.Errorf("hk called: %s %s", r.Method, r.URL.Path)
		http.Error(w, "hk called", http.StatusInternalServerError)
	}))
	t.Cleanup(hkBomb.Close)
	hubHits := 0
	hubBomb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hubHits++
		t.Errorf("hub called: %s %s", r.Method, r.URL.Path)
		http.Error(w, "hub called", http.StatusInternalServerError)
	}))
	t.Cleanup(hubBomb.Close)
	hk := newFakeAssistantHK(t)
	hub := newFakeAssistantHub(t)
	certs := chatTestCerts(t, chatTestSigningKey())
	srv, _ := assistantWriteServer(t, hk, hub, assistantWritesAll, func(cfg *assistantConfig) {
		cfg.AuthMode = assistantAuthModeAccessUser
		cfg.CFTeam = "team"
		cfg.CFAUD = assistantTestAUD
		cfg.CFCertsURL = certs.URL
		cfg.CFSubjects = []string{accessUserSub}
		cfg.HKURL = hkBomb.URL
		cfg.HubURL = hubBomb.URL
	})
	if srv.writes != assistantWritesAll {
		t.Fatalf("writes set=%d, want all", srv.writes)
	}
	if srv.hub != nil || srv.drainKick != nil || srv.limiter != nil {
		t.Fatal("access-user wired the hub drainer")
	}
	jwt := chatSignJWTClaims(t, chatTestSigningKey(), "k1", accessUserBaseClaims(time.Now()))
	got := accessUserListedTools(t, srv, jwt)
	for _, name := range got {
		if name == "answer_decision" || name == "answer_question" || name == "deliver" {
			t.Fatalf("write tool listed: %v", got)
		}
	}
	if strings.Join(got, ",") != "targets,pending_list,pending_detail,progress,poll" {
		t.Fatalf("tools/list=%v", got)
	}
	calls := []struct {
		name string
		args map[string]any
	}{
		{"answer_decision", map[string]any{"request_id": "dr-1-1", "option": "A", "text": "ship it"}},
		{"answer_question", map[string]any{"id": "Q-20261012-70", "revision": 1, "text": "the lane stays"}},
		{"deliver", map[string]any{"target": "ops", "text": "lane notice", "idempotency_key": "idem-key-1"}},
	}
	for _, call := range calls {
		result := accessUserTool(t, srv, jwt, call.name, call.args)
		if result.httpCode != http.StatusOK || !result.isError || !strings.Contains(result.text, `"error":"writes_blocked_access_user"`) {
			t.Fatalf("%s: code=%d err=%v text=%s", call.name, result.httpCode, result.isError, result.text)
		}
	}
	if hkHits != 0 || hubHits != 0 {
		t.Fatalf("upstream calls hk=%d hub=%d", hkHits, hubHits)
	}
	if len(hk.requestLog()) != 0 || hub.postCount() != 0 {
		t.Fatalf("stand-in fakes were called hk=%v hub=%d", hk.requestLog(), hub.postCount())
	}
}

func accessUserTool(t *testing.T, srv *assistantServer, jwt, tool string, args map[string]any) assistantToolResult {
	t.Helper()
	params, err := json.Marshal(map[string]any{"name": tool, "arguments": args})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": json.RawMessage(params)})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body))
	req.Header.Set("Cf-Access-Jwt-Assertion", jwt)
	writer := httptest.NewRecorder()
	srv.ServeHTTP(writer, req)
	out := assistantToolResult{httpCode: writer.Code}
	if writer.Code != http.StatusOK {
		out.text = writer.Body.String()
		return out
	}
	var response struct {
		Result *struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
		Error *assistantRPCError `json:"error"`
	}
	if err := json.Unmarshal(writer.Body.Bytes(), &response); err != nil {
		t.Fatalf("rpc response undecodable: %v body=%q", err, writer.Body.String())
	}
	out.rpcErr = response.Error
	if response.Result == nil {
		return out
	}
	out.isError = response.Result.IsError
	if len(response.Result.Content) == 1 {
		out.text = response.Result.Content[0].Text
	}
	if !out.isError && out.text != "" {
		_ = json.Unmarshal([]byte(out.text), &out.decoded)
	}
	return out
}

func bigIntBytes(value int64) []byte {
	if value == 0 {
		return []byte{0}
	}
	var raw [8]byte
	n := 0
	for value > 0 {
		raw[n] = byte(value)
		value >>= 8
		n++
	}
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		out[i] = raw[n-1-i]
	}
	return out
}

func signJWTWithHeader(t *testing.T, key *rsa.PrivateKey, header map[string]any, claims map[string]any) string {
	t.Helper()
	headerJSON, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	payloadJSON, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	encodedHeader := base64.RawURLEncoding.EncodeToString(headerJSON)
	encodedPayload := base64.RawURLEncoding.EncodeToString(payloadJSON)
	digest := sha256.Sum256([]byte(encodedHeader + "." + encodedPayload))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return encodedHeader + "." + encodedPayload + "." + base64.RawURLEncoding.EncodeToString(signature)
}

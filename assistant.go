package panewire

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The panewire-assistant binary (MGH-36) is the thin external surface that
// lets the berry assistant read decision state and — only when the
// operator flips the writes flag — apply answers and deliver lane text over
// MCP. It holds no durable state of its own: every read is proxied to
// handoffkeep at call time and every owed lane notice lives in
// handoffkeep's notification outbox, so the process can be killed and
// restarted at any moment without losing or duplicating anything. All
// operator credentials live only in server-side mode-0600 files. Bearer
// mode authenticates with a dedicated bearer token. Service mode adds a
// Cloudflare Access service token whose common_name must be on an
// allowlist (the hub's verifier cannot be reused: it has no identity
// binding). Access-user mode instead requires a verified user assertion
// and does not accept the bearer.

const (
	// assistantDefaultListen keeps the binary loopback-only by default; an
	// operator must opt in to a wider bind through the config file.
	assistantDefaultListen = "127.0.0.1:9471"
	// assistantMaxRequestBytes bounds one JSON-RPC request body.
	assistantMaxRequestBytes = 256 << 10
	// assistantHKTimeout bounds one upstream call — hk or hub, read or
	// write.
	assistantHKTimeout = 10 * time.Second
	// assistantDrainIntervalDefault is the outbox drainer's base period;
	// each pass actually fires at interval ±25% jitter.
	assistantDrainIntervalDefault = 30 * time.Second
	assistantDrainIntervalMin     = time.Second
	assistantDrainIntervalMax     = time.Hour
	// assistantDrainPassTimeout bounds one drain pass (well under the 60s
	// WriteTimeout).
	assistantDrainPassTimeout = 30 * time.Second
	// assistantWriteRateDefaults are the per-identity write limit:
	// writes/min steady rate and the burst the token bucket holds.
	assistantWriteRatePerMinDefault = 10
	assistantWriteBurstDefault      = 5
)

// The writes flag is a set, not a bool (brief amendment 1): the operator
// can run answers without the free-text injection tool, or deliver without
// answers. Exact values only — a typo refuses startup rather than silently
// landing on a different surface.
const (
	assistantWritesOff = iota
	assistantWritesAnswer
	assistantWritesDeliver
	assistantWritesAll
)

// Auth modes. Empty in the config file keeps today's inference (a
// configured service gate is service mode, otherwise bearer). The
// access-user spelling is exact: any other text refuses startup.
const (
	assistantAuthModeBearer     = "bearer"
	assistantAuthModeService    = "service"
	assistantAuthModeAccessUser = "access-user"
)

// assistantConfig is the parsed server-side configuration. Every byte of it
// is secret-adjacent: no field is ever rendered into a log line, an error
// string, or a tool result.
type assistantConfig struct {
	// Listen is the bind address; defaults to loopback.
	Listen string
	// HKURL and HKToken are the handoffkeep read credentials.
	HKURL   string
	HKToken string
	// HKCFID and HKCFSecret carry the optional Cloudflare Access service
	// token for an hk URL behind Access; they attach only as a pair and only
	// to requests against the configured URL (note 1200).
	HKCFID     string
	HKCFSecret string
	// HubURL and HubToken are the hub operator credential for the write
	// surface: POST /v1/relay/events for deliver and the outbox drainer.
	// Writes refuse to start without them.
	HubURL   string
	HubToken string
	// Writes is the write-tool set: assistantWritesOff (default) lists no
	// write tool and answers writes_disabled; assistantWritesAnswer enables
	// the two answer tools and the drainer; assistantWritesDeliver enables
	// deliver and the drainer; assistantWritesAll enables all three.
	Writes int
	// DrainInterval is the outbox drainer's base period (±25% jitter);
	// defaults to 30 s.
	DrainInterval time.Duration
	// WriteRatePerMin and WriteBurst are the per-identity write rate
	// limit; defaults 10/min, burst 5.
	WriteRatePerMin int
	WriteBurst      int
	// TokenFile is the berry-facing bearer token file (mode 0600).
	TokenFile string
	// TargetsFile is the operator-edited opaque-target mapping (mode 0600).
	TargetsFile string
	// ClientName is the audit identity label for requests authenticated by
	// bearer alone; Cloudflare-authenticated requests log service:<name>.
	ClientName string
	// CFTeam, CFAUD and CFServiceNames configure the optional inbound
	// Cloudflare Access gate. CFServiceNames is a comma-separated allowlist
	// of service-token common_name values — identity binding, which the
	// hub's CF verifier lacks by design.
	CFTeam         string
	CFAUD          string
	CFServiceNames []string
	// CFCertsURL is the Access JWKS document. Optional in service mode
	// (the team default is used); required in access-user mode.
	CFCertsURL string
	// AuthMode is bearer, service, or access-user. Empty keeps the
	// inference described above.
	AuthMode string
	// CFSubjects is the access-user allowlist of stable subs. The
	// principal key is (issuer, sub). Email is never an entry.
	CFSubjects []string
}

// assistantConfigError is a startup refusal. The text names the failed
// setting, never its value.
type assistantConfigError struct{ reason string }

func (e *assistantConfigError) Error() string { return e.reason }

func configError(reason string) error { return &assistantConfigError{reason: reason} }

// loadAssistantConfig reads the env-style config file (mode 0600, like the
// hub's handoffkeep env) and validates every field. Any failure refuses
// startup; a partial configuration is never silently half-applied.
func loadAssistantConfig(path string) (assistantConfig, error) {
	values, err := loadMode0600Env(path)
	if err != nil {
		return assistantConfig{}, configError("config file must be a regular mode-0600 file")
	}
	cfg := assistantConfig{
		Listen:          strings.TrimSpace(values["PANEWIRE_ASSISTANT_LISTEN"]),
		HKURL:           values["HANDOFFKEEP_URL"],
		HKToken:         values["HANDOFFKEEP_TOKEN"],
		HKCFID:          strings.TrimSpace(values["HANDOFFKEEP_CF_ACCESS_CLIENT_ID"]),
		HKCFSecret:      strings.TrimSpace(values["HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET"]),
		HubURL:          values["PANEWIRE_ASSISTANT_HUB_URL"],
		HubToken:        values["PANEWIRE_ASSISTANT_HUB_TOKEN"],
		DrainInterval:   assistantDrainIntervalDefault,
		WriteRatePerMin: assistantWriteRatePerMinDefault,
		WriteBurst:      assistantWriteBurstDefault,
		TokenFile:       strings.TrimSpace(values["PANEWIRE_ASSISTANT_TOKEN_FILE"]),
		TargetsFile:     strings.TrimSpace(values["PANEWIRE_ASSISTANT_TARGETS_FILE"]),
		ClientName:      strings.TrimSpace(values["PANEWIRE_ASSISTANT_CLIENT_NAME"]),
		CFTeam:          strings.TrimSpace(values["PANEWIRE_ASSISTANT_CF_TEAM"]),
		CFAUD:           strings.TrimSpace(values["PANEWIRE_ASSISTANT_CF_AUD"]),
		CFCertsURL:      strings.TrimSpace(values["PANEWIRE_ASSISTANT_CF_CERTS_URL"]),
	}
	if names := strings.TrimSpace(values["PANEWIRE_ASSISTANT_CF_SERVICE_NAMES"]); names != "" {
		for _, name := range strings.Split(names, ",") {
			name = strings.TrimSpace(name)
			if name != "" {
				cfg.CFServiceNames = append(cfg.CFServiceNames, name)
			}
		}
	}
	// Exact spelling, already trimmed by the env loader. A case variant
	// or unknown word refuses in validate; it is not folded to a mode.
	cfg.AuthMode = values["PANEWIRE_ASSISTANT_AUTH_MODE"]
	if subs := values["PANEWIRE_ASSISTANT_CF_SUBJECTS"]; subs != "" {
		for _, sub := range strings.Split(subs, ",") {
			sub = strings.TrimSpace(sub)
			if sub != "" {
				cfg.CFSubjects = append(cfg.CFSubjects, sub)
			}
		}
	}
	// Exact values only (amendment E): a typo, case variant or padded value
	// must refuse startup, never silently degrade to a different set.
	switch flag := values["PANEWIRE_ASSISTANT_WRITES"]; flag {
	case "", "0", "false", "no", "off":
		cfg.Writes = assistantWritesOff
	case "answer":
		cfg.Writes = assistantWritesAnswer
	case "deliver":
		cfg.Writes = assistantWritesDeliver
	case "all":
		cfg.Writes = assistantWritesAll
	default:
		return assistantConfig{}, configError("assistant writes flag must be answer, deliver or all")
	}
	if raw := strings.TrimSpace(values["PANEWIRE_ASSISTANT_DRAIN_INTERVAL"]); raw != "" {
		interval, err := time.ParseDuration(raw)
		if err != nil {
			return assistantConfig{}, configError("assistant drain interval is not a duration")
		}
		cfg.DrainInterval = interval
	}
	if raw := strings.TrimSpace(values["PANEWIRE_ASSISTANT_WRITE_RATE_PER_MIN"]); raw != "" {
		rate, err := strconv.Atoi(raw)
		if err != nil || rate < 1 {
			return assistantConfig{}, configError("assistant write rate per minute is not a positive number")
		}
		cfg.WriteRatePerMin = rate
	}
	if raw := strings.TrimSpace(values["PANEWIRE_ASSISTANT_WRITE_BURST"]); raw != "" {
		burst, err := strconv.Atoi(raw)
		if err != nil || burst < 1 {
			return assistantConfig{}, configError("assistant write burst is not a positive number")
		}
		cfg.WriteBurst = burst
	}
	if cfg.Listen == "" {
		cfg.Listen = assistantDefaultListen
	}
	if cfg.ClientName == "" {
		cfg.ClientName = "bearer"
	}
	if err := cfg.validate(); err != nil {
		return assistantConfig{}, err
	}
	return cfg, nil
}

func (c assistantConfig) validate() error {
	if !validHandoffkeepBaseURL(c.HKURL) || !validHandoffkeepToken(c.HKToken) {
		return configError("handoffkeep url or token missing or invalid")
	}
	// The outbound Access pair is all-or-nothing, same rule as the hk remote
	// client (note 1200): one half set is a config error, never a partial send.
	if (c.HKCFID == "") != (c.HKCFSecret == "") {
		return configError("cf access client id and secret must be set together")
	}
	// The hub pair is likewise all-or-nothing. With writes enabled it is
	// required outright: a write-enabled binary without a hub credential
	// could apply answers whose lane notices it can never send, which is
	// worse than refusing to run.
	if (c.HubURL == "") != (c.HubToken == "") {
		return configError("hub url and token must be set together")
	}
	if c.HubURL != "" && (!validHandoffkeepBaseURL(c.HubURL) || !validHandoffkeepToken(c.HubToken)) {
		return configError("hub url or token invalid")
	}
	if c.Writes != assistantWritesOff && (c.HubURL == "" || c.HubToken == "") {
		return configError("assistant writes require the hub url and token")
	}
	// A zero interval means "unset" for a struct built outside the config
	// loader (the loader always writes the default); an explicit value is
	// bounded so a typo cannot spin or freeze the drain loop.
	if c.DrainInterval != 0 && (c.DrainInterval < assistantDrainIntervalMin || c.DrainInterval > assistantDrainIntervalMax) {
		return configError("assistant drain interval out of range")
	}
	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		return configError("listen address must be host:port")
	}
	switch c.AuthMode {
	case assistantAuthModeAccessUser:
		return c.validateAccessUser()
	case assistantAuthModeBearer:
		return c.validateBearerMode()
	case assistantAuthModeService:
		return c.validateServiceMode()
	case "":
		return c.validateLegacyAuth()
	default:
		return configError("auth mode unknown")
	}
}

func (c assistantConfig) accessConfigPresent() bool {
	return c.CFTeam != "" || c.CFAUD != "" || len(c.CFServiceNames) != 0 || c.CFCertsURL != "" || len(c.CFSubjects) != 0
}

func (c assistantConfig) validateBearerMode() error {
	if c.TokenFile == "" || c.TargetsFile == "" {
		return configError("token file and targets file are required")
	}
	if c.accessConfigPresent() {
		return configError("bearer mode rejects access gate")
	}
	return nil
}

func (c assistantConfig) validateServiceMode() error {
	if c.TokenFile == "" || c.TargetsFile == "" {
		return configError("token file and targets file are required")
	}
	if len(c.CFSubjects) != 0 {
		return configError("service mode rejects subject allowlist")
	}
	return c.validateServiceGate()
}

// validateLegacyAuth is the pre-mode config: no mode key, service gate
// when the CF triple is complete, bearer otherwise.
func (c assistantConfig) validateLegacyAuth() error {
	if c.TokenFile == "" || c.TargetsFile == "" {
		return configError("token file and targets file are required")
	}
	if len(c.CFSubjects) != 0 {
		return configError("subject allowlist requires access-user mode")
	}
	if c.CFTeam != "" || c.CFAUD != "" || len(c.CFServiceNames) != 0 || c.CFCertsURL != "" {
		return c.validateServiceGate()
	}
	return nil
}

func (c assistantConfig) validateServiceGate() error {
	// The inbound Access gate is all-or-nothing and service-name bound: an
	// enabled gate without an allowlist would verify any Access identity,
	// which is exactly the hub verifier's weakness this binary must not copy.
	if c.CFTeam == "" || c.CFAUD == "" || len(c.CFServiceNames) == 0 {
		return configError("cf access gate requires team, aud and service names together")
	}
	if !hubCFAccessTeamPattern.MatchString(c.CFTeam) {
		return configError("cf access team invalid")
	}
	if c.CFCertsURL != "" && !validHandoffkeepBaseURL(c.CFCertsURL) {
		return configError("cf access certs url invalid")
	}
	return nil
}

func (c assistantConfig) validateAccessUser() error {
	if c.TargetsFile == "" {
		return configError("targets file is required")
	}
	if c.CFTeam == "" {
		return configError("access-user mode requires team domain")
	}
	if !hubCFAccessTeamPattern.MatchString(c.CFTeam) {
		return configError("cf access team invalid")
	}
	if c.CFAUD == "" {
		return configError("access-user mode requires aud")
	}
	if len(c.CFAUD) > 256 {
		return configError("cf access configuration is invalid")
	}
	if c.CFCertsURL == "" {
		return configError("access-user mode requires jwks url")
	}
	if !validHandoffkeepBaseURL(c.CFCertsURL) {
		return configError("cf access certs url invalid")
	}
	if len(c.CFSubjects) == 0 {
		return configError("access-user mode requires subject allowlist")
	}
	return nil
}

// loadAssistantBearer reads the berry-facing bearer token. The file must be a
// regular mode-0600 file holding one token; the stored value is its SHA-256 so
// the raw token never lives in process memory longer than needed and
// comparison is constant-time.
func loadAssistantBearer(path string) ([32]byte, error) {
	var sum [32]byte
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 {
		return sum, configError("token file must be a regular mode-0600 file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return sum, configError("token file unreadable")
	}
	token := strings.TrimSpace(string(raw))
	if !validHandoffkeepToken(token) {
		return sum, configError("token file holds no usable token")
	}
	return sha256.Sum256([]byte(token)), nil
}

// assistantPrincipal is the verified caller. Later profile code reads this
// value; it is not a display name. Issuer and Subject are the stable key.
// Audiences is every aud value from the token, not only the one that
// matched this assistant. Email is display only and is never part of
// Label. Label is the audit and rate-limit key.
type assistantPrincipal struct {
	Mode      string
	Issuer    string
	Subject   string
	Audiences []string
	Email     string
	Label     string
}

type assistantPrincipalKey struct{}

func withAssistantPrincipal(ctx context.Context, p assistantPrincipal) context.Context {
	return context.WithValue(ctx, assistantPrincipalKey{}, p)
}

func assistantPrincipalFrom(ctx context.Context) assistantPrincipal {
	p, _ := ctx.Value(assistantPrincipalKey{}).(assistantPrincipal)
	return p
}

func assistantAuditEmail(email string) string {
	email = strings.TrimSpace(email)
	if len(email) > 254 {
		return utf8PrefixWithin(email, 254)
	}
	return email
}

// assistantAuditAudiences logs a slice on every line, including the empty
// one, so a missing audience list is [] and not an omitted field.
func assistantAuditAudiences(audiences []string) []string {
	if audiences == nil {
		return []string{}
	}
	return audiences
}

// assistantAuditPrincipal is the structured caller recorded on each audit
// line. It is the same value request handling already holds.
func assistantAuditPrincipal(p assistantPrincipal) map[string]any {
	return map[string]any{
		"mode":      p.Mode,
		"issuer":    p.Issuer,
		"subject":   p.Subject,
		"audiences": assistantAuditAudiences(p.Audiences),
		"email":     assistantAuditEmail(p.Email),
	}
}

// accessUserWritesBlocked is the single seam that keeps writes off in
// Access-user mode. Profile enforcement is later work. List, call, and
// hub-drainer wiring all consult it, so removing the check re-opens the
// three together. The configured writes set is left as the operator set it.
func accessUserWritesBlocked(mode string) bool {
	return mode == assistantAuthModeAccessUser
}

// assistantServer is the whole runtime: auth, the targets map path, the hk
// and (when writes are enabled) hub clients, the outbox drainer state and
// the audit log. It keeps no durable state — the outbox lives in
// handoffkeep.
type assistantServer struct {
	tokenHash   [32]byte
	authMode    string
	cf          *assistantCFVerifier
	hk          *assistantHK
	targetsPath string
	clientName  string
	audit       *slog.Logger
	httpClient  *http.Client
	// writes is the enabled write-tool set; hub exists exactly when any
	// write tool is on.
	writes int
	hub    *assistantHub
	// drainInterval is the periodic pass base period. passMu serializes
	// drain passes; drainKick (buffer 1) carries the async pass a write
	// tool kicks so at most one pass runs and one more waits.
	drainInterval time.Duration
	passMu        sync.Mutex
	drainKick     chan struct{}
	// limiter is the per-identity write rate limit (default 10/min,
	// burst 5): a leaked credential cannot flood lanes faster than this.
	limiter *writeRateLimiter
}

type assistantServerDeps struct {
	// HKHTTPClient, HubHTTPClient and CFHTTPClient are test seams;
	// production leaves them nil. Logger receives the audit stream; nil
	// defaults to stderr.
	HKHTTPClient  *http.Client
	HubHTTPClient *http.Client
	CFHTTPClient  *http.Client
	Logger        *slog.Logger
}

func newAssistantServer(cfg assistantConfig, deps assistantServerDeps) (*assistantServer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	mode := cfg.AuthMode
	if mode == "" {
		if cfg.CFTeam != "" {
			mode = assistantAuthModeService
		} else {
			mode = assistantAuthModeBearer
		}
	}
	var tokenHash [32]byte
	// Access-user does not require a bearer file. A file that is present
	// is still loaded so a malformed file refuses startup, and so the
	// hash exists for tests that kill a bearer-fallback mutant. The
	// request path below never compares it in this mode.
	if mode != assistantAuthModeAccessUser || cfg.TokenFile != "" {
		var err error
		tokenHash, err = loadAssistantBearer(cfg.TokenFile)
		if err != nil {
			return nil, err
		}
	}
	// The targets file is validated at startup — a bad mode or schema refuses
	// the binary — and re-validated on every targets read so an edit (or a
	// chmod) takes effect immediately, always fail-closed.
	if _, err := loadAssistantTargets(cfg.TargetsFile); err != nil {
		return nil, err
	}
	hk, err := newAssistantHK(cfg.HKURL, cfg.HKToken, cfg.HKCFID, cfg.HKCFSecret, deps.HKHTTPClient)
	if err != nil {
		return nil, err
	}
	var hub *assistantHub
	var drainKick chan struct{}
	var limiter *writeRateLimiter
	// Access-user keeps the writes flag visible and still refuses to build
	// the hub client or the drainer. Bearer and service modes are unchanged.
	if cfg.Writes != assistantWritesOff && !accessUserWritesBlocked(mode) {
		hub, err = newAssistantHub(cfg.HubURL, cfg.HubToken, deps.HubHTTPClient)
		if err != nil {
			return nil, err
		}
		drainKick = make(chan struct{}, 1)
		ratePerMin, burst := cfg.WriteRatePerMin, cfg.WriteBurst
		if ratePerMin <= 0 {
			ratePerMin = assistantWriteRatePerMinDefault
		}
		if burst <= 0 {
			burst = assistantWriteBurstDefault
		}
		limiter = newWriteRateLimiter(ratePerMin, burst)
	}
	var cf *assistantCFVerifier
	switch mode {
	case assistantAuthModeService:
		cf, err = newAssistantCFVerifier(cfg.CFTeam, cfg.CFAUD, cfg.CFCertsURL, cfg.CFServiceNames, deps.CFHTTPClient)
		if err != nil {
			return nil, err
		}
	case assistantAuthModeAccessUser:
		cf, err = newAssistantAccessUserVerifier(cfg.CFTeam, cfg.CFAUD, cfg.CFCertsURL, cfg.CFSubjects, deps.CFHTTPClient)
		if err != nil {
			return nil, err
		}
	case assistantAuthModeBearer:
	default:
		return nil, configError("auth mode unknown")
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}
	drainInterval := cfg.DrainInterval
	if drainInterval == 0 {
		drainInterval = assistantDrainIntervalDefault
	}
	return &assistantServer{
		tokenHash:     tokenHash,
		authMode:      mode,
		cf:            cf,
		hk:            hk,
		targetsPath:   cfg.TargetsFile,
		clientName:    cfg.ClientName,
		audit:         logger,
		writes:        cfg.Writes,
		hub:           hub,
		drainInterval: drainInterval,
		drainKick:     drainKick,
		limiter:       limiter,
	}, nil
}

// writeEnabled reports whether the named write tool is in the enabled set.
// Read tools are never gated here — they answer for every flag value.
func (s *assistantServer) writeEnabled(tool string) bool {
	return assistantWriteSetIncludes(s.writes, tool)
}

// authenticate reports the verified principal. Access-user mode never
// consults the bearer: a valid bearer is not a fallback, and a missing
// bearer is not a failure when the user assertion verifies. Bearer and
// service modes still require the bearer exactly as before.
func (s *assistantServer) authenticate(r *http.Request) (assistantPrincipal, string, bool) {
	if s.authMode == assistantAuthModeAccessUser {
		if s.cf == nil {
			return assistantPrincipal{Mode: s.authMode}, "jwks_unavailable", false
		}
		principal, reason := s.cf.authenticateAccessUser(r)
		if reason != "" {
			return principal, reason, false
		}
		return principal, "", true
	}
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	token, found := strings.CutPrefix(header, "Bearer ")
	if !found {
		return assistantPrincipal{Mode: s.authMode}, "", false
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	if subtle.ConstantTimeCompare(sum[:], s.tokenHash[:]) != 1 {
		return assistantPrincipal{Mode: s.authMode}, "", false
	}
	if s.cf != nil {
		identity, ok := s.cf.authenticate(r)
		if !ok {
			return assistantPrincipal{Mode: assistantAuthModeService}, "", false
		}
		return assistantPrincipal{Mode: assistantAuthModeService, Label: identity}, "", true
	}
	return assistantPrincipal{Mode: assistantAuthModeBearer, Label: s.clientName}, "", true
}

// assistantAccessProtectedResourcePath is the metadata URL Cloudflare
// Access serves at the edge. The origin does not know the team, the
// issuer, or the AUD. It only refuses to answer this path: a 401 would
// block discovery, and a metadata body would shadow the edge document.
func assistantAccessProtectedResourcePath(p string) bool {
	return path.Clean(p) == "/.well-known/cloudflare-access-protected-resource/mcp"
}

// ServeHTTP applies auth to every request, then the MCP route table. There
// is no health check. The one path that is not authenticated is the Access
// protected-resource path, and that path is not served.
func (s *assistantServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if assistantAccessProtectedResourcePath(r.URL.Path) {
		s.logAudit(r.Context(), "-", r.Method+" "+r.URL.Path, "-", "-", "not_found")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"not_found"}`))
		return
	}
	principal, reason, ok := s.authenticate(r)
	r = r.WithContext(withAssistantPrincipal(r.Context(), principal))
	if !ok {
		// The caller always sees the same generic error. Access-user
		// failures record a distinct reason on the audit line only.
		result := "unauthorized"
		if reason != "" {
			result = reason
		}
		s.logAudit(r.Context(), "-", r.Method+" "+r.URL.Path, "-", "-", result)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
		return
	}
	identity := principal.Label
	if r.URL.Path != "/mcp" {
		s.logAudit(r.Context(), identity, r.Method+" "+r.URL.Path, "-", "-", "not_found")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"not_found"}`))
		return
	}
	if r.Method == http.MethodGet || r.Method == http.MethodDelete {
		// Streamable HTTP GET/DELETE manage optional server streams and
		// sessions; this stateless server has neither (spec-legal 405).
		s.logAudit(r.Context(), identity, r.Method+" /mcp", "-", "-", "method_not_allowed")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = w.Write([]byte(`{"error":"method_not_allowed"}`))
		return
	}
	if r.Method != http.MethodPost {
		s.logAudit(r.Context(), identity, r.Method+" /mcp", "-", "-", "method_not_allowed")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = w.Write([]byte(`{"error":"method_not_allowed"}`))
		return
	}
	s.serveMCP(w, r, identity)
}

// logAudit writes one audit line per request/tool call. It logs only parsed,
// whitelisted fields: never headers, never bodies, never the token.
func (s *assistantServer) logAudit(ctx context.Context, identity, rpc, tool, subject, result string) {
	subject = utf8PrefixWithin(subject, 160)
	p := assistantPrincipalFrom(ctx)
	// "subject" on this line is the tool's target or request id. The
	// verified principal's subject lives on principal.subject so the two
	// cannot overwrite each other.
	s.audit.Info("assistant_request",
		"identity", identity,
		"auth_mode", p.Mode,
		"principal", assistantAuditPrincipal(p),
		"issuer", p.Issuer,
		"audiences", assistantAuditAudiences(p.Audiences),
		"iss", p.Issuer,
		"sub", p.Subject,
		"email", assistantAuditEmail(p.Email),
		"rpc", rpc,
		"tool", tool,
		"subject", subject,
		"result", result,
	)
}

// RunAssistantServer is the binary entry path used by cmd/panewire-assistant:
// load config, bind, serve until killed. Startup refusals are printed as
// named reasons; no secret value is ever printed.
func RunAssistantServer(args []string, stderr io.Writer) int {
	var configPath string
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "-config" && i+1 < len(args):
			configPath = args[i+1]
			i++
		case strings.HasPrefix(args[i], "-config="):
			configPath = strings.TrimPrefix(args[i], "-config=")
		default:
			fmt.Fprintln(stderr, "usage: panewire-assistant -config <mode-0600 env file>")
			return 2
		}
	}
	if configPath == "" {
		fmt.Fprintln(stderr, "usage: panewire-assistant -config <mode-0600 env file>")
		return 2
	}
	cfg, err := loadAssistantConfig(configPath)
	if err != nil {
		fmt.Fprintf(stderr, "panewire-assistant: %v\n", err)
		return 1
	}
	server, err := newAssistantServer(cfg, assistantServerDeps{})
	if err != nil {
		fmt.Fprintf(stderr, "panewire-assistant: %v\n", err)
		return 1
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		fmt.Fprintln(stderr, "panewire-assistant: listen failed")
		return 1
	}
	server.logAudit(context.Background(), "-", "startup", "-", "-", "listening "+listener.Addr().String())
	if server.hub != nil {
		// The outbox drainer runs for the process lifetime: one pass at
		// startup (a predecessor's crash leaves owed rows), then one per
		// jittered interval.
		go server.drainLoop(context.Background())
	}
	httpServer := &http.Server{
		Handler:           server,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}
	if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(stderr, "panewire-assistant: serve failed")
		return 1
	}
	return 0
}

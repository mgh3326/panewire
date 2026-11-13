package fleet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/durable"
	"github.com/mgh3326/panewire/internal/hkcred"
	"github.com/mgh3326/panewire/internal/quota/agy"
)

// The bench catalog lives in handoffkeep. This client reads and writes it
// directly, with pacelever's cache rules, so no catalog action runs a
// pacelever process (hk 3249).
const (
	catalogTTL      = time.Hour
	catalogStaleMax = 24 * time.Hour
	catalogTimeout  = 8 * time.Second
	catalogMaxBody  = 16 << 20

	catalogSourceServer     = "server"
	catalogSourceCache      = "cache"
	catalogSourceCacheStale = "cache-stale"
)

// CatalogInfo is the provenance of one catalog read: the source it came from,
// whether it is stale, and the age of the cache entry in seconds.
type CatalogInfo struct {
	Source string   `json:"source"`
	Stale  bool     `json:"stale"`
	AgeS   *float64 `json:"age_s"`
}

// FreshServer reports a read that came from the endpoint, not a cache.
func (i CatalogInfo) FreshServer() bool { return i.Source == catalogSourceServer && !i.Stale }

// CatalogEntry is one bench catalog row as handoffkeep stores it. CatalogRow
// is the subset admission and selection use; the remaining fields are needed
// to list and push the catalog without losing the operator's decision.
type CatalogEntry struct {
	Profile             string   `json:"profile"`
	Effort              string   `json:"effort"`
	ModelID             string   `json:"model_id"`
	Family              string   `json:"family,omitempty"`
	Pool                string   `json:"pool"`
	Grade               string   `json:"grade"`
	Score               *float64 `json:"score"`
	Gate                string   `json:"gate"`
	GateReason          *string  `json:"gate_reason"`
	RetiredAt           *string  `json:"retired_at"`
	BoundaryVersion     string   `json:"boundary_version,omitempty"`
	DeviationRef        string   `json:"deviation_ref,omitempty"`
	DecidedBy           string   `json:"decided_by,omitempty"`
	DecidedAt           string   `json:"decided_at,omitempty"`
	BenchmarkSource     string   `json:"benchmark_source,omitempty"`
	BenchmarkAnnotation string   `json:"benchmark_annotation,omitempty"`
	// Group is the metering group of an agy rung ("gemini", "3p"), empty
	// for other pools and for a rung that maps to no group.
	Group string `json:"group,omitempty"`
}

// row is the admission-facing projection of an entry.
func (e CatalogEntry) row() CatalogRow {
	return CatalogRow{Profile: e.Profile, Effort: e.Effort, Grade: e.Grade, ModelID: e.ModelID, Pool: e.Pool, Gate: e.Gate, Score: e.Score, RetiredAt: e.RetiredAt, Group: e.Group, Family: e.Family}
}

// CatalogData is one catalog read. Raw carries the rows exactly as they were
// received (with unknown-pool rows removed) so an artifact writer can preserve
// fields this package does not model.
type CatalogData struct {
	Rows     []CatalogEntry
	Raw      []json.RawMessage
	Info     CatalogInfo
	Warnings []string
}

// CatalogClientConfig names an endpoint explicitly. The zero value resolves
// panewire's svc-panewire hk credential (internal/hkcred).
type CatalogClientConfig struct {
	URL, Token, CFClientID, CFClientSecret string
	CacheDir                               string
	Now                                    func() time.Time
	Client                                 *http.Client
	// Freshness is pacelever's [bench] catalog freshness; the zero value is
	// pacelever's defaults, which are panewire's own.
	Freshness CatalogFreshness
}

// CatalogFreshness is how long a cached catalog is used without asking
// handoffkeep (TTLS) and how old a last-good cache may be before it is stale
// (StaleMaxS), in seconds: pacelever's [bench] catalog_ttl_s and
// catalog_stale_max_s (v0.3.28 bench.go ReadRows). Zero or less is the
// default, one hour and one day.
type CatalogFreshness struct {
	TTLS, StaleMaxS float64
}

func (f CatalogFreshness) ttl() float64 {
	if f.TTLS <= 0 {
		return catalogTTL.Seconds()
	}
	return f.TTLS
}

func (f CatalogFreshness) staleMax() float64 {
	if f.StaleMaxS <= 0 {
		return catalogStaleMax.Seconds()
	}
	return f.StaleMaxS
}

// CatalogClient reads and writes the handoffkeep bench catalog. Redirects are
// refused so authentication never follows another origin, and errors never
// echo the endpoint or credential.
type CatalogClient struct {
	base, token, cfID, cfSecret string
	cacheDir                    string
	now                         func() time.Time
	client                      *http.Client
	freshness                   CatalogFreshness
}

// NewCatalogClient resolves credentials and builds the catalog client. An
// explicit URL and token take precedence; otherwise panewire's svc-panewire
// credential applies (internal/hkcred): HANDOFFKEEP_URL and
// HANDOFFKEEP_TOKEN_SVC_PANEWIRE in the environment, else the mode-0600
// ~/.config/panewire/hk-svc.env (PANEWIRE_HK_ENV overrides the path). An
// unconfigured credential is an *hkcred.UnconfiguredError naming the missing
// keys or file.
func NewCatalogClient(cfg CatalogClientConfig) (*CatalogClient, error) {
	creds := catalogCredentials{URL: cfg.URL, Token: cfg.Token, CFClientID: cfg.CFClientID, CFClientSecret: cfg.CFClientSecret}
	if creds.URL == "" && creds.Token == "" {
		loaded, err := hkcred.Load()
		if err != nil {
			return nil, err
		}
		creds = catalogCredentials{URL: loaded.URL, Token: loaded.Token, CFClientID: loaded.CFID, CFClientSecret: loaded.CFSecret}
	}
	if err := creds.validate(); err != nil {
		return nil, err
	}
	cacheDir := cfg.CacheDir
	if cacheDir == "" {
		cacheDir = filepath.Join(cacheBase(), "panewire", "bench")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &CatalogClient{base: strings.TrimRight(creds.URL, "/"), token: creds.Token, cfID: creds.CFClientID, cfSecret: creds.CFClientSecret, cacheDir: cacheDir, now: now, client: cfg.Client, freshness: cfg.Freshness}, nil
}

type catalogCredentials struct {
	URL, Token, CFClientID, CFClientSecret string
}

func (c catalogCredentials) validate() error {
	if c.URL == "" || c.Token == "" {
		return errors.New("handoffkeep credentials unavailable")
	}
	u, err := url.Parse(c.URL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("invalid catalog endpoint")
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && (isLoopbackHost(u.Hostname()) || isTailnetHost(u.Hostname()))) {
		return errors.New("catalog endpoint requires HTTPS")
	}
	if (c.CFClientID == "") != (c.CFClientSecret == "") {
		return errors.New("CF Access credential pair incomplete")
	}
	return nil
}

func cacheBase() string {
	if v := os.Getenv("XDG_CACHE_HOME"); v != "" {
		return v
	}
	return expand("~/.cache")
}

func (c *CatalogClient) endpointID() string {
	sum := sha256.Sum256([]byte(c.base))
	return hex.EncodeToString(sum[:8])
}

func (c *CatalogClient) cachePath() string {
	return filepath.Join(c.cacheDir, c.endpointID()+"-catalog.json")
}

type catalogCache struct {
	FetchedAt  string            `json:"fetched_at"`
	EndpointID string            `json:"endpoint_id"`
	Rows       []json.RawMessage `json:"rows"`
}

// catalogCacheEntry is this endpoint's cache entry as loadCache reads it.
// dated is false when its fetched_at is missing or unreadable: such an entry
// has no age, so it is never fresh and, served without a refresh, always
// stale — as pacelever reads it fetched at the zero time.
type catalogCacheEntry struct {
	catalogCache
	age   float64
	dated bool
}

// ageS is the entry's age for CatalogInfo, nil when it has none.
func (e catalogCacheEntry) ageS() *float64 {
	if !e.dated {
		return nil
	}
	age := e.age
	return &age
}

// loadCache reads this endpoint's cache entry and its age in seconds. It
// only reads.
func (c *CatalogClient) loadCache() (catalogCacheEntry, bool) {
	var cache catalogCache
	data, err := os.ReadFile(c.cachePath())
	if err != nil || json.Unmarshal(data, &cache) != nil || cache.EndpointID != c.endpointID() || cache.Rows == nil {
		return catalogCacheEntry{}, false
	}
	fetched, err := time.Parse(time.RFC3339Nano, cache.FetchedAt)
	if err != nil {
		return catalogCacheEntry{catalogCache: cache}, true
	}
	return catalogCacheEntry{catalogCache: cache, age: c.now().Sub(fetched).Seconds(), dated: true}, true
}

// fresh reports a cache age inside the TTL.
func (c *CatalogClient) fresh(age float64) bool {
	return age >= 0 && age < c.freshness.ttl()
}

// freshEntry reports an entry that may be used without asking the endpoint:
// a dated one inside the TTL. An undated entry is never fresh, whatever the
// TTL.
func (c *CatalogClient) freshEntry(e catalogCacheEntry) bool {
	return e.dated && c.fresh(e.age)
}

// staleEntry reports an entry served without a refresh as stale: undated, or
// dated outside [0, stale maximum).
func (c *CatalogClient) staleEntry(e catalogCacheEntry) bool {
	return !e.dated || e.age < 0 || e.age >= c.freshness.staleMax()
}

// ReadCached returns the cache entry when it is younger than the TTL, and
// false otherwise. It never asks the endpoint and never writes, so a
// comparison beside another lookup can read the catalog without changing what
// that lookup reads.
func (c *CatalogClient) ReadCached() (CatalogData, bool) {
	entry, ok := c.loadCache()
	if !ok || !c.freshEntry(entry) {
		return CatalogData{}, false
	}
	data, err := c.decode(entry.Rows, catalogSourceCache, false, entry.ageS())
	return data, err == nil
}

// Read returns the catalog. A cache entry younger than the TTL is used
// without a request; otherwise the endpoint is read and the cache replaced.
// When the endpoint fails, a cache entry for the same endpoint is served, and
// one older than the stale maximum is marked stale rather than dropped.
func (c *CatalogClient) Read(ctx context.Context, noCache bool) (CatalogData, error) {
	entry, haveCache := c.loadCache()
	if haveCache && !noCache && c.freshEntry(entry) {
		return c.decode(entry.Rows, catalogSourceCache, false, entry.ageS())
	}
	rows, err := c.fetch(ctx)
	if err == nil {
		cache := catalogCache{FetchedAt: c.now().UTC().Format(time.RFC3339Nano), EndpointID: c.endpointID(), Rows: rows}
		data, marshalErr := json.Marshal(cache)
		if marshalErr != nil {
			return CatalogData{}, errors.New("cannot persist bench cache")
		}
		if mkErr := os.MkdirAll(filepath.Dir(c.cachePath()), 0700); mkErr != nil {
			return CatalogData{}, errors.New("cannot persist bench cache")
		}
		if writeErr := durable.ReplaceFileAtomically(c.cachePath(), ".bench-catalog-*", data, 0600); writeErr != nil {
			return CatalogData{}, errors.New("cannot persist bench cache")
		}
		zero := 0.0
		return c.decode(rows, catalogSourceServer, false, &zero)
	}
	if haveCache {
		source, stale := catalogSourceCache, false
		if c.staleEntry(entry) {
			source, stale = catalogSourceCacheStale, true
		}
		if data, decodeErr := c.decode(entry.Rows, source, stale, entry.ageS()); decodeErr == nil {
			return data, nil
		}
	}
	return CatalogData{}, err
}

func (c *CatalogClient) decode(raw []json.RawMessage, source string, stale bool, age *float64) (CatalogData, error) {
	rows, warnings, keep, err := decodeCatalogRows(raw)
	if err != nil {
		return CatalogData{}, err
	}
	selected := make([]json.RawMessage, len(keep))
	for i, index := range keep {
		selected[i] = raw[index]
	}
	return CatalogData{Rows: rows, Raw: selected, Info: CatalogInfo{Source: source, Stale: stale, AgeS: age}, Warnings: warnings}, nil
}

func (c *CatalogClient) fetch(ctx context.Context) ([]json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, catalogTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/bench/catalog?include_retired=1", nil)
	if err != nil {
		return nil, errors.New("invalid catalog request")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "panewire")
	if c.cfID != "" {
		req.Header.Set("CF-Access-Client-Id", c.cfID)
		req.Header.Set("CF-Access-Client-Secret", c.cfSecret)
	}
	client := c.client
	if client == nil {
		client = &http.Client{Timeout: catalogTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New("handoffkeep request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &BackendError{Status: resp.StatusCode}
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, catalogMaxBody+1))
	if err != nil || len(data) > catalogMaxBody {
		return nil, errors.New("handoffkeep response is not valid JSON")
	}
	return decodeCatalogEnvelope(data)
}

func decodeCatalogEnvelope(data []byte) ([]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var envelope struct {
		Catalog json.RawMessage `json:"catalog"`
	}
	if decoder.Decode(&envelope) != nil || len(envelope.Catalog) == 0 {
		return nil, errors.New("invalid bench catalog JSON")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return nil, errors.New("invalid bench catalog JSON")
	}
	var rows []json.RawMessage
	if json.Unmarshal(envelope.Catalog, &rows) != nil || rows == nil {
		return nil, errors.New("invalid bench catalog JSON")
	}
	return rows, nil
}

// decodeCatalogRows applies pacelever's catalog validation: rows without a
// profile, model, known grade or known gate are skipped with a warning; a row
// in a pool panewire does not serve is ignored with a warning; rungs are
// unique by profile@effort; and a profile's grades must not improve as effort
// rises.
func decodeCatalogRows(raw []json.RawMessage) ([]CatalogEntry, []string, []int, error) {
	if len(raw) == 0 {
		return nil, nil, nil, errors.New("empty bench catalog")
	}
	seen := map[string]bool{}
	var rows []CatalogEntry
	var warnings []string
	var keep []int
	for i, item := range raw {
		var row CatalogEntry
		if json.Unmarshal(item, &row) != nil {
			return nil, nil, nil, errors.New("invalid catalog row types")
		}
		if row.Profile == "" || row.ModelID == "" || !validCatalogGrade(row.Grade) || (row.Gate != "default" && row.Gate != "escalation" && row.Gate != "consult_only") {
			warnings = append(warnings, fmt.Sprintf("catalog_row_invalid:%s@%s", row.Profile, row.Effort))
			continue
		}
		if !catalogPool(row.Pool) {
			warnings = append(warnings, fmt.Sprintf("unknown pool [%s] ignored", row.Pool))
			continue
		}
		key := row.Profile + "@" + row.Effort
		if seen[key] {
			return nil, nil, nil, errors.New("duplicate catalog rung")
		}
		seen[key] = true
		if row.Pool == "agy" && row.Group == "" {
			if group := agy.Group(row.ModelID, ""); group != agy.GroupUnknown {
				row.Group = group
			}
		}
		rows = append(rows, row)
		keep = append(keep, i)
	}
	if len(rows) == 0 {
		return nil, nil, nil, errors.New("catalog contains no supported pool rows")
	}
	if err := validateCatalogMonotonic(rows); err != nil {
		return nil, nil, nil, err
	}
	return rows, warnings, keep, nil
}

// catalogPool is the set of pools pacelever's catalog accepts: the probed
// providers plus the two prepaid credit pools.
func catalogPool(pool string) bool {
	return slices.Contains(admissionPolicyPools, pool)
}

var catalogEfforts = []string{"", "off", "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"}

func catalogEffortRank(effort string) int {
	if i := slices.Index(catalogEfforts, effort); i >= 0 {
		return i
	}
	return len(catalogEfforts)
}

func catalogGradeRank(grade string) int {
	if rank, ok := gradeOrder[grade]; ok {
		return rank
	}
	return len(gradeOrder)
}

// validateCatalogMonotonic refuses a catalog whose higher-effort rung carries
// a better grade than a lower-effort rung of the same profile. Retired rungs,
// rungless rows and unrated rows do not participate.
func validateCatalogMonotonic(rows []CatalogEntry) error {
	byProfile := map[string][]CatalogEntry{}
	for _, row := range rows {
		if row.RetiredAt == nil && row.Effort != "" && row.Grade != gradeUnrated {
			byProfile[row.Profile] = append(byProfile[row.Profile], row)
		}
	}
	profiles := make([]string, 0, len(byProfile))
	for profile := range byProfile {
		profiles = append(profiles, profile)
	}
	sort.Strings(profiles)
	for _, profile := range profiles {
		rungs := byProfile[profile]
		sort.SliceStable(rungs, func(i, j int) bool { return catalogEffortRank(rungs[i].Effort) < catalogEffortRank(rungs[j].Effort) })
		for i := 1; i < len(rungs); i++ {
			if catalogGradeRank(rungs[i].Grade) < catalogGradeRank(rungs[i-1].Grade) {
				return fmt.Errorf("catalog effort grades are not monotonic for %s", profile)
			}
		}
	}
	return nil
}

// CatalogWire is the row shape handoffkeep accepts. Local fields (placement,
// billing, family, group) never leave the caller, exactly as in pacelever.
func CatalogWire(rows []CatalogEntry) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		wire := map[string]any{
			"profile": row.Profile, "effort": row.Effort, "model_id": row.ModelID, "pool": row.Pool,
			"grade": row.Grade, "score": row.Score, "gate": row.Gate, "gate_reason": row.GateReason,
			"boundary_version": row.BoundaryVersion, "deviation_ref": row.DeviationRef,
			"decided_at": row.DecidedAt, "decided_by": row.DecidedBy, "retired_at": row.RetiredAt,
		}
		if row.BenchmarkSource != "" {
			wire["benchmark_source"] = row.BenchmarkSource
		}
		if row.BenchmarkAnnotation != "" {
			wire["benchmark_annotation"] = row.BenchmarkAnnotation
		}
		out = append(out, wire)
	}
	return out
}

// ValidateCatalog checks a dry-run artifact and a catalog write: a non-empty
// catalog, a known grade for every row, operator provenance and a monotonic
// grade ladder per profile.
func ValidateCatalog(rows []CatalogEntry) error {
	if len(rows) == 0 {
		return errors.New("non-empty catalog required")
	}
	if err := validateCatalogMonotonic(rows); err != nil {
		return err
	}
	for _, row := range rows {
		if !validCatalogGrade(row.Grade) {
			return fmt.Errorf("invalid catalog grade %q", row.Grade)
		}
		if row.DecidedBy == "" || row.DeviationRef == "" {
			return errors.New("catalog provenance required")
		}
		if _, err := time.Parse(time.RFC3339Nano, row.DecidedAt); err != nil {
			return errors.New("catalog decided_at must include timezone")
		}
	}
	return nil
}

// PushCatalog writes the catalog through handoffkeep's operator endpoint.
func (c *CatalogClient) PushCatalog(ctx context.Context, rows []CatalogEntry) error {
	if err := ValidateCatalog(rows); err != nil {
		return err
	}
	return c.request(ctx, http.MethodPut, "/v1/bench/catalog", map[string]any{"catalog": CatalogWire(rows)}, nil)
}

func (c *CatalogClient) request(ctx context.Context, method, path string, body, out any) error {
	ctx, cancel := context.WithTimeout(ctx, catalogTimeout)
	defer cancel()
	data, err := json.Marshal(body)
	if err != nil {
		return errors.New("invalid request JSON")
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(data))
	if err != nil {
		return errors.New("invalid catalog request")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "panewire")
	if c.cfID != "" {
		req.Header.Set("CF-Access-Client-Id", c.cfID)
		req.Header.Set("CF-Access-Client-Secret", c.cfSecret)
	}
	client := c.client
	if client == nil {
		client = &http.Client{Timeout: catalogTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("handoffkeep request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &BackendError{Status: resp.StatusCode}
	}
	if out != nil {
		if err := json.NewDecoder(io.LimitReader(resp.Body, catalogMaxBody)).Decode(out); err != nil {
			return errors.New("handoffkeep response is not valid JSON")
		}
	}
	return nil
}

package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	neturl "net/url"
	"slices"
	"strings"
	"time"
)

// Pool concurrency cap (docs/config.md `[quota].pool_max_concurrent`). A pool
// with a cap admits a candidate only while fewer live spawns than the cap run
// on it fleet-wide. A candidate over the cap is dropped `pool_at_cap` and
// selection moves on; it never waits for a slot.
//
// The reserved key `devin-free` caps only the SWE-2 free rungs
// (devinFreeRungs). They draw on Devin's free-model quota while the paid devin
// rungs draw on the account's paid window, so the two quotas are capped apart
// even though every rung shares the catalog pool `devin`.

const (
	reasonPoolAtCap = "pool_at_cap"
	// devinFreeCapKey is the reserved pool_max_concurrent key for the SWE-2
	// free rungs.
	devinFreeCapKey = "devin-free"
	// degradedPoolCapCount marks a selection that could not read the live
	// count: the cap did not apply to it (fail-open).
	degradedPoolCapCount = "pool_cap_count_unavailable"
	poolCapReadTimeout   = 5 * time.Second
)

// validatePoolMaxConcurrent checks `[quota].pool_max_concurrent`: a table of
// pool name to a positive integer.
func validatePoolMaxConcurrent(quota map[string]any) error {
	value, ok := quota["pool_max_concurrent"]
	if !ok {
		return nil
	}
	table, isTable := value.(map[string]any)
	if !isTable {
		return fail(exitConfig, "quota.pool_max_concurrent must map pool names to positive integers")
	}
	for pool, limit := range table {
		if n, isInt := limit.(int64); !validID(pool) || !isInt || n < 1 {
			return fail(exitConfig, "quota.pool_max_concurrent.%s must be a positive integer", pool)
		}
	}
	return nil
}

// poolMaxConcurrent reads the caps. The config was validated at load; an entry
// that is not a positive integer reads as no cap.
func poolMaxConcurrent(quota map[string]any) map[string]int {
	table, _ := quota["pool_max_concurrent"].(map[string]any)
	caps := make(map[string]int, len(table))
	for pool, limit := range table {
		if n, ok := limit.(int64); ok && n >= 1 {
			caps[pool] = int(n)
		}
	}
	return caps
}

// liveSpawnsReply is the body of the hub's GET /v1/spawns/live: live hub
// spawns by the profile each was placed on.
type liveSpawnsReply struct {
	Profiles map[string]int `json:"profiles"`
}

// liveSpawnProfiles counts live spawns by placed profile. The hub's own App
// reads its operations directly; any other App reads them from the hub.
func (a *App) liveSpawnProfiles(ctx context.Context) (map[string]int, error) {
	if a.LiveSpawnProfiles != nil {
		return a.LiveSpawnProfiles(ctx, a.RequestID)
	}
	c, err := newHubHTTP(a.Config.Hub)
	if err != nil {
		return nil, err
	}
	path := "/v1/spawns/live"
	if a.RequestID != "" {
		path += "?except=" + neturl.QueryEscape(a.RequestID)
	}
	status, data, err := c.do(ctx, http.MethodGet, path, nil, poolCapReadTimeout)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, classify(status, data)
	}
	var reply liveSpawnsReply
	if json.Unmarshal(data, &reply) != nil || reply.Profiles == nil {
		return nil, &HubError{Kind: hubDown, Status: status}
	}
	return reply.Profiles, nil
}

// poolCapGate answers "is this candidate over a cap" for one selection. The
// live count is read once, and only when a candidate reaches a capped key.
type poolCapGate struct {
	a           *App
	caps        map[string]int
	rows        []CatalogRow
	loaded      bool
	live        map[string]int // by pool
	freeLive    int            // SWE-2 free rungs (devinFreeCapKey)
	unavailable bool
}

func (a *App) newPoolCapGate(rows []CatalogRow) *poolCapGate {
	return &poolCapGate{a: a, caps: poolMaxConcurrent(a.Config.Quota), rows: rows}
}

// full names the cap this candidate is over: its pool's cap, then the reserved
// devin-free cap for a SWE-2 free rung. It returns "" when no cap applies, so
// a free rung must pass both keys. A key without a cap, or a count that cannot
// be read, is never full.
func (g *poolCapGate) full(ctx context.Context, row CatalogRow) string {
	keys := []string{row.Pool}
	if _, free := devinFreeRungs[row.Profile]; free {
		keys = append(keys, devinFreeCapKey)
	}
	for _, key := range keys {
		limit, capped := g.caps[key]
		if !capped {
			continue
		}
		if !g.loaded {
			g.load(ctx)
		}
		if !g.unavailable && g.count(key) >= limit {
			return key
		}
	}
	return ""
}

func (g *poolCapGate) count(key string) int {
	if key == devinFreeCapKey {
		return g.freeLive
	}
	return g.live[key]
}

func (g *poolCapGate) load(ctx context.Context) {
	g.loaded = true
	ctx, cancel := context.WithTimeout(ctx, poolCapReadTimeout)
	defer cancel()
	profiles, err := g.a.liveSpawnProfiles(ctx)
	if err != nil {
		g.unavailable = true
		g.a.warn("%s: %v; the pool cap does not apply to this selection", degradedPoolCapCount, err)
		return
	}
	g.live = g.byPool(profiles)
	g.freeLive = g.freeRungSpawns(profiles)
}

// canon maps a placed profile (a seat name or a catalog profile) to the
// catalog profile it runs.
func (g *poolCapGate) canon(profile string) string {
	if seat, ok := g.a.Policy.Profiles[profile]; ok && seat.Canon != "" {
		return seat.Canon
	}
	return profile
}

// byPool folds live spawns by placed profile (a seat name or a catalog
// profile) into live spawns by catalog pool.
func (g *poolCapGate) byPool(profiles map[string]int) map[string]int {
	poolOf := map[string]string{}
	for _, row := range g.rows {
		if row.Profile != "" && row.Pool != "" {
			poolOf[row.Profile] = row.Pool
		}
	}
	live := map[string]int{}
	for _, profile := range slices.Sorted(maps.Keys(profiles)) {
		if pool, ok := poolOf[g.canon(profile)]; ok {
			live[pool] += profiles[profile]
		}
	}
	return live
}

// freeRungSpawns counts live spawns of the SWE-2 free rungs. A placed seat
// counts under the catalog profile it runs, so an alias is counted once.
func (g *poolCapGate) freeRungSpawns(profiles map[string]int) int {
	live := 0
	for _, profile := range slices.Sorted(maps.Keys(profiles)) {
		if _, free := devinFreeRungs[g.canon(profile)]; free {
			live += profiles[profile]
		}
	}
	return live
}

// degraded names what the selection record reports when the count was needed
// and unreadable.
func (g *poolCapGate) degraded() []string {
	if g.unavailable {
		return []string{degradedPoolCapCount}
	}
	return nil
}

// poolAtCapNote is the refusal suffix for a selection that dropped a candidate
// for pool_at_cap: it names the caps (a pool, or the reserved devin-free key)
// and their limits.
func (p *selPlan) poolAtCapNote() string {
	var caps []string
	for _, c := range p.cands {
		if c.reason == reasonPoolAtCap && c.cap != "" && !slices.Contains(caps, c.cap) {
			caps = append(caps, c.cap)
		}
	}
	if len(caps) == 0 {
		return ""
	}
	slices.Sort(caps)
	for i, key := range caps {
		caps[i] = fmt.Sprintf("%s=%d", key, p.poolCaps[key])
	}
	return "; " + reasonPoolAtCap + ": " + strings.Join(caps, ", ") + " at [quota].pool_max_concurrent"
}

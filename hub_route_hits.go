package panewire

import (
	"net/http"
	"sort"
	"sync"
	"time"
)

// The hub has no access log, so nothing could show that a route is unused. These
// counters give the next audit that evidence: once an hour the hub logs each
// route's hits since the last line. Only the mux pattern is recorded, never a
// path value, a query or a header. See docs/hub-r6.md.

const hubRouteHitsInterval = time.Hour

type hubRouteHits struct {
	mu     sync.Mutex
	counts map[string]uint64
	logged time.Time
}

func (hits *hubRouteHits) add(pattern string) {
	if pattern == "" {
		return // No route matched; nothing here is a route to keep or remove.
	}
	hits.mu.Lock()
	defer hits.mu.Unlock()
	if hits.counts == nil {
		hits.counts = make(map[string]uint64)
	}
	hits.counts[pattern]++
}

// drain answers the counts when an interval has passed since the last drain, and
// starts the next interval. The first call only starts the clock.
func (hits *hubRouteHits) drain(now time.Time) map[string]uint64 {
	hits.mu.Lock()
	defer hits.mu.Unlock()
	if hits.logged.IsZero() {
		hits.logged = now
		return nil
	}
	if now.Sub(hits.logged) < hubRouteHitsInterval {
		return nil
	}
	drained := hits.counts
	hits.counts, hits.logged = nil, now
	return drained
}

// countRoutes wraps the hub's mux. A request counts on arrival, not on return:
// a websocket's handler runs for as long as it stays connected, so a subscriber
// would otherwise show no use at all.
func (h *HubServer) countRoutes(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, pattern := mux.Handler(request)
		h.routeHits.add(pattern)
		mux.ServeHTTP(writer, request)
	})
}

func (h *HubServer) logRouteHits(now time.Time) {
	drained := h.routeHits.drain(now)
	patterns := make([]string, 0, len(drained))
	for pattern := range drained {
		patterns = append(patterns, pattern)
	}
	sort.Strings(patterns)
	for _, pattern := range patterns {
		h.logger.Info("hub route hits", "route", pattern, "count", drained[pattern], "interval", hubRouteHitsInterval.String())
	}
}

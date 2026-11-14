package panewire

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
)

// t4403HK answers hk's cursor list reads the way hk pages them: rows with an
// id above after_id, in id order, at most limit of them. stall ignores
// after_id and keeps answering the first page, which is a cursor that never
// advances.
type t4403HK struct {
	ids      []int64
	stall    bool
	requests atomic.Int64
}

func (f *t4403HK) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.requests.Add(1)
	q := r.URL.Query()
	after, _ := strconv.ParseInt(q.Get("after_id"), 10, 64)
	if f.stall {
		after = 0
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	var page []int64
	for _, id := range f.ids {
		if id > after && len(page) < limit {
			page = append(page, id)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/v1/tasks":
		// awaiting_decision on every tenth task, so the count must skip them.
		tasks := make([]map[string]any, 0, len(page))
		for _, id := range page {
			tasks = append(tasks, map[string]any{"id": id, "awaiting_decision": id%10 == 0})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"project": q.Get("project"), "tasks": tasks})
	case "/v1/relay/events":
		events := make([]map[string]any, 0, len(page))
		for _, id := range page {
			events = append(events, map[string]any{"id": id, "kind": "lane.event", "job_id": "job-" + strconv.FormatInt(id, 10), "owner_lane": "lane-a", "event_id": "evt-" + strconv.FormatInt(id, 10)})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"events": events})
	default:
		http.NotFound(w, r)
	}
}

func t4403Rows(n int64) []int64 {
	ids := make([]int64, 0, n)
	for id := int64(1); id <= n; id++ {
		ids = append(ids, id)
	}
	return ids
}

func t4403Client(t *testing.T, fake *t4403HK) *HubServer {
	t.Helper()
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	client, err := newHandoffkeepRelayClient(hubHandoffkeepEnv{URL: server.URL, Token: "fixture-token"}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return &HubServer{handoffkeep: client}
}

// Three task pages (1000, 1000, 500 rows): every page is read, and the 250
// awaiting-decision tasks are left out of the count.
func TestHKTaskPagingReadsEveryPage_t4403(t *testing.T) {
	fake := &t4403HK{ids: t4403Rows(2500)}
	h := t4403Client(t, fake)
	count, err := h.drainWakeStateCount(context.Background(), "panewire", "backlog")
	if err != nil {
		t.Fatalf("drainWakeStateCount: %v", err)
	}
	if want := 2250; count != want {
		t.Fatalf("count = %d, want %d", count, want)
	}
	if got := fake.requests.Load(); got != 3 {
		t.Fatalf("requests = %d, want 3 (one per page)", got)
	}
}

// A cursor that repeats its first page must fail the read after one more
// request, not loop and not return a partial count.
func TestHKTaskPagingStopsOnStalledCursor_t4403(t *testing.T) {
	fake := &t4403HK{ids: t4403Rows(2500), stall: true}
	h := t4403Client(t, fake)
	if _, err := h.drainWakeStateCount(context.Background(), "panewire", "backlog"); err == nil {
		t.Fatal("drainWakeStateCount returned no error for a stalled cursor")
	}
	if got := fake.requests.Load(); got != 2 {
		t.Fatalf("requests = %d, want 2 (first page, then the repeat that proves the stall)", got)
	}
}

// The target sits on the third 200-row page, so the lookup only finds it if it
// reads every page.
func TestHKRelayEventPagingFindsRowOnLastPage_t4403(t *testing.T) {
	fake := &t4403HK{ids: t4403Rows(450)}
	h := t4403Client(t, fake)
	record, found, err := h.relayEventByTransportID(context.Background(), "job-450")
	if err != nil {
		t.Fatalf("relayEventByTransportID: %v", err)
	}
	if !found || record.ID != 450 {
		t.Fatalf("found=%v id=%d, want the row with id 450", found, record.ID)
	}
	if got := fake.requests.Load(); got != 3 {
		t.Fatalf("requests = %d, want 3 (one per page)", got)
	}
}

// The target is absent and the cursor stalls: the answer is an error, never a
// clean not-found, because a not-found would tell the caller there is no row.
func TestHKRelayEventPagingStopsOnStalledCursor_t4403(t *testing.T) {
	fake := &t4403HK{ids: t4403Rows(450), stall: true}
	h := t4403Client(t, fake)
	_, found, err := h.relayEventByTransportID(context.Background(), "job-999")
	if err == nil {
		t.Fatal("relayEventByTransportID returned no error for a stalled cursor")
	}
	if found {
		t.Fatal("relayEventByTransportID reported a row for a stalled cursor")
	}
	if got := fake.requests.Load(); got != 2 {
		t.Fatalf("requests = %d, want 2", got)
	}
}

package panewire

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeHerdrPaneServer answers herdr's NDJSON protocol on a unix socket and
// records every pane.report_metadata request. reply returns the response
// member after "id", either `"result":{...}` or `"error":{...}`.
type fakeHerdrPaneServer struct {
	socket  string
	reply   func(params map[string]any) string
	mu      sync.Mutex
	reports []map[string]any
}

func okPaneReply(map[string]any) string { return `"result":{"type":"ok"}` }

func startFakeHerdrPaneServer(t *testing.T, reply func(params map[string]any) string) *fakeHerdrPaneServer {
	t.Helper()
	// A short root keeps the socket path under the unix socket length limit.
	root, err := os.MkdirTemp("/tmp", "pw-pm-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	listener, err := net.Listen("unix", filepath.Join(root, "herdr.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	server := &fakeHerdrPaneServer{socket: filepath.Join(root, "herdr.sock"), reply: reply}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.serve(conn)
		}
	}()
	return server
}

func (s *fakeHerdrPaneServer) serve(conn net.Conn) {
	defer conn.Close()
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return
	}
	var req struct {
		ID     string         `json:"id"`
		Method string         `json:"method"`
		Params map[string]any `json:"params"`
	}
	if json.Unmarshal(line, &req) != nil {
		return
	}
	if req.Method == "pane.report_metadata" {
		s.mu.Lock()
		s.reports = append(s.reports, req.Params)
		s.mu.Unlock()
	}
	body := s.reply(req.Params)
	if body == "" {
		return // no reply: the client sees the connection close
	}
	_, _ = fmt.Fprintf(conn, "{\"id\":%q,%s}\n", req.ID, body)
}

func (s *fakeHerdrPaneServer) reportsCopy() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]map[string]any(nil), s.reports...)
}

// latestReport returns the most recent report for pane, if any.
func (s *fakeHerdrPaneServer) latestReport(pane string) (map[string]any, bool) {
	reports := s.reportsCopy()
	for i := len(reports) - 1; i >= 0; i-- {
		if reports[i]["pane_id"] == pane {
			return reports[i], true
		}
	}
	return nil, false
}

func newPaneMetaStore(t *testing.T) *Store {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), "panewire.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func paneJobRow(jobID, pane, owner string, spawned time.Time) stallJobRow {
	return stallJobRow{
		JobID:       jobID,
		Attempt:     1,
		OwnerLane:   owner,
		ParentLane:  "parent-lane",
		PaneID:      pane,
		ClaimedAt:   spawned,
		SpawnedAt:   spawned,
		LastEventAt: spawned,
	}
}

func seedPaneJob(t *testing.T, store *Store, job stallJobRow) {
	t.Helper()
	if err := store.upsertStallJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
}

func newTestPaneMetadataPublisher(store *Store, socket string) *paneMetadataPublisher {
	return newPaneMetadataPublisher(store, socket, slog.New(slog.DiscardHandler))
}

func TestPaneMetadataPublishesJobTokensWithSourceAndTTL(t *testing.T) {
	store := newPaneMetaStore(t)
	server := startFakeHerdrPaneServer(t, okPaneReply)
	seedPaneJob(t, store, paneJobRow("job-1", "pane-1", "lane-owner", time.Now()))

	newTestPaneMetadataPublisher(store, server.socket).cycle(context.Background())

	reports := server.reportsCopy()
	if len(reports) != 1 {
		t.Fatalf("reports=%d, want 1: %v", len(reports), reports)
	}
	report := reports[0]
	if report["source"] != "panewire" || report["pane_id"] != "pane-1" {
		t.Fatalf("source/pane = %v/%v, want panewire/pane-1", report["source"], report["pane_id"])
	}
	if ttl, _ := report["ttl_ms"].(float64); ttl != float64(paneMetadataTTL.Milliseconds()) {
		t.Fatalf("ttl_ms = %v, want %d", report["ttl_ms"], paneMetadataTTL.Milliseconds())
	}
	if seq, _ := report["seq"].(float64); seq <= 0 {
		t.Fatalf("seq = %v, want a positive sequence", report["seq"])
	}
	// Exactly the pw_ keys panewire owns, and no top-level field beyond the
	// five the publisher sets (no title, display_agent, or clear_* fields).
	if want := map[string]any{"pw_job": "job-1", "pw_owner": "lane-owner"}; !reflect.DeepEqual(report["tokens"], want) {
		t.Fatalf("tokens = %v, want %v", report["tokens"], want)
	}
	if len(report) != 5 {
		t.Fatalf("report has %d fields, want 5: %v", len(report), report)
	}
}

func TestPaneMetadataReportsOnChangeAndRefreshesBeforeTTL(t *testing.T) {
	// The refresh must land before herdr expires the keys.
	if paneMetadataRefreshAfter+paneMetadataPublishInterval > paneMetadataTTL {
		t.Fatalf("refresh %v + interval %v exceeds TTL %v", paneMetadataRefreshAfter, paneMetadataPublishInterval, paneMetadataTTL)
	}
	store := newPaneMetaStore(t)
	server := startFakeHerdrPaneServer(t, okPaneReply)
	seedPaneJob(t, store, paneJobRow("job-1", "pane-1", "lane-owner", time.Now()))
	p := newTestPaneMetadataPublisher(store, server.socket)
	base := time.Unix(1_000_000, 0)
	now := base
	p.now = func() time.Time { return now }
	ctx := context.Background()

	p.cycle(ctx)
	now = base.Add(paneMetadataPublishInterval)
	p.cycle(ctx)
	if got := len(server.reportsCopy()); got != 1 {
		t.Fatalf("unchanged pane re-reported before refresh: reports=%d, want 1", got)
	}
	now = base.Add(paneMetadataRefreshAfter)
	p.cycle(ctx)
	now = base.Add(paneMetadataRefreshAfter + paneMetadataPublishInterval)
	p.cycle(ctx)

	reports := server.reportsCopy()
	if len(reports) != 2 {
		t.Fatalf("reports=%d, want 2 (initial + one refresh)", len(reports))
	}
	if reports[1]["seq"].(float64) <= reports[0]["seq"].(float64) {
		t.Fatalf("seq did not advance: %v then %v", reports[0]["seq"], reports[1]["seq"])
	}
}

func TestPaneMetadataHoldAndReleaseRepublishHeldCount(t *testing.T) {
	store := newPaneMetaStore(t)
	server := startFakeHerdrPaneServer(t, okPaneReply)
	seedPaneJob(t, store, paneJobRow("job-1", "pane-1", "lane-owner", time.Now()))
	p := newTestPaneMetadataPublisher(store, server.socket)
	p.cycle(context.Background())
	store.SetRelayHeldObserver(p.Kick)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// Run's ticker is paneMetadataPublishInterval, so a republish within the
	// eventually window comes from the observer's kick, not from a tick.
	go p.Run(ctx)

	held := relayHeld{Pane: "pane-1", Lane: "lane-owner", JobID: "job-1", Text: "held", EventID: 7, HeldSince: time.Now(), MaxWait: time.Minute}
	if _, err := store.InsertRelayHeld(context.Background(), held); err != nil {
		t.Fatal(err)
	}
	heldValue := func() (any, bool) {
		report, ok := server.latestReport("pane-1")
		if !ok {
			return nil, false
		}
		tokens, _ := report["tokens"].(map[string]any)
		value, present := tokens["pw_held"]
		return value, present
	}
	eventually(t, 5*time.Second, "hold republishes pw_held=1", func() bool {
		value, present := heldValue()
		return present && value == "1"
	})

	if _, err := store.DeleteRelayHeld(context.Background(), 7); err != nil {
		t.Fatal(err)
	}
	// A release sends the key as null: herdr clears just that key.
	eventually(t, 5*time.Second, "release clears pw_held", func() bool {
		value, present := heldValue()
		return present && value == nil
	})
}

func TestPaneMetadataTerminalJobClearsItsKeys(t *testing.T) {
	store := newPaneMetaStore(t)
	server := startFakeHerdrPaneServer(t, okPaneReply)
	job := paneJobRow("job-1", "pane-1", "lane-owner", time.Now())
	seedPaneJob(t, store, job)
	p := newTestPaneMetadataPublisher(store, server.socket)
	p.cycle(context.Background())

	job.Terminal = true
	seedPaneJob(t, store, job)
	p.cycle(context.Background())

	reports := server.reportsCopy()
	if len(reports) != 2 {
		t.Fatalf("reports=%d, want 2 (publish then clear)", len(reports))
	}
	clear := reports[1]
	tokens, _ := clear["tokens"].(map[string]any)
	for _, key := range []string{"pw_job", "pw_owner"} {
		if value, present := tokens[key]; !present || value != nil {
			t.Fatalf("clear tokens[%s] = %v (present=%v), want null", key, value, present)
		}
	}
	for key := range tokens {
		if !strings.HasPrefix(key, "pw_") {
			t.Fatalf("token %q is not a panewire key", key)
		}
	}
	if _, hasTTL := clear["ttl_ms"]; hasTTL {
		t.Fatalf("clear carries ttl_ms: %v", clear)
	}
	p.cycle(context.Background())
	if got := len(server.reportsCopy()); got != 2 {
		t.Fatalf("cleared pane re-reported: reports=%d, want 2", got)
	}
}

func TestPaneMetadataStopsAfterHerdrRefusal(t *testing.T) {
	store := newPaneMetaStore(t)
	server := startFakeHerdrPaneServer(t, func(map[string]any) string {
		return `"error":{"code":"invalid_metadata_token","message":"invalid metadata token key: bad key"}`
	})
	seedPaneJob(t, store, paneJobRow("job-1", "pane-1", "lane-owner", time.Now()))
	p := newTestPaneMetadataPublisher(store, server.socket)
	p.cycle(context.Background())
	seedPaneJob(t, store, paneJobRow("job-2", "pane-2", "lane-owner", time.Now()))
	p.cycle(context.Background())

	if got := len(server.reportsCopy()); got != 1 {
		t.Fatalf("reports=%d after refusal, want 1: publishing must stop", got)
	}
	if !p.stopped {
		t.Fatal("publisher not stopped after a refusal")
	}
}

func TestPaneMetadataPaneNotFoundSkipsOnlyThatPane(t *testing.T) {
	store := newPaneMetaStore(t)
	server := startFakeHerdrPaneServer(t, func(params map[string]any) string {
		if params["pane_id"] == "pane-gone" {
			return `"error":{"code":"pane_not_found","message":"pane not found"}`
		}
		return okPaneReply(params)
	})
	seedPaneJob(t, store, paneJobRow("job-gone", "pane-gone", "lane-owner", time.Now()))
	seedPaneJob(t, store, paneJobRow("job-live", "pane-live", "lane-owner", time.Now()))
	p := newTestPaneMetadataPublisher(store, server.socket)

	p.cycle(context.Background())
	if p.stopped {
		t.Fatal("pane_not_found stopped the publisher; it must skip only that pane")
	}
	if _, ok := server.latestReport("pane-live"); !ok {
		t.Fatal("live pane not published after a sibling pane_not_found")
	}
	if _, tracked := p.published["pane-gone"]; tracked {
		t.Fatal("closed pane still tracked as published")
	}
	// The closed pane is retried next cycle; the live pane is not resent.
	p.cycle(context.Background())
	if got := len(server.reportsCopy()); got != 3 {
		t.Fatalf("reports=%d, want 3 (2 first cycle, retry of gone pane)", got)
	}
}

func TestPaneMetadataValueIsCutOnUTF8Boundary(t *testing.T) {
	store := newPaneMetaStore(t)
	server := startFakeHerdrPaneServer(t, okPaneReply)
	// 100 two-byte runes: 200 bytes, bounded to 64 whole runes (128 bytes).
	seedPaneJob(t, store, paneJobRow("job-1", "pane-1", strings.Repeat("é", 100), time.Now()))

	newTestPaneMetadataPublisher(store, server.socket).cycle(context.Background())

	report, ok := server.latestReport("pane-1")
	if !ok {
		t.Fatal("no report")
	}
	tokens, _ := report["tokens"].(map[string]any)
	if got := tokens["pw_owner"]; got != strings.Repeat("é", 64) {
		t.Fatalf("pw_owner = %q (%d bytes), want 64 whole runes", got, len(got.(string)))
	}
}

func TestPaneMetadataCapabilityNeedsMethodAndTokensField(t *testing.T) {
	schema := func(methods, props string) string {
		return `{"protocol":20,"schema_version":1,"schemas":{"request":{"methods":` + methods +
			`,"$defs":{"PaneReportMetadataParams":{"properties":{` + props + `}}}}}}`
	}
	cases := []struct {
		name string
		body string
		want bool
	}{
		{"method and tokens", schema(`["pane.report_metadata"]`, `"pane_id":{},"tokens":{}`), true},
		{"method without tokens field", schema(`["pane.report_metadata"]`, `"pane_id":{}`), false},
		{"tokens without method", schema(`["pane.read"]`, `"tokens":{}`), false},
	}
	for _, tc := range cases {
		guard, err := GuardSchema(strings.NewReader(tc.body))
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if guard.PaneMetadata != tc.want {
			t.Fatalf("%s: PaneMetadata=%v, want %v", tc.name, guard.PaneMetadata, tc.want)
		}
	}
}

func TestPaneMetadataTransportFailureRetriesWithoutStopping(t *testing.T) {
	store := newPaneMetaStore(t)
	var calls atomic.Int32
	server := startFakeHerdrPaneServer(t, func(params map[string]any) string {
		if calls.Add(1) == 1 {
			return "" // first request: the connection drops with no reply
		}
		return okPaneReply(params)
	})
	seedPaneJob(t, store, paneJobRow("job-1", "pane-1", "lane-owner", time.Now()))
	p := newTestPaneMetadataPublisher(store, server.socket)

	p.cycle(context.Background())
	if p.stopped {
		t.Fatal("a dropped connection stopped publishing; it must retry on the next cycle")
	}
	p.cycle(context.Background())
	if _, ok := p.published["pane-1"]; !ok {
		t.Fatal("pane not published after the retry")
	}
	if got := len(server.reportsCopy()); got != 2 {
		t.Fatalf("reports=%d, want 2 (dropped attempt, then retry)", got)
	}
}

func TestPaneMetadataReportIsBoundedWhenHerdrNeverReplies(t *testing.T) {
	saved := paneMetadataCallTimeout
	paneMetadataCallTimeout = 100 * time.Millisecond
	t.Cleanup(func() { paneMetadataCallTimeout = saved })
	release := make(chan struct{})
	server := startFakeHerdrPaneServer(t, func(map[string]any) string {
		<-release // herdr accepts the request and never answers
		return ""
	})
	t.Cleanup(func() { close(release) })
	client, err := NewHerdrClient(server.socket)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	done := make(chan error, 1)
	go func() {
		done <- client.ReportPaneMetadata(context.Background(), paneMetadataReport{
			PaneID: "pane-1", Source: paneMetadataSource,
			Tokens: map[string]any{paneTokenJob: "job-1"}, TTLms: 1000, Seq: 1,
		})
	}()
	// The 10 s bound only fails the test when the call never returns; the
	// passing path returns as soon as the 100 ms call timeout fires.
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a call with no reply returned nil")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ReportPaneMetadata blocked with no caller deadline")
	}
}

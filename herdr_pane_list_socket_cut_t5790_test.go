package panewire

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// socketCutHerdr is a herdr socket double. The first cutCount pane.list replies
// are cut at the 8192 byte boundary and the connection is closed, as herdr cuts
// a large reply; later pane.list calls and every other method are answered whole.
type socketCutHerdr struct {
	path     string
	cutCount int
	// holdOpen keeps a cut connection open after its bytes, so the reader sees a
	// read timeout instead of EOF; release ends the hold at cleanup.
	holdOpen bool
	cutAt    int
	delay    time.Duration
	pauseAt  int
	pause    time.Duration
	cutFrom  int
	holdFor  time.Duration
	// pauseOnly limits the pause to the first pauseOnly pane.list replies.
	pauseOnly int
	release   chan struct{}
	mu        sync.Mutex
	paneList  int
}

// socketCutPaneRow is large enough that its pane.list reply runs past 8192 bytes.
var socketCutPaneRow = map[string]any{
	"pane_id": "w1:p1", "tab_id": "w1:t1", "agent_status": "idle",
	"label": "a", "cwd": strings.Repeat("/w", 6000),
}

func newSocketCutHerdr(t *testing.T, cutCount int) *socketCutHerdr {
	t.Helper()
	return newSocketCutHerdrWith(t, socketCutOpts{cutCount: cutCount, cutAt: 8192})
}

// socketCutOpts configures the herdr double: cutCount pane.list replies are cut
// at cutAt bytes; holdOpen keeps a cut connection open; delay holds every
// pane.list reply back before it is written, as simulated latency.
type socketCutOpts struct {
	cutCount int
	cutAt    int
	holdOpen bool
	delay    time.Duration
	// pauseAt and pause hold every whole pane.list reply back for pause after its
	// first pauseAt bytes, as a stalled reply.
	pauseAt int
	pause   time.Duration
	// cutFrom cuts every pane.list reply from the cutFrom-th on (1-based; 0 = none),
	// in addition to cutCount. holdFor, when no holdOpen, keeps a cut connection
	// silent for holdFor before it closes: a stalled cut that then ends.
	cutFrom   int
	holdFor   time.Duration
	pauseOnly int
}

func newSocketCutHerdrWith(t *testing.T, opts socketCutOpts) *socketCutHerdr {
	t.Helper()
	cutCount, holdOpen := opts.cutCount, opts.holdOpen
	// unix sockets cap sun_path near 104 bytes, so the socket lives in short /tmp.
	dir, err := os.MkdirTemp("/tmp", "t5790")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	listener, err := net.Listen("unix", filepath.Join(dir, "h.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	server := &socketCutHerdr{path: listener.Addr().String(), cutCount: cutCount, holdOpen: holdOpen, cutAt: opts.cutAt, delay: opts.delay, pauseAt: opts.pauseAt, pause: opts.pause, cutFrom: opts.cutFrom, holdFor: opts.holdFor, pauseOnly: opts.pauseOnly, release: make(chan struct{})}
	t.Cleanup(func() { close(server.release) })
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

func (s *socketCutHerdr) serve(conn net.Conn) {
	defer conn.Close()
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return
	}
	var req struct {
		ID     string `json:"id"`
		Method string `json:"method"`
	}
	if json.Unmarshal(line, &req) != nil {
		return
	}
	var result any
	cut, paused := false, false
	switch req.Method {
	case "agent.list":
		result = map[string]any{"agents": []any{}}
	case "tab.list":
		result = map[string]any{"tabs": []any{}}
	case "pane.list":
		s.mu.Lock()
		s.paneList++
		n := s.paneList
		s.mu.Unlock()
		cut = n <= s.cutCount || (s.cutFrom > 0 && n >= s.cutFrom)
		paused = s.pause > 0 && (s.pauseOnly == 0 || n <= s.pauseOnly)
		result = map[string]any{"panes": []any{socketCutPaneRow}}
	default:
		return
	}
	raw, err := json.Marshal(map[string]any{"id": req.ID, "result": result})
	if err != nil {
		return
	}
	if req.Method == "pane.list" && s.delay > 0 {
		// Simulated latency: the reply starts only after delay.
		time.Sleep(s.delay)
	}
	if cut {
		// The reply stops at the boundary with no newline; the connection closes.
		_, _ = conn.Write(raw[:s.cutAt])
		if s.holdOpen {
			<-s.release
		} else if s.holdFor > 0 {
			// Simulated stall: the cut connection stays silent, then closes.
			time.Sleep(s.holdFor)
		}
		return
	}
	whole := append(raw, '\n')
	if paused {
		// Simulated stall: the reply pauses after pauseAt bytes, then completes.
		_, _ = conn.Write(whole[:s.pauseAt])
		time.Sleep(s.pause)
		_, _ = conn.Write(whole[s.pauseAt:])
		return
	}
	_, _ = conn.Write(whole)
}

func (s *socketCutHerdr) paneListCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.paneList
}

// A pane.list reply cut at the socket boundary is read again once, and the
// census gets the full inventory. Without the typed cut error the socket reader
// returns a bare EOF, which is not retried, so paneListOK stays false (fails on base).
func TestFleetCensusSocketPaneListCutRetriedOnce(t *testing.T) {
	server := newSocketCutHerdr(t, 1)
	client, err := NewHerdrClient(server.path)
	if err != nil {
		t.Fatalf("dial stub herdr: %v", err)
	}
	defer func() { _ = client.Close() }()
	view, err := readFleetCensusLocal(t.Context(), client)
	if err != nil {
		t.Fatalf("readFleetCensusLocal: %v", err)
	}
	if got := server.paneListCalls(); got != 2 {
		t.Fatalf("pane.list calls = %d, want the cut reply and one retry", got)
	}
	if !view.paneListOK {
		t.Fatal("pane.list cut on the socket read as unreadable; want the retry to read the full inventory")
	}
	if len(view.panes) != 1 || view.panes[0].PaneID != "w1:p1" {
		t.Fatalf("panes %+v, want the one pane from the retried reply", view.panes)
	}
}

// A pane.list reply still cut after the one retry stays unknown: paneListOK
// is false and no pane is reported absent.
func TestFleetCensusSocketPaneListCutTwiceStaysUnknown(t *testing.T) {
	server := newSocketCutHerdr(t, 2)
	client, err := NewHerdrClient(server.path)
	if err != nil {
		t.Fatalf("dial stub herdr: %v", err)
	}
	defer func() { _ = client.Close() }()
	view, err := readFleetCensusLocal(t.Context(), client)
	if err != nil {
		t.Fatalf("readFleetCensusLocal: %v", err)
	}
	if got := server.paneListCalls(); got != 2 {
		t.Fatalf("pane.list calls = %d, want exactly one retry", got)
	}
	if view.paneListOK || len(view.panes) != 0 {
		t.Fatalf("paneListOK=%v panes=%+v, want an unknown pane set after two cut replies", view.paneListOK, view.panes)
	}
}

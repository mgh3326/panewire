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
	release  chan struct{}
	mu       sync.Mutex
	paneList int
}

// socketCutPaneRow is large enough that its pane.list reply runs past 8192 bytes.
var socketCutPaneRow = map[string]any{
	"pane_id": "w1:p1", "tab_id": "w1:t1", "agent_status": "idle",
	"label": "a", "cwd": strings.Repeat("/w", 6000),
}

func newSocketCutHerdr(t *testing.T, cutCount int) *socketCutHerdr {
	t.Helper()
	return newSocketCutHerdrMode(t, cutCount, false, 8192)
}

// newSocketCutHerdrMode is newSocketCutHerdr, with holdOpen set when a cut
// connection must stay open after its bytes, and cutAt the byte count a cut
// reply stops at.
func newSocketCutHerdrMode(t *testing.T, cutCount int, holdOpen bool, cutAt int) *socketCutHerdr {
	t.Helper()
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
	server := &socketCutHerdr{path: listener.Addr().String(), cutCount: cutCount, holdOpen: holdOpen, cutAt: cutAt, release: make(chan struct{})}
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
	cut := false
	switch req.Method {
	case "agent.list":
		result = map[string]any{"agents": []any{}}
	case "tab.list":
		result = map[string]any{"tabs": []any{}}
	case "pane.list":
		s.mu.Lock()
		s.paneList++
		cut = s.paneList <= s.cutCount
		s.mu.Unlock()
		result = map[string]any{"panes": []any{socketCutPaneRow}}
	default:
		return
	}
	raw, err := json.Marshal(map[string]any{"id": req.ID, "result": result})
	if err != nil {
		return
	}
	if cut {
		// The reply stops at the boundary with no newline; the connection closes.
		_, _ = conn.Write(raw[:s.cutAt])
		if s.holdOpen {
			<-s.release
		}
		return
	}
	_, _ = conn.Write(append(raw, '\n'))
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

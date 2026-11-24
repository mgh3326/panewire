package panewire

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// worktreeListReplyOverPeer runs a worktree.list whose common dir is absent,
// so the node answers invalid_args common_dir_missing without running git, and
// returns the raw node.command.result the hub reads off the socket.
func worktreeListReplyOverPeer(t *testing.T, advertise func(*hubClientConnection)) []byte {
	t.Helper()
	replies := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if _, payload, err := conn.Read(r.Context()); err == nil {
			replies <- payload
		}
	}))
	defer server.Close()
	conn, _, err := websocket.Dial(t.Context(), r6WSURL(server.URL, ""), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	peer := &hubClientConnection{connection: conn}
	advertise(peer)
	args, _ := json.Marshal(map[string]any{"common": filepath.Join(t.TempDir(), "missing")})
	client := &HubClient{}
	client.handleNodeCommand(t.Context(), peer, hubOutboundMessage{Type: "node.command", RequestID: "cmd-1", CommandOp: nodeCommandWorktreeList, CommandArgs: args})
	select {
	case payload := <-replies:
		return payload
	case <-time.After(10 * time.Second):
		t.Fatal("node sent no node.command.result")
	}
	return nil
}

// hk 6061: a hub before #812 refuses a worktree.list invalid_args reply that
// carries detail, so the node sends common_dir_missing only to a hub that
// advertised worktree_list_detail in hub.features. Any other hub gets the
// four-field invalid_args reply it already accepts.
func TestNodeWorktreeListDetailOnlyForAHubThatAdvertisedIt(t *testing.T) {
	for name, test := range map[string]struct {
		advertise func(*hubClientConnection)
		detail    bool
	}{
		"hub did not advertise": {advertise: func(*hubClientConnection) {}},
		"hub advertised":        {advertise: func(peer *hubClientConnection) { peer.acceptsWorktreeListDetail.Store(true) }, detail: true},
	} {
		t.Run(name, func(t *testing.T) {
			payload := worktreeListReplyOverPeer(t, test.advertise)
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(payload, &fields); err != nil {
				t.Fatalf("reply=%s err=%v", payload, err)
			}
			if _, carried := fields["detail"]; carried != test.detail {
				t.Fatalf("reply=%s, want detail present=%v", payload, test.detail)
			}
			want := 4 // type, request_id, op, error: the frame an older hub accepts
			if test.detail {
				want = 5
			}
			if len(fields) != want {
				t.Fatalf("reply has %d fields, want %d: %s", len(fields), want, payload)
			}
			result, ok := parseHubNodeCommandResult(payload)
			if !ok || result.Error != "invalid_args" || result.Op != nodeCommandWorktreeList {
				t.Fatalf("hub refuses reply %s (ok=%v)", payload, ok)
			}
			if test.detail && result.Detail != nodeWorktreeListCommonDirMissing {
				t.Fatalf("detail=%q, want %s", result.Detail, nodeWorktreeListCommonDirMissing)
			}
		})
	}
}

func TestHubFeaturesAdvertiseWorktreeListDetail(t *testing.T) {
	if !slices.Contains(hubFeaturesFrame().Features, hubFeatureWorktreeListDetail) {
		t.Fatalf("hub.features=%v, want %s", hubFeaturesFrame().Features, hubFeatureWorktreeListDetail)
	}
}

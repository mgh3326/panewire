package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"
)

// decisionReply asks the local daemon that owns the delivery journals.
func decisionReply(ctx context.Context, pane, lane string, since time.Time) (bool, error) {
	path := os.Getenv("PANEWIRE_SOCKET")
	if path == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return false, err
		}
		path = filepath.Join(home, "Library", "Application Support", "panewire", "panewire.sock")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	req := map[string]any{"op": "decision.reply", "target": pane, "owner_lane": lane, "since_ms": since.UnixMilli(), "timeout_ms": 2000}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return false, err
	}
	var response struct {
		OK     bool   `json:"ok"`
		Error  string `json:"error"`
		Result bool   `json:"result"`
	}
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		return false, err
	}
	if !response.OK {
		return false, fmt.Errorf("decision reply query: %s", response.Error)
	}
	return response.Result, nil
}

package panewire

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type nodeWorktreeListArgs struct {
	Common string `json:"common"`
	Cursor string `json:"cursor,omitempty"`
	Limit  *int   `json:"limit,omitempty"`
}
type nodeWorktreeStatus struct {
	Dirty     bool   `json:"dirty"`
	Untracked int    `json:"untracked"`
	Ignored   int    `json:"ignored"`
	Error     string `json:"error,omitempty"`
}
type nodeListedWorktree struct {
	Path     string             `json:"path"`
	Branch   string             `json:"branch"`
	Head     string             `json:"head"`
	Detached bool               `json:"detached"`
	Status   nodeWorktreeStatus `json:"status"`
}
type nodeWorktreeListResult struct {
	Worktrees  []nodeListedWorktree `json:"worktrees"`
	NextCursor string               `json:"next_cursor"`
	Total      int                  `json:"total"`
}

func nodeWorktreeListFields(raw json.RawMessage) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return false
	}
	if _, ok := fields["common"]; !ok {
		return false
	}
	for k, v := range fields {
		if (k != "common" && k != "cursor" && k != "limit") || isJSONNull(v) {
			return false
		}
	}
	return true
}

var nodeListCursorKey = func() []byte {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}()

func nodeListCursor(snapshot [32]byte, offset int) string {
	payload := fmt.Sprintf("%x:%d", snapshot, offset)
	mac := hmac.New(sha256.New, nodeListCursorKey)
	mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString([]byte(payload + ":" + fmt.Sprintf("%x", mac.Sum(nil))))
}
func nodeListCursorOffset(cursor string, snapshot [32]byte, total int) (int, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, false
	}
	parts := strings.Split(string(raw), ":")
	if len(parts) != 3 || parts[0] != fmt.Sprintf("%x", snapshot) {
		return 0, false
	}
	n, err := strconv.Atoi(parts[1])
	if err != nil || n <= 0 || n >= total {
		return 0, false
	}
	return n, hmac.Equal([]byte(cursor), []byte(nodeListCursor(snapshot, n)))
}

func nodeWorktreeList(ctx context.Context, git relayCommandRunner, args nodeWorktreeListArgs) (any, string) {
	out, err := git(ctx, "--no-optional-locks", "-C", args.Common, "worktree", "list", "--porcelain", "-z")
	if err != nil {
		return nil, "herdr_failed"
	}
	var rows []nodeListedWorktree
	for _, field := range strings.Split(string(out), "\x00") {
		switch {
		case strings.HasPrefix(field, "worktree "):
			rows = append(rows, nodeListedWorktree{Path: strings.TrimPrefix(field, "worktree ")})
		case len(rows) == 0:
		case strings.HasPrefix(field, "HEAD "):
			rows[len(rows)-1].Head = strings.TrimPrefix(field, "HEAD ")
		case strings.HasPrefix(field, "branch "):
			rows[len(rows)-1].Branch = strings.TrimPrefix(strings.TrimPrefix(field, "branch "), "refs/heads/")
		case field == "detached":
			rows[len(rows)-1].Detached = true
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Path < rows[j].Path })
	snapshotBytes, _ := json.Marshal(struct {
		Common string
		Rows   []nodeListedWorktree
	}{args.Common, rows})
	snapshot := sha256.Sum256(snapshotBytes)
	start := 0
	if args.Cursor != "" {
		var ok bool
		start, ok = nodeListCursorOffset(args.Cursor, snapshot, len(rows))
		if !ok {
			return nil, "invalid_args"
		}
	}
	limit := 50
	if args.Limit != nil {
		limit = *args.Limit
	}
	result := nodeWorktreeListResult{Worktrees: make([]nodeListedWorktree, 0), Total: len(rows)}
	for i := start; i < len(rows) && i < start+limit; i++ {
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < 250*time.Millisecond {
			if len(result.Worktrees) == 0 {
				return nil, "herdr_failed"
			}
			result.NextCursor = nodeListCursor(snapshot, i)
			break
		}
		rows[i].Status = nodeListStatus(ctx, git, rows[i].Path)
		result.Worktrees = append(result.Worktrees, rows[i])
		result.NextCursor = ""
		if i+1 < len(rows) {
			result.NextCursor = nodeListCursor(snapshot, i+1)
		}
		encoded, _ := json.Marshal(result)
		if len(encoded) > nodeCommandMaxResultBytes {
			result.Worktrees = result.Worktrees[:len(result.Worktrees)-1]
			if len(result.Worktrees) == 0 {
				return nil, "result_too_large"
			}
			result.NextCursor = nodeListCursor(snapshot, i)
			break
		}
	}
	return result, ""
}

func nodeListStatus(ctx context.Context, git relayCommandRunner, path string) nodeWorktreeStatus {
	// Refuse symlink components before passing the reported path to Git.
	clean := filepath.Clean(path)
	resolved, err := filepath.EvalSymlinks(clean)
	if err != nil || resolved != clean {
		return nodeWorktreeStatus{Error: "worktree unavailable or symlinked"}
	}
	info, err := os.Stat(clean)
	if err != nil || !info.IsDir() {
		return nodeWorktreeStatus{Error: "worktree unavailable"}
	}
	statusCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	out, err := git(statusCtx, "--no-optional-locks", "-C", clean, "status", "--porcelain=v2", "--ignored", "-z")
	if err != nil {
		return nodeWorktreeStatus{Error: "status unavailable"}
	}
	var status nodeWorktreeStatus
	skipRenamePath := false
	for _, record := range strings.Split(string(out), "\x00") {
		if skipRenamePath {
			skipRenamePath = false
			continue
		}
		if record == "" {
			continue
		}
		switch record[0] {
		case '?':
			status.Untracked++
			status.Dirty = true
		case '!':
			status.Ignored++
		case '1', '2', 'u':
			status.Dirty = true
			if record[0] == '2' {
				skipRenamePath = true
			}
		}
	}
	return status
}

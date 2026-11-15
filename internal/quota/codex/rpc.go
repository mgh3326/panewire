package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/mgh3326/panewire/internal/quota"
)

// The app-server source: spawn `codex -s read-only -a never app-server`,
// handshake initialize/initialized, then call account/rateLimits/read. The
// probe never writes credentials and runs only when a file login is already
// present, so it cannot surface a browser sign-in. Any failure sends the
// caller to the wham usage API.

const rpcTimeout = 15 * time.Second

// BinaryOverrideEnv names the environment override of the codex executable.
const BinaryOverrideEnv = "PANEWIRE_CODEX_BIN"

const legacyBinaryOverrideEnv = "PACELEVER_CODEX_BIN"

// Transport is the stdin/stdout line channel of one app-server process. Tests
// inject a fake so no test needs a real CLI. Close kills and reaps the
// process and may be called more than once.
type Transport interface {
	Send([]byte) error
	Recv() ([]byte, error)
	Close()
}

// binary resolves the codex CLI: the environment override, the configured
// path, PATH, then the desktop app's bundled copy. "" means none.
func (c Config) binary() string {
	explicit, _ := c.lookupEnv(BinaryOverrideEnv)
	if strings.TrimSpace(explicit) == "" {
		explicit, _ = c.lookupEnv(legacyBinaryOverrideEnv)
	}
	if strings.TrimSpace(explicit) != "" {
		return c.expandHome(strings.TrimSpace(explicit))
	}
	if c.Binary != "" {
		return c.expandHome(c.Binary)
	}
	lookPath := c.LookPath
	if lookPath == nil {
		lookPath = exec.LookPath
	}
	if path, err := lookPath("codex"); err == nil {
		return path
	}
	goos := c.GOOS
	if goos == "" {
		goos = runtime.GOOS
	}
	if goos == "darwin" {
		isFile := c.IsFile
		if isFile == nil {
			isFile = regularFile
		}
		for _, path := range []string{
			"/Applications/Codex.app/Contents/Resources/codex",
			filepath.Join(c.home(), "Applications", "Codex.app", "Contents", "Resources", "codex"),
		} {
			if isFile(path) {
				return path
			}
		}
	}
	return ""
}

func (c Config) expandHome(path string) string {
	if rest, ok := strings.CutPrefix(path, "~/"); ok {
		return filepath.Join(c.home(), rest)
	}
	return path
}

func regularFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

type rpcMessage struct {
	ID    json.RawMessage `json:"id"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
	Result json.RawMessage `json:"result"`
}

func rpcSend(t Transport, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return t.Send(b)
}

// rpcCall sends one request and waits for its response, skipping server
// notifications (messages with no id).
func rpcCall(t Transport, id int, method string, params any) (json.RawMessage, error) {
	if err := rpcSend(t, map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}); err != nil {
		return nil, err
	}
	want := strconv.Itoa(id)
	for {
		line, err := t.Recv()
		if err != nil {
			return nil, err
		}
		var msg rpcMessage
		if json.Unmarshal(line, &msg) != nil || string(msg.ID) != want {
			continue
		}
		if msg.Error != nil {
			return nil, fmt.Errorf("rpc: %s", msg.Error.Message)
		}
		return msg.Result, nil
	}
}

type rpcWindow struct {
	UsedPct  *float64 `json:"usedPercent"`
	ResetsAt float64  `json:"resetsAt"`
	Mins     float64  `json:"windowDurationMins"`
}

func (w rpcWindow) bucket(scope quota.Scope) quota.Bucket {
	reset := ""
	if w.ResetsAt > 0 {
		reset = time.Unix(int64(w.ResetsAt), 0).UTC().Format(time.RFC3339Nano)
	}
	return quota.Bucket{Window: windowLabel(w.Mins * 60), UsedPct: validated(w.UsedPct), ResetsAt: reset, Scope: scope}
}

// limitName trims the codex prefix from a per-limit name: "codex_x" becomes
// "X", "codex x" becomes "x".
func limitName(s string) string {
	if rest, ok := cutPrefixFold(s, "codex_"); ok && rest != "" {
		r, size := utf8.DecodeRuneInString(rest)
		return string(unicode.ToUpper(r)) + rest[size:]
	}
	if rest, ok := cutPrefixFold(s, "codex "); ok && rest != "" {
		return rest
	}
	return s
}

// cutPrefixFold is strings.CutPrefix ignoring case. prefix is ASCII.
func cutPrefixFold(s, prefix string) (string, bool) {
	if len(s) < len(prefix) || !strings.EqualFold(s[:len(prefix)], prefix) {
		return s, false
	}
	return s[len(prefix):], true
}

// fetchRPC runs the whole exchange and reports false on any failure so the
// caller falls back to the usage API; it never invents a value. The
// app-server is always closed, on success, failure and timeout.
func (c Config) fetchRPC(ctx context.Context, dir string) (quota.Observation, bool) {
	observed := quota.Observation{Source: quota.SourceUsageRPC}
	binary := c.binary()
	if binary == "" {
		return observed, false
	}
	timeout := c.RPCTimeout
	if timeout <= 0 {
		timeout = rpcTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := c.StartRPC
	if start == nil {
		start = startAppServer
	}
	t, err := start(ctx, binary, dir, dir)
	if err != nil {
		return observed, false
	}
	defer t.Close()
	// A hung server leaves Recv blocked; closing the transport at the
	// deadline kills it and unblocks the read.
	defer context.AfterFunc(ctx, t.Close)()
	if _, err = rpcCall(t, 1, "initialize", map[string]any{"clientInfo": map[string]any{"name": "panewire", "version": "1"}}); err != nil {
		return observed, false
	}
	if err = rpcSend(t, map[string]any{"jsonrpc": "2.0", "method": "initialized"}); err != nil {
		return observed, false
	}
	result, err := rpcCall(t, 2, "account/rateLimits/read", map[string]any{})
	if err != nil {
		return observed, false
	}
	var rl struct {
		RateLimits struct {
			PlanType  string     `json:"planType"`
			Primary   *rpcWindow `json:"primary"`
			Secondary *rpcWindow `json:"secondary"`
		} `json:"rateLimits"`
		ByLimit map[string]struct {
			LimitName string     `json:"limitName"`
			LimitID   string     `json:"limitId"`
			Primary   *rpcWindow `json:"primary"`
			Secondary *rpcWindow `json:"secondary"`
		} `json:"rateLimitsByLimitId"`
	}
	if json.Unmarshal(result, &rl) != nil {
		return observed, false
	}
	observed.Plan = rl.RateLimits.PlanType
	for _, w := range []*rpcWindow{rl.RateLimits.Primary, rl.RateLimits.Secondary} {
		if w != nil {
			observed.Buckets = append(observed.Buckets, w.bucket(quota.Scope{Kind: quota.ScopeAccount}))
		}
	}
	var ids []string
	for id := range rl.ByLimit {
		if id != "codex" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		x := rl.ByLimit[id]
		name := x.LimitName
		if name == "" {
			name = x.LimitID
		}
		if name == "" {
			name = id
		}
		name = limitName(name)
		for _, w := range []*rpcWindow{x.Primary, x.Secondary} {
			if w != nil {
				observed.Buckets = append(observed.Buckets, w.bucket(quota.Scope{Kind: quota.ScopeModel, Name: name}))
			}
		}
	}
	if len(observed.Buckets) == 0 {
		if rl.RateLimits.PlanType != "free" {
			return observed, false
		}
		observed.Buckets = append(observed.Buckets, quota.Bucket{Window: "free", UsedPct: quota.Pct(0), Scope: quota.Scope{Kind: quota.ScopeAccount}})
	}
	// pacelever also asks who is logged in, for an email-based fingerprint. The
	// account_id fingerprint always wins and, without one, the shared key is
	// empty, so the answer is never used here; the call is kept so the server
	// sees the same exchange, and its failure discards nothing already read.
	_, _ = rpcCall(t, 3, "account/read", map[string]any{"refreshToken": false})
	// Windows without a value cannot be published; the usage API gets a turn.
	return observed, hasMeasuredBucket(observed.Buckets)
}

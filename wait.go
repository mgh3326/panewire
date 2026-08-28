package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

const (
	ExitOK                = 0
	ExitUsage             = 2
	ExitTimeout           = 3
	ExitDaemonUnavailable = 4
	ExitConditionInvalid  = 5
	ExitDeliveryFailure   = 6
	ExitInternal          = 70
)

type codedError struct {
	code int
	err  error
}

func (e *codedError) Error() string { return e.err.Error() }
func (e *codedError) Unwrap() error { return e.err }
func ExitCode(err error) int {
	if err == nil {
		return ExitOK
	}
	if e, ok := err.(*codedError); ok {
		return e.code
	}
	return ExitInternal
}
func timeoutError() error { return &codedError{ExitTimeout, fmt.Errorf("timeout")} }

type FileWaitResult struct {
	Path, Digest string
	DigestReads  int
	Size         int64
	Modified     time.Time
}

func WaitFile(ctx context.Context, store *Store, path string, settle time.Duration) (FileWaitResult, error) {
	if settle < 0 {
		return FileWaitResult{}, &codedError{ExitUsage, fmt.Errorf("negative settle")}
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var lastSize int64 = -1
	var lastMod time.Time
	var stable time.Time
	for {
		info, err := os.Stat(path)
		if err == nil && info.Mode().IsRegular() {
			if info.Size() != lastSize || !info.ModTime().Equal(lastMod) {
				lastSize, lastMod, stable = info.Size(), info.ModTime(), time.Now()
			}
			if settle == 0 || time.Since(stable) >= settle {
				f, openErr := os.Open(path)
				if openErr == nil {
					h := sha256.New()
					_, readErr := io.Copy(h, f)
					_ = f.Close()
					if readErr == nil {
						sum := hex.EncodeToString(h.Sum(nil))
						if store != nil {
							_ = store.RecordEvent(ctx, Event{Source: "inbox", Kind: "inbox.file_created", Path: path, Payload: json.RawMessage(fmt.Sprintf(`{"size":%d,"mtime_ms":%d,"sha256":"%s"}`, info.Size(), info.ModTime().UnixMilli(), sum))})
						}
						return FileWaitResult{Path: path, Digest: sum, DigestReads: 1, Size: info.Size(), Modified: info.ModTime()}, nil
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return FileWaitResult{}, timeoutError()
		case <-ticker.C:
		}
	}
}

type AgentWaitResult struct {
	Target, Status string
	SettleResets   int
}

func WaitAgent(ctx context.Context, client *HerdrClient, target, status string, settle, timeout time.Duration) (AgentWaitResult, error) {
	if !validStatus(status) || settle < 0 {
		return AgentWaitResult{}, &codedError{ExitConditionInvalid, fmt.Errorf("invalid agent wait condition")}
	}
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := client.Call(deadlineCtx, "agent.list", map[string]any{})
	if err != nil {
		return AgentWaitResult{}, &codedError{ExitDaemonUnavailable, err}
	}
	current, paneID, ok := snapshotStatus(result, target)
	if !ok {
		return AgentWaitResult{}, &codedError{ExitConditionInvalid, fmt.Errorf("agent target not found: %s", target)}
	}
	events, err := client.Subscribe(deadlineCtx)
	if err != nil {
		return AgentWaitResult{}, &codedError{ExitDaemonUnavailable, err}
	}
	started := time.Time{}
	resets := 0
	if current == status {
		started = time.Now()
	}
	for {
		if !started.IsZero() && time.Since(started) >= settle {
			return AgentWaitResult{target, status, resets}, nil
		}
		select {
		case <-deadlineCtx.Done():
			return AgentWaitResult{}, timeoutError()
		case ev, ok := <-events:
			if !ok {
				return AgentWaitResult{}, &codedError{ExitDaemonUnavailable, fmt.Errorf("herdr event connection closed")}
			}
			if ev.PaneID == paneID && ev.AgentStatus != "" {
				if ev.AgentStatus == status {
					if started.IsZero() {
						started = time.Now()
					}
				} else {
					if !started.IsZero() {
						resets++
					}
					started = time.Time{}
				}
			}
		case <-time.After(minDuration(10*time.Millisecond, settle+time.Millisecond)):
		}
	}
}
func validStatus(s string) bool {
	switch s {
	case "idle", "working", "blocked", "done", "unknown":
		return true
	}
	return false
}
func minDuration(a, b time.Duration) time.Duration {
	if b <= 0 {
		return a
	}
	if a < b {
		return a
	}
	return b
}
func snapshotStatus(raw json.RawMessage, target string) (string, string, bool) {
	var top struct {
		Agents []map[string]any `json:"agents"`
	}
	if json.Unmarshal(raw, &top) != nil {
		return "", "", false
	}
	for _, a := range top.Agents {
		n, _ := a["agent"].(string)
		name, _ := a["name"].(string)
		if n == target || name == target {
			st, _ := a["agent_status"].(string)
			pane, _ := a["pane_id"].(string)
			return st, pane, true
		}
	}
	return "", "", false
}

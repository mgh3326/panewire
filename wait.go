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
	ExitPartial           = 7
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

// WaitAgent resolves target through the same resolver as prompt --to
// (#497), then pins the wait on pane_id + agent_session rather than the
// name: a rename mid-wait cannot sever the observation, while an agent
// leaving the pane or a different session occupying it fails the wait
// instead of silently watching the wrong occupant.
func WaitAgent(ctx context.Context, client *HerdrClient, target, status string, settle, timeout time.Duration) (AgentWaitResult, error) {
	if !validStatus(status) || settle < 0 {
		return AgentWaitResult{}, &codedError{ExitConditionInvalid, fmt.Errorf("invalid agent wait condition")}
	}
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	pane, err := resolveTarget(deadlineCtx, client, target)
	if err != nil {
		return AgentWaitResult{}, err
	}
	if pane.PaneID == "" {
		return AgentWaitResult{}, &codedError{ExitConditionInvalid, fmt.Errorf("agent target %q resolved without pane_id", target)}
	}
	paneID, session := pane.PaneID, pane.AgentSession
	current := pane.Status
	events, _, err := client.Subscribe(deadlineCtx)
	if err != nil {
		return AgentWaitResult{}, &codedError{ExitDaemonUnavailable, err}
	}
	// sameOccupant re-proves the pane still holds the resolved agent instance.
	// A herdr too old to report agent_session can only prove occupancy.
	sameOccupant := func() error {
		occupantSession, occupied, occErr := paneOccupant(deadlineCtx, client, paneID)
		if occErr != nil {
			return &codedError{ExitDaemonUnavailable, occErr}
		}
		if !occupied {
			return &codedError{ExitConditionInvalid, fmt.Errorf("agent target gone: pane %s has no agent", paneID)}
		}
		if session != "" && occupantSession != "" && occupantSession != session {
			return &codedError{ExitConditionInvalid, fmt.Errorf("agent target replaced: pane %s holds a different agent_session", paneID)}
		}
		return nil
	}
	started := time.Time{}
	resets := 0
	if current == status {
		started = time.Now()
	}
	occupantTick := time.NewTicker(time.Second)
	defer occupantTick.Stop()
	for {
		if !started.IsZero() && time.Since(started) >= settle {
			return AgentWaitResult{target, status, resets}, nil
		}
		select {
		case <-deadlineCtx.Done():
			return AgentWaitResult{}, timeoutError()
		case <-occupantTick.C:
			if err := sameOccupant(); err != nil {
				return AgentWaitResult{}, err
			}
		case ev, ok := <-events:
			if !ok {
				return AgentWaitResult{}, &codedError{ExitDaemonUnavailable, fmt.Errorf("herdr event connection closed")}
			}
			if ev.PaneID == paneID && ev.AgentStatus != "" {
				if err := sameOccupant(); err != nil {
					return AgentWaitResult{}, err
				}
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

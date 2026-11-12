package panewire

// hk 4143: Close aborts the inbox store under an in-flight flush. The store
// error that follows is the shutdown, so the flush logs no Error line and
// counts no failure.

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// closeAfterChecks is a close context that reports Close from its (after+1)th
// Err call on. The flush entry check is the first call, so the store error
// that follows it is the one a Close cut.
type closeAfterChecks struct {
	context.Context
	after int
	calls int
}

func (c *closeAfterChecks) Err() error {
	c.calls++
	if c.calls > c.after {
		return context.Canceled
	}
	return nil
}

func TestInboxFlushCutByCloseLogsNoError(t *testing.T) {
	fake, hk, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	_ = fake
	now := time.Now()
	h, _ := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", hk, &now)
	var logs bytes.Buffer
	h.logger = slog.New(slog.NewTextHandler(&logs, nil))
	if _, err := h.inbox.store.db.Exec(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES('b1','lane-a','digest')`); err != nil {
		t.Fatal(err)
	}
	// Close's order: cancel the close context, then abort the store. The store
	// calls of this pass fail; the context reports the cut from the first check
	// after the entry check.
	h.inbox.store.abort()
	h.closeCtx = &closeAfterChecks{Context: h.closeCtx, after: 1}
	h.flushInbox()
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("a flush cut by Close logged an error:\n%s", logs.String())
	}
	if n := h.InboxFlushFailureCount(); n != 0 {
		t.Fatalf("a flush cut by Close counted %d failures, want 0", n)
	}
}

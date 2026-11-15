package panewire

// hk 4143: Close aborts the inbox store under an in-flight flush. The store
// error that follows is the shutdown, so the flush logs no Error line and
// counts no failure.

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
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

// cancelOnDoneContext is live at flushInbox's entry check and canceled when
// pendingBatches derives its store operation context.
type cancelOnDoneContext struct {
	context.Context
	calls int
}

func (c *cancelOnDoneContext) Err() error {
	c.calls++
	if c.calls > 1 {
		return context.Canceled
	}
	return nil
}

func (c *cancelOnDoneContext) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
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

func TestInboxFlushListingErrorDistinguishesClose(t *testing.T) {
	setup := func(t *testing.T, failListing bool) (*HubServer, *bytes.Buffer, func()) {
		_, hk, closeServer := newFakeHandoffkeep(t)
		now := time.Now()
		h, _ := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", hk, &now)
		var logs bytes.Buffer
		h.logger = slog.New(slog.NewTextHandler(&logs, nil))
		if failListing {
			// With no source items, makeBatches completes without reading the
			// batch table; pendingBatches then fails deterministically on SELECT.
			if _, err := h.inbox.store.db.Exec(`DROP TABLE lane_inbox_batches`); err != nil {
				t.Fatal(err)
			}
		}
		return h, &logs, closeServer
	}
	t.Run("close cancellation", func(t *testing.T) {
		h, logs, closeServer := setup(t, false)
		defer closeServer()
		h.closeCtx = &cancelOnDoneContext{Context: context.Background()}
		h.flushInbox()
		if strings.Contains(logs.String(), "level=ERROR") {
			t.Fatalf("listing cut by Close logged an error:\n%s", logs.String())
		}
		if n := h.InboxFlushFailureCount(); n != 0 {
			t.Fatalf("listing cut by Close counted %d failures, want 0", n)
		}
	})
	t.Run("real store error", func(t *testing.T) {
		h, logs, closeServer := setup(t, true)
		defer closeServer()
		h.flushInbox()
		if !strings.Contains(logs.String(), "level=ERROR") || !strings.Contains(logs.String(), "inbox batch listing failed") {
			t.Fatalf("real listing failure did not log an error:\n%s", logs.String())
		}
		if n := h.InboxFlushFailureCount(); n != 1 {
			t.Fatalf("real listing failure counted %d failures, want 1", n)
		}
	})
}

// A handoff that is accepted while Close lands is recorded after the store
// aborts. That recording error is the shutdown, not a flush failure.
func TestInboxFlushHandoffCutByCloseLogsNoError(t *testing.T) {
	fake, hk, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	now := time.Now()
	h, _ := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", hk, &now)
	var logs bytes.Buffer
	h.logger = slog.New(slog.NewTextHandler(&logs, nil))
	if _, err := h.inbox.store.db.Exec(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES('b1','lane-a','digest')`); err != nil {
		t.Fatal(err)
	}
	// Close's order, landing between the accepted handoff and markBatchSent:
	// cancel the close context, then abort the store.
	fake.observe = func(method, _ string) {
		if method == http.MethodPost {
			h.closeCancel()
			h.inbox.store.abort()
		}
	}
	h.flushInbox()
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("a handoff cut by Close logged an error:\n%s", logs.String())
	}
	if n := h.InboxFlushFailureCount(); n != 0 {
		t.Fatalf("a handoff cut by Close counted %d failures, want 0", n)
	}
}

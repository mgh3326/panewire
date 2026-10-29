package panewire

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestQuietLegacyBatchQuarantine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	s, err := openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	// Recreate the round-1 schema and its pending multiline batch.
	if _, err := s.db.Exec(`DROP TABLE lane_inbox_batch_quarantine`); err != nil {
		t.Fatal(err)
	}
	for _, b := range []laneInboxBatch{{"legacy", "lane-a", "old digest\nsecond line"}, {"current", "lane-a", "one current digest"}} {
		if _, err := s.db.Exec(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES(?,?,?)`, b.ID, b.Lane, b.Text); err != nil {
			t.Fatal(err)
		}
	}
	s.db.Close()
	fake, hk, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	now := time.Now()
	h, agent := quietHub(t, path, "default", hk, &now)
	var reason, original string
	if err := h.inbox.store.db.QueryRow(`SELECT reason,text FROM lane_inbox_batch_quarantine JOIN lane_inbox_batches ON batch_id=id WHERE id='legacy'`).Scan(&reason, &original); err != nil || reason == "" || original != "old digest\nsecond line" {
		t.Fatalf("startup quarantine reason=%q original=%q err=%v", reason, original, err)
	}
	h.probeInboxRelayContract(context.Background())
	h.flushInbox()
	if h.inboxDigestDisabled() || len(agent.relays) != 1 {
		t.Fatal("legacy batch blocked the current digest")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.stored) != 1 || fake.stored[0].EventID != "current" {
		t.Fatalf("legacy text reached handoffkeep: %+v", fake.stored)
	}
}

func TestQuietBatchValidationBeforeSend(t *testing.T) {
	fake, hk, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	now := time.Now()
	h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", hk, &now)
	invalid := []string{"two\nlines", "tab\there", "nul\x00here", "delete\x7f", "control\u0085", "", string([]byte{0xff}), strings.Repeat("é", 1025)}
	for i, text := range invalid {
		if _, err := h.inbox.store.db.Exec(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES(?,?,?)`, fmt.Sprint("bad-", i), "lane-a", text); err != nil {
			t.Fatal(err)
		}
	}
	// An exact byte-boundary batch after all invalid batches still sends.
	if _, err := h.inbox.store.db.Exec(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES('valid','lane-a',?)`, strings.Repeat("é", 1024)); err != nil {
		t.Fatal(err)
	}
	h.flushInbox()
	var quarantined int
	if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM lane_inbox_batch_quarantine WHERE reason<>''`).Scan(&quarantined); err != nil || quarantined != len(invalid) {
		t.Fatal(quarantined, err)
	}
	if h.inboxDigestDisabled() || len(agent.relays) != 1 {
		t.Fatal("invalid batch blocked valid batch")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.stored) != 1 || fake.stored[0].EventID != "valid" {
		t.Fatalf("invalid batch sent: %+v", fake.stored)
	}
}

func TestQuietBatchPermanentRejectionQuarantines(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusNotFound, http.StatusUnprocessableEntity} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			fake, hk, closeServer := newFakeHandoffkeep(t)
			defer closeServer()
			now := time.Now()
			path := filepath.Join(t.TempDir(), "inbox.sqlite3")
			h, agent := quietHub(t, path, "default", hk, &now)
			if _, err := h.inbox.store.db.Exec(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES('rejected','lane-a','rejected digest')`); err != nil {
				t.Fatal(err)
			}
			if _, err := h.inbox.store.put("due", "lane-a", "lane.event", "inbox", quietEvent("due", "FYI overdue"), now.Add(-time.Hour), time.Minute, 0); err != nil {
				t.Fatal(err)
			}
			fake.mu.Lock()
			fake.status = status
			// The first POST uses its captured rejection; later POSTs succeed.
			fake.observe = func(method, path string) {
				if method == http.MethodPost && path == "/v1/relay/events" {
					fake.mu.Lock()
					fake.status = http.StatusCreated
					fake.mu.Unlock()
				}
			}
			fake.mu.Unlock()
			h.flushInbox()
			var reason string
			if err := h.inbox.store.db.QueryRow(`SELECT reason FROM lane_inbox_batch_quarantine WHERE batch_id='rejected'`).Scan(&reason); err != nil || !strings.Contains(reason, fmt.Sprint(status)) {
				t.Fatal(reason, err)
			}
			batches, err := h.inbox.store.pendingBatches()
			if err != nil || len(batches) != 0 || h.inboxDigestDisabled() || len(agent.relays) != 1 {
				t.Fatalf("batches=%v disabled=%v injections=%d err=%v", batches, h.inboxDigestDisabled(), len(agent.relays), err)
			}
			h.Close()
			h2, agent2 := quietHub(t, path, "default", hk, &now)
			h2.flushInbox()
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.calls) != 2 || len(fake.stored) != 1 || len(agent2.relays) != 0 {
				t.Fatalf("quarantine retried or escalation blocked: calls=%d stored=%v", len(fake.calls), fake.stored)
			}
		})
	}
}

func TestQuietBatchRetryableRejectionBacksOff(t *testing.T) {
	for _, status := range []int{401, 403, 408, 425, 429, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			fake, hk, closeServer := newFakeHandoffkeep(t)
			defer closeServer()
			now := time.Now()
			h, _ := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", hk, &now)
			if _, err := h.inbox.store.db.Exec(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES('retry','lane-a','retry digest'),('next','lane-a','next digest')`); err != nil {
				t.Fatal(err)
			}
			fake.mu.Lock()
			fake.status = status
			fake.mu.Unlock()
			h.flushInbox()
			h.flushInbox()
			batches, err := h.inbox.store.pendingBatches()
			if err != nil || len(batches) != 2 || h.InboxFlushFailureCount() != 1 || h.inboxDigestDisabled() {
				t.Fatal(batches, err)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.calls) != 1 {
				t.Fatal("retryable rejection bypassed backoff", len(fake.calls))
			}
		})
	}
}

func TestQuietMustActPriority(t *testing.T) {
	p, err := parseDeliveryPolicy([]byte(`default: inbox
rules:
  - replay: true
    class: inbox
  - match: '.'
    class: inbox
  - tags: [ATTENTION]
    class: interrupt
lanes:
  lane-a:
    rules:
      - match: '.'
        class: inbox
      - tags: [CHECK_NOW]
        class: interrupt
`))
	if err != nil {
		t.Fatal(err)
	}
	for _, tag := range []string{"BLOCKER", "Ready", "esc", "bOuNcE", "Urgent", "NEEDS_DECISION", "Attention", "check_now"} {
		for _, field := range []string{"text", "report", "question", "reason"} {
			for _, replay := range []bool{false, true} {
				e := hubJobEventPayload{DeliveryClass: "inbox", Replay: replay}
				value := "FYI " + tag + ": merged, install broken"
				switch field {
				case "text":
					e.Text = value
				case "report":
					e.ReportLastLine = value
				case "question":
					e.Question = value
				case "reason":
					e.Reason = value
				}
				if got := p.classify("lane-a", "job.completed", e, replay); got != "interrupt" {
					t.Fatalf("tag=%s field=%s replay=%v class=%s", tag, field, replay, got)
				}
			}
		}
	}
	for _, text := range []string{"Readying", "escape", "preBLOCKER", "BOUNCED", "urgently", "needs_decision_next", "inattention", "check_now_later"} {
		if got := p.classify("lane-a", "lane.event", quietEvent("lookalike", text), true); got != "inbox" {
			t.Fatalf("lookalike %q classified %s", text, got)
		}
	}
}

func TestQuietMustActBypassesDigest(t *testing.T) {
	fake, hk, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	now := time.Now()
	policy := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(policy, []byte(strings.Replace(string(defaultDeliveryPolicy), "digest_window: 0s", "digest_window: 1m", 1)), 0600); err != nil {
		t.Fatal(err)
	}
	h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), policy, hk, &now)
	e := quietEvent("must-act", "Blocker: merged, install broken")
	e.DeliveryClass, e.Replay = "inbox", true
	if r := h.relayLaneEvent(e, agent); r.PersistFailed || len(agent.relays) != 1 {
		t.Fatal("must-act delayed or kept quiet", r)
	}
	h.flushInbox()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.stored) != 1 {
		t.Fatal("must-act also entered a digest", fake.stored)
	}
}

func TestQuietStartupMustActEscalatesOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	s, err := openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for i, text := range []string{"Blocker: merged, install broken", "ready | report", "FYI ordinary", "ESC already read", "Urgent legacy claim"} {
		e := quietEvent(fmt.Sprint(i), text)
		e.DeliveryClass, e.Replay = "inbox", true
		item, err := s.put(fmt.Sprint(i), "lane-a", "lane.event", "inbox", e, now, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if i == 3 {
			if _, err := s.read(item.ID, "lane-a", now); err != nil {
				t.Fatal(err)
			}
		}
		if i == 4 {
			if _, err := s.db.Exec(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES('legacy','lane-a','old'||char(10)||'digest')`); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec(`UPDATE lane_inbox SET escalation_batch='legacy' WHERE id=?`, item.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
	s.db.Close()
	fake, hk, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	h, agent := quietHub(t, path, "default", hk, &now)
	h.flushInbox()
	batches, err := h.inbox.store.pendingBatches()
	if err != nil || len(batches) != 0 || len(agent.relays) != 1 {
		t.Fatal("startup escalation missing", batches, err)
	}
	injection := <-agent.relays
	if !strings.Contains(injection.Text, "3 unread inbox items") {
		t.Fatal("startup recovered wrong items", injection)
	}
	var claimed int
	if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM lane_inbox WHERE escalation_batch<>'' AND escalation_batch<>'legacy'`).Scan(&claimed); err != nil || claimed != 3 {
		t.Fatal(claimed, err)
	}
	h.Close()
	h2, agent2 := quietHub(t, path, "default", hk, &now)
	h2.flushInbox()
	h2.flushInbox()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(agent2.relays) != 0 || len(fake.stored) != 1 {
		t.Fatal("startup escalated an item twice", fake.stored)
	}
}

func TestQuietKillSwitchBypassesStore(t *testing.T) {
	for _, mode := range []string{"off", "empty path"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "inbox.sqlite3")
			now := time.Now()
			s, err := openLaneInboxStore(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.put("saved", "lane-a", "lane.event", "inbox", quietEvent("saved", "Blocker: FYI"), now.Add(-time.Hour), time.Minute, 0); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES('queued','lane-a','old'||char(10)||'digest')`); err != nil {
				t.Fatal(err)
			}
			s.db.Close()
			policy, configuredPath := "off", path
			if mode == "empty path" {
				policy, configuredPath = "default", ""
			}
			fake, hk, closeServer := newFakeHandoffkeep(t)
			defer closeServer()
			h, agent := quietHub(t, configuredPath, policy, hk, &now)
			if h.inbox != nil {
				t.Fatal("kill switch opened inbox")
			}
			h.runInbox(context.Background()) // No probe or background loop.
			h.flushInbox()
			e := quietEvent("new", "FYI new explicit inbox")
			e.DeliveryClass = "inbox"
			if r := h.relayLaneEvent(e, agent); r.PersistFailed || len(agent.relays) != 1 {
				t.Fatal("kill switch did not restore injection", r)
			}
			// Inspect without reopening through the migration/recovery path.
			var items, claims, quarantines, sent int
			s.db, err = sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.db.Close()
			if err := s.db.QueryRow(`SELECT count(*),count(NULLIF(escalation_batch,'')) FROM lane_inbox`).Scan(&items, &claims); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM lane_inbox_batch_quarantine`).Scan(&quarantines); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT sent FROM lane_inbox_batches WHERE id='queued'`).Scan(&sent); err != nil {
				t.Fatal(err)
			}
			if items != 1 || claims != 0 || quarantines != 0 || sent != 0 {
				t.Fatalf("off mutated inbox: items=%d claims=%d quarantine=%d sent=%d", items, claims, quarantines, sent)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.calls) != 1 || len(fake.stored) != 1 || fake.stored[0].EventID != "new" {
				t.Fatal("off flushed or probed", fake.calls)
			}
		})
	}
	// Even a path that cannot be opened is harmless when the policy is off.
	if inbox, err := newHubInbox("off", t.TempDir()); err != nil || inbox != nil {
		t.Fatal(inbox, err)
	}
}

func TestQuietInterruptStoreFailureFallsThrough(t *testing.T) {
	for _, kind := range []string{"lane.event", "job.completed"} {
		t.Run(kind, func(t *testing.T) {
			_, hk, closeServer := newFakeHandoffkeep(t)
			defer closeServer()
			now := time.Now()
			h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", hk, &now)
			h.inbox.store.db.Close()
			e := quietEvent("must-act", "Blocker: merged, install broken")
			e.DeliveryClass = "inbox"
			if kind == "lane.event" {
				if r := h.relayLaneEvent(e, agent); r.PersistFailed || r.ID == 0 {
					t.Fatal("store failure blocked interrupt", r)
				}
			} else {
				e.JobID, e.ReportPath = "quiet-job", "report.md"
				h.relayJobEventFrom("host-a", kind, e)
			}
			if len(agent.relays) != 1 || len(agent.persisted) != 1 {
				t.Fatalf("interrupt did not inject/ack: relays=%d acks=%d", len(agent.relays), len(agent.persisted))
			}
		})
	}
}

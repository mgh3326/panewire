package panewire_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	panewire "github.com/mgh3326/panewire"
	"github.com/mgh3326/panewire/stage2/adapters/memory"
	"github.com/mgh3326/panewire/stage2/adapters/supabase"
	"github.com/mgh3326/panewire/stage2/core"
	_ "modernc.org/sqlite"
)

var errR1Crash = errors.New("fixture crash")

type r1Rig struct {
	t             *testing.T
	ctx           context.Context
	now           time.Time
	root          string
	inboxRoot     string
	namespaceRoot string
	stagingRoot   string
	transport     *memory.Adapter
	senderStore   *core.MetadataStore
	receiverStore *core.MetadataStore
	sender        *core.Sender
}

func newR1Rig(t *testing.T) *r1Rig {
	t.Helper()
	root := t.TempDir()
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	senderStore, err := core.OpenMetadataStore(filepath.Join(root, "sender.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	receiverStore, err := core.OpenMetadataStore(filepath.Join(root, "receiver.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = senderStore.Close()
		_ = receiverStore.Close()
	})
	transport := memory.New(now)
	return &r1Rig{
		t:             t,
		ctx:           t.Context(),
		now:           now,
		root:          root,
		inboxRoot:     filepath.Join(root, "inbox"),
		namespaceRoot: filepath.Join(root, "inbox", "jobs"),
		stagingRoot:   filepath.Join(root, ".stage2-private"),
		transport:     transport,
		senderStore:   senderStore,
		receiverStore: receiverStore,
		sender:        &core.Sender{Store: senderStore, Transport: transport, Now: transport.Now},
	}
}

func (r *r1Rig) receiver(t *testing.T, gate core.Gate, pane core.PaneValidator, hook func(string) error) *core.Receiver {
	t.Helper()
	receiver, err := core.NewReceiver(core.ReceiverConfig{
		MachineID:   "receiver",
		Namespaces:  map[string]string{"jobs": r.namespaceRoot},
		InboxRoot:   r.inboxRoot,
		StagingRoot: r.stagingRoot,
		Store:       r.receiverStore,
		Transport:   r.transport,
		Gate:        gate,
		Pane:        pane,
		Now:         r.transport.Now,
		CrashHook:   hook,
	})
	if err != nil {
		t.Fatal(err)
	}
	return receiver
}

func (r *r1Rig) source(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(r.root, name)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func (r *r1Rig) submit(t *testing.T, source, messageID, logicalPath string, spawn bool) core.OutboxRecord {
	t.Helper()
	record, err := r.sender.Submit(r.ctx, core.Submission{
		MessageID:      messageID,
		SourcePath:     source,
		Source:         core.Identity{MachineID: "sender", InstanceID: "boot-a"},
		Destination:    core.Destination{MachineID: "receiver", InboxNamespace: "jobs", LogicalPath: logicalPath},
		Expect:         core.Expectation{MachineID: "receiver"},
		Classification: core.ClassificationPublic,
		Spawn:          core.Spawn{Requested: spawn},
	})
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func r1Envelope(now time.Time, messageID, destination, logicalPath string, data []byte) core.Envelope {
	h := sha256.Sum256(data)
	return core.Envelope{
		SchemaVersion: core.SchemaVersion,
		MessageID:     messageID,
		DeliveryID:    core.DeliveryIDFor(messageID, destination),
		MessageKind:   core.MessageKindInbox,
		Source:        core.Identity{MachineID: "sender", InstanceID: "boot-a"},
		Destination:   core.Destination{MachineID: destination, InboxNamespace: "jobs", LogicalPath: logicalPath},
		Expect:        core.Expectation{MachineID: destination},
		Payload: core.PayloadMeta{
			Mode:           "inline",
			ContentType:    "text/markdown; charset=utf-8",
			SizeBytes:      int64(len(data)),
			SHA256:         hex.EncodeToString(h[:]),
			Classification: core.ClassificationPublic,
		},
		CreatedAt: now,
		ExpiresAt: now.Add(core.DefaultTTL),
	}
}

func r1Poll(t *testing.T, receiver *core.Receiver) {
	t.Helper()
	if err := receiver.PollOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func r1FinalPath(r *r1Rig, logicalPath string) string {
	return filepath.Join(r.namespaceRoot, filepath.FromSlash(logicalPath))
}

func r1ExpectFile(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("file content=%q want=%q", got, want)
	}
}

func r1NoFiles(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("unexpected consumer-visible entries under %s: %v", root, entries)
	}
}

type r1Gate struct {
	mu            sync.Mutex
	spawnCalls    int
	lookupCalls   int
	spawnReceipt  core.GateReceipt
	lookupReceipt core.GateReceipt
	spawnErr      error
	lookupErr     error
}

func (g *r1Gate) Spawn(context.Context, string) (core.GateReceipt, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.spawnCalls++
	return g.spawnReceipt, g.spawnErr
}

func (g *r1Gate) Lookup(context.Context, string) (core.GateReceipt, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.lookupCalls++
	return g.lookupReceipt, g.lookupErr
}

func (g *r1Gate) SpawnCalls() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.spawnCalls
}

type r1Pane struct {
	err   error
	calls int
}

func (p *r1Pane) ValidatePane(context.Context, core.Envelope) error {
	p.calls++
	return p.err
}

func r1DestinationMismatch(t *testing.T) {
	rig := newR1Rig(t)
	gate := &r1Gate{spawnReceipt: core.GateReceipt{Accepted: true, Durable: true}}
	receiver := rig.receiver(t, gate, nil, nil)
	body := []byte("destination-mismatch")
	wrong := r1Envelope(rig.now, "destination-wrong", "other-machine", "wrong.md", body)
	rig.transport.Inject("receiver", wrong, body)
	r1Poll(t, receiver)
	snapshot, ok := rig.transport.Snapshot(wrong.DeliveryID)
	if !ok || snapshot.Fetches != 0 || !snapshot.Acked {
		t.Fatalf("mismatch must not fetch body and must terminal-ack: %+v ok=%v", snapshot, ok)
	}
	r1NoFiles(t, rig.namespaceRoot)
	record, found, err := rig.receiverStore.InboxByDelivery(t.Context(), wrong.DeliveryID)
	if err != nil || !found || record.TerminalReason != core.CodeDestination {
		t.Fatalf("terminal metadata=%+v found=%v err=%v", record, found, err)
	}

	pane := &r1Pane{err: fmt.Errorf("identity mismatch")}
	good := r1Envelope(rig.now, "pane-wrong", "receiver", "pane.md", []byte("pane-mismatch"))
	good.Expect.Pane.Name = "exact-agent"
	good.Spawn.Requested = true
	rig.transport.Inject("receiver", good, []byte("pane-mismatch"))
	receiver = rig.receiver(t, gate, pane, nil)
	r1Poll(t, receiver)
	if _, err := os.Stat(r1FinalPath(rig, "pane.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pane mismatch made consumer file: %v", err)
	}
	if gate.SpawnCalls() != 0 || pane.calls != 1 {
		t.Fatalf("pane mismatch gate=%d pane=%d", gate.SpawnCalls(), pane.calls)
	}
}

func r1LogicalPathTraversalRejected(t *testing.T) {
	rig := newR1Rig(t)
	gate := &r1Gate{spawnReceipt: core.GateReceipt{Accepted: true, Durable: true}}
	receiver := rig.receiver(t, gate, nil, nil)
	body := []byte("path-boundary")
	for i, logical := range []string{"/absolute.md", "../escape.md", "nested/../../escape.md", "dot/../escape.md"} {
		env := r1Envelope(rig.now, fmt.Sprintf("traversal-%d", i), "receiver", logical, body)
		rig.transport.Inject("receiver", env, body)
		r1Poll(t, receiver)
		snapshot, _ := rig.transport.Snapshot(env.DeliveryID)
		if snapshot.Fetches != 0 || !snapshot.Acked {
			t.Fatalf("logical=%q snapshot=%+v", logical, snapshot)
		}
	}
	outside := filepath.Join(rig.root, "outside")
	if err := os.MkdirAll(outside, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(rig.namespaceRoot, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(rig.namespaceRoot, "link")); err != nil {
		t.Fatal(err)
	}
	symlink := r1Envelope(rig.now, "symlink", "receiver", "link/escape.md", body)
	rig.transport.Inject("receiver", symlink, body)
	r1Poll(t, receiver)
	if _, err := os.Stat(filepath.Join(outside, "escape.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("symlink escape wrote outside namespace: %v", err)
	}

	racePath := filepath.Join(rig.namespaceRoot, "race")
	if err := os.MkdirAll(racePath, 0700); err != nil {
		t.Fatal(err)
	}
	raceHook := func(point string) error {
		if point == "before_rename" {
			if err := os.RemoveAll(racePath); err != nil {
				return err
			}
			return os.Symlink(outside, racePath)
		}
		return nil
	}
	receiver = rig.receiver(t, gate, nil, raceHook)
	race := r1Envelope(rig.now, "race", "receiver", "race/escape.md", body)
	rig.transport.Inject("receiver", race, body)
	r1Poll(t, receiver)
	if _, err := os.Stat(filepath.Join(outside, "escape.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rename race wrote outside namespace: %v", err)
	}
	if gate.SpawnCalls() != 0 {
		t.Fatalf("path rejection must not call wrk: %d", gate.SpawnCalls())
	}
}

func r1DuplicateRedelivery(t *testing.T) {
	rig := newR1Rig(t)
	gate := &r1Gate{spawnReceipt: core.GateReceipt{Accepted: true, Durable: true}}
	receiver := rig.receiver(t, gate, nil, nil)
	body := []byte("duplicate-safe")
	env := r1Envelope(rig.now, "same-message", "receiver", "duplicates/brief.md", body)
	env.Spawn.Requested = true
	for i := 0; i < 10; i++ {
		rig.transport.Inject("receiver", env, body)
		r1Poll(t, receiver)
	}
	r1ExpectFile(t, r1FinalPath(rig, "duplicates/brief.md"), body)
	if gate.SpawnCalls() != 1 {
		t.Fatalf("gate calls=%d want 1", gate.SpawnCalls())
	}
	snapshot, ok := rig.transport.Snapshot(env.DeliveryID)
	if !ok || !snapshot.Acked || snapshot.Acks != 1 {
		t.Fatalf("redelivery must ack repeatably: %+v ok=%v", snapshot, ok)
	}
	collision := r1Envelope(rig.now, "different-message", "receiver", "duplicates/brief.md", []byte("different-content"))
	rig.transport.Inject("receiver", collision, []byte("different-content"))
	r1Poll(t, receiver)
	r1ExpectFile(t, r1FinalPath(rig, "duplicates/brief.md"), body)
	collisionRecord, found, err := rig.receiverStore.InboxByDelivery(t.Context(), collision.DeliveryID)
	if err != nil || !found || collisionRecord.TerminalReason != core.CodeCollision {
		t.Fatalf("logical collision metadata=%+v found=%v err=%v", collisionRecord, found, err)
	}
}

func r1SenderCrashBeforeVsAfterPublish(t *testing.T) {
	before := newR1Rig(t)
	source := before.source(t, "before.md", []byte("canonical-before"))
	record := before.submit(t, source, "sender-before", "sender/before.md", false)
	// Simulate process exit after durable SUBMITTED and before any publish.
	restarted := &core.Sender{Store: before.senderStore, Transport: before.transport, Now: before.transport.Now}
	if err := restarted.PublishPending(t.Context()); err != nil {
		t.Fatal(err)
	}
	if before.transport.LogicalPublishes() != 1 {
		t.Fatalf("pre-publish recovery logical publishes=%d", before.transport.LogicalPublishes())
	}
	stored, found, err := before.senderStore.OutboxByDelivery(t.Context(), record.DeliveryID)
	if err != nil || !found || stored.MessageID != "sender-before" || stored.State != core.OutboxPublished {
		t.Fatalf("recovered outbox=%+v found=%v err=%v", stored, found, err)
	}

	after := newR1Rig(t)
	afterSource := after.source(t, "after.md", []byte("canonical-after"))
	afterRecord := after.submit(t, afterSource, "sender-after", "sender/after.md", false)
	after.sender.Hooks.AfterTransportPublish = func(core.OutboxRecord, core.PublishReceipt) error { return errR1Crash }
	if err := after.sender.PublishPending(t.Context()); !errors.Is(err, errR1Crash) {
		t.Fatalf("publish crash error=%v", err)
	}
	if after.transport.LogicalPublishes() != 1 {
		t.Fatalf("accepted pre-commit publish count=%d", after.transport.LogicalPublishes())
	}
	restarted = &core.Sender{Store: after.senderStore, Transport: after.transport, Now: after.transport.Now}
	if err := restarted.PublishPending(t.Context()); err != nil {
		t.Fatal(err)
	}
	if after.transport.LogicalPublishes() != 1 || after.transport.PublishAttempts() != 2 {
		t.Fatalf("post-accept recovery must reuse unique delivery: logical=%d attempts=%d", after.transport.LogicalPublishes(), after.transport.PublishAttempts())
	}
	stored, found, err = after.senderStore.OutboxByDelivery(t.Context(), afterRecord.DeliveryID)
	if err != nil || !found || stored.State != core.OutboxPublished || stored.MessageID != "sender-after" {
		t.Fatalf("post-crash outbox=%+v found=%v err=%v", stored, found, err)
	}
}

func r1CrashBeforeInboxWrite(t *testing.T) {
	rig := newR1Rig(t)
	body := []byte("restart-before-rename")
	env := r1Envelope(rig.now, "before-rename", "receiver", "crash/before.md", body)
	point := os.Getenv("PW_FIXTURE_CRASH_AT")
	if point == "" {
		point = "before_rename"
	}
	receiver := rig.receiver(t, nil, nil, func(at string) error {
		if at == point {
			return errR1Crash
		}
		return nil
	})
	rig.transport.Inject("receiver", env, body)
	if err := receiver.PollOnce(t.Context()); !errors.Is(err, errR1Crash) {
		t.Fatalf("crash error=%v", err)
	}
	if _, err := os.Stat(r1FinalPath(rig, "crash/before.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("final file exists before rename crash: %v", err)
	}
	stageInfo, err := os.Stat(rig.stagingRoot)
	if err != nil || stageInfo.Mode().Perm() != 0700 {
		t.Fatalf("private staging mode=%v err=%v", stageInfo.Mode(), err)
	}
	entries, err := os.ReadDir(rig.stagingRoot)
	if err != nil || len(entries) != 1 {
		t.Fatalf("private staging entries=%v err=%v", entries, err)
	}
	fileInfo, err := entries[0].Info()
	if err != nil || fileInfo.Mode().Perm() != 0600 {
		t.Fatalf("private staging file mode=%v err=%v", fileInfo.Mode(), err)
	}
	receiver = rig.receiver(t, nil, nil, nil)
	rig.transport.RedeliverAll()
	r1Poll(t, receiver)
	r1ExpectFile(t, r1FinalPath(rig, "crash/before.md"), body)
	r1NoFiles(t, rig.stagingRoot)
	snapshot, _ := rig.transport.Snapshot(env.DeliveryID)
	if !snapshot.Acked {
		t.Fatal("recovered delivery was not acked")
	}
}

func r1CrashAfterInboxWriteBeforeAck(t *testing.T) {
	rig := newR1Rig(t)
	body := []byte("restart-after-rename")
	env := r1Envelope(rig.now, "after-rename", "receiver", "crash/after.md", body)
	env.Spawn.Requested = true
	gate := &r1Gate{spawnReceipt: core.GateReceipt{Accepted: true, Durable: true}}
	point := os.Getenv("PW_FIXTURE_CRASH_AT")
	if point == "" {
		point = "after_rename"
	}
	receiver := rig.receiver(t, gate, nil, func(at string) error {
		if at == point {
			return errR1Crash
		}
		return nil
	})
	rig.transport.Inject("receiver", env, body)
	if err := receiver.PollOnce(t.Context()); !errors.Is(err, errR1Crash) {
		t.Fatalf("crash error=%v", err)
	}
	r1ExpectFile(t, r1FinalPath(rig, "crash/after.md"), body)
	before, _ := rig.transport.Snapshot(env.DeliveryID)
	receiver = rig.receiver(t, gate, nil, nil)
	rig.transport.RedeliverAll()
	r1Poll(t, receiver)
	after, _ := rig.transport.Snapshot(env.DeliveryID)
	if after.Fetches != before.Fetches || gate.SpawnCalls() != 1 || !after.Acked {
		t.Fatalf("recovery rewrote/fetched/spawned duplicate: before=%+v after=%+v gate=%d", before, after, gate.SpawnCalls())
	}
}

func r1OfflineReceiverReconnect(t *testing.T) {
	rig := newR1Rig(t)
	source := rig.source(t, "offline.md", []byte("offline-then-poll"))
	record := rig.submit(t, source, "offline-message", "offline/brief.md", false)
	if err := rig.sender.PublishPending(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, ok := rig.transport.Snapshot(record.DeliveryID); !ok {
		t.Fatal("message was not retained for offline receiver")
	}
	rig.transport.Advance(time.Hour) // no Realtime hint; normal polling repairs it.
	receiver := rig.receiver(t, nil, nil, nil)
	r1Poll(t, receiver)
	r1ExpectFile(t, r1FinalPath(rig, "offline/brief.md"), []byte("offline-then-poll"))
	snapshot, _ := rig.transport.Snapshot(record.DeliveryID)
	if !snapshot.Acked {
		t.Fatal("offline delivery did not ack after polling")
	}
}

func r1TransportOutageBackoff(t *testing.T) {
	rig := newR1Rig(t)
	source := rig.source(t, "outage.md", []byte("canonical-stays-local"))
	record := rig.submit(t, source, "outage-message", "outage/brief.md", false)
	rig.sender.Draw = func(upper int64) int64 { return upper / 2 }
	rig.transport.SetOutage(true)
	if err := rig.sender.PublishPending(t.Context()); err == nil {
		t.Fatal("outage publish unexpectedly succeeded")
	}
	stored, found, err := rig.senderStore.OutboxByDelivery(t.Context(), record.DeliveryID)
	if err != nil || !found || stored.MessageID != record.MessageID || stored.State != core.OutboxSubmitted {
		t.Fatalf("outage outbox=%+v found=%v err=%v", stored, found, err)
	}
	if _, err := os.Stat(source); err != nil {
		t.Fatalf("canonical source disappeared during outage: %v", err)
	}
	if err := rig.sender.PublishPending(t.Context()); err != nil {
		t.Fatalf("publisher retried before full-jitter delay: %v", err)
	}
	storedAgain, found, err := rig.senderStore.OutboxByDelivery(t.Context(), record.DeliveryID)
	if err != nil || !found || storedAgain.Attempts != stored.Attempts {
		t.Fatalf("backoff retry count=%d initial=%d found=%v err=%v", storedAgain.Attempts, stored.Attempts, found, err)
	}
	for attempt := 1; attempt <= 16; attempt++ {
		delay := core.FullJitterBackoff(attempt, func(upper int64) int64 { return upper / 2 })
		if delay < 0 || delay > core.RetryCap(attempt) || core.RetryCap(attempt) > 5*time.Minute {
			t.Fatalf("attempt=%d delay=%s cap=%s", attempt, delay, core.RetryCap(attempt))
		}
	}
	rig.transport.SetOutage(false)
	rig.transport.Advance(core.FullJitterBackoff(stored.Attempts, rig.sender.Draw))
	if err := rig.sender.PublishPending(t.Context()); err != nil {
		t.Fatal(err)
	}
	if rig.transport.LogicalPublishes() != 1 {
		t.Fatalf("recovered publish logical rows=%d", rig.transport.LogicalPublishes())
	}
}

func r1ExpiredAndOversizedPayload(t *testing.T) {
	rig := newR1Rig(t)
	receiver := rig.receiver(t, nil, nil, nil)
	expired := r1Envelope(rig.now.Add(-2*time.Hour), "expired", "receiver", "reject/expired.md", []byte("expired"))
	expired.ExpiresAt = rig.now.Add(-time.Second)
	rig.transport.Inject("receiver", expired, []byte("expired"))
	r1Poll(t, receiver)
	snapshot, _ := rig.transport.Snapshot(expired.DeliveryID)
	if snapshot.Fetches != 0 || !snapshot.Acked {
		t.Fatalf("expired payload materialized: %+v", snapshot)
	}

	over := r1Envelope(rig.now, "oversized", "receiver", "reject/oversized.md", []byte("small"))
	over.Payload.SizeBytes = core.MaxInlineBytes + 1
	rig.transport.Inject("receiver", over, []byte("small"))
	r1Poll(t, receiver)
	snapshot, _ = rig.transport.Snapshot(over.DeliveryID)
	if snapshot.Fetches != 0 || !snapshot.Acked {
		t.Fatalf("oversized declaration materialized: %+v", snapshot)
	}
	source := rig.source(t, "sender-expired.md", []byte("sender expiry"))
	senderExpired, err := rig.sender.Submit(t.Context(), core.Submission{
		MessageID:      "sender-expired",
		SourcePath:     source,
		Source:         core.Identity{MachineID: "sender"},
		Destination:    core.Destination{MachineID: "receiver", InboxNamespace: "jobs", LogicalPath: "reject/sender-expired.md"},
		Classification: core.ClassificationPublic,
		CreatedAt:      rig.now.Add(-2 * time.Hour),
		ExpiresAt:      rig.now.Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.sender.PublishPending(t.Context()); err != nil {
		t.Fatal(err)
	}
	stored, found, err := rig.senderStore.OutboxByDelivery(t.Context(), senderExpired.DeliveryID)
	if err != nil || !found || stored.State != core.OutboxExpired {
		t.Fatalf("sender expiry state=%+v found=%v err=%v", stored, found, err)
	}
	r1NoFiles(t, rig.namespaceRoot)
}

func r1TamperedHash(t *testing.T) {
	rig := newR1Rig(t)
	receiver := rig.receiver(t, nil, nil, nil)
	body := []byte("hash-original")
	env := r1Envelope(rig.now, "tampered", "receiver", "tampered.md", body)
	tampered := append([]byte(nil), body...)
	tampered[0] ^= 1
	rig.transport.Inject("receiver", env, tampered)
	r1Poll(t, receiver)
	if _, err := os.Stat(r1FinalPath(rig, "tampered.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tampered bytes became consumer-visible: %v", err)
	}
	snapshot, _ := rig.transport.Snapshot(env.DeliveryID)
	if snapshot.Fetches != 1 || !snapshot.Acked {
		t.Fatalf("tampered hash path=%+v", snapshot)
	}
	record, found, err := rig.receiverStore.InboxByDelivery(t.Context(), env.DeliveryID)
	if err != nil || !found || record.TerminalReason != core.CodeHash {
		t.Fatalf("tampered terminal record=%+v found=%v err=%v", record, found, err)
	}
}

func r1CompanyDataFailClosed(t *testing.T) {
	rig := newR1Rig(t)
	source := rig.source(t, "classification.md", []byte("allow-listed only"))
	for i, class := range []core.Classification{"company", "unknown", ""} {
		_, err := rig.sender.Submit(t.Context(), core.Submission{
			MessageID:      fmt.Sprintf("company-send-%d", i),
			SourcePath:     source,
			Source:         core.Identity{MachineID: "sender"},
			Destination:    core.Destination{MachineID: "receiver", InboxNamespace: "jobs", LogicalPath: fmt.Sprintf("class/send-%d.md", i)},
			Classification: class,
		})
		if err == nil {
			t.Fatalf("classification %q published from sender", class)
		}
	}
	if rig.transport.LogicalPublishes() != 0 {
		t.Fatalf("classification failures reached adapter: %d", rig.transport.LogicalPublishes())
	}
	receiver := rig.receiver(t, nil, nil, nil)
	for i, class := range []core.Classification{"company", "unknown", ""} {
		env := r1Envelope(rig.now, fmt.Sprintf("company-receive-%d", i), "receiver", fmt.Sprintf("class/receive-%d.md", i), []byte("blocked"))
		env.Payload.Classification = class
		rig.transport.Inject("receiver", env, []byte("blocked"))
		r1Poll(t, receiver)
		snapshot, _ := rig.transport.Snapshot(env.DeliveryID)
		if snapshot.Fetches != 0 || !snapshot.Acked {
			t.Fatalf("classification %q received body: %+v", class, snapshot)
		}
	}
	r1NoFiles(t, rig.namespaceRoot)
}

func r1SecretRepoLeakGuard(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	configRoot := filepath.Join(home, ".config", "panewire")
	if err := os.MkdirAll(configRoot, 0700); err != nil {
		t.Fatal(err)
	}
	marker := fmt.Sprintf("fixture-secret-%d", time.Now().UnixNano())
	credential := filepath.Join(configRoot, "supabase-session")
	if err := os.WriteFile(credential, []byte(marker+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := supabase.LoadAccessToken(credential)
	if err != nil || got != marker {
		t.Fatalf("fixture credential load failed: got length=%d err=%v", len(got), err)
	}
	if err := os.Chmod(credential, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := supabase.LoadAccessToken(credential); err == nil {
		t.Fatal("non-0600 credential was accepted")
	}

	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && (entry.Name() == ".git" || entry.Name() == ".cache") {
			return filepath.SkipDir
		}
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		info, err := entry.Info()
		if err != nil || info.Size() > 2<<20 {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(data, []byte(marker)) {
			return fmt.Errorf("fixture secret present in repository artifact")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var captured bytes.Buffer
	_, _ = captured.WriteString("stage2 metadata-only fixture log")
	if strings.Contains(captured.String(), marker) {
		t.Fatal("fixture token appeared in captured logs")
	}
}

func r1SQLiteBodyAbsence(t *testing.T) {
	rig := newR1Rig(t)
	marker := fmt.Sprintf("body-smuggle-%d", time.Now().UnixNano())
	source := rig.source(t, "body-marker.md", []byte("safe body "+marker))
	record := rig.submit(t, source, "sqlite-success", "sqlite/success.md", false)
	if err := rig.sender.PublishPending(t.Context()); err != nil {
		t.Fatal(err)
	}
	r1Poll(t, rig.receiver(t, nil, nil, nil))
	r1ExpectFile(t, r1FinalPath(rig, "sqlite/success.md"), []byte("safe body "+marker))

	// A failed tampered delivery and an outage retry exercise failure and retry
	// paths without ever passing raw payload text to SQLite metadata.
	tampered := r1Envelope(rig.now, "sqlite-failure", "receiver", "sqlite/failure.md", []byte(marker))
	rig.transport.Inject("receiver", tampered, []byte(marker+"-mutated"))
	r1Poll(t, rig.receiver(t, nil, nil, nil))
	retrySource := rig.source(t, "body-retry.md", []byte(marker+" retry"))
	_ = rig.submit(t, retrySource, "sqlite-retry", "sqlite/retry.md", false)
	rig.transport.SetOutage(true)
	_ = rig.sender.PublishPending(t.Context())
	rig.transport.SetOutage(false)

	q := newR1HTTPQueue(rig.now)
	server := httptest.NewServer(q)
	defer server.Close()
	httpAdapter, err := supabase.New(supabase.Config{BaseURL: server.URL, AllowInsecureForTests: true, AccessToken: "fixture-only"})
	if err != nil {
		t.Fatal(err)
	}
	unknown := []byte(`{"schema_version":2,"message_id":"unknown-field","delivery_id":"` + core.DeliveryIDFor("unknown-field", "receiver") + `","message_kind":"inbox.delivery","source":{"machine_id":"sender"},"destination":{"machine_id":"receiver","inbox_namespace":"jobs","logical_path":"sqlite/unknown.md"},"expect":{"machine_id":"receiver"},"payload":{"mode":"inline","content_type":"text/markdown; charset=utf-8","size_bytes":0,"sha256":"` + strings.Repeat("0", 64) + `","classification":"public"},"created_at":"2026-08-29T12:00:00Z","expires_at":"2026-08-30T12:00:00Z","unknown_body_marker":"` + marker + `"}`)
	q.injectRaw("receiver", unknown)
	poisonReceiver, err := core.NewReceiver(core.ReceiverConfig{
		MachineID:   "receiver",
		Namespaces:  map[string]string{"jobs": rig.namespaceRoot},
		InboxRoot:   rig.inboxRoot,
		StagingRoot: rig.stagingRoot,
		Store:       rig.receiverStore,
		Transport:   httpAdapter,
		Now:         func() time.Time { return rig.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := poisonReceiver.PollOnce(t.Context()); err != nil {
		t.Fatalf("unknown envelope poison handling failed: %v", err)
	}
	if !q.allAcked() {
		t.Fatal("unknown envelope was not terminally acked")
	}

	if record.SourcePath == "" {
		t.Fatal("success record unexpectedly lost canonical path")
	}
	for _, path := range []string{rig.senderStore.Path(), rig.receiverStore.Path()} {
		r1AssertSQLiteMetadataOnly(t, path, marker)
	}
	var logs bytes.Buffer
	_, _ = logs.WriteString("metadata-only log")
	if strings.Contains(logs.String(), marker) {
		t.Fatal("body marker appeared in logs")
	}
}

func r1AssertSQLiteMetadataOnly(t *testing.T, file, marker string) {
	t.Helper()
	db, err := sql.Open("sqlite", file)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, name)
	}
	for _, table := range tables {
		info, err := db.Query(`PRAGMA table_info("` + table + `")`)
		if err != nil {
			t.Fatal(err)
		}
		var columns []string
		for info.Next() {
			var cid int
			var name, kind string
			var notNull, pk int
			var defaultValue any
			if err := info.Scan(&cid, &name, &kind, &notNull, &defaultValue, &pk); err != nil {
				_ = info.Close()
				t.Fatal(err)
			}
			if strings.Contains(strings.ToLower(name), "body") || strings.Contains(strings.ToLower(kind), "blob") {
				_ = info.Close()
				t.Fatalf("body-bearing SQLite schema field %s.%s %s", table, name, kind)
			}
			columns = append(columns, `"`+name+`"`)
		}
		if err := info.Close(); err != nil {
			t.Fatal(err)
		}
		if len(columns) == 0 {
			continue
		}
		values, err := db.Query(`SELECT ` + strings.Join(columns, ",") + ` FROM "` + table + `"`)
		if err != nil {
			t.Fatal(err)
		}
		for values.Next() {
			entry := make([]any, len(columns))
			ptrs := make([]any, len(columns))
			for i := range entry {
				ptrs[i] = &entry[i]
			}
			if err := values.Scan(ptrs...); err != nil {
				_ = values.Close()
				t.Fatal(err)
			}
			for _, value := range entry {
				switch v := value.(type) {
				case string:
					if strings.Contains(v, marker) {
						_ = values.Close()
						t.Fatalf("body marker persisted in %s", table)
					}
				case []byte:
					if bytes.Contains(v, []byte(marker)) {
						_ = values.Close()
						t.Fatalf("body marker persisted in %s", table)
					}
				}
			}
		}
		if err := values.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func r1WrkGateDenialNoBypass(t *testing.T) {
	rig := newR1Rig(t)
	gate := &r1Gate{spawnReceipt: core.GateReceipt{Accepted: false, Durable: true, Found: true}}
	receiver := rig.receiver(t, gate, nil, nil)
	body := []byte("gate-denied")
	env := r1Envelope(rig.now, "gate-denial", "receiver", "gate/brief.md", body)
	env.Spawn.Requested = true
	rig.transport.Inject("receiver", env, body)
	r1Poll(t, receiver)
	if gate.SpawnCalls() != 1 {
		t.Fatalf("wrk gate calls=%d want 1", gate.SpawnCalls())
	}
	r1ExpectFile(t, r1FinalPath(rig, "gate/brief.md"), body)
	record, found, err := rig.receiverStore.InboxByDelivery(t.Context(), env.DeliveryID)
	if err != nil || !found || record.TerminalReason != core.CodeGateDenied {
		t.Fatalf("gate denial metadata=%+v found=%v err=%v", record, found, err)
	}
	// A redelivery can only ack the durable denial; it cannot try another gate
	// call or invoke a direct agent process because core has no such API.
	rig.transport.Inject("receiver", env, body)
	r1Poll(t, receiver)
	if gate.SpawnCalls() != 1 {
		t.Fatalf("redelivery bypassed durable gate denial: %d calls", gate.SpawnCalls())
	}

	unknownRig := newR1Rig(t)
	unknownGate := &r1Gate{spawnErr: fmt.Errorf("ambiguous wrk result"), lookupReceipt: core.GateReceipt{Found: false}}
	unknownReceiver := unknownRig.receiver(t, unknownGate, nil, nil)
	unknown := r1Envelope(unknownRig.now, "gate-unknown", "receiver", "gate/unknown.md", []byte("unknown"))
	unknown.Spawn.Requested = true
	unknownRig.transport.Inject("receiver", unknown, []byte("unknown"))
	if err := unknownReceiver.PollOnce(t.Context()); err == nil {
		t.Fatal("ambiguous wrk result was accepted")
	}
	unknownRig.transport.RedeliverAll()
	if err := unknownReceiver.PollOnce(t.Context()); err == nil {
		t.Fatal("unproven wrk lookup was accepted")
	}
	if unknownGate.SpawnCalls() != 1 || unknownGate.lookupCalls != 1 {
		t.Fatalf("SPAWN_UNKNOWN respawned or skipped lookup: spawn=%d lookup=%d", unknownGate.SpawnCalls(), unknownGate.lookupCalls)
	}
}

func r1ReverseCompletionIdempotent(t *testing.T) {
	rig := newR1Rig(t)
	originalSource := rig.source(t, "original.md", []byte("original request"))
	original := rig.submit(t, originalSource, "original-message", "original/brief.md", false)
	completionRoot := filepath.Join(rig.root, "completion-inbox")
	completionStage := filepath.Join(rig.root, ".completion-stage")
	receiver, err := core.NewReceiver(core.ReceiverConfig{
		MachineID:   "sender",
		Namespaces:  map[string]string{"jobs": completionRoot},
		InboxRoot:   completionRoot,
		StagingRoot: completionStage,
		Store:       rig.senderStore,
		Transport:   rig.transport,
		Now:         rig.transport.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	first := r1CompletionEnvelope(rig.now, original.MessageID, "done", "reply/one.md", []byte("completed"))
	rig.transport.Inject("sender", first, []byte("completed"))
	r1Poll(t, receiver)
	rig.transport.Inject("sender", first, []byte("completed"))
	r1Poll(t, receiver)
	conflict := r1CompletionEnvelope(rig.now, original.MessageID, "failed", "reply/two.md", []byte("failed"))
	rig.transport.Inject("sender", conflict, []byte("failed"))
	r1Poll(t, receiver)

	completion, found, err := rig.senderStore.CompletionByCorrelation(t.Context(), original.MessageID)
	if err != nil || !found || completion.CompletionID != first.MessageID || completion.CausationID != "done" {
		t.Fatalf("first completion did not win: %+v found=%v err=%v", completion, found, err)
	}
	anomalies, err := rig.senderStore.CompletionAnomalyCount(t.Context(), original.MessageID)
	if err != nil || anomalies != 1 {
		t.Fatalf("completion anomalies=%d err=%v", anomalies, err)
	}
	stored, found, err := rig.senderStore.OutboxByDelivery(t.Context(), original.DeliveryID)
	if err != nil || !found || stored.State != core.OutboxCompleted {
		t.Fatalf("original outbox terminal state=%+v found=%v err=%v", stored, found, err)
	}
}

func r1CompletionEnvelope(now time.Time, correlation, causation, logicalPath string, data []byte) core.Envelope {
	h := sha256.Sum256(data)
	hash := hex.EncodeToString(h[:])
	messageID := core.CompletionIDFor(correlation, causation, hash)
	env := r1Envelope(now, messageID, "sender", logicalPath, data)
	env.MessageKind = core.MessageKindCompletion
	env.Source = core.Identity{MachineID: "receiver"}
	env.Expect = core.Expectation{MachineID: "sender"}
	env.CorrelationID = correlation
	env.CausationID = causation
	return env
}

func r1AdapterContract(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	t.Run("memory", func(t *testing.T) {
		adapter := memory.New(now)
		r1RunAdapterContract(t, adapter, now, adapter.RedeliverAll)
	})
	t.Run("supabase_http_fake", func(t *testing.T) {
		queue := newR1HTTPQueue(now)
		server := httptest.NewServer(queue)
		defer server.Close()
		adapter, err := supabase.New(supabase.Config{BaseURL: server.URL, AccessToken: "fixture-only", AllowInsecureForTests: true})
		if err != nil {
			t.Fatal(err)
		}
		r1RunAdapterContract(t, adapter, now, queue.redeliverAll)
	})
}

func r1RunAdapterContract(t *testing.T, adapter core.Transport, now time.Time, redeliver func()) {
	t.Helper()
	body := []byte("adapter-contract")
	env := r1Envelope(now, "adapter-message", "receiver", "adapter/brief.md", body)
	receipt, err := adapter.Publish(t.Context(), env, io.NopCloser(bytes.NewReader(body)))
	if err != nil || receipt.Duplicate {
		t.Fatalf("first publish receipt=%+v err=%v", receipt, err)
	}
	duplicate, err := adapter.Publish(t.Context(), env, io.NopCloser(bytes.NewReader(body)))
	if err != nil || !duplicate.Duplicate {
		t.Fatalf("idempotent publish receipt=%+v err=%v", duplicate, err)
	}
	var first core.Delivery
	if err := adapter.Receive(t.Context(), core.Destination{MachineID: "receiver"}, func(_ context.Context, d core.Delivery) error {
		first = d
		return nil
	}); err != nil || first.Token == "" {
		t.Fatalf("claim err=%v delivery=%+v", err, first)
	}
	redeliver()
	var redelivered core.Delivery
	if err := adapter.Receive(t.Context(), core.Destination{MachineID: "receiver"}, func(_ context.Context, d core.Delivery) error {
		redelivered = d
		return nil
	}); err != nil || redelivered.Envelope.DeliveryID != env.DeliveryID {
		t.Fatalf("redelivery err=%v delivery=%+v", err, redelivered)
	}
	reader, err := adapter.FetchPayload(t.Context(), redelivered, core.MaxInlineBytes+1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || string(got) != string(body) {
		t.Fatalf("fetched payload=%q err=%v", got, err)
	}
	if err := adapter.Ack(t.Context(), redelivered.Token, core.AckAccepted); err != nil {
		t.Fatal(err)
	}
	claims := 0
	if err := adapter.Receive(t.Context(), core.Destination{MachineID: "receiver"}, func(context.Context, core.Delivery) error {
		claims++
		return nil
	}); err != nil || claims != 0 {
		t.Fatalf("acked delivery was claimable claims=%d err=%v", claims, err)
	}
	expired := r1Envelope(now.Add(-2*time.Hour), "adapter-expired", "receiver", "adapter/expired.md", []byte("expired"))
	expired.ExpiresAt = now.Add(-time.Second)
	if _, err := adapter.Publish(t.Context(), expired, io.NopCloser(bytes.NewReader([]byte("expired")))); err != nil {
		t.Fatal(err)
	}
	var expiredDelivery core.Delivery
	if err := adapter.Receive(t.Context(), core.Destination{MachineID: "receiver"}, func(_ context.Context, d core.Delivery) error {
		expiredDelivery = d
		return nil
	}); err != nil || expiredDelivery.Envelope.DeliveryID != expired.DeliveryID {
		t.Fatalf("expiry contract claim err=%v delivery=%+v", err, expiredDelivery)
	}
	if err := adapter.Ack(t.Context(), expiredDelivery.Token, core.AckTerminalReject); err != nil {
		t.Fatal(err)
	}
}

func r1CoreNoSDKLeakage(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "list", "-deps", "./stage2/core")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, output)
	}
	lower := strings.ToLower(string(output))
	for _, forbidden := range []string{"supabase", "nats", "herdr"} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("core dependency output leaked %q", forbidden)
		}
	}
}

type r1HTTPRow struct {
	envelope    core.Envelope
	rawEnvelope json.RawMessage
	payload     []byte
	destination string
	token       string
	acked       bool
}

type r1HTTPQueue struct {
	mu   sync.Mutex
	now  time.Time
	seq  int
	rows map[string]*r1HTTPRow
}

func newR1HTTPQueue(now time.Time) *r1HTTPQueue {
	return &r1HTTPQueue{now: now, rows: map[string]*r1HTTPRow{}}
}

func (q *r1HTTPQueue) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch req.URL.Path {
	case "/rest/v1/":
		w.WriteHeader(http.StatusNoContent)
		return
	case "/rest/v1/rpc/panewire_publish":
		var in struct {
			Envelope   core.Envelope `json:"p_envelope"`
			PayloadB64 string        `json:"p_payload_b64"`
		}
		if json.NewDecoder(req.Body).Decode(&in) != nil {
			http.Error(w, "bad publish", http.StatusBadRequest)
			return
		}
		payload, err := base64.StdEncoding.DecodeString(in.PayloadB64)
		if err != nil {
			http.Error(w, "bad payload", http.StatusBadRequest)
			return
		}
		q.mu.Lock()
		_, duplicate := q.rows[in.Envelope.DeliveryID]
		if !duplicate {
			q.rows[in.Envelope.DeliveryID] = &r1HTTPRow{envelope: in.Envelope, payload: payload, destination: in.Envelope.Destination.MachineID}
		}
		q.mu.Unlock()
		_ = json.NewEncoder(w).Encode([]map[string]any{{"message_id": in.Envelope.MessageID, "delivery_id": in.Envelope.DeliveryID, "accepted_at": q.now, "duplicate": duplicate}})
		return
	case "/rest/v1/rpc/panewire_claim":
		var in struct {
			DestinationMachineID string `json:"p_destination_machine_id"`
		}
		if json.NewDecoder(req.Body).Decode(&in) != nil {
			http.Error(w, "bad claim", http.StatusBadRequest)
			return
		}
		q.mu.Lock()
		keys := make([]string, 0, len(q.rows))
		for key := range q.rows {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		var out []map[string]any
		for _, key := range keys {
			row := q.rows[key]
			if row.acked || row.destination != in.DestinationMachineID {
				continue
			}
			q.seq++
			row.token = fmt.Sprintf("http-%d", q.seq)
			raw := row.rawEnvelope
			if raw == nil {
				raw, _ = json.Marshal(row.envelope)
			}
			out = append(out, map[string]any{"token": row.token, "destination_machine_id": row.destination, "visibility_deadline": q.now.Add(30 * time.Second), "envelope": json.RawMessage(raw)})
		}
		q.mu.Unlock()
		_ = json.NewEncoder(w).Encode(out)
		return
	case "/rest/v1/rpc/panewire_fetch_payload":
		var in struct {
			Token string `json:"p_token"`
		}
		_ = json.NewDecoder(req.Body).Decode(&in)
		q.mu.Lock()
		defer q.mu.Unlock()
		for _, row := range q.rows {
			if row.token == in.Token && !row.acked {
				_ = json.NewEncoder(w).Encode([]map[string]string{{"payload_b64": base64.StdEncoding.EncodeToString(row.payload)}})
				return
			}
		}
		http.Error(w, "missing token", http.StatusNotFound)
		return
	case "/rest/v1/rpc/panewire_ack":
		var in struct {
			Token string `json:"p_token"`
		}
		_ = json.NewDecoder(req.Body).Decode(&in)
		q.mu.Lock()
		defer q.mu.Unlock()
		for _, row := range q.rows {
			if row.token == in.Token {
				row.acked, row.payload = true, nil
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		http.Error(w, "missing token", http.StatusNotFound)
		return
	case "/rest/v1/rpc/panewire_message_status":
		var in struct {
			DeliveryID string `json:"p_delivery_id"`
		}
		_ = json.NewDecoder(req.Body).Decode(&in)
		q.mu.Lock()
		defer q.mu.Unlock()
		if row := q.rows[in.DeliveryID]; row != nil {
			state := "claimed"
			if row.acked {
				state = "acked"
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{{"state": state, "body_erased": row.payload == nil, "acked_at": q.now}})
			return
		}
		http.Error(w, "missing delivery", http.StatusNotFound)
		return
	default:
		http.NotFound(w, req)
	}
}

func (q *r1HTTPQueue) redeliverAll() {}

func (q *r1HTTPQueue) allAcked() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, row := range q.rows {
		if !row.acked {
			return false
		}
	}
	return true
}

func (q *r1HTTPQueue) injectRaw(destination string, raw json.RawMessage) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.seq++
	q.rows[fmt.Sprintf("raw-%d", q.seq)] = &r1HTTPRow{destination: destination, rawEnvelope: append(json.RawMessage(nil), raw...)}
}

type r1LoopProbe struct {
	mu    sync.Mutex
	calls int
}

func (p *r1LoopProbe) PublishPending(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return nil
}

func (p *r1LoopProbe) Calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func TestStage2DefaultOffDoesNotEnterLoop(t *testing.T) {
	store := panewire.NewMemoryStore(t)
	probe := &r1LoopProbe{}
	shortDir, err := os.MkdirTemp("/tmp", "pw-s2-gate-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(shortDir) })
	socket := filepath.Join(shortDir, "panewire.sock")
	daemon := panewire.NewDaemon(panewire.Config{
		Store:         store,
		SocketPath:    socket,
		SchemaCommand: []string{"sh", "-c", "printf '{}\n'"},
		Stage2:        panewire.Stage2Config{Enabled: false, Publisher: probe},
	})
	if err := daemon.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = daemon.Stop() })
	time.Sleep(25 * time.Millisecond)
	if probe.Calls() != 0 {
		t.Fatalf("default-off stage2 loop entered %d times", probe.Calls())
	}
}

func TestStage2SubmitCLICommitsOnlyOutboxMetadata(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "submit.md")
	if err := os.WriteFile(source, []byte("CLI-SUBMIT-BODY"), 0600); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(root, "metadata.sqlite3")
	if code := panewire.RunCLI([]string{
		"submit", "--db", dbPath, "--file", source,
		"--from-machine", "sender", "--destination-machine", "receiver",
		"--namespace", "jobs", "--logical-path", "cli/brief.md",
		"--classification", "public",
	}, panewire.CLIConfig{}); code != panewire.ExitOK {
		t.Fatalf("submit exit=%d", code)
	}
	store, err := core.OpenMetadataStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	pending, err := store.PendingOutbox(t.Context(), time.Now().UTC())
	if err != nil || len(pending) != 1 || pending[0].SourcePath != source {
		t.Fatalf("CLI outbox pending=%+v err=%v", pending, err)
	}
	r1AssertSQLiteMetadataOnly(t, dbPath, "CLI-SUBMIT-BODY")
}

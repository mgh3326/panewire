package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type Submission struct {
	MessageID                  string
	SourcePath                 string
	Source                     Identity
	Destination                Destination
	Expect                     Expectation
	Classification             Classification
	PolicyVersion              string
	ContentType                string
	MessageKind                MessageKind
	CorrelationID, CausationID string
	Reply                      Reply
	Spawn                      Spawn
	CreatedAt, ExpiresAt       time.Time
}

type SenderHooks struct {
	// AfterTransportPublish is a fixture seam for the durable crash window
	// between an adapter receipt and the local PUBLISHED commit.
	AfterTransportPublish func(OutboxRecord, PublishReceipt) error
}

type Sender struct {
	Store     *MetadataStore
	Transport Transport
	Now       func() time.Time
	// Draw supplies full-jitter samples. A nil value uses a cryptographic
	// random draw; fixtures inject deterministic samples without sleeping.
	Draw  func(upperExclusive int64) int64
	Hooks SenderHooks
}

func (s *Sender) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

// Submit hashes the canonical local source, commits metadata-only SUBMITTED,
// and only then returns success. It never copies source bytes into SQLite.
func (s *Sender) Submit(ctx context.Context, in Submission) (OutboxRecord, error) {
	if s.Store == nil {
		return OutboxRecord{}, fmt.Errorf("stage2 sender has no metadata store")
	}
	now := s.now()
	if in.CreatedAt.IsZero() {
		in.CreatedAt = now
	}
	in.CreatedAt = in.CreatedAt.UTC()
	if in.ExpiresAt.IsZero() {
		in.ExpiresAt = in.CreatedAt.Add(DefaultTTL)
	}
	in.ExpiresAt = in.ExpiresAt.UTC()
	if in.ExpiresAt.Sub(in.CreatedAt) <= 0 || in.ExpiresAt.Sub(in.CreatedAt) > MaxTTL {
		return OutboxRecord{}, validation(CodeExpired, "submit expiry is outside allowed TTL")
	}
	if !in.Classification.Allowed() {
		return OutboxRecord{}, validation(CodeClassification, "submit classification is not allow-listed")
	}
	if in.Source.MachineID == "" || in.Destination.MachineID == "" || in.Destination.InboxNamespace == "" {
		return OutboxRecord{}, validation(CodeSchema, "submit identity or destination is missing")
	}
	if _, err := ValidateLogicalPath(in.Destination.LogicalPath); err != nil {
		return OutboxRecord{}, err
	}
	if in.Expect.MachineID == "" {
		in.Expect.MachineID = in.Destination.MachineID
	}
	if in.Expect.MachineID != in.Destination.MachineID {
		return OutboxRecord{}, validation(CodeDestination, "submit expectation differs from destination")
	}
	if in.MessageKind == "" {
		in.MessageKind = MessageKindInbox
	}
	if in.MessageKind != MessageKindInbox && in.MessageKind != MessageKindCompletion {
		return OutboxRecord{}, validation(CodeSchema, "unsupported submit message kind")
	}
	if in.PolicyVersion == "" {
		in.PolicyVersion = "stage2-allowlist-v1"
	}
	if in.ContentType == "" {
		in.ContentType = "text/markdown; charset=utf-8"
	}
	abs, err := filepath.Abs(in.SourcePath)
	if err != nil {
		return OutboxRecord{}, fmt.Errorf("resolve canonical source path: %w", err)
	}
	hash, size, err := hashFile(abs)
	if err != nil {
		return OutboxRecord{}, err
	}
	if size > MaxInlineBytes {
		return OutboxRecord{}, validation(CodeDeclaredSize, "source exceeds 192 KiB inline cap")
	}
	if in.MessageKind == MessageKindCompletion {
		if in.CorrelationID == "" || in.CausationID == "" {
			return OutboxRecord{}, validation(CodeSchema, "completion correlation or causation is missing")
		}
		expected := CompletionIDFor(in.CorrelationID, in.CausationID, hash)
		if in.MessageID == "" {
			in.MessageID = expected
		} else if in.MessageID != expected {
			return OutboxRecord{}, validation(CodeSchema, "completion ID is not stable derived ID")
		}
	} else if in.MessageID == "" {
		in.MessageID, err = NewMessageID()
		if err != nil {
			return OutboxRecord{}, err
		}
	}
	r := OutboxRecord{
		MessageID:            in.MessageID,
		DestinationMachineID: in.Destination.MachineID,
		DeliveryID:           DeliveryIDFor(in.MessageID, in.Destination.MachineID),
		SourcePath:           abs,
		SHA256:               hash,
		SizeBytes:            size,
		InboxNamespace:       in.Destination.InboxNamespace,
		LogicalPath:          in.Destination.LogicalPath,
		Classification:       in.Classification,
		ContentType:          in.ContentType,
		PolicyVersion:        in.PolicyVersion,
		MessageKind:          in.MessageKind,
		Source:               in.Source,
		Expect:               in.Expect,
		CorrelationID:        in.CorrelationID,
		CausationID:          in.CausationID,
		Reply:                in.Reply,
		Spawn:                in.Spawn,
		CreatedAt:            in.CreatedAt,
		ExpiresAt:            in.ExpiresAt,
		UpdatedAt:            now,
		State:                OutboxSubmitted,
	}
	inserted, err := s.Store.InsertOutbox(ctx, r)
	if err != nil {
		return OutboxRecord{}, err
	}
	if !inserted {
		existing, ok, getErr := s.Store.OutboxByDelivery(ctx, r.DeliveryID)
		if getErr != nil {
			return OutboxRecord{}, getErr
		}
		if !ok {
			return OutboxRecord{}, fmt.Errorf("outbox idempotency row disappeared")
		}
		return existing, nil
	}
	return r, nil
}

func (s *Sender) PublishPending(ctx context.Context) error {
	if s.Store == nil || s.Transport == nil {
		return fmt.Errorf("stage2 publisher is not configured")
	}
	pending, err := s.Store.PendingOutbox(ctx, s.now())
	if err != nil {
		return err
	}
	var first error
	for _, r := range pending {
		if !r.LastAttemptAt.IsZero() && nowBeforeRetry(r, s.now(), s.Draw) {
			continue
		}
		if err := s.PublishOne(ctx, r.DeliveryID); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func nowBeforeRetry(record OutboxRecord, now time.Time, draw func(int64) int64) bool {
	delay := FullJitterBackoff(record.Attempts, draw)
	return now.Before(record.LastAttemptAt.Add(delay))
}

func (s *Sender) PublishOne(ctx context.Context, deliveryID string) error {
	if s.Store == nil || s.Transport == nil {
		return fmt.Errorf("stage2 publisher is not configured")
	}
	r, found, err := s.Store.OutboxByDelivery(ctx, deliveryID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("outbox row not found")
	}
	if r.State != OutboxSubmitted {
		return nil
	}
	now := s.now()
	if !now.Before(r.ExpiresAt) {
		return s.Store.MarkOutboxExpired(ctx, r.DeliveryID, now)
	}
	actualHash, actualSize, err := hashFile(r.SourcePath)
	if err != nil {
		_ = s.Store.MarkOutboxAttempt(ctx, r.DeliveryID, now, PublishReceipt{}, "source_read")
		return err
	}
	if actualHash != r.SHA256 || actualSize != r.SizeBytes {
		err := validation(CodeHash, "canonical source changed after submission")
		_ = s.Store.MarkOutboxAttempt(ctx, r.DeliveryID, now, PublishReceipt{}, ValidationCode(err))
		return err
	}
	f, err := os.Open(r.SourcePath)
	if err != nil {
		return err
	}
	defer f.Close()
	receipt, err := s.Transport.Publish(ctx, r.Envelope(), f)
	if err != nil {
		_ = s.Store.MarkOutboxAttempt(ctx, r.DeliveryID, now, PublishReceipt{}, "transport_publish")
		return err
	}
	if s.Hooks.AfterTransportPublish != nil {
		if err := s.Hooks.AfterTransportPublish(r, receipt); err != nil {
			return err
		}
	}
	return s.Store.MarkOutboxAttempt(ctx, r.DeliveryID, now, receipt, "")
}

func hashFile(file string) (string, int64, error) {
	f, err := os.Open(file)
	if err != nil {
		return "", 0, fmt.Errorf("open canonical source: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, MaxInlineBytes+1))
	if err != nil {
		return "", 0, fmt.Errorf("hash canonical source: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

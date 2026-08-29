package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

type ReceiverConfig struct {
	MachineID   string
	Namespaces  map[string]string // approved namespace name -> trusted root
	InboxRoot   string            // watcher root; staging must be outside it
	StagingRoot string            // private 0700 sibling on the same filesystem
	Store       *MetadataStore
	Transport   Transport
	Gate        Gate
	Pane        PaneValidator
	Now         func() time.Time
	// CrashHook is intentionally an injected fixture seam. Production does not
	// inspect test environment variables or invoke it unless configured.
	CrashHook func(point string) error
}

type Receiver struct{ cfg ReceiverConfig }

func NewReceiver(cfg ReceiverConfig) (*Receiver, error) {
	if cfg.MachineID == "" || cfg.Store == nil || cfg.Transport == nil {
		return nil, fmt.Errorf("stage2 receiver needs machine identity, store, and transport")
	}
	if cfg.StagingRoot == "" || cfg.InboxRoot == "" {
		return nil, fmt.Errorf("stage2 receiver needs explicit staging and inbox roots")
	}
	inbox, err := filepath.Abs(cfg.InboxRoot)
	if err != nil {
		return nil, err
	}
	stage, err := filepath.Abs(cfg.StagingRoot)
	if err != nil {
		return nil, err
	}
	if isWithin(stage, inbox) {
		return nil, fmt.Errorf("private staging must be outside watcher root")
	}
	if err := ensurePrivateDirectory(stage); err != nil {
		return nil, err
	}
	var stageStat unix.Stat_t
	if err := unix.Stat(stage, &stageStat); err != nil {
		return nil, fmt.Errorf("stat private staging: %w", err)
	}
	for name, root := range cfg.Namespaces {
		if name == "" || root == "" {
			return nil, fmt.Errorf("empty approved namespace")
		}
		absolute, err := filepath.Abs(root)
		if err != nil {
			return nil, err
		}
		if err := ensureDirectory(absolute); err != nil {
			return nil, fmt.Errorf("prepare namespace %q: %w", name, err)
		}
		var namespaceStat unix.Stat_t
		if err := unix.Stat(absolute, &namespaceStat); err != nil {
			return nil, fmt.Errorf("stat namespace %q: %w", name, err)
		}
		if namespaceStat.Dev != stageStat.Dev {
			return nil, fmt.Errorf("private staging and namespace %q must share a filesystem", name)
		}
		cfg.Namespaces[name] = absolute
	}
	cfg.InboxRoot, cfg.StagingRoot = inbox, stage
	return &Receiver{cfg: cfg}, nil
}

func (r *Receiver) now() time.Time {
	if r.cfg.Now != nil {
		return r.cfg.Now().UTC()
	}
	return time.Now().UTC()
}

func (r *Receiver) PollOnce(ctx context.Context) error {
	return r.cfg.Transport.Receive(ctx, Destination{MachineID: r.cfg.MachineID}, r.handle)
}

func (r *Receiver) handle(ctx context.Context, delivery Delivery) error {
	now := r.now()
	if err := ValidateBeforeFetch(delivery.Envelope, delivery.DestinationMachineID, r.cfg.MachineID, now); err != nil {
		return r.terminal(ctx, delivery, err)
	}
	if _, ok := r.cfg.Namespaces[delivery.Envelope.Destination.InboxNamespace]; !ok {
		return r.terminal(ctx, delivery, validation(CodeLogicalPath, "inbox namespace is not approved"))
	}
	record, _, err := r.cfg.Store.ReserveInbox(ctx, delivery.Envelope, now)
	if err != nil {
		return r.terminal(ctx, delivery, err)
	}

	switch record.State {
	case InboxAcked:
		if record.TerminalReason != "" {
			return r.ack(ctx, delivery, AckTerminalReject)
		}
		return r.ack(ctx, delivery, AckAccepted)
	case InboxTerminalReject, InboxGateDenied:
		return r.ack(ctx, delivery, AckTerminalReject)
	case InboxSpawnRequested, InboxSpawnUnknown:
		return r.recoverGate(ctx, delivery, record)
	case InboxSpawnAccepted, InboxCompleted:
		return r.ack(ctx, delivery, AckAccepted)
	case InboxLogicalPublished:
		return r.afterPublished(ctx, delivery, record)
	case InboxStaged:
		published, err := r.publishedMatches(delivery.Envelope)
		if err != nil {
			return r.terminal(ctx, delivery, err)
		}
		if published {
			if err := r.cfg.Store.MarkPublished(ctx, record.DeliveryID, now); err != nil {
				return err
			}
			record.State = InboxLogicalPublished
			return r.afterPublished(ctx, delivery, record)
		}
		if err := r.removeOwnedStaging(record, delivery.Envelope); err != nil {
			return err
		}
	}

	stagePath, err := r.stagePayload(ctx, delivery)
	if err != nil {
		var v *ValidationError
		if errors.As(err, &v) {
			return r.terminal(ctx, delivery, err)
		}
		return err
	}
	if err := r.cfg.Store.MarkStaged(ctx, delivery.Envelope.DeliveryID, stagePath, now); err != nil {
		return err
	}
	if r.cfg.Pane != nil && delivery.Envelope.Expect.Pane.Requested() {
		if err := r.cfg.Pane.ValidatePane(ctx, delivery.Envelope); err != nil {
			_ = r.removeStagePath(stagePath, delivery.Envelope.MessageID)
			return r.terminal(ctx, delivery, validation(CodePaneMismatch, "exact pane validation failed"))
		}
	}
	if r.cfg.CrashHook != nil {
		if err := r.cfg.CrashHook("before_rename"); err != nil {
			return err
		}
	}
	if _, err := r.publishStage(delivery.Envelope, stagePath); err != nil {
		return r.terminal(ctx, delivery, err)
	}
	if r.cfg.CrashHook != nil {
		if err := r.cfg.CrashHook("after_rename"); err != nil {
			return err
		}
	}
	if err := r.cfg.Store.MarkPublished(ctx, delivery.Envelope.DeliveryID, now); err != nil {
		return err
	}
	return r.afterPublished(ctx, delivery, record)
}

func (r *Receiver) afterPublished(ctx context.Context, delivery Delivery, record InboxRecord) error {
	now := r.now()
	if delivery.Envelope.MessageKind == MessageKindCompletion {
		completion := CompletionRecord{
			CorrelationID:   delivery.Envelope.CorrelationID,
			CompletionID:    delivery.Envelope.MessageID,
			CausationID:     delivery.Envelope.CausationID,
			ResultHash:      delivery.Envelope.Payload.SHA256,
			TerminalOutcome: delivery.Envelope.CausationID,
			CreatedAt:       now,
		}
		first, _, err := r.cfg.Store.RecordCompletion(ctx, completion)
		if err != nil {
			return err
		}
		if first {
			if err := r.cfg.Store.MarkOutboxCompleted(ctx, delivery.Envelope.CorrelationID, now); err != nil {
				return err
			}
		}
		if err := r.cfg.Store.MarkCompleted(ctx, delivery.Envelope.DeliveryID, delivery.Envelope.MessageID, now); err != nil {
			return err
		}
		return r.ack(ctx, delivery, AckAccepted)
	}
	if !delivery.Envelope.Spawn.Requested {
		return r.ack(ctx, delivery, AckAccepted)
	}
	if r.cfg.Gate == nil {
		if err := r.cfg.Store.MarkGateState(ctx, delivery.Envelope.DeliveryID, InboxSpawnUnknown, CodeManualReconcile, now); err != nil {
			return err
		}
		return validation(CodeManualReconcile, "spawn requested without a wrk gate")
	}
	if err := r.cfg.Store.MarkGateState(ctx, delivery.Envelope.DeliveryID, InboxSpawnRequested, "", now); err != nil {
		return err
	}
	receipt, err := r.cfg.Gate.Spawn(ctx, delivery.Envelope.DeliveryID)
	if err != nil || !receipt.Durable {
		_ = r.cfg.Store.MarkGateState(ctx, delivery.Envelope.DeliveryID, InboxSpawnUnknown, CodeManualReconcile, now)
		if err != nil {
			return err
		}
		return validation(CodeManualReconcile, "wrk receipt is not durable")
	}
	if !receipt.Accepted {
		if err := r.cfg.Store.MarkGateState(ctx, delivery.Envelope.DeliveryID, InboxGateDenied, CodeGateDenied, now); err != nil {
			return err
		}
		return r.ack(ctx, delivery, AckTerminalReject)
	}
	if err := r.cfg.Store.MarkGateState(ctx, delivery.Envelope.DeliveryID, InboxSpawnAccepted, "", now); err != nil {
		return err
	}
	return r.ack(ctx, delivery, AckAccepted)
}

func (r *Receiver) recoverGate(ctx context.Context, delivery Delivery, record InboxRecord) error {
	if r.cfg.Gate == nil {
		return validation(CodeManualReconcile, "wrk gate is unavailable")
	}
	now := r.now()
	receipt, err := r.cfg.Gate.Lookup(ctx, delivery.Envelope.DeliveryID)
	if err != nil || !receipt.Found || !receipt.Durable {
		_ = r.cfg.Store.MarkGateState(ctx, record.DeliveryID, InboxSpawnUnknown, CodeManualReconcile, now)
		if err != nil {
			return err
		}
		return validation(CodeManualReconcile, "wrk lookup did not prove an accepted outcome")
	}
	if !receipt.Accepted {
		if err := r.cfg.Store.MarkGateState(ctx, record.DeliveryID, InboxGateDenied, CodeGateDenied, now); err != nil {
			return err
		}
		return r.ack(ctx, delivery, AckTerminalReject)
	}
	if err := r.cfg.Store.MarkGateState(ctx, record.DeliveryID, InboxSpawnAccepted, "", now); err != nil {
		return err
	}
	return r.ack(ctx, delivery, AckAccepted)
}

func (r *Receiver) terminal(ctx context.Context, delivery Delivery, cause error) error {
	now := r.now()
	env := terminalEnvelope(delivery, r.cfg.MachineID)
	record, _, err := r.cfg.Store.ReserveInbox(ctx, env, now)
	if err != nil {
		return err
	}
	if err := r.cfg.Store.MarkTerminal(ctx, record.DeliveryID, ValidationCode(cause), "", now); err != nil {
		return err
	}
	return r.ack(ctx, Delivery{Token: delivery.Token, Envelope: env, DestinationMachineID: delivery.DestinationMachineID}, AckTerminalReject)
}

func terminalEnvelope(delivery Delivery, receiver string) Envelope {
	env := delivery.Envelope
	if env.DeliveryID == "" {
		sum := sha256.Sum256([]byte(env.MessageID + "\x00" + delivery.DestinationMachineID))
		env.DeliveryID = "reject-" + hex.EncodeToString(sum[:])
	}
	if env.MessageID == "" {
		env.MessageID = env.DeliveryID
	}
	if env.Destination.MachineID == "" {
		env.Destination.MachineID = receiver
	}
	if env.Destination.InboxNamespace == "" {
		env.Destination.InboxNamespace = "__rejected__"
	}
	if env.Destination.LogicalPath == "" {
		env.Destination.LogicalPath = "rejected-" + env.DeliveryID
	}
	return env
}

func (r *Receiver) ack(ctx context.Context, delivery Delivery, disposition AckDisposition) error {
	if err := r.cfg.Transport.Ack(ctx, delivery.Token, disposition); err != nil {
		return err
	}
	return r.cfg.Store.MarkAcked(ctx, delivery.Envelope.DeliveryID, r.now())
}

func (r *Receiver) stagePayload(ctx context.Context, delivery Delivery) (string, error) {
	reader, err := r.cfg.Transport.FetchPayload(ctx, delivery, MaxInlineBytes+1)
	if err != nil {
		return "", err
	}
	defer reader.Close()
	path := r.stagingPath(delivery.Envelope.MessageID)
	if err := r.removeStagePath(path, delivery.Envelope.MessageID); err != nil {
		return "", err
	}
	stageDir, err := openDirectoryNoFollow(r.cfg.StagingRoot)
	if err != nil {
		return "", fmt.Errorf("open private staging root: %w", err)
	}
	defer unix.Close(stageDir)
	fd, err := unix.Openat(stageDir, filepath.Base(path), unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return "", fmt.Errorf("create private staging: %w", err)
	}
	f := os.NewFile(uintptr(fd), "private-stage")
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(f, h), io.LimitReader(reader, delivery.Envelope.Payload.SizeBytes+1))
	syncErr := f.Sync()
	closeErr := f.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil {
		_ = r.removeStagePath(path, delivery.Envelope.MessageID)
		return "", fmt.Errorf("write private staging: %w", firstError(copyErr, syncErr, closeErr))
	}
	if n != delivery.Envelope.Payload.SizeBytes || n > MaxInlineBytes {
		_ = r.removeStagePath(path, delivery.Envelope.MessageID)
		return "", validation(CodeActualSize, "decoded payload size does not match declaration")
	}
	if actual := hex.EncodeToString(h.Sum(nil)); actual != delivery.Envelope.Payload.SHA256 {
		_ = r.removeStagePath(path, delivery.Envelope.MessageID)
		return "", validation(CodeHash, "decoded payload hash does not match declaration")
	}
	return path, nil
}

func (r *Receiver) stagingPath(messageID string) string {
	sum := sha256.Sum256([]byte("stage2-message:" + messageID))
	return filepath.Join(r.cfg.StagingRoot, hex.EncodeToString(sum[:])+".stage")
}

func (r *Receiver) removeOwnedStaging(record InboxRecord, env Envelope) error {
	if record.StagingPath == "" || record.StagingPath != r.stagingPath(env.MessageID) {
		return nil
	}
	return r.removeStagePath(record.StagingPath, env.MessageID)
}

func (r *Receiver) removeStagePath(file, messageID string) error {
	if file != r.stagingPath(messageID) {
		return fmt.Errorf("refusing to remove unowned staging path")
	}
	fd, err := openDirectoryNoFollow(r.cfg.StagingRoot)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	err = unix.Unlinkat(fd, filepath.Base(file), 0)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	return err
}

func (r *Receiver) publishedMatches(env Envelope) (bool, error) {
	root, ok := r.cfg.Namespaces[env.Destination.InboxNamespace]
	if !ok {
		return false, validation(CodeLogicalPath, "namespace is not approved")
	}
	parts, err := ValidateLogicalPath(env.Destination.LogicalPath)
	if err != nil {
		return false, err
	}
	parent, leaf, err := openSecureParent(root, parts, false)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ENOENT) {
		return false, nil
	}
	if err != nil {
		return false, validation(CodeLogicalPath, "open trusted parent failed")
	}
	defer unix.Close(parent)
	exists, matches, err := fileMatchesAt(parent, leaf, env.Payload.SHA256, env.Payload.SizeBytes)
	if err != nil {
		return false, err
	}
	return exists && matches, nil
}

func (r *Receiver) publishStage(env Envelope, stagePath string) (string, error) {
	root, ok := r.cfg.Namespaces[env.Destination.InboxNamespace]
	if !ok {
		return "", validation(CodeLogicalPath, "namespace is not approved")
	}
	parts, err := ValidateLogicalPath(env.Destination.LogicalPath)
	if err != nil {
		return "", err
	}
	parent, leaf, err := openSecureParent(root, parts, true)
	if err != nil {
		return "", validation(CodeLogicalPath, "open trusted destination failed")
	}
	defer unix.Close(parent)
	exists, matches, err := fileMatchesAt(parent, leaf, env.Payload.SHA256, env.Payload.SizeBytes)
	if err != nil {
		return "", err
	}
	stageDir, stageLeaf := filepath.Dir(stagePath), filepath.Base(stagePath)
	stageFD, err := openDirectoryNoFollow(stageDir)
	if err != nil {
		return "", err
	}
	defer unix.Close(stageFD)
	if exists {
		if !matches {
			return "", validation(CodeCollision, "consumer-visible target belongs to different content")
		}
		if err := unix.Unlinkat(stageFD, stageLeaf, 0); err != nil && !errors.Is(err, unix.ENOENT) {
			return "", err
		}
		return filepath.Join(root, filepath.FromSlash(env.Destination.LogicalPath)), nil
	}
	// Both directories are opened with O_NOFOLLOW. The platform-specific
	// no-replace rename therefore resolves the final leaf beneath trusted
	// directory descriptors and never overwrites a collision raced into place.
	if err := renameNoReplace(stageFD, stageLeaf, parent, leaf); err != nil {
		return "", err
	}
	if err := unix.Fsync(parent); err != nil {
		return "", err
	}
	return filepath.Join(root, filepath.FromSlash(env.Destination.LogicalPath)), nil
}

func ensurePrivateDirectory(dir string) error {
	if err := ensureDirectory(dir); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return err
	}
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if info.Mode().Perm() != 0700 {
		return fmt.Errorf("private staging mode is %o, want 0700", info.Mode().Perm())
	}
	return nil
}

func ensureDirectory(dir string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("trusted directory is not a real directory")
	}
	return nil
}

func isWithin(child, parent string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func openDirectoryNoFollow(dir string) (int, error) {
	return unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
}

func openSecureParent(root string, parts []string, create bool) (int, string, error) {
	if len(parts) == 0 {
		return -1, "", validation(CodeLogicalPath, "empty path")
	}
	fd, err := openDirectoryNoFollow(root)
	if err != nil {
		return -1, "", err
	}
	for _, part := range parts[:len(parts)-1] {
		if create {
			if err := unix.Mkdirat(fd, part, 0700); err != nil && !errors.Is(err, unix.EEXIST) {
				unix.Close(fd)
				return -1, "", err
			}
		}
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
		unix.Close(fd)
		if err != nil {
			return -1, "", err
		}
		fd = next
	}
	return fd, parts[len(parts)-1], nil
}

func fileMatchesAt(parent int, leaf, expectedHash string, expectedSize int64) (exists bool, matches bool, err error) {
	fd, err := unix.Openat(parent, leaf, unix.O_RDONLY|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return false, false, nil
	}
	if err != nil {
		return false, false, validation(CodeLogicalPath, "final leaf is not a regular no-follow file")
	}
	f := os.NewFile(uintptr(fd), "verified-final")
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return false, false, err
	}
	if !info.Mode().IsRegular() {
		return true, false, validation(CodeCollision, "final target is not regular")
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, MaxInlineBytes+1))
	if err != nil {
		return true, false, err
	}
	return true, n == expectedSize && hex.EncodeToString(h.Sum(nil)) == expectedHash, nil
}

func firstError(values ...error) error {
	for _, err := range values {
		if err != nil {
			return err
		}
	}
	return nil
}

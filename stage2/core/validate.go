package core

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

const (
	CodeSchema           = "schema"
	CodeDestination      = "destination_mismatch"
	CodeLogicalPath      = "logical_path"
	CodeExpired          = "expired"
	CodeClassification   = "classification"
	CodeDeclaredSize     = "declared_size"
	CodeActualSize       = "actual_size"
	CodeHash             = "hash_mismatch"
	CodeCollision        = "logical_path_collision"
	CodePaneMismatch     = "pane_mismatch"
	CodeGateDenied       = "gate_denied"
	CodeGateNotInstalled = "gate_not_installed"
	CodeManualReconcile  = "manual_reconciliation"
)

// ValidationError is safe to persist as metadata: it intentionally carries no
// raw message or payload value.
type ValidationError struct {
	Code string
	Err  error
}

func (e *ValidationError) Error() string {
	if e.Err == nil {
		return e.Code
	}
	return e.Code + ": " + e.Err.Error()
}

func (e *ValidationError) Unwrap() error { return e.Err }

func validation(code, format string, args ...any) error {
	return &ValidationError{Code: code, Err: fmt.Errorf(format, args...)}
}

func ValidationCode(err error) string {
	var v *ValidationError
	if errors.As(err, &v) {
		return v.Code
	}
	return "internal"
}

func ValidateLogicalPath(value string) ([]string, error) {
	if value == "" || value == "." {
		return nil, validation(CodeLogicalPath, "path is empty or dot")
	}
	if strings.ContainsRune(value, '\x00') || strings.Contains(value, "\\") {
		return nil, validation(CodeLogicalPath, "path contains ambiguous separator")
	}
	if strings.HasPrefix(value, "/") || path.IsAbs(value) {
		return nil, validation(CodeLogicalPath, "path is absolute")
	}
	if path.Clean(value) != value {
		return nil, validation(CodeLogicalPath, "path is not normalized")
	}
	parts := strings.Split(value, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return nil, validation(CodeLogicalPath, "path contains traversal")
		}
	}
	return parts, nil
}

// ValidateBeforeFetch implements the contractually required ordering through
// declared-size validation. Callers must not fetch payload bytes before it
// succeeds.
func ValidateBeforeFetch(env Envelope, claimedDestination string, receiverMachineID string, now time.Time) error {
	if env.SchemaVersion != SchemaVersion {
		return validation(CodeSchema, "unsupported schema version")
	}
	if env.MessageID == "" || env.DeliveryID == "" || env.Source.MachineID == "" || env.MessageKind == "" {
		return validation(CodeSchema, "required envelope metadata missing")
	}
	if env.MessageKind != MessageKindInbox && env.MessageKind != MessageKindCompletion {
		return validation(CodeSchema, "unsupported message kind")
	}
	if env.MessageKind == MessageKindCompletion && (env.CorrelationID == "" || env.CausationID == "") {
		return validation(CodeSchema, "completion correlation or causation is missing")
	}
	if env.Destination.MachineID == "" || env.Destination.InboxNamespace == "" || env.Expect.MachineID == "" {
		return validation(CodeSchema, "required destination metadata missing")
	}
	if env.DeliveryID != DeliveryIDFor(env.MessageID, env.Destination.MachineID) {
		return validation(CodeSchema, "delivery ID does not match message and destination")
	}
	if env.Destination.MachineID != receiverMachineID || env.Expect.MachineID != receiverMachineID || claimedDestination != receiverMachineID {
		return validation(CodeDestination, "claimed or envelope destination does not match receiver")
	}
	if _, err := ValidateLogicalPath(env.Destination.LogicalPath); err != nil {
		return err
	}
	if env.ExpiresAt.IsZero() || !now.Before(env.ExpiresAt) {
		return validation(CodeExpired, "message is expired")
	}
	if env.CreatedAt.IsZero() || env.ExpiresAt.Sub(env.CreatedAt) > MaxTTL {
		return validation(CodeExpired, "expiry exceeds hard maximum")
	}
	if !env.Payload.Classification.Allowed() {
		return validation(CodeClassification, "classification is not allow-listed")
	}
	if env.Payload.Mode != "inline" || env.Payload.SizeBytes < 0 || env.Payload.SizeBytes > MaxInlineBytes || !IsHexSHA256(env.Payload.SHA256) {
		return validation(CodeDeclaredSize, "inline payload metadata is invalid or oversized")
	}
	if env.Spawn.Requested && env.Spawn.Label == "" {
		return validation(CodeSchema, "spawn request is missing its policy label")
	}
	if !env.Spawn.Requested && env.Spawn.Label != "" {
		return validation(CodeSchema, "spawn label is present without a spawn request")
	}
	if env.MessageKind == MessageKindCompletion && env.MessageID != CompletionIDFor(env.CorrelationID, env.CausationID, env.Payload.SHA256) {
		return validation(CodeSchema, "completion ID is not the stable derived ID")
	}
	return nil
}

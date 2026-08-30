// Package core owns the transport-neutral stage 2 delivery contract.
//
// It deliberately contains no transport SDK or wire representation. Transport
// SDKs and wire representations belong in adapter packages.
package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	SchemaVersion  = 2
	MaxInlineBytes = 192 * 1024
	DefaultTTL     = 72 * time.Hour
	MaxTTL         = 7 * 24 * time.Hour
)

type MessageKind string

const (
	MessageKindInbox      MessageKind = "inbox.delivery"
	MessageKindCompletion MessageKind = "workflow.completion"
)

type Classification string

const (
	ClassificationPublic             Classification = "public"
	ClassificationPersonalNonCompany Classification = "personal_non_company"
)

func (c Classification) Allowed() bool {
	return c == ClassificationPublic || c == ClassificationPersonalNonCompany
}

type Identity struct {
	MachineID  string `json:"machine_id"`
	InstanceID string `json:"instance_id,omitempty"`
}

type Destination struct {
	MachineID      string `json:"machine_id"`
	InboxNamespace string `json:"inbox_namespace"`
	LogicalPath    string `json:"logical_path"`
}

type PaneExpectation struct {
	Name        string `json:"name,omitempty"`
	Label       string `json:"label,omitempty"`
	CWD         string `json:"cwd,omitempty"`
	WorkspaceID string `json:"workspace_id,omitempty"`
}

func (p PaneExpectation) Requested() bool {
	return p.Name != "" || p.Label != "" || p.CWD != "" || p.WorkspaceID != ""
}

type Expectation struct {
	MachineID string          `json:"machine_id"`
	Pane      PaneExpectation `json:"pane,omitempty"`
}

type PayloadMeta struct {
	Mode           string         `json:"mode"`
	ContentType    string         `json:"content_type"`
	SizeBytes      int64          `json:"size_bytes"`
	SHA256         string         `json:"sha256"`
	Classification Classification `json:"classification"`
}

type Reply struct {
	DestinationMachineID string `json:"destination_machine_id,omitempty"`
	CorrelationID        string `json:"correlation_id,omitempty"`
	Requested            bool   `json:"requested,omitempty"`
}

// Spawn is an explicit stage2 extension of the illustrative D0 envelope. It
// carries only the request bit and a receiving-policy lookup label: actual
// admission and every execution setting remain delegated to the local wrk
// gate, never the sender.
type Spawn struct {
	Requested bool   `json:"requested,omitempty"`
	Label     string `json:"label,omitempty"`
}

// Envelope contains metadata only. Payload bytes always travel through a
// PayloadReader owned by a transport adapter.
type Envelope struct {
	SchemaVersion int         `json:"schema_version"`
	MessageID     string      `json:"message_id"`
	DeliveryID    string      `json:"delivery_id"`
	MessageKind   MessageKind `json:"message_kind"`
	Source        Identity    `json:"source"`
	Destination   Destination `json:"destination"`
	Expect        Expectation `json:"expect"`
	Payload       PayloadMeta `json:"payload"`
	CreatedAt     time.Time   `json:"created_at"`
	ExpiresAt     time.Time   `json:"expires_at"`
	CorrelationID string      `json:"correlation_id,omitempty"`
	CausationID   string      `json:"causation_id,omitempty"`
	Reply         Reply       `json:"reply,omitempty"`
	Spawn         Spawn       `json:"spawn,omitempty"`
}

// PayloadReader is purposefully standard-library shaped; core never needs to
// know how a transport stores or retrieves its bytes.
type PayloadReader interface {
	io.Reader
	io.Closer
}

type OpaqueDeliveryToken string

type Delivery struct {
	Envelope             Envelope
	Token                OpaqueDeliveryToken
	DestinationMachineID string
	VisibilityDeadline   time.Time
}

type PublishReceipt struct {
	MessageID  string
	DeliveryID string
	AcceptedAt time.Time
	Duplicate  bool
}

type AckDisposition string

const (
	AckAccepted       AckDisposition = "accepted"
	AckTerminalReject AckDisposition = "terminal_reject"
)

type TransportHealth struct {
	Healthy bool
	Detail  string
}

type DeliveryHandler func(context.Context, Delivery) error

// Transport is the sole adapter boundary used by sender and receiver core.
// Token values are opaque outside their adapter.
type Transport interface {
	Publish(context.Context, Envelope, PayloadReader) (PublishReceipt, error)
	Receive(context.Context, Destination, DeliveryHandler) error
	FetchPayload(context.Context, Delivery, int64) (PayloadReader, error)
	Ack(context.Context, OpaqueDeliveryToken, AckDisposition) error
	Health(context.Context) (TransportHealth, error)
}

type GateReceipt struct {
	Accepted      bool
	Durable       bool
	Found         bool
	Detail        string
	RejectionCode string
}

// GateSpawnRequest deliberately carries no model, workspace, tier, or CWD.
// Those values are selected only by the receiving machine's local policy.
// PromptPath is the already-materialized consumer-visible logical path.
type GateSpawnRequest struct {
	DeliveryID string
	Label      string
	PromptPath string
}

// Gate is a narrow, mockable representation of the live wrk contract. Core
// provides the stable job key, sender-selected label, and materialized prompt;
// the receiving adapter supplies every local launch setting.
type Gate interface {
	Spawn(context.Context, GateSpawnRequest) (GateReceipt, error)
	Lookup(context.Context, string) (GateReceipt, error)
}

type PaneValidator interface {
	ValidatePane(context.Context, Envelope) error
}

func NewMessageID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("new UUIDv7: %w", err)
	}
	return id.String(), nil
}

func DeliveryIDFor(messageID, destinationMachineID string) string {
	h := sha256.New()
	_, _ = io.WriteString(h, "delivery:v1")
	_, _ = io.WriteString(h, messageID)
	_, _ = io.WriteString(h, destinationMachineID)
	return hex.EncodeToString(h.Sum(nil))
}

func CompletionIDFor(originalMessageID, terminalOutcome, resultHash string) string {
	h := sha256.New()
	_, _ = io.WriteString(h, "completion:v1")
	_, _ = io.WriteString(h, originalMessageID)
	_, _ = io.WriteString(h, terminalOutcome)
	_, _ = io.WriteString(h, resultHash)
	return hex.EncodeToString(h.Sum(nil))
}

func IsHexSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && strings.ToLower(value) == value
}

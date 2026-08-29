// Package memory supplies a deterministic, body-buffering fake transport for
// stage2 fixtures. It is not a persistence layer and is never used as the
// system of record.
package memory

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/mgh3326/panewire/stage2/core"
)

type Snapshot struct {
	MessageID, DeliveryID, DestinationMachineID string
	Acked                                       bool
	VisibleAt                                   time.Time
	Fetches, Acks                               int
}

type row struct {
	envelope      core.Envelope
	destination   string
	bytes         []byte
	acked         bool
	visibleAt     time.Time
	token         core.OpaqueDeliveryToken
	fetches, acks int
}

type Adapter struct {
	mu              sync.Mutex
	rows            map[string]*row
	now             time.Time
	tokenSequence   int
	publishAttempts int
	outage          bool
}

func New(now time.Time) *Adapter {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return &Adapter{rows: map[string]*row{}, now: now.UTC()}
}

func (a *Adapter) Now() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.now
}

func (a *Adapter) SetNow(now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.now = now.UTC()
}

func (a *Adapter) Advance(d time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.now = a.now.Add(d)
}

func (a *Adapter) SetOutage(value bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.outage = value
}

func (a *Adapter) Publish(ctx context.Context, env core.Envelope, reader core.PayloadReader) (core.PublishReceipt, error) {
	if err := ctx.Err(); err != nil {
		return core.PublishReceipt{}, err
	}
	a.mu.Lock()
	if a.outage {
		a.mu.Unlock()
		return core.PublishReceipt{}, fmt.Errorf("memory transport outage")
	}
	a.publishAttempts++
	if old, ok := a.rows[env.DeliveryID]; ok {
		if old.envelope.MessageID != env.MessageID || old.destination != env.Destination.MachineID || old.envelope.Payload.SHA256 != env.Payload.SHA256 || old.envelope.Payload.SizeBytes != env.Payload.SizeBytes {
			a.mu.Unlock()
			return core.PublishReceipt{}, fmt.Errorf("immutable publish conflict")
		}
		now := a.now
		a.mu.Unlock()
		return core.PublishReceipt{MessageID: env.MessageID, DeliveryID: env.DeliveryID, AcceptedAt: now, Duplicate: true}, nil
	}
	a.mu.Unlock()
	data, err := io.ReadAll(io.LimitReader(reader, core.MaxInlineBytes+1))
	if err != nil {
		return core.PublishReceipt{}, err
	}
	if int64(len(data)) != env.Payload.SizeBytes || len(data) > core.MaxInlineBytes {
		return core.PublishReceipt{}, fmt.Errorf("transport received invalid inline size")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.outage {
		return core.PublishReceipt{}, fmt.Errorf("memory transport outage")
	}
	if old, ok := a.rows[env.DeliveryID]; ok {
		return core.PublishReceipt{MessageID: old.envelope.MessageID, DeliveryID: env.DeliveryID, AcceptedAt: a.now, Duplicate: true}, nil
	}
	a.rows[env.DeliveryID] = &row{envelope: env, destination: env.Destination.MachineID, bytes: append([]byte(nil), data...), visibleAt: a.now}
	return core.PublishReceipt{MessageID: env.MessageID, DeliveryID: env.DeliveryID, AcceptedAt: a.now}, nil
}

func (a *Adapter) Receive(ctx context.Context, destination core.Destination, handler core.DeliveryHandler) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	if a.outage {
		a.mu.Unlock()
		return fmt.Errorf("memory transport outage")
	}
	keys := make([]string, 0, len(a.rows))
	for key := range a.rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var deliveries []core.Delivery
	for _, key := range keys {
		r := a.rows[key]
		if r.acked || r.destination != destination.MachineID || r.visibleAt.After(a.now) {
			continue
		}
		a.tokenSequence++
		r.token = core.OpaqueDeliveryToken(fmt.Sprintf("memory-%d", a.tokenSequence))
		r.visibleAt = a.now.Add(30 * time.Second)
		deliveries = append(deliveries, core.Delivery{Envelope: r.envelope, Token: r.token, DestinationMachineID: r.destination, VisibilityDeadline: r.visibleAt})
	}
	a.mu.Unlock()
	var first error
	for _, d := range deliveries {
		if err := handler(ctx, d); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (a *Adapter) FetchPayload(ctx context.Context, d core.Delivery, limit int64) (core.PayloadReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.outage {
		return nil, fmt.Errorf("memory transport outage")
	}
	r, ok := a.rows[d.Envelope.DeliveryID]
	if !ok || r.acked || r.token != d.Token {
		return nil, fmt.Errorf("delivery token is not claimable")
	}
	if int64(len(r.bytes)) > limit {
		return nil, fmt.Errorf("payload exceeds fetch limit")
	}
	r.fetches++
	return io.NopCloser(bytes.NewReader(append([]byte(nil), r.bytes...))), nil
}

func (a *Adapter) Ack(ctx context.Context, token core.OpaqueDeliveryToken, disposition core.AckDisposition) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.outage {
		return fmt.Errorf("memory transport outage")
	}
	for _, r := range a.rows {
		if r.token != token {
			continue
		}
		r.acked = true
		r.bytes = nil // ack removes the bounded transport copy.
		r.acks++
		return nil
	}
	return fmt.Errorf("delivery token not found")
}

func (a *Adapter) Health(context.Context) (core.TransportHealth, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.outage {
		return core.TransportHealth{Healthy: false, Detail: "memory outage"}, nil
	}
	return core.TransportHealth{Healthy: true, Detail: "memory"}, nil
}

// Inject routes an envelope by claimed destination independently of its
// envelope destination. Fixtures use this to prove mismatch validation occurs
// before FetchPayload.
func (a *Adapter) Inject(claimedDestination string, env core.Envelope, data []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.rows[env.DeliveryID] = &row{envelope: env, destination: claimedDestination, bytes: append([]byte(nil), data...), visibleAt: a.now}
}

func (a *Adapter) Tamper(deliveryID string, mutate func([]byte)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if r := a.rows[deliveryID]; r != nil {
		mutate(r.bytes)
	}
}

func (a *Adapter) RedeliverAll() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, r := range a.rows {
		if !r.acked {
			r.visibleAt = a.now
		}
	}
}

func (a *Adapter) PublishAttempts() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.publishAttempts
}

func (a *Adapter) LogicalPublishes() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.rows)
}

func (a *Adapter) Snapshot(deliveryID string) (Snapshot, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, ok := a.rows[deliveryID]
	if !ok {
		return Snapshot{}, false
	}
	return Snapshot{MessageID: r.envelope.MessageID, DeliveryID: deliveryID, DestinationMachineID: r.destination, Acked: r.acked, VisibleAt: r.visibleAt, Fetches: r.fetches, Acks: r.acks}, true
}

var _ core.Transport = (*Adapter)(nil)

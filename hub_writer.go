package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// hubWriteTimeout bounds one frame write to a node. coder/websocket closes the
// whole link when a write context expires mid-frame, so this bound is the
// dead-peer guard: a peer that accepts no bytes for this long loses the link.
// Caller deadlines never reach an admitted write (see writeJSONContext).
const hubWriteTimeout = 5 * time.Second

// hubWriteMutex retains the node's single-writer invariant while allowing an
// independently timed command to abandon admission without a stray writer.
// Its zero value is ready.
type hubWriteMutex struct {
	once sync.Once
	slot chan struct{}
}

func (m *hubWriteMutex) Lock() {
	_ = m.lockContext(context.Background())
}

func (m *hubWriteMutex) lockContext(ctx context.Context) error {
	m.once.Do(func() { m.slot = make(chan struct{}, 1) })
	select {
	case m.slot <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-m.slot
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *hubWriteMutex) Unlock() { <-m.slot }

func (agent *hubAgent) writeJSON(value any) error {
	return agent.writeJSONContext(context.Background(), value)
}

// writeJSONContext sends one JSON text frame. The caller's ctx only governs
// admission (waiting for the writer slot) and how long the caller waits. Once
// admitted, the frame is written by a goroutine under its own hubWriteTimeout,
// so a caller deadline that expires mid-write returns to the caller without
// cancelling the frame: cancelling a started frame closes the node link, and
// one slow command must never drop it. A caller that gives up may still have
// its frame delivered; callers already treat that as an unknown outcome.
func (agent *hubAgent) writeJSONContext(ctx context.Context, value any) error {
	// The frame is encoded here, in the caller's goroutine, so the writer never
	// reads a value the caller may still own after it has returned. Encode
	// matches wsjson.Write: one text message holding the JSON and a newline.
	var frame bytes.Buffer
	if err := json.NewEncoder(&frame).Encode(value); err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}
	if err := agent.writeMu.lockContext(ctx); err != nil {
		return err
	}
	written := make(chan error, 1)
	go func() {
		defer agent.writeMu.Unlock()
		writeCtx, cancel := context.WithTimeout(context.Background(), hubWriteTimeout)
		defer cancel()
		written <- agent.conn.Write(writeCtx, websocket.MessageText, frame.Bytes())
	}()
	select {
	case err := <-written:
		return err
	case <-ctx.Done():
		// A write that finished as the deadline fired is a success, not a failure.
		select {
		case err := <-written:
			return err
		default:
			return ctx.Err()
		}
	}
}

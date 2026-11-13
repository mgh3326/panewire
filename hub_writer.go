package panewire

import (
	"context"
	"sync"

	"github.com/coder/websocket/wsjson"
)

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

func (agent *hubAgent) writeJSONContext(ctx context.Context, value any) error {
	if err := agent.writeMu.lockContext(ctx); err != nil {
		return err
	}
	defer agent.writeMu.Unlock()
	return wsjson.Write(ctx, agent.conn, value)
}

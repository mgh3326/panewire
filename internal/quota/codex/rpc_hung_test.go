package codex

import (
	"errors"
	"sync"
)

// blockingRPC never answers: Recv blocks until Close, as a hung app-server's
// pipe would until it is killed.
type blockingRPC struct {
	mu       sync.Mutex
	closed   bool
	released chan struct{}
}

func (b *blockingRPC) Send([]byte) error { return nil }

func (b *blockingRPC) Recv() ([]byte, error) {
	<-b.released
	return nil, errors.New("closed")
}

func (b *blockingRPC) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.closed = true
		close(b.released)
	}
}

func (b *blockingRPC) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

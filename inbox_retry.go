package panewire

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/mgh3326/panewire/internal/hubretry"
)

// inboxAttemptTimeout bounds one attempt at a short inbox call: the 5 seconds
// one call had before retries. The retry budget spans attempts, so it is not
// one of these.
const inboxAttemptTimeout = 5 * time.Second

// inboxAnswer is one read of an inbox answer. The body is read inside the
// attempt, so a retry never holds a response open.
type inboxAnswer struct {
	status int
	body   []byte
}

// inboxCall is one inbox request. Every inbox command the CLI sends is
// idempotent: list, count, status and wait are reads, and read or ack only
// marks an item again, so the call may be sent again on a transport error, a
// 502 or a 503 (docs/hub-uninterrupted-calls.md).
type inboxCall struct {
	method  string
	path    string
	query   url.Values
	body    []byte
	timeout time.Duration // one attempt
}

func (c inboxCall) send(ctx context.Context, client inboxHTTPClient, policy hubretry.Policy) (inboxAnswer, hubretry.Stats, error) {
	return hubretry.Do(ctx, policy, func(ctx context.Context) (inboxAnswer, int, error) {
		attemptCtx, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		var reader io.Reader
		if c.body != nil {
			reader = bytes.NewReader(c.body)
		}
		response, err := client.do(attemptCtx, c.method, c.path, c.query, reader)
		if err != nil {
			return inboxAnswer{}, 0, err
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
		if err != nil {
			return inboxAnswer{}, response.StatusCode, err
		}
		return inboxAnswer{status: response.StatusCode, body: raw}, response.StatusCode, nil
	}, nil)
}

// reportHubRetries prints the one line a command owes when its hub calls were
// sent again. It goes to stderr so the command's stdout stays as it was.
func reportHubRetries(stderr io.Writer, stats hubretry.Stats) {
	if stats.Retries > 0 {
		fmt.Fprintln(stderr, stats.Line())
	}
}

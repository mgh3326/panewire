package panewire

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

type inboxDeadlineClient struct {
	remaining time.Duration
}

func (c *inboxDeadlineClient) do(ctx context.Context, method, path string, query url.Values, body io.Reader) (*http.Response, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, context.DeadlineExceeded
	}
	c.remaining = time.Until(deadline)
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"lane-a":0}`))}, nil
}

func TestInboxNodeRequestHasEdgeLatencyMargin(t *testing.T) {
	client := &inboxDeadlineClient{}
	var stdout, stderr bytes.Buffer
	code := executeInbox(client, &stdout, &stderr, inboxRequest{Command: "count", Lane: "lane-a", Timeout: 60 * time.Second}, false)
	if code != ExitOK {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if client.remaining < 14*time.Second || client.remaining > 15*time.Second {
		t.Fatalf("hub request budget=%s, want about 15s to cover the measured 3–5s edge latency", client.remaining)
	}
}

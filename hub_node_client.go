package panewire

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/mgh3326/panewire/internal/hubretry"
)

// nodeHubClient sends one request with the daemon's node credential over the
// configured hub URL list. The local socket ops (inbox, lanes.put) use it so
// the node's token, Cloudflare Access pair and endpoint order stay inside the
// daemon instead of being handed to each caller.
type nodeHubClient struct {
	ctx        context.Context
	client     *HubClient
	noFailover bool
	// retry bounds how an idempotent call is sent again; the zero value is the
	// production policy (hubretry.Budget). See docs/hub-uninterrupted-calls.md.
	retry hubretry.Policy
}

// do sends the call, and sends it again on a transport error, 502 or 503 when
// nodeHubIdempotent says the call may be repeated. Any other call is sent once.
func (c nodeHubClient) do(ctx context.Context, method, path string, query url.Values, body io.Reader) (*http.Response, error) {
	var payload []byte
	if body != nil {
		payload, _ = io.ReadAll(body)
	}
	if !nodeHubIdempotent(method, path) {
		return c.send(ctx, method, path, query, payload, body != nil)
	}
	response, stats, err := hubretry.Do(ctx, c.retry, func(ctx context.Context) (*http.Response, int, error) {
		if daemonErr := c.ctx.Err(); daemonErr != nil {
			return nil, 0, daemonErr
		}
		response, err := c.send(ctx, method, path, query, payload, body != nil)
		if response == nil {
			return nil, 0, err
		}
		return response, response.StatusCode, err
	}, func(response *http.Response) {
		// A transport failure has no response to release.
		if response != nil {
			response.Body.Close()
		}
	})
	if stats.Retries > 0 {
		slog.Info("hub call retried", "method", method, "path", path, "retries", stats.Retries, "waited", stats.Waited)
	}
	return response, err
}

// nodeHubIdempotent reports whether a node→hub call may be sent again. Every
// GET, PUT and DELETE may: repeating one leaves the hub where one send would.
// Of the POSTs only the inbox marks may, because marking an item again changes
// nothing. Any other POST (a job close or reap, a node command, a wake, a burst
// request) would act twice, so it is sent once.
func nodeHubIdempotent(method, path string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete:
		return true
	case http.MethodPost:
		if path == "/v1/inbox/ack" {
			return true
		}
		if id, found := strings.CutPrefix(path, "/v1/inbox/"); found {
			if id, found = strings.CutSuffix(id, "/read"); found {
				n, err := strconv.ParseInt(id, 10, 64)
				return err == nil && n > 0
			}
		}
	}
	return false
}

// send tries each configured hub endpoint once, in order, and returns the first
// answer. A 502 or 503 is an answer: it is returned, not failed over.
func (c nodeHubClient) send(ctx context.Context, method, path string, query url.Values, payload []byte, hasBody bool) (*http.Response, error) {
	var last error
	endpoints := c.client.endpoints
	if c.noFailover && len(endpoints) > 1 {
		endpoints = endpoints[:1]
	}
	for _, endpoint := range endpoints {
		base, err := url.Parse(endpoint)
		if err != nil {
			last = err
			continue
		}
		if base.Scheme == "wss" {
			base.Scheme = "https"
		} else {
			base.Scheme = "http"
		}
		base.Path = ""
		hc := &hubOperatorClient{base: base, client: &http.Client{Transport: hubGuardedTransport{base: http.DefaultTransport}, CheckRedirect: func(*http.Request, []*http.Request) error { return errHubOperatorRedirect }}, token: c.client.token, machineID: c.client.machineID, cf: hubCFAccessEnv{ClientID: c.client.cfAccessClientID, ClientSecret: c.client.cfAccessSecret}}
		callCtx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(c.ctx, cancel)
		var reader io.Reader
		if hasBody {
			reader = bytes.NewReader(payload)
		}
		response, err := hc.do(callCtx, method, path, query, reader)
		if err == nil {
			response.Body = &nodeHubResponseBody{ReadCloser: response.Body, cancel: cancel, stop: stop}
			return response, nil
		}
		stop()
		cancel()
		last = err
		if c.ctx.Err() != nil || ctx.Err() != nil {
			break
		}
	}
	if last == nil {
		last = errors.New("hub unavailable")
	}
	return nil, last
}

// nodeHubResponseBody releases the per-endpoint call context when the caller
// closes the body.
type nodeHubResponseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
	stop   func() bool
}

func (b *nodeHubResponseBody) Close() error {
	err := b.ReadCloser.Close()
	b.stop()
	b.cancel()
	return err
}

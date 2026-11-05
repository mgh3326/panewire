package panewire

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
)

// nodeHubClient sends one request with the daemon's node credential over the
// configured hub URL list. The local socket ops (inbox, lanes.put) use it so
// the node's token, Cloudflare Access pair and endpoint order stay inside the
// daemon instead of being handed to each caller.
type nodeHubClient struct {
	ctx    context.Context
	client *HubClient
}

func (c nodeHubClient) do(ctx context.Context, method, path string, query url.Values, body io.Reader) (*http.Response, error) {
	var payload []byte
	if body != nil {
		payload, _ = io.ReadAll(body)
	}
	var last error
	for _, endpoint := range c.client.endpoints {
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
		if body != nil {
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

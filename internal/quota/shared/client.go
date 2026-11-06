// Package shared is the node's side of handoffkeep's shared quota store: one
// account's latest reading, the lease that serialises its measurement, the
// observation a measurement publishes and the host presence record.
//
// The wire is the one pacelever speaks (docs/quota.md), so readers of either
// writer's observations keep working.
package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	defaultTimeout = 5 * time.Second
	responseLimit  = 16 << 20
	// KindAuto is the kind of a provider measurement.
	KindAuto = "auto"
)

// Bucket is one published window. The scope is "account" or "model".
type Bucket struct {
	Window   string     `json:"window"`
	UsedPct  float64    `json:"used_pct"`
	ResetsAt *time.Time `json:"resets_at,omitempty"`
	Scope    string     `json:"scope"`
	Model    string     `json:"model,omitempty"`
}

// Observation is a stored reading. ConfirmedAt is set by the store when a later
// measurement republished the same values; a reading is valid as of the later
// of MeasuredAt and ConfirmedAt.
type Observation struct {
	ID          int64      `json:"id"`
	Kind        string     `json:"kind"`
	Window      string     `json:"window,omitempty"`
	MeasuredAt  time.Time  `json:"measured_at"`
	ConfirmedAt *time.Time `json:"confirmed_at,omitempty"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	Plan        string     `json:"plan,omitempty"`
	Buckets     []Bucket   `json:"buckets"`
}

// Latest is the store's answer for one account; Observation is nil when none
// has been published.
type Latest struct {
	Observation *Observation `json:"observation"`
}

// Lease is the answer to a lease request: who holds the account's measurement
// lease, until when, and whether the caller got it.
type Lease struct {
	Holder    string    `json:"holder"`
	ExpiresAt time.Time `json:"expires_at"`
	Acquired  bool      `json:"acquired"`
}

// Presence says which host holds an account, for placement.
type Presence struct {
	HostID     string    `json:"host_id"`
	AccountKey string    `json:"account_key"`
	LastSeen   time.Time `json:"last_seen"`
}

// StatusError is a non-2xx answer. Its text never carries the response body.
type StatusError struct{ Status int }

func (e *StatusError) Error() string {
	return fmt.Sprintf("handoffkeep HTTP %d (response withheld)", e.Status)
}

// Client talks to handoffkeep's /v1/quota endpoints.
type Client struct {
	// BaseURL is the handoffkeep origin, e.g. https://hk.example.
	BaseURL string
	// Auth puts the bearer token (and any Access pair) on a request.
	Auth func(*http.Request)
	// HTTP is the transport; nil is a 5 s client that refuses redirects.
	HTTP *http.Client
}

func accountPath(key, suffix string) string { return "/v1/quota/accounts/" + key + "/" + suffix }

// Latest reads the account's latest reading.
func (c Client) Latest(ctx context.Context, key string) (Latest, error) {
	var out Latest
	err := c.send(ctx, http.MethodGet, accountPath(key, "latest"), nil, &out)
	return out, err
}

// Lease asks for the account's measurement lease for ttlSeconds.
func (c Client) Lease(ctx context.Context, key, holder string, ttlSeconds int) (Lease, error) {
	var out Lease
	err := c.send(ctx, http.MethodPost, accountPath(key, "lease"), map[string]any{"holder": holder, "ttl_s": ttlSeconds}, &out)
	return out, err
}

// Publish stores a reading under the held lease. ConfirmedAt is the store's to
// set, so it is never sent.
func (c Client) Publish(ctx context.Context, key, holder string, observation Observation) error {
	observation.ConfirmedAt = nil
	return c.send(ctx, http.MethodPut, accountPath(key, "observation"), map[string]any{"holder": holder, "observation": observation}, nil)
}

// Release gives the lease back.
func (c Client) Release(ctx context.Context, key, holder string) error {
	return c.send(ctx, http.MethodDelete, accountPath(key, "lease"), map[string]any{"holder": holder}, nil)
}

// PublishPresence records that a host holds an account.
func (c Client) PublishPresence(ctx context.Context, record Presence) error {
	return c.send(ctx, http.MethodPut, "/v1/quota/presence", record, nil)
}

func (c Client) send(ctx context.Context, method, path string, body, out any) error {
	var payload io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("invalid request JSON")
		}
		payload = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.BaseURL, "/")+path, payload)
	if err != nil {
		return fmt.Errorf("invalid backend request")
	}
	if c.Auth != nil {
		c.Auth(request)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "panewire")
	client := c.HTTP
	if client == nil {
		client = &http.Client{Timeout: defaultTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("handoffkeep request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return &StatusError{Status: response.StatusCode}
	}
	if out != nil && json.NewDecoder(io.LimitReader(response.Body, responseLimit)).Decode(out) != nil {
		return fmt.Errorf("handoffkeep response is not valid JSON")
	}
	return nil
}

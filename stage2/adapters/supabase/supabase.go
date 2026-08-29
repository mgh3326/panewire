// Package supabase implements the stage2 adapter boundary with PostgREST/RPC
// HTTPS calls. It intentionally uses net/http rather than a Supabase SDK so
// core stays SDK-free and contract tests can run against httptest.Server.
package supabase

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mgh3326/panewire/stage2/core"
)

type Config struct {
	BaseURL               string
	AccessToken           string
	CredentialPath        string
	HTTPClient            *http.Client
	Visibility            time.Duration
	AllowInsecureForTests bool
}

type Adapter struct {
	baseURL    *url.URL
	access     string
	httpClient *http.Client
	visibility time.Duration
}

func New(cfg Config) (*Adapter, error) {
	u, err := url.Parse(cfg.BaseURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("invalid Supabase base URL")
	}
	if u.Scheme != "https" && !cfg.AllowInsecureForTests {
		return nil, fmt.Errorf("Supabase adapter requires HTTPS")
	}
	if cfg.AccessToken == "" && cfg.CredentialPath != "" {
		credential, err := LoadAccessToken(cfg.CredentialPath)
		if err != nil {
			return nil, err
		}
		cfg.AccessToken = credential
	}
	client := cfg.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	visibility := cfg.Visibility
	if visibility <= 0 {
		visibility = 30 * time.Second
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	return &Adapter{baseURL: u, access: cfg.AccessToken, httpClient: client, visibility: visibility}, nil
}

// LoadAccessToken is opt-in: no adapter constructor probes a default home
// path. This keeps tests and offline operation from ever touching a real
// ~/.config/panewire directory unless an explicit caller-authorized path is
// supplied.
func LoadAccessToken(file string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve credential home: %w", err)
	}
	root := filepath.Join(home, ".config", "panewire")
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	absFile, err := filepath.Abs(file)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(absRoot, absFile)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == "." {
		return "", fmt.Errorf("credential path is outside ~/.config/panewire")
	}
	info, err := os.Lstat(absFile)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return "", fmt.Errorf("credential file must be a regular mode-0600 file")
	}
	data, err := os.ReadFile(absFile)
	if err != nil {
		return "", err
	}
	if value := strings.TrimSpace(string(data)); value != "" {
		return value, nil
	}
	return "", fmt.Errorf("credential file is empty")
}

type publishRequest struct {
	Envelope   core.Envelope `json:"envelope"`
	PayloadB64 string        `json:"payload_b64"`
}

type publishResponse struct {
	MessageID  string    `json:"message_id"`
	DeliveryID string    `json:"delivery_id"`
	AcceptedAt time.Time `json:"accepted_at"`
	Duplicate  bool      `json:"duplicate"`
}

func (a *Adapter) Publish(ctx context.Context, env core.Envelope, reader core.PayloadReader) (core.PublishReceipt, error) {
	data, err := io.ReadAll(io.LimitReader(reader, core.MaxInlineBytes+1))
	if err != nil {
		return core.PublishReceipt{}, err
	}
	if len(data) > core.MaxInlineBytes || int64(len(data)) != env.Payload.SizeBytes {
		return core.PublishReceipt{}, fmt.Errorf("Supabase publish inline size is invalid")
	}
	var out publishResponse
	if err := a.call(ctx, http.MethodPost, "/rest/v1/panewire_queue?on_conflict=delivery_id", publishRequest{Envelope: env, PayloadB64: base64.StdEncoding.EncodeToString(data)}, &out); err != nil {
		return core.PublishReceipt{}, err
	}
	if out.MessageID == "" {
		out.MessageID = env.MessageID
	}
	if out.DeliveryID == "" {
		out.DeliveryID = env.DeliveryID
	}
	if out.AcceptedAt.IsZero() {
		out.AcceptedAt = time.Now().UTC()
	}
	return core.PublishReceipt{MessageID: out.MessageID, DeliveryID: out.DeliveryID, AcceptedAt: out.AcceptedAt, Duplicate: out.Duplicate}, nil
}

type claimRequest struct {
	DestinationMachineID string `json:"destination_machine_id"`
	VisibilitySeconds    int64  `json:"visibility_seconds"`
}

type claimRow struct {
	Token                string          `json:"token"`
	DestinationMachineID string          `json:"destination_machine_id"`
	VisibilityDeadline   time.Time       `json:"visibility_deadline"`
	Envelope             json.RawMessage `json:"envelope"`
}

func (a *Adapter) Receive(ctx context.Context, destination core.Destination, handler core.DeliveryHandler) error {
	var rows []claimRow
	if err := a.call(ctx, http.MethodPost, "/rest/v1/rpc/panewire_claim", claimRequest{DestinationMachineID: destination.MachineID, VisibilitySeconds: int64(a.visibility.Seconds())}, &rows); err != nil {
		return err
	}
	var first error
	for _, row := range rows {
		env, err := decodeEnvelope(row.Envelope)
		if err != nil {
			// Preserve neither the raw unknown field nor the raw envelope. A
			// synthetic schema-poison envelope gives core enough safe metadata to
			// write a terminal reject and ack this claim without fetching bytes.
			if err := handler(ctx, core.Delivery{Envelope: poisonEnvelope(row), Token: core.OpaqueDeliveryToken(row.Token), DestinationMachineID: row.DestinationMachineID, VisibilityDeadline: row.VisibilityDeadline}); err != nil && first == nil {
				first = err
			}
			continue
		}
		if err := handler(ctx, core.Delivery{Envelope: env, Token: core.OpaqueDeliveryToken(row.Token), DestinationMachineID: row.DestinationMachineID, VisibilityDeadline: row.VisibilityDeadline}); err != nil && first == nil {
			first = err
		}
	}
	return first
}

type fetchRequest struct {
	Token string `json:"token"`
}

type fetchResponse struct {
	PayloadB64 string `json:"payload_b64"`
}

func (a *Adapter) FetchPayload(ctx context.Context, delivery core.Delivery, limit int64) (core.PayloadReader, error) {
	var out fetchResponse
	if err := a.call(ctx, http.MethodPost, "/rest/v1/rpc/panewire_fetch_payload", fetchRequest{Token: string(delivery.Token)}, &out); err != nil {
		return nil, err
	}
	data, err := base64.StdEncoding.DecodeString(out.PayloadB64)
	if err != nil {
		return nil, fmt.Errorf("invalid Supabase payload encoding")
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("Supabase payload exceeds caller limit")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

type ackRequest struct {
	Token       string              `json:"token"`
	Disposition core.AckDisposition `json:"disposition"`
}

func (a *Adapter) Ack(ctx context.Context, token core.OpaqueDeliveryToken, disposition core.AckDisposition) error {
	return a.call(ctx, http.MethodPost, "/rest/v1/rpc/panewire_ack", ackRequest{Token: string(token), Disposition: disposition}, nil)
}

func (a *Adapter) Health(ctx context.Context) (core.TransportHealth, error) {
	if err := a.call(ctx, http.MethodGet, "/rest/v1/", nil, nil); err != nil {
		return core.TransportHealth{Healthy: false, Detail: "Supabase unavailable"}, err
	}
	return core.TransportHealth{Healthy: true, Detail: "Supabase PostgREST/RPC"}, nil
}

func (a *Adapter) call(ctx context.Context, method, endpoint string, input any, output any) error {
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	u := *a.baseURL
	if strings.Contains(endpoint, "?") {
		parts := strings.SplitN(endpoint, "?", 2)
		u.Path = strings.TrimSuffix(u.Path, "/") + parts[0]
		u.RawQuery = parts[1]
	} else {
		u.Path = strings.TrimSuffix(u.Path, "/") + endpoint
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if a.access != "" {
		req.Header.Set("Authorization", "Bearer "+a.access)
	}
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("Supabase HTTP request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Do not return response text: it can contain untrusted fields or a
		// reflected credential and must not reach logs/audit metadata.
		return fmt.Errorf("Supabase HTTP status %d", resp.StatusCode)
	}
	if output == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 2<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return fmt.Errorf("decode Supabase response: %w", err)
	}
	return nil
}

func decodeEnvelope(raw json.RawMessage) (core.Envelope, error) {
	var env core.Envelope
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&env); err != nil {
		return core.Envelope{}, fmt.Errorf("decode transport envelope: %w", err)
	}
	return env, nil
}

func poisonEnvelope(row claimRow) core.Envelope {
	sum := sha256.Sum256(row.Envelope)
	id := "poison-" + hex.EncodeToString(sum[:])
	return core.Envelope{
		MessageID:  id,
		DeliveryID: id,
		Destination: core.Destination{
			MachineID:      row.DestinationMachineID,
			InboxNamespace: "__rejected__",
			LogicalPath:    "rejected-" + hex.EncodeToString(sum[:8]),
		},
		Expect: core.Expectation{MachineID: row.DestinationMachineID},
	}
}

var _ core.Transport = (*Adapter)(nil)

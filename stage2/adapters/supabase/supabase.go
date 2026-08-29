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
	"sync"
	"time"

	"github.com/mgh3326/panewire/stage2/core"
)

type Config struct {
	BaseURL               string
	AccessToken           string
	RefreshToken          string
	APIKey                string
	Schema                string
	CredentialPath        string
	HTTPClient            *http.Client
	Visibility            time.Duration
	AllowInsecureForTests bool
}

type Adapter struct {
	baseURL    *url.URL
	mu         sync.RWMutex
	refreshMu  sync.Mutex
	access     string
	refresh    string
	apiKey     string
	schema     string
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
	schema := cfg.Schema
	if schema == "" {
		schema = "panewire"
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	return &Adapter{baseURL: u, access: cfg.AccessToken, refresh: cfg.RefreshToken, apiKey: cfg.APIKey, schema: schema, httpClient: client, visibility: visibility}, nil
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
	Envelope   core.Envelope `json:"p_envelope"`
	PayloadB64 string        `json:"p_payload_b64"`
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
	var rows []publishResponse
	if err := a.call(ctx, http.MethodPost, "/rest/v1/rpc/panewire_publish", publishRequest{Envelope: env, PayloadB64: base64.StdEncoding.EncodeToString(data)}, &rows); err != nil {
		return core.PublishReceipt{}, err
	}
	if len(rows) != 1 {
		return core.PublishReceipt{}, fmt.Errorf("Supabase publish returned an unexpected receipt count")
	}
	out := rows[0]
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
	DestinationMachineID string `json:"p_destination_machine_id"`
	VisibilitySeconds    int64  `json:"p_visibility_seconds"`
	Limit                int64  `json:"p_limit"`
}

type claimRow struct {
	Token                string          `json:"token"`
	DestinationMachineID string          `json:"destination_machine_id"`
	VisibilityDeadline   time.Time       `json:"visibility_deadline"`
	Envelope             json.RawMessage `json:"envelope"`
}

func (a *Adapter) Receive(ctx context.Context, destination core.Destination, handler core.DeliveryHandler) error {
	var rows []claimRow
	if err := a.call(ctx, http.MethodPost, "/rest/v1/rpc/panewire_claim", claimRequest{DestinationMachineID: destination.MachineID, VisibilitySeconds: int64(a.visibility.Seconds()), Limit: 32}, &rows); err != nil {
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
	Token string `json:"p_token"`
}

type fetchResponse struct {
	PayloadB64 string `json:"payload_b64"`
}

func (a *Adapter) FetchPayload(ctx context.Context, delivery core.Delivery, limit int64) (core.PayloadReader, error) {
	var rows []fetchResponse
	if err := a.call(ctx, http.MethodPost, "/rest/v1/rpc/panewire_fetch_payload", fetchRequest{Token: string(delivery.Token)}, &rows); err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, fmt.Errorf("Supabase payload fetch returned an unexpected row count")
	}
	out := rows[0]
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
	Token       string              `json:"p_token"`
	Disposition core.AckDisposition `json:"p_disposition"`
}

func (a *Adapter) Ack(ctx context.Context, token core.OpaqueDeliveryToken, disposition core.AckDisposition) error {
	return a.call(ctx, http.MethodPost, "/rest/v1/rpc/panewire_ack", ackRequest{Token: string(token), Disposition: disposition}, nil)
}

// MessageStatus exposes no body bytes.  The operator smoke tool uses it only
// to prove that the terminal acknowledgement erased transport body storage.
type MessageStatus struct {
	State      string    `json:"state"`
	BodyErased bool      `json:"body_erased"`
	AckedAt    time.Time `json:"acked_at"`
}

type messageStatusRequest struct {
	DeliveryID string `json:"p_delivery_id"`
}

func (a *Adapter) MessageStatus(ctx context.Context, deliveryID string) (MessageStatus, error) {
	var rows []MessageStatus
	if err := a.call(ctx, http.MethodPost, "/rest/v1/rpc/panewire_message_status", messageStatusRequest{DeliveryID: deliveryID}, &rows); err != nil {
		return MessageStatus{}, err
	}
	if len(rows) != 1 {
		return MessageStatus{}, fmt.Errorf("Supabase message status returned an unexpected row count")
	}
	return rows[0], nil
}

func (a *Adapter) Health(ctx context.Context) (core.TransportHealth, error) {
	if err := a.call(ctx, http.MethodGet, "/rest/v1/", nil, nil); err != nil {
		return core.TransportHealth{Healthy: false, Detail: "Supabase unavailable"}, err
	}
	return core.TransportHealth{Healthy: true, Detail: "Supabase PostgREST/RPC"}, nil
}

func (a *Adapter) call(ctx context.Context, method, endpoint string, input any, output any) error {
	var encoded []byte
	if input != nil {
		var err error
		encoded, err = json.Marshal(input)
		if err != nil {
			return err
		}
	}
	access := a.accessToken()
	status, err := a.callOnce(ctx, method, endpoint, encoded, access, output)
	if status != http.StatusUnauthorized {
		return err
	}
	if err := a.refreshAccessToken(ctx, access); err != nil {
		return fmt.Errorf("Supabase token refresh failed")
	}
	_, err = a.callOnce(ctx, method, endpoint, encoded, a.accessToken(), output)
	return err
}

func (a *Adapter) callOnce(ctx context.Context, method, endpoint string, encoded []byte, access string, output any) (int, error) {
	var body io.Reader
	if encoded != nil {
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
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if access != "" {
		req.Header.Set("Authorization", "Bearer "+access)
	}
	if a.apiKey != "" {
		req.Header.Set("apikey", a.apiKey)
	}
	if a.schema != "" {
		if method == http.MethodGet || method == http.MethodHead {
			req.Header.Set("Accept-Profile", a.schema)
		} else {
			req.Header.Set("Content-Profile", a.schema)
		}
	}
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("Supabase HTTP request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Do not return response text: it can contain untrusted fields or a
		// reflected credential and must not reach logs/audit metadata.
		return resp.StatusCode, fmt.Errorf("Supabase HTTP status %d", resp.StatusCode)
	}
	if output == nil || resp.StatusCode == http.StatusNoContent {
		return resp.StatusCode, nil
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 2<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return resp.StatusCode, fmt.Errorf("decode Supabase response: %w", err)
	}
	return resp.StatusCode, nil
}

func (a *Adapter) accessToken() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.access
}

// refreshAccessToken serializes refreshes so concurrent sender/receiver calls
// do not rotate the same credential twice. A caller whose rejected token was
// already replaced by a peer simply retries with the newer access token.
func (a *Adapter) refreshAccessToken(ctx context.Context, rejectedAccess string) error {
	a.refreshMu.Lock()
	defer a.refreshMu.Unlock()
	a.mu.RLock()
	if a.access != rejectedAccess && a.access != "" {
		a.mu.RUnlock()
		return nil
	}
	refresh := a.refresh
	a.mu.RUnlock()
	if refresh == "" {
		return fmt.Errorf("Supabase refresh token is unavailable")
	}
	body, err := json.Marshal(map[string]string{"refresh_token": refresh})
	if err != nil {
		return err
	}
	u := *a.baseURL
	u.Path = strings.TrimSuffix(u.Path, "/") + "/auth/v1/token"
	u.RawQuery = "grant_type=refresh_token"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if a.apiKey != "" {
		req.Header.Set("apikey", a.apiKey)
	}
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("Supabase refresh request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Supabase refresh rejected")
	}
	var session struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&session); err != nil || session.AccessToken == "" {
		return fmt.Errorf("Supabase refresh response is invalid")
	}
	a.mu.Lock()
	a.access = session.AccessToken
	if session.RefreshToken != "" {
		a.refresh = session.RefreshToken
	}
	a.mu.Unlock()
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

package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	handoffkeepTimeout     = 10 * time.Second
	handoffkeepReplayLimit = 200
	// relayReplayMaxAttempts bounds how often one durable row may be
	// re-injected across hub restarts. Without it a row whose destination
	// never acknowledges is re-injected on every startup, forever.
	relayReplayMaxAttempts = 3
)

type hubHandoffkeepEnv struct {
	URL   string
	Token string
}

// loadHubHandoffkeepEnv requires an explicit mode-0600 file and never includes
// a credential value in an error, matching loadHubTelegramEnv.
func loadHubHandoffkeepEnv(path string) (hubHandoffkeepEnv, error) {
	values, err := loadMode0600Env(path)
	if err != nil {
		return hubHandoffkeepEnv{}, errors.New("hub handoffkeep env must be a regular mode-0600 file")
	}
	env := hubHandoffkeepEnv{URL: values["HANDOFFKEEP_URL"], Token: values["HANDOFFKEEP_TOKEN"]}
	if !validHandoffkeepBaseURL(env.URL) || !validHandoffkeepToken(env.Token) {
		return hubHandoffkeepEnv{}, errors.New("hub handoffkeep env is missing required values")
	}
	return env, nil
}

func validHandoffkeepToken(value string) bool {
	return value != "" && len(value) <= 512 && !strings.ContainsAny(value, "\x00\r\n\t ")
}

// validHandoffkeepBaseURL allows plaintext only where the hub already accepts
// it for its own listener: loopback and the tailnet range. Everything else
// must be https so a token never crosses a routed network in the clear.
func validHandoffkeepBaseURL(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return false
	}
	if parsed.Scheme == "https" {
		return true
	}
	if parsed.Scheme != "http" {
		return false
	}
	host := parsed.Hostname()
	ip := net.ParseIP(host)
	return host == "localhost" || (ip != nil && (ip.IsLoopback() || isTailnetIPv4(ip)))
}

// handoffkeepRelayClient is the hub's only durable dependency. The hub keeps no
// relay state of its own; this is the canonical record.
type handoffkeepRelayClient struct {
	baseURL *url.URL
	token   string
	client  *http.Client
}

func newHandoffkeepRelayClient(env hubHandoffkeepEnv, httpClient *http.Client) (*handoffkeepRelayClient, error) {
	if !validHandoffkeepBaseURL(env.URL) || !validHandoffkeepToken(env.Token) {
		return nil, errors.New("hub handoffkeep configuration is invalid")
	}
	parsed, err := url.Parse(strings.TrimSpace(env.URL))
	if err != nil {
		return nil, errors.New("hub handoffkeep configuration is invalid")
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/")
	if httpClient == nil {
		httpClient = &http.Client{Timeout: handoffkeepTimeout}
	}
	return &handoffkeepRelayClient{baseURL: parsed, token: env.Token, client: httpClient}, nil
}

// handoffkeepRelayEventRequest carries exactly the request-owned columns. The
// server rejects unknown fields, so response-only columns must not appear here.
type handoffkeepRelayEventRequest struct {
	Kind           string `json:"kind"`
	JobID          string `json:"job_id"`
	Epoch          int    `json:"epoch"`
	OwnerLane      string `json:"owner_lane"`
	Machine        string `json:"machine"`
	PaneID         string `json:"pane_id"`
	ReportPath     string `json:"report_path"`
	ReportLastLine string `json:"report_last_line"`
	Question       string `json:"question"`
	PR             string `json:"pr"`
	Head           string `json:"head"`
	Reason         string `json:"reason"`
	EventID        string `json:"event_id,omitempty"`
	Text           string `json:"text,omitempty"`
	EventTime      string `json:"event_time"`
}

type handoffkeepRelayEvent struct {
	ID             int64  `json:"id"`
	Kind           string `json:"kind"`
	JobID          string `json:"job_id"`
	Epoch          int    `json:"epoch"`
	OwnerLane      string `json:"owner_lane"`
	Machine        string `json:"machine"`
	PaneID         string `json:"pane_id"`
	ReportPath     string `json:"report_path"`
	ReportLastLine string `json:"report_last_line"`
	Question       string `json:"question"`
	PR             string `json:"pr"`
	Head           string `json:"head"`
	Reason         string `json:"reason"`
	EventID        string `json:"event_id"`
	Text           string `json:"text"`
	Attempts       int    `json:"attempts"`
	// DeliveredAt is read only by the startup replay gate. It arrives as JSON
	// null for an undelivered row, which decodes to the empty string.
	DeliveredAt string `json:"delivered_at"`
}

func (c *handoffkeepRelayClient) endpoint(path string) string {
	target := *c.baseURL
	target.Path = strings.TrimSuffix(target.Path, "/") + path
	return target.String()
}

func (c *handoffkeepRelayClient) do(ctx context.Context, method, endpoint string, body []byte) (int, []byte, error) {
	requestContext, cancel := context.WithTimeout(ctx, handoffkeepTimeout)
	defer cancel()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(requestContext, method, endpoint, reader)
	if err != nil {
		return 0, nil, errors.New("handoffkeep request failed")
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.client.Do(request)
	if err != nil {
		return 0, nil, errors.New("handoffkeep request failed")
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	return response.StatusCode, payload, nil
}

// appendEvent persists one relay event. 201 is a new row and 200 is the
// existing row for the same idempotency key; both are success.
func (c *handoffkeepRelayClient) appendEvent(ctx context.Context, event handoffkeepRelayEventRequest) (handoffkeepRelayEvent, int, error) {
	body, err := json.Marshal(event)
	if err != nil {
		return handoffkeepRelayEvent{}, 0, errors.New("handoffkeep request encoding failed")
	}
	status, payload, err := c.do(ctx, http.MethodPost, c.endpoint("/v1/relay/events"), body)
	if err != nil {
		return handoffkeepRelayEvent{}, 0, err
	}
	if status != http.StatusCreated && status != http.StatusOK {
		return handoffkeepRelayEvent{}, status, errors.New("handoffkeep rejected the relay event")
	}
	var stored handoffkeepRelayEvent
	if json.Unmarshal(payload, &stored) != nil || stored.ID < 1 {
		return handoffkeepRelayEvent{}, status, errors.New("handoffkeep returned an unusable relay event")
	}
	return stored, status, nil
}

func (c *handoffkeepRelayClient) markDelivered(ctx context.Context, id int64, machine, pane string) error {
	if id < 1 {
		return errors.New("handoffkeep relay event id is invalid")
	}
	body, err := json.Marshal(struct {
		Machine string `json:"machine"`
		Pane    string `json:"pane"`
	}{Machine: machine, Pane: pane})
	if err != nil {
		return errors.New("handoffkeep request encoding failed")
	}
	status, _, err := c.do(ctx, http.MethodPost, c.endpoint("/v1/relay/events/"+strconv.FormatInt(id, 10)+"/delivered"), body)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return errors.New("handoffkeep rejected the relay delivery")
	}
	return nil
}

func (c *handoffkeepRelayClient) listUndelivered(ctx context.Context, lane string, limit int) ([]handoffkeepRelayEvent, error) {
	if limit <= 0 {
		limit = handoffkeepReplayLimit
	}
	query := url.Values{}
	query.Set("undelivered", "1")
	query.Set("limit", strconv.Itoa(limit))
	if lane != "" {
		query.Set("lane", lane)
	}
	status, payload, err := c.do(ctx, http.MethodGet, c.endpoint("/v1/relay/events")+"?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, errors.New("handoffkeep rejected the relay event query")
	}
	var result struct {
		Events []handoffkeepRelayEvent `json:"events"`
	}
	if json.Unmarshal(payload, &result) != nil {
		return nil, errors.New("handoffkeep returned an unusable relay event list")
	}
	return result.Events, nil
}

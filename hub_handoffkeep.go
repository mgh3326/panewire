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

	"github.com/mgh3326/panewire/internal/hkcred"
)

const (
	handoffkeepTimeout     = 10 * time.Second
	handoffkeepReplayLimit = 200
	// relayReplayMaxAttempts bounds how often one durable row may be
	// re-injected across hub restarts. Without it a row whose destination
	// never acknowledges is re-injected on every startup, forever.
	relayReplayMaxAttempts = 3
	// relayUnconfirmedMarkLane is the owner_lane of the durable marker rows
	// a relay.unconfirmed leaves behind (#1002). The slash is load-bearing:
	// the value matches neither hubAgentLabelPattern nor LaneNamePattern, so
	// no producer event and no lanes-file entry can ever name it, and no
	// marker row can ever resolve a route or count as a lane's backlog.
	relayUnconfirmedMarkLane = "hub/unconfirmed"
	// relayUnconfirmedMarkEventPrefix prefixes a marker row's event_id. The
	// rest of the value is the durable id of the row it marks.
	relayUnconfirmedMarkEventPrefix = "unconfirmed-mark-"
	// relayUnconfirmedDigestMarkSuffix ends the event_id of a marker whose row
	// was an inbox digest or escalation batch when it was marked.
	relayUnconfirmedDigestMarkSuffix = "-digest"
	// relayUnconfirmedRetireReason is the replay-retired suffix stamped on a
	// marked row once its one labelled replay has been attempted.
	relayUnconfirmedRetireReason = "unconfirmed"
	// relayUnconfirmedInboxRetireReason retires a marked row whose lane inbox
	// item is already delivered, read, acked or superseded: the receiver has
	// the event, so its one labelled replay would only repeat it.
	relayUnconfirmedInboxRetireReason = "inbox-delivered"
	// relayDigestNotReplayedRetireReason retires a marked row that is an
	// inbox digest or escalation batch: its count is stale after a restart, so
	// the still-unread items are released and escalated afresh instead.
	relayDigestNotReplayedRetireReason = "digest-not-replayed"
	// relayUnconfirmedMarkRetireReason retires a marker row whose original
	// row is already closed, exhausted, or gone.
	relayUnconfirmedMarkRetireReason = "unconfirmed-mark"
)

type hubHandoffkeepEnv struct {
	URL   string
	Token string
	// CFID and CFSecret are an optional Cloudflare Access service-token pair
	// for a handoffkeep behind CF Access. Both or neither.
	CFID     string
	CFSecret string
}

// withoutCFRedirects returns httpClient unchanged when no CF Access pair is
// configured. With a pair it returns a copy that never follows a redirect:
// net/http strips Authorization on a cross-host redirect but forwards custom
// headers such as CF-Access-Client-Secret, and handoffkeep does not redirect.
func (env hubHandoffkeepEnv) withoutCFRedirects(httpClient *http.Client) *http.Client {
	if env.CFID == "" {
		return httpClient
	}
	copied := *httpClient
	copied.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copied
}

// setAuth puts the bearer token and, when configured, the CF Access pair on a
// handoffkeep request. Every hub/node handoffkeep client uses it, so a node
// whose handoffkeep sits behind CF Access is not answered 403.
func (env hubHandoffkeepEnv) setAuth(request *http.Request) {
	request.Header.Set("Authorization", "Bearer "+env.Token)
	if env.CFID != "" {
		request.Header.Set("CF-Access-Client-Id", env.CFID)
		request.Header.Set("CF-Access-Client-Secret", env.CFSecret)
	}
}

// loadHubHandoffkeepEnv requires an explicit mode-0600 file and never includes
// a credential value in an error, like loadMode0600Env. It reads only
// files the operator named: the hub's --handoffkeep-env and a
// node's spawn.json handoffkeep_env. The bearer is
// HANDOFFKEEP_TOKEN_SVC_PANEWIRE, else the file's legacy key
// (hkcred.ExplicitFileToken), so files written before hk 3570 keep working.
// A node on defaults never comes here; see hubSpawnConfig.handoffkeepEnv.
func loadHubHandoffkeepEnv(path string) (hubHandoffkeepEnv, error) {
	values, err := loadMode0600Env(path)
	if err != nil {
		return hubHandoffkeepEnv{}, errors.New("hub handoffkeep env must be a regular mode-0600 file")
	}
	env := hubHandoffkeepEnv{URL: values[hkcred.URLKey], Token: hkcred.ExplicitFileToken(values), CFID: values[hkcred.CFIDKey], CFSecret: values[hkcred.CFSecretKey]}
	if err := env.validate(); err != nil {
		return hubHandoffkeepEnv{}, err
	}
	return env, nil
}

// handoffkeepEnv is the node's hk endpoint and credential under its spawn
// config. A node on defaults resolves exactly as hkcred.Load does — the
// HANDOFFKEEP_URL + HANDOFFKEEP_TOKEN_SVC_PANEWIRE environment pair, else
// hk-svc.env with the svc key only — and an unconfigured one is refused with
// the typed *hkcred.UnconfiguredError. A file spawn.json names reads through
// loadHubHandoffkeepEnv and keeps the HANDOFFKEEP_TOKEN fallback.
func (config hubSpawnConfig) handoffkeepEnv() (hubHandoffkeepEnv, error) {
	if config.HandoffkeepEnvDefault {
		creds, err := hkcred.Load()
		if err != nil {
			return hubHandoffkeepEnv{}, err
		}
		env := hubHandoffkeepEnv{URL: creds.URL, Token: creds.Token, CFID: creds.CFID, CFSecret: creds.CFSecret}
		if err := env.validate(); err != nil {
			return hubHandoffkeepEnv{}, err
		}
		return env, nil
	}
	if config.HandoffkeepEnv == "" {
		return hubHandoffkeepEnv{}, errors.New("handoffkeep env unset")
	}
	return loadHubHandoffkeepEnv(config.HandoffkeepEnv)
}

// validate applies the hub's endpoint and token rules to a resolved
// credential. Its errors carry no value.
func (env hubHandoffkeepEnv) validate() error {
	if !validHandoffkeepBaseURL(env.URL) || !validHandoffkeepToken(env.Token) {
		return errors.New("hub handoffkeep env is missing required values")
	}
	if (env.CFID == "") != (env.CFSecret == "") || (env.CFID != "" && (!validHandoffkeepToken(env.CFID) || !validHandoffkeepToken(env.CFSecret) || !strings.HasPrefix(strings.ToLower(strings.TrimSpace(env.URL)), "https://"))) {
		return errors.New("hub handoffkeep env has an incomplete or invalid CF Access pair")
	}
	return nil
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
	env     hubHandoffkeepEnv
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
	return &handoffkeepRelayClient{baseURL: parsed, env: env, client: env.withoutCFRedirects(httpClient)}, nil
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
	// DeliveredTo is handoffkeep's delivered_to ("machine/pane"). A row the
	// replay gate already retired carries hub/replay-retired:<reason> here;
	// the hub reads it so a row that still lists as undelivered is not
	// retired and announced a second time (#658).
	DeliveredTo string `json:"delivered_to"`
	// ReceivedAt is handoffkeep's own insert time. Only the replay age gate
	// reads it; a row that lacks it has no provable age and is never aged out.
	ReceivedAt string `json:"received_at,omitempty"`
	// EventTime is the event's own timestamp from the original POST. The
	// #1002 unconfirmed replay label reads it to name the first send; a row
	// written without one falls back to ReceivedAt.
	EventTime string `json:"event_time,omitempty"`
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
	c.env.setAuth(request)
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

// checkLaneEventContract probes whether the server accepts lane.event relay
// traffic — handoffkeep schema v7 — without writing a durable row. Servers old
// enough to predate lane.event reject the kind filter itself with a 4xx; a
// server that accepts it answers 200. The hub reads the status only; the page
// body is irrelevant at limit=1.
func (c *handoffkeepRelayClient) checkLaneEventContract(ctx context.Context) (int, error) {
	status, _, err := c.do(ctx, http.MethodGet, c.endpoint("/v1/relay/events")+"?kind=lane.event&limit=1", nil)
	return status, err
}

// handoffkeepContractError is only for the read-only startup capability probe:
// an unsupported kind filter or missing route indicates a contract gap.
func handoffkeepContractError(status int) bool {
	return status == http.StatusBadRequest || status == http.StatusNotFound
}

// listUndelivered reads one durable cursor page. kind is optional for legacy
// startup replay; lane.event registration replay always supplies it so older
// job rows cannot hide a directly addressed notification behind the page cap.
func (c *handoffkeepRelayClient) listUndelivered(ctx context.Context, lane, kind string, afterID int64, limit int) ([]handoffkeepRelayEvent, error) {
	if limit <= 0 {
		limit = handoffkeepReplayLimit
	}
	query := url.Values{}
	query.Set("undelivered", "1")
	query.Set("limit", strconv.Itoa(limit))
	if lane != "" {
		query.Set("lane", lane)
	}
	if kind != "" {
		query.Set("kind", kind)
	}
	if afterID > 0 {
		query.Set("after_id", strconv.FormatInt(afterID, 10))
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

// handoffkeepTask is the hk task field the drain-wake count reads: the ID
// that pages the list.
type handoffkeepTask struct {
	ID int64 `json:"id"`
}

// listTasks reads one page of hk tasks for project in state, in ID order after
// afterID. Drain wake pages until a short page (hub_drain_wake.go).
func (c *handoffkeepRelayClient) listTasks(ctx context.Context, project, state string, afterID int64, limit int) ([]handoffkeepTask, error) {
	query := url.Values{}
	query.Set("project", project)
	query.Set("state", state)
	query.Set("limit", strconv.Itoa(limit))
	if afterID > 0 {
		query.Set("after_id", strconv.FormatInt(afterID, 10))
	}
	status, payload, err := c.do(ctx, http.MethodGet, c.endpoint("/v1/tasks")+"?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, errors.New("handoffkeep rejected the task query")
	}
	var result struct {
		Tasks []handoffkeepTask `json:"tasks"`
	}
	if json.Unmarshal(payload, &result) != nil {
		return nil, errors.New("handoffkeep returned an unusable task list")
	}
	return result.Tasks, nil
}

// relayEvent reads the one row with this id, in any delivery state. The
// listing cursor is exclusive and ordered by id, so a one-row page after
// id-1 is exactly that row or proof it does not exist.
func (c *handoffkeepRelayClient) relayEvent(ctx context.Context, id int64) (handoffkeepRelayEvent, bool, error) {
	if id < 1 {
		return handoffkeepRelayEvent{}, false, errors.New("handoffkeep relay event id is invalid")
	}
	records, err := c.listRelayEvents(ctx, "", id-1, 1)
	if err != nil {
		return handoffkeepRelayEvent{}, false, err
	}
	if len(records) != 1 || records[0].ID != id {
		return handoffkeepRelayEvent{}, false, nil
	}
	return records[0], true, nil
}

// listRelayEvents reads one cursor page of durable relay rows regardless of
// delivery state. Cold retry link recovery needs the full history: a source
// row was marked delivered when its directive went out, and a retry marker
// row can sit in either state.
func (c *handoffkeepRelayClient) listRelayEvents(ctx context.Context, kind string, afterID int64, limit int) ([]handoffkeepRelayEvent, error) {
	if limit <= 0 {
		limit = handoffkeepReplayLimit
	}
	query := url.Values{}
	query.Set("limit", strconv.Itoa(limit))
	if kind != "" {
		query.Set("kind", kind)
	}
	if afterID > 0 {
		query.Set("after_id", strconv.FormatInt(afterID, 10))
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

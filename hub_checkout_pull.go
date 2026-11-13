package panewire

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"time"
)

// The hub half of checkout.pull (docs/checkout-pull.md). POST
// /v1/fleet/checkout-pull fans the op out to every known node, lists a node
// that is not connected instead of waiting for it, and writes one deploy record
// to hk. The generic node-command endpoint refuses checkout.pull, so this route
// is the only way to make the op run and the only writer of its record.

const (
	// hubCheckoutPullWait bounds the whole order. It is longer than the node's
	// own run budget (nodeCheckoutPullRunTimeout), so a node answers first.
	hubCheckoutPullWait    = 5 * time.Minute
	hubCheckoutPullMaxBody = 4 << 10

	hubCheckoutDeployKind   = "deploy"
	hubCheckoutRecordKind   = "checkout-pull"
	hubCheckoutRecordWrote  = "written"
	hubCheckoutRecordNoHK   = "unconfigured"
	hubCheckoutRecordFailed = "failed"

	hubCheckoutScopeDenied = "scope_forbidden"
	hubCheckoutNodeGone    = "node_unavailable"
)

type hubCheckoutPullRequest struct {
	Checkout string `json:"checkout"`
	Rev      string `json:"rev"`
}

// hubCheckoutPullHost is one row of the order's answer: the node's state, and
// either its result or the closed error that stood in for it.
type hubCheckoutPullHost struct {
	Machine string          `json:"machine"`
	State   string          `json:"state"`
	Error   string          `json:"error,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
}

type hubCheckoutPullRecord struct {
	Key     string `json:"key,omitempty"`
	Outcome string `json:"outcome"`
}

type hubCheckoutPullResponse struct {
	RequestID string                `json:"request_id"`
	Checkout  string                `json:"checkout"`
	Rev       string                `json:"rev,omitempty"`
	Record    hubCheckoutPullRecord `json:"record"`
	Hosts     []hubCheckoutPullHost `json:"hosts"`
}

// hubCheckoutDeployRecord is the document body written to hk: what was asked
// and what each host answered. It carries no token and no request body.
type hubCheckoutDeployRecord struct {
	Kind      string                `json:"kind"`
	Checkout  string                `json:"checkout"`
	Rev       string                `json:"rev,omitempty"`
	RequestID string                `json:"request_id"`
	At        string                `json:"at"`
	Hosts     []hubCheckoutPullHost `json:"hosts"`
}

// decodeHubCheckoutRevs validates a heartbeat's checkouts object. Any entry
// that is not a valid name, short sha and branch refuses the whole heartbeat,
// as every other heartbeat field does.
func decodeHubCheckoutRevs(raw json.RawMessage) (map[string]HubCheckoutRev, bool) {
	var checkouts map[string]HubCheckoutRev
	if json.Unmarshal(raw, &checkouts) != nil || checkouts == nil || len(checkouts) > nodeCheckoutMaxHeartbeat {
		return nil, false
	}
	for name, rev := range checkouts {
		if !nodeCheckoutNamePattern.MatchString(name) || !nodeCheckoutShortSHA.MatchString(rev.Sha) {
			return nil, false
		}
		if rev.Branch != "" && !validNodeRefName(rev.Branch) {
			return nil, false
		}
	}
	return checkouts, true
}

// cloneHubCheckoutRevs copies the map so a reader of HubNode cannot change the
// record. A nil record stays nil, which the JSON form omits.
func cloneHubCheckoutRevs(checkouts map[string]HubCheckoutRev) map[string]HubCheckoutRev {
	if checkouts == nil {
		return nil
	}
	out := make(map[string]HubCheckoutRev, len(checkouts))
	for name, rev := range checkouts {
		out[name] = rev
	}
	return out
}

// handleCheckoutPull answers the order with one row per known node. A node
// that is not connected, or that the caller may not command, is listed with its
// error; the order does not wait for it.
func (h *HubServer) handleCheckoutPull(writer http.ResponseWriter, request *http.Request) {
	if !h.authenticateScoped(request) {
		hubUnauthorized(writer)
		return
	}
	var body hubCheckoutPullRequest
	if !decodeHubJSON(writer, request, hubCheckoutPullMaxBody, &body) || !nodeCheckoutNamePattern.MatchString(body.Checkout) || (body.Rev != "" && !nodeCheckoutRevPattern.MatchString(body.Rev)) {
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	args, _ := json.Marshal(nodeCheckoutPullArgs{Checkout: body.Checkout, Rev: body.Rev})

	nodes := h.Nodes()
	rows := make([]hubCheckoutPullHost, len(nodes))
	pending := make([]*hubNodeCommandPending, len(nodes))
	requestIDs := make([]string, len(nodes))
	for index, node := range nodes {
		rows[index] = hubCheckoutPullHost{Machine: node.MachineID, State: node.State}
		if h.authorize(request, hubScopeNodeCommand+nodeCommandCheckoutPull, node.MachineID) != hubAccessGranted {
			rows[index].Error = hubCheckoutScopeDenied
			continue
		}
		wait, requestID, state, sent := h.sendNodeCommand(node.MachineID, nodeCommandCheckoutPull, args)
		rows[index].State = state
		if !sent {
			rows[index].Error = hubCheckoutNodeGone
			continue
		}
		pending[index], requestIDs[index] = wait, requestID
	}

	// One shared deadline for every node that was sent the order. Once it has
	// passed, a node that has not answered is a timeout, not a wait.
	timer := time.NewTimer(hubNodeWait(h.checkoutPullWait, hubCheckoutPullWait))
	defer timer.Stop()
	expired := false
	for index, wait := range pending {
		if wait == nil {
			continue
		}
		var result hubNodeCommandResult
		if expired {
			select {
			case result = <-wait.done:
			default:
				h.forgetNodeCommand(requestIDs[index])
				result.Error = "node_timeout"
			}
		} else {
			result = h.awaitNodeCommand(request, requestIDs[index], wait, timer.C)
			expired = result.Error == "node_timeout"
		}
		if result.Error != "" {
			rows[index].Error = result.Error
			continue
		}
		rows[index].Result = result.Result
	}

	orderID := newHubNodeCommandID()
	now := h.now().UTC()
	record := h.writeCheckoutPullRecord(request.Context(), body, orderID, rows, now)
	h.logger.Info("checkout pull", "checkout", body.Checkout, "rev", body.Rev, "hosts", len(rows), "record", record.Outcome)
	writeJSON(writer, http.StatusOK, hubCheckoutPullResponse{RequestID: orderID, Checkout: body.Checkout, Rev: body.Rev, Record: record, Hosts: rows})
}

// writeCheckoutPullRecord writes the order's one deploy record to hk, through
// the hub's handoffkeep client, under deploy/<checkout>/<date>-<rev>-<order id>.
// With no target rev the rev part is the first revision a host reached, and it
// is left out when no host reached one.
func (h *HubServer) writeCheckoutPullRecord(ctx context.Context, body hubCheckoutPullRequest, orderID string, rows []hubCheckoutPullHost, now time.Time) hubCheckoutPullRecord {
	key := hubCheckoutDeployKey(body.Checkout, body.Rev, orderID, rows, now)
	if h.handoffkeep == nil {
		return hubCheckoutPullRecord{Key: key, Outcome: hubCheckoutRecordNoHK}
	}
	encoded, err := json.Marshal(hubCheckoutDeployRecord{Kind: hubCheckoutRecordKind, Checkout: body.Checkout, Rev: body.Rev, RequestID: orderID, At: now.Format(time.RFC3339), Hosts: rows})
	if err != nil {
		return hubCheckoutPullRecord{Key: key, Outcome: hubCheckoutRecordFailed}
	}
	payload, err := json.Marshal(struct {
		Key  string `json:"key"`
		Kind string `json:"kind"`
		Body string `json:"body"`
	}{Key: key, Kind: hubCheckoutDeployKind, Body: string(encoded)})
	if err != nil {
		return hubCheckoutPullRecord{Key: key, Outcome: hubCheckoutRecordFailed}
	}
	endpoint, err := handoffkeepDocumentEndpoint(h.handoffkeep, key)
	if err != nil {
		return hubCheckoutPullRecord{Key: key, Outcome: hubCheckoutRecordFailed}
	}
	status, _, err := h.handoffkeep.do(ctx, http.MethodPut, endpoint, payload)
	if err != nil || (status != http.StatusOK && status != http.StatusCreated) {
		return hubCheckoutPullRecord{Key: key, Outcome: hubCheckoutRecordFailed}
	}
	return hubCheckoutPullRecord{Key: key, Outcome: hubCheckoutRecordWrote}
}

// handoffkeepDocumentEndpoint is the URL of one hk document. The key holds
// slashes, so it is escaped as one path segment: setting RawPath keeps %2F on
// the wire, where endpoint's unescaped path would encode the % a second time.
func handoffkeepDocumentEndpoint(client *handoffkeepRelayClient, key string) (string, error) {
	target, err := url.Parse(client.endpoint("/v1/documents/"))
	if err != nil {
		return "", err
	}
	base := target.EscapedPath()
	target.Path += key
	target.RawPath = base + url.PathEscape(key)
	return target.String(), nil
}

// hubCheckoutDeployKey names an order's record. rows are in machine order, so
// the fallback is the same on every retry of the same answers.
func hubCheckoutDeployKey(checkout, rev, orderID string, rows []hubCheckoutPullHost, now time.Time) string {
	name := now.Format("2006-01-02")
	target := rev
	if target == "" {
		target = firstCheckoutPullTarget(rows)
	}
	if target != "" {
		name += "-" + target
	}
	// The order id makes the key unique per order: two orders for the same
	// checkout, revision and day must not share, or overwrite, one document.
	return "deploy/" + checkout + "/" + name + "-" + orderID
}

// firstCheckoutPullTarget is the revision the first host that reported one
// reached, in machine order.
func firstCheckoutPullTarget(rows []hubCheckoutPullHost) string {
	sorted := append([]hubCheckoutPullHost(nil), rows...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Machine < sorted[j].Machine })
	for _, row := range sorted {
		var result struct {
			To string `json:"to"`
		}
		if len(row.Result) > 0 && json.Unmarshal(row.Result, &result) == nil && nodeShaPattern.MatchString(result.To) {
			return result.To
		}
	}
	return ""
}

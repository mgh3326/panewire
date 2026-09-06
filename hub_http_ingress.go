package panewire

import (
	"encoding/json"
	"io"
	"net/http"
)

// JSON permits every decoded byte to arrive as a six-byte \u escape. Leave
// room for the fixed fields and the bounded event ID too, so a valid 8192-byte
// sink text is never rejected merely because its JSON representation is large.
const hubRelayIngressMaxBodyBytes = 64 << 10

// hubRelayIngressRequest is intentionally distinct from the node websocket
// payload: an operator names a lane directly and there is no producer node to
// acknowledge after handoffkeep accepts the durable row.
type hubRelayIngressRequest struct {
	Kind    string `json:"kind"`
	Lane    string `json:"lane"`
	EventID string `json:"event_id"`
	Text    string `json:"text"`
	Label   string `json:"label"`
	Host    string `json:"host"`
	Deliver string `json:"deliver,omitempty"`
}

type hubRelayIngressResponse struct {
	ID      int64  `json:"id"`
	EventID string `json:"event_id"`
	Lane    string `json:"lane"`
	Routed  bool   `json:"routed"`
	Machine string `json:"machine"`
}

func decodeHubJSON(writer http.ResponseWriter, request *http.Request, maxBytes int64, target any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maxBytes))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		return false
	}
	var trailing any
	return decoder.Decode(&trailing) == io.EOF
}

func decodeHubRelayIngressRequest(writer http.ResponseWriter, request *http.Request) (hubRelayIngressRequest, bool) {
	var body hubRelayIngressRequest
	if !decodeHubJSON(writer, request, hubRelayIngressMaxBodyBytes, &body) {
		return hubRelayIngressRequest{}, false
	}
	if body.Kind != "lane.event" || !hubAgentLabelPattern.MatchString(body.Lane) || !validLaneEventID(body.EventID) || !validLaneEventText(body.Text) || !hubAgentLabelPattern.MatchString(body.Label) {
		return hubRelayIngressRequest{}, false
	}
	if body.Host == "" {
		body.Host = "hub"
	}
	if !machineIDPattern.MatchString(body.Host) {
		return hubRelayIngressRequest{}, false
	}
	if body.Deliver != "" && !validRelayDeliver(body.Deliver) {
		return hubRelayIngressRequest{}, false
	}
	return body, true
}

func writeHubJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func (h *HubServer) handleRelayIngress(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	body, valid := decodeHubRelayIngressRequest(writer, request)
	if !valid {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	event := hubJobEventPayload{
		JobID:         laneEventTransportID(body.Lane, body.EventID),
		Epoch:         1,
		OwnerLane:     body.Lane,
		EventID:       body.EventID,
		Text:          body.Text,
		Label:         body.Label,
		Host:          body.Host,
		Reason:        "http_ingress:" + body.Label,
		DeliverPolicy: body.Deliver,
	}
	result := h.relayLaneEvent(event, nil)
	if result.RejectedTooLong {
		writeHubJSON(writer, http.StatusBadRequest, map[string]string{"error": "text_too_long"})
		return
	}
	if result.PersistFailed {
		writeHubJSON(writer, http.StatusBadGateway, map[string]string{"error": "persist_failed"})
		return
	}
	if result.Duplicate {
		writeHubJSON(writer, http.StatusConflict, struct {
			Error string `json:"error"`
			ID    int64  `json:"id"`
		}{Error: "duplicate_event_id", ID: result.ID})
		return
	}
	h.broadcastRelayHTTPIngress(body, result.Routed)
	writeHubJSON(writer, http.StatusCreated, hubRelayIngressResponse{ID: result.ID, EventID: body.EventID, Lane: body.Lane, Routed: result.Routed, Machine: result.Machine})
}

func (h *HubServer) broadcastRelayHTTPIngress(request hubRelayIngressRequest, routed bool) {
	payload, _ := json.Marshal(struct {
		Label   string `json:"label"`
		Lane    string `json:"lane"`
		EventID string `json:"event_id"`
		Routed  bool   `json:"routed"`
	}{Label: request.Label, Lane: request.Lane, EventID: request.EventID, Routed: routed})
	h.broadcast(hubEvent{Kind: "relay.http_ingress", Payload: payload, Received: h.now().UTC()})
}

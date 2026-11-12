package panewire

import "time"

const nodeRelayQueueCapacity = 128

type hubNodeRelayMessage struct {
	machineID  string
	remoteAddr string
	agent      *hubAgent
	payload    []byte
}

type hubNodeRelayQueue struct {
	messages chan hubNodeRelayMessage
}

// queueNodeRelayMessage moves relay-producing node events onto one ordered
// worker per machine. A full queue refuses the connection so the node's
// unacknowledged outbox can replay the event after the worker drains.
func (h *HubServer) queueNodeRelayMessage(machineID, remoteAddr string, agent *hubAgent, payload []byte) (handled, enqueued bool) {
	message, ok := parseHubInbound(payload)
	if !ok || message.Type != "event" || !nodeRelayMessageKind(message.Kind) {
		return false, true
	}
	queued := hubNodeRelayMessage{machineID: machineID, remoteAddr: remoteAddr, agent: agent, payload: append([]byte(nil), payload...)}
	h.mu.Lock()
	if h.nodeRelayQueues == nil {
		h.nodeRelayQueues = make(map[string]*hubNodeRelayQueue)
	}
	queue := h.nodeRelayQueues[machineID]
	if queue == nil {
		queue = &hubNodeRelayQueue{messages: make(chan hubNodeRelayMessage, nodeRelayQueueCapacity)}
		h.nodeRelayQueues[machineID] = queue
		go h.runNodeRelayQueue(machineID, queue)
	}
	select {
	case queue.messages <- queued:
		h.mu.Unlock()
		return true, true
	default:
		h.mu.Unlock()
		// Do not disconnect and lose one-shot relay.dropped reports. At
		// saturation, process the durable event before reading more messages.
		h.logger.Warn("node relay queue full; processing event inline", "machine", machineID, "kind", message.Kind)
		h.handleAgentMessage(machineID, remoteAddr, agent, queued.payload)
		return true, true
	}
}

func nodeRelayMessageKind(kind string) bool {
	return kind == "lane.event" || kind == "job.completed" || kind == "job.escalate" || kind == "job.joined" || relayTerminalSignalKinds[kind] || kind == "relay.dropped" || kind == "relay.delivered" || kind == "relay.unconfirmed"
}

func (h *HubServer) runNodeRelayQueue(machineID string, queue *hubNodeRelayQueue) {
	idle := time.NewTimer(time.Minute)
	defer idle.Stop()
	for {
		select {
		case <-h.closeCtx.Done():
			return
		case <-idle.C:
			h.mu.Lock()
			record := h.nodes[machineID]
			if h.nodeRelayQueues[machineID] == queue && (record == nil || record.agent == nil) && len(queue.messages) == 0 {
				delete(h.nodeRelayQueues, machineID)
				h.mu.Unlock()
				return
			}
			h.mu.Unlock()
			idle.Reset(time.Minute)
		case message := <-queue.messages:
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(time.Minute)
			kind, _ := queuedNodeRelayKind(message.payload)
			if kind == "relay.delivered" || kind == "relay.unconfirmed" {
				// Ack authorization is bound to machine/pane in handleRelayAck;
				// it deliberately remains valid after a socket is superseded.
				h.handleAgentMessage(message.machineID, message.remoteAddr, message.agent, message.payload)
				continue
			}
			h.mu.Lock()
			record := h.nodes[machineID]
			current := record != nil && record.agent == message.agent
			h.mu.Unlock()
			if message.agent.transient {
				current = h.agentCredentialCurrent(machineID, message.agent)
			}
			if kind == "relay.dropped" || current {
				h.handleAgentMessage(message.machineID, message.remoteAddr, message.agent, message.payload)
			}
		}
	}
}

func queuedNodeRelayKind(payload []byte) (string, bool) {
	message, ok := parseHubInbound(payload)
	if !ok || message.Type != "event" {
		return "", false
	}
	return message.Kind, true
}

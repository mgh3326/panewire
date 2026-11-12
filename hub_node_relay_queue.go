package panewire

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
	h.mu.Unlock()
	queued := hubNodeRelayMessage{machineID: machineID, remoteAddr: remoteAddr, agent: agent, payload: append([]byte(nil), payload...)}
	select {
	case queue.messages <- queued:
		return true, true
	default:
		return true, false
	}
}

func nodeRelayMessageKind(kind string) bool {
	return kind == "lane.event" || kind == "job.completed" || kind == "job.escalate" || kind == "job.joined" || relayTerminalSignalKinds[kind] || kind == "relay.dropped"
}

func (h *HubServer) runNodeRelayQueue(machineID string, queue *hubNodeRelayQueue) {
	for {
		select {
		case <-h.closeCtx.Done():
			return
		case message := <-queue.messages:
			h.mu.Lock()
			record := h.nodes[machineID]
			current := record != nil && record.agent == message.agent
			h.mu.Unlock()
			if current {
				h.handleAgentMessage(message.machineID, message.remoteAddr, message.agent, message.payload)
			}
		}
	}
}

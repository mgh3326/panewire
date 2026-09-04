package panewire

import "time"

// r19aHubState contains the independent lane/acknowledgement controls.  It is
// deliberately grouped so later hub transport extensions do not need to edit
// HubServer's shared field and constructor blocks.
type r19aHubState struct {
	relayAckTimeout        time.Duration
	relayPending           map[string]relayPending
	relayTimeouts          map[string]struct{}
	acceptingOverride      map[string]string
	acceptingOverridesPath string
}

func (h *HubServer) initR19a(config HubServerConfig, overrides map[string]string) {
	h.r19a = r19aHubState{
		relayAckTimeout: config.RelayAckTimeout, relayPending: make(map[string]relayPending), relayTimeouts: make(map[string]struct{}),
		acceptingOverride: overrides, acceptingOverridesPath: config.AcceptingOverridesPath,
	}
	h.relayAckTimeout = config.RelayAckTimeout
	h.relayPending = h.r19a.relayPending
	h.relayTimeouts = h.r19a.relayTimeouts
	h.acceptingOverride = overrides
	h.acceptingOverridesPath = config.AcceptingOverridesPath
}

func (client *HubClient) initR19a(config HubClientConfig) {
	client.relayInjectTimeout = relayInjectTimeout(config.RelayInjectTimeout)
}

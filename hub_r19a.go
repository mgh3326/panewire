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

type r19aClientState struct {
	relayInjectTimeout time.Duration
}

func newR19aHubState(config HubServerConfig, overrides map[string]string) r19aHubState {
	return r19aHubState{
		relayAckTimeout: config.RelayAckTimeout, relayPending: make(map[string]relayPending), relayTimeouts: make(map[string]struct{}),
		acceptingOverride: overrides, acceptingOverridesPath: config.AcceptingOverridesPath,
	}
}

func newR19aClientState(config HubClientConfig) r19aClientState {
	return r19aClientState{relayInjectTimeout: relayInjectTimeout(config.RelayInjectTimeout)}
}

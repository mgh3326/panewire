package panewire

import "time"

// hubRelayControlState contains the independent lane/acknowledgement controls
// introduced in round R19a. It is deliberately grouped so later hub transport
// extensions do not need to edit HubServer's shared field and constructor
// blocks.
type hubRelayControlState struct {
	relayAckTimeout        time.Duration
	relayPending           map[string]relayPending
	relayTimeouts          map[string]struct{}
	relayTimeoutOrder      lruIndex[string]
	acceptingOverride      map[string]acceptingOverride
	acceptingOverridesPath string
}

func (s *hubRelayControlState) rememberRelayTimeout(jobID string) bool {
	if _, already := s.relayTimeouts[jobID]; already {
		s.relayTimeoutOrder.touch(jobID, relayTimeoutsMaxEntries)
		return false
	}
	_, evicted, overflowed := s.relayTimeoutOrder.touch(jobID, relayTimeoutsMaxEntries)
	if overflowed {
		// An eviction may allow one extra relay.unconfirmed broadcast later; it
		// never changes the durable delivery state or retry budget.
		delete(s.relayTimeouts, evicted)
	}
	s.relayTimeouts[jobID] = struct{}{}
	return true
}

func (s *hubRelayControlState) forgetRelayTimeout(jobID string) {
	delete(s.relayTimeouts, jobID)
	s.relayTimeoutOrder.forget(jobID)
}

type clientRelayControlState struct {
	relayInjectTimeout time.Duration
}

func newHubRelayControlState(config HubServerConfig, overrides map[string]acceptingOverride) hubRelayControlState {
	return hubRelayControlState{
		relayAckTimeout: config.RelayAckTimeout, relayPending: make(map[string]relayPending), relayTimeouts: make(map[string]struct{}),
		acceptingOverride: overrides, acceptingOverridesPath: config.AcceptingOverridesPath,
	}
}

func newClientRelayControlState(config HubClientConfig) clientRelayControlState {
	return clientRelayControlState{relayInjectTimeout: relayInjectTimeout(config.RelayInjectTimeout)}
}

package panewire

import (
	"errors"
	"net/http"
	"os"
	"strings"

	"github.com/coder/websocket"
)

// Hub service scopes. A client with a HUB_SCOPES_<client> line in the hub
// auth file is a service: it gets exactly the listed scopes and can never act
// as a node. The contract and the scope table live in docs/hub-scopes.md.
const (
	hubScopeNodesAgentsRead = "nodes.agents.read"
	hubScopeNodeCommand     = "node.command:" // + op, granted @<machine> or @*
	hubScopeSpawn           = "spawn"
	hubScopeWake            = "wake"
	hubScopeInboxRead       = "inbox.read"
	hubScopeInboxAck        = "inbox.ack"

	hubScopeAnyMachine = "*"
)

// hubAuthSet is one immutable load of the hub auth file. tokens holds the
// operator and node credentials exactly as before scopes existed; services
// are kept apart so a service id is never a known node, an alert node, or a
// websocket identity.
type hubAuthSet struct {
	tokens   map[string]string
	services map[string]hubServiceClient
}

type hubServiceClient struct {
	token  string
	grants []hubScopeGrant
}

// hubScopeGrant is one parsed scope: name is the scope without its resource
// ("spawn", "node.command:pane.close", "inbox.read"), resource is a machine
// id, "*" for any machine, a lane, or empty for nodes.agents.read.
type hubScopeGrant struct {
	name     string
	resource string
}

type hubCallerKind int

const (
	hubCallerNone hubCallerKind = iota
	hubCallerOperator
	hubCallerNode
	hubCallerService
)

type hubCaller struct {
	kind   hubCallerKind
	id     string
	grants []hubScopeGrant
}

// hubAccess is authorize's answer. Denied keeps the long-standing 401 for
// unauthenticated callers and for nodes on routes they never had; Forbidden
// is a 403 for an authenticated service whose scopes do not cover the
// request, so its client can report a scope error instead of an auth one.
type hubAccess int

const (
	hubAccessDenied hubAccess = iota
	hubAccessForbidden
	hubAccessGranted
)

// loadHubAuthConfig reads and validates the hub auth file as one set.
func loadHubAuthConfig(path string) (hubAuthSet, error) {
	tokens, scopes, err := readHubAuthFile(path)
	if err != nil {
		return hubAuthSet{}, err
	}
	return newHubAuthSet(tokens, scopes)
}

// readHubAuthFile returns the raw HUB_TOKEN_<id> and HUB_SCOPES_<id> lines.
// Any other key, or a key given twice, is invalid: a repeated line would
// otherwise let the last one silently replace a token or widen a scope set.
// Errors never reflect a token or a scope value.
func readHubAuthFile(path string) (tokens, scopes map[string]string, err error) {
	entries, err := loadMode0600EnvEntries(path)
	if err != nil {
		return nil, nil, errors.New("hub auth file must be a regular mode-0600 file")
	}
	tokens = make(map[string]string, len(entries))
	scopes = map[string]string{}
	for _, entry := range entries {
		if id, found := strings.CutPrefix(entry.key, "HUB_TOKEN_"); found {
			if _, repeated := tokens[id]; repeated || !machineIDPattern.MatchString(id) || !validHubToken(entry.value) {
				return nil, nil, errors.New("hub auth file is invalid")
			}
			tokens[id] = entry.value
			continue
		}
		if id, found := strings.CutPrefix(entry.key, "HUB_SCOPES_"); found {
			if _, repeated := scopes[id]; repeated || !machineIDPattern.MatchString(id) {
				return nil, nil, errors.New("hub auth file is invalid")
			}
			scopes[id] = entry.value
			continue
		}
		return nil, nil, errors.New("hub auth file is invalid")
	}
	if !validHubToken(tokens[hubOperatorMachineID]) {
		return nil, nil, errors.New("hub auth file is missing the operator token")
	}
	if _, err := newHubAuthSet(tokens, scopes); err != nil {
		return nil, nil, err
	}
	return tokens, scopes, nil
}

// newHubAuthSet splits clients with a scopes line out of tokens. A scopes
// line for operator, one without a token, a malformed scope, or a service
// token shared with any other client refuses the whole set.
func newHubAuthSet(tokens map[string]string, scopes map[string]string) (hubAuthSet, error) {
	set := hubAuthSet{tokens: make(map[string]string, len(tokens)), services: make(map[string]hubServiceClient, len(scopes))}
	for id, raw := range scopes {
		if id == hubOperatorMachineID {
			return hubAuthSet{}, errors.New("hub auth file must not scope the operator")
		}
		token, exists := tokens[id]
		if !exists {
			return hubAuthSet{}, errors.New("hub auth file has scopes without a token")
		}
		grants, err := parseHubScopes(raw)
		if err != nil {
			return hubAuthSet{}, err
		}
		set.services[id] = hubServiceClient{token: token, grants: grants}
	}
	seen := make(map[string]string, len(tokens))
	for id, token := range tokens {
		if _, service := set.services[id]; !service {
			set.tokens[id] = token
		}
		if other, shared := seen[token]; shared {
			_, idService := set.services[id]
			_, otherService := set.services[other]
			if idService || otherService {
				return hubAuthSet{}, errors.New("hub auth file reuses a service token")
			}
		}
		seen[token] = id
	}
	return set, nil
}

// parseHubScopes parses a comma-separated scope list. Each scope names its
// resource; an unknown scope or node-command op refuses the line so a typo
// never loads as a silent no-op.
func parseHubScopes(raw string) ([]hubScopeGrant, error) {
	invalid := errors.New("hub auth file has an invalid scope")
	var grants []hubScopeGrant
	for _, item := range strings.Split(raw, ",") {
		scope := strings.TrimSpace(item)
		switch {
		case scope == hubScopeNodesAgentsRead:
			grants = append(grants, hubScopeGrant{name: scope})
		case strings.HasPrefix(scope, hubScopeInboxRead+":"), strings.HasPrefix(scope, hubScopeInboxAck+":"):
			name, lane, _ := strings.Cut(scope, ":")
			if !validReportRelayLaneName(lane) {
				return nil, invalid
			}
			grants = append(grants, hubScopeGrant{name: name, resource: lane})
		default:
			at := strings.LastIndex(scope, "@")
			if at < 0 {
				return nil, invalid
			}
			name, machine := scope[:at], scope[at+1:]
			if machine != hubScopeAnyMachine && (!machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID) {
				return nil, invalid
			}
			op, isCommand := strings.CutPrefix(name, hubScopeNodeCommand)
			if !(name == hubScopeSpawn || name == hubScopeWake || (isCommand && knownNodeCommandOp(op))) {
				return nil, invalid
			}
			grants = append(grants, hubScopeGrant{name: name, resource: machine})
		}
	}
	return grants, nil
}

func hubGrantsAllow(grants []hubScopeGrant, scope, resource string) bool {
	for _, grant := range grants {
		if grant.name != scope {
			continue
		}
		if grant.resource == resource {
			return true
		}
		// "*" is a machine wildcard only; lane scopes always name one lane.
		if grant.resource == hubScopeAnyMachine && scope != hubScopeInboxRead && scope != hubScopeInboxAck {
			return true
		}
	}
	return false
}

// currentAuth answers the auth set to check a request against, reloading the
// auth file first when it changed on disk. A file that no longer loads keeps
// the previous set and logs one alert per bad version, so a broken edit can
// never lock every client out. Removing a client's lines revokes it on the
// next request.
func (h *HubServer) currentAuth() *hubAuthSet {
	if h.authPath != "" && h.reloadAuth() {
		// Outside authMu: the sweep takes h.mu, and paths holding h.mu read
		// the set with auth.Load() only.
		h.closeRevokedAgents()
	}
	return h.auth.Load()
}

// reloadAuth answers whether it stored a new auth set.
func (h *HubServer) reloadAuth() bool {
	// The stat runs outside authMu so a slow filesystem delays each request by
	// its own stat instead of queueing every request behind one.
	info, err := os.Lstat(h.authPath)
	h.authMu.Lock()
	defer h.authMu.Unlock()
	if err != nil {
		h.recordAuthReloadFailure(nil, "unreadable")
		return false
	}
	h.authFailedMissing = false
	if sameHubAuthFile(h.authObserved, info) || sameHubAuthFile(h.authFailed, info) {
		return false
	}
	set, err := loadHubAuthConfig(h.authPath)
	if err != nil {
		h.recordAuthReloadFailure(info, "invalid")
		return false
	}
	// Alert nodes are fixed at startup; one cannot become a service by reload
	// (it would keep a presence record and raise node-down alerts). Removing
	// its lines outright is still a revocation and loads.
	for machine := range h.alertNodes {
		if _, service := set.services[machine]; service {
			h.recordAuthReloadFailure(info, "alert_node_scoped")
			return false
		}
	}
	h.auth.Store(&set)
	h.authObserved, h.authFailed = info, nil
	h.logger.Info("hub auth reloaded", "nodes", len(set.tokens)-1, "services", len(set.services))
	return true
}

// agentCredentialCurrent answers whether the token a node websocket opened
// with is still that machine's node token. A machine whose lines were
// removed, whose token rotated, or that became a service fails it: a service
// is never a node, not even on a socket opened before the reload.
func (h *HubServer) agentCredentialCurrent(machineID string, agent *hubAgent) bool {
	token, node := h.auth.Load().tokens[machineID]
	return node && hubTokenMatches(token, agent.credential)
}

// closeRevokedAgents closes every registered node websocket whose credential
// the current auth set no longer holds. Closing ends its read loop, which
// disconnects the record as any dropped connection would.
func (h *HubServer) closeRevokedAgents() {
	var revoked []*hubAgent
	var machines []string
	h.mu.Lock()
	for machineID, record := range h.nodes {
		if record.agent != nil && !h.agentCredentialCurrent(machineID, record.agent) {
			revoked = append(revoked, record.agent)
			machines = append(machines, machineID)
		}
	}
	h.mu.Unlock()
	for index, agent := range revoked {
		h.logger.Warn("hub closed a node websocket whose credential was revoked", "machine_id", machines[index])
		go func(agent *hubAgent) { _ = agent.conn.Close(websocket.StatusPolicyViolation, "credential revoked") }(agent)
	}
}

// closeRevokedAgent is the read loop's half of the sweep: it disconnects the
// record before closing, so no push is routed to the socket meanwhile.
func (h *HubServer) closeRevokedAgent(machineID string, agent *hubAgent) {
	h.logger.Warn("hub closed a node websocket whose credential was revoked", "machine_id", machineID)
	h.disconnect(machineID, agent)
	_ = agent.conn.Close(websocket.StatusPolicyViolation, "credential revoked")
}

// recordAuthReloadFailure logs once per failing file version. The observed
// version is not advanced, so any later write — which moves the change time
// sameHubAuthFile compares — reloads.
func (h *HubServer) recordAuthReloadFailure(info os.FileInfo, failure string) {
	if info == nil && h.authFailedMissing {
		return
	}
	h.authFailed, h.authFailedMissing = info, info == nil
	h.logger.Error("hub auth reload failed; keeping the previous credentials", "alert", true, "failure", failure)
}

// sameHubAuthFile treats the file as unchanged only when its identity, mode,
// size, mtime and change time all match. The change time is set by the
// kernel on every write or chmod and cannot be restored from userspace, so an
// in-place rewrite that keeps the size and restores the mtime still reloads.
func sameHubAuthFile(previous, current os.FileInfo) bool {
	return previous != nil && os.SameFile(previous, current) && previous.ModTime().Equal(current.ModTime()) && previous.Size() == current.Size() && previous.Mode() == current.Mode() && hubFileChangeTime(previous) == hubFileChangeTime(current)
}

// authenticate identifies the caller. The operator bearer needs no machine
// header; nodes and services present X-Panewire-Machine-ID with their token.
func (h *HubServer) authenticate(request *http.Request) hubCaller {
	auth := h.currentAuth()
	bearer := hubRequestBearerToken(request)
	if hubTokenMatches(auth.tokens[hubOperatorMachineID], bearer) {
		return hubCaller{kind: hubCallerOperator, id: hubOperatorMachineID}
	}
	id := strings.TrimSpace(request.Header.Get(hubMachineIDHeader))
	if id == hubOperatorMachineID || !machineIDPattern.MatchString(id) {
		return hubCaller{}
	}
	if token, node := auth.tokens[id]; node {
		if hubTokenMatches(token, bearer) {
			return hubCaller{kind: hubCallerNode, id: id}
		}
		return hubCaller{}
	}
	if service, exists := auth.services[id]; exists && hubTokenMatches(service.token, bearer) {
		return hubCaller{kind: hubCallerService, id: id, grants: service.grants}
	}
	return hubCaller{}
}

// authorize is the single decision for scoped hub routes: the nodes reads,
// node commands, spawn, wake and the inbox. The operator may do everything;
// a node keeps exactly its pre-scope rights (its own lanes' inbox, nothing
// else here); a service gets what its scopes name for this resource.
func (h *HubServer) authorize(request *http.Request, scope, resource string) hubAccess {
	caller := h.authenticate(request)
	switch caller.kind {
	case hubCallerOperator:
		return hubAccessGranted
	case hubCallerNode:
		if (scope == hubScopeInboxRead || scope == hubScopeInboxAck) && resource != "" {
			if route, exists := loadReportRelayRoutes(h.reportRelayPath)[resource]; exists && route.Machine == caller.id {
				return hubAccessGranted
			}
		}
		return hubAccessDenied
	case hubCallerService:
		if hubGrantsAllow(caller.grants, scope, resource) {
			return hubAccessGranted
		}
		h.logger.Warn("hub scope refused", "client", caller.id, "scope", scope, "resource", resource)
		return hubAccessForbidden
	}
	return hubAccessDenied
}

// authenticateScoped admits the callers that may reach a scoped route whose
// resource is only known after the body is read: the operator and services.
// Nodes and unauthenticated callers get the same 401 as before scopes, ahead
// of any body parsing; the scope itself is checked by authorize afterwards.
func (h *HubServer) authenticateScoped(request *http.Request) bool {
	kind := h.authenticate(request).kind
	return kind == hubCallerOperator || kind == hubCallerService
}

func writeHubAccessDenied(writer http.ResponseWriter, access hubAccess) {
	if access == hubAccessForbidden {
		writeJSON(writer, http.StatusForbidden, map[string]string{"error": "scope_forbidden"})
		return
	}
	hubUnauthorized(writer)
}

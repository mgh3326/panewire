package panewire

import (
	"errors"
	"net/http"
	"os"
	"strings"
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
// Any other key is invalid. Errors never reflect a token or a scope value.
func readHubAuthFile(path string) (tokens, scopes map[string]string, err error) {
	values, err := loadMode0600Env(path)
	if err != nil {
		return nil, nil, errors.New("hub auth file must be a regular mode-0600 file")
	}
	tokens = make(map[string]string, len(values))
	scopes = map[string]string{}
	for key, value := range values {
		if id, found := strings.CutPrefix(key, "HUB_TOKEN_"); found {
			if !machineIDPattern.MatchString(id) || !validHubToken(value) {
				return nil, nil, errors.New("hub auth file is invalid")
			}
			tokens[id] = value
			continue
		}
		if id, found := strings.CutPrefix(key, "HUB_SCOPES_"); found {
			if !machineIDPattern.MatchString(id) {
				return nil, nil, errors.New("hub auth file is invalid")
			}
			scopes[id] = value
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
	if h.authPath != "" {
		h.reloadAuth()
	}
	return h.auth.Load()
}

func (h *HubServer) reloadAuth() {
	// The stat runs outside authMu so a slow filesystem delays each request by
	// its own stat instead of queueing every request behind one.
	info, err := os.Lstat(h.authPath)
	h.authMu.Lock()
	defer h.authMu.Unlock()
	if err != nil {
		h.recordAuthReloadFailure(nil, "unreadable")
		return
	}
	h.authFailedMissing = false
	if sameHubAuthFile(h.authObserved, info) || sameHubAuthFile(h.authFailed, info) {
		return
	}
	set, err := loadHubAuthConfig(h.authPath)
	if err != nil {
		h.recordAuthReloadFailure(info, "invalid")
		return
	}
	// Alert nodes are fixed at startup; one cannot become a service by reload
	// (it would keep a presence record and raise node-down alerts). Removing
	// its lines outright is still a revocation and loads.
	for machine := range h.alertNodes {
		if _, service := set.services[machine]; service {
			h.recordAuthReloadFailure(info, "alert_node_scoped")
			return
		}
	}
	h.auth.Store(&set)
	h.authObserved, h.authFailed = info, nil
	h.logger.Info("hub auth reloaded", "nodes", len(set.tokens)-1, "services", len(set.services))
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

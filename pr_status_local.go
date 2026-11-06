package panewire

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"time"
)

// `panewire pr status` without a connection asks the local node daemon, as
// `panewire inbox` does. The node holds the hub credential, so a worker holds
// none: the daemon forwards the read-only pr.status op to the PR-status host
// through the hub (docs/pr-status.md#transport).

// localPRStatusRequest is the body of a local "pr.status" op.
type localPRStatusRequest struct {
	Machine string           `json:"machine"`
	Args    nodePRStatusArgs `json:"args"`
}

// localPRStatusResult is the daemon's answer. Exactly one of Result and Why is
// set; Why is the sentence the CLI prints when it falls back to gh.
type localPRStatusResult struct {
	Result *nodePRStatusResult `json:"result,omitempty"`
	Why    string              `json:"why,omitempty"`
}

// localUnknownOperation is the daemon's answer to an op it does not have; the
// CLI reads it as "this daemon predates the op".
const localUnknownOperation = "unknown operation"

var errInvalidPRStatusRequest = errors.New("pr.status: invalid request")

const (
	prStatusLocalDialTimeout = 5 * time.Second
	// prStatusLocalSlack leaves the daemon room to report the hub's own timeout.
	prStatusLocalSlack = 5 * time.Second
)

// askPRStatusLocal returns the node daemon's answer, or nil with the reason
// the local node could not give one.
func askPRStatusLocal(socket, machine string, args nodePRStatusArgs) (*nodePRStatusResult, string) {
	if socket == "" {
		socket = socketPathFromEnv()
	}
	conn, err := net.DialTimeout("unix", socket, prStatusLocalDialTimeout)
	if err != nil {
		return nil, "local node is not running (no daemon socket)"
	}
	defer conn.Close()
	timeout := nodeReapClientTimeout + prStatusLocalSlack
	timer := time.AfterFunc(timeout, func() { _ = conn.Close() })
	defer timer.Stop()
	request := localRequest{Op: "pr.status", PRStatus: &localPRStatusRequest{Machine: machine, Args: args}, TimeoutMS: timeout.Milliseconds()}
	if json.NewEncoder(conn).Encode(request) != nil {
		return nil, "local node is not running (daemon connection failed)"
	}
	scan := bufio.NewScanner(conn)
	scan.Buffer(make([]byte, 4096), 64<<20)
	if !scan.Scan() {
		return nil, "local node is not running (daemon connection failed)"
	}
	var response struct {
		OK     bool                `json:"ok"`
		Error  string              `json:"error"`
		Result localPRStatusResult `json:"result"`
	}
	if json.Unmarshal(scan.Bytes(), &response) != nil {
		return nil, "local node answered an unreadable reply"
	}
	switch {
	case !response.OK && response.Error == localUnknownOperation:
		return nil, "local node does not support pr.status (its daemon predates it)"
	case !response.OK:
		return nil, "local node refused the pr.status request"
	case response.Result.Why != "":
		return nil, response.Result.Why
	case response.Result.Result == nil || !validNodePRStatusResult(*response.Result.Result):
		return nil, "local node answered an unusable pr.status result"
	}
	return response.Result.Result, ""
}

// handlePRStatus forwards one pr.status read to machine through the hub with
// this node's own credential. A failure to get an answer is a Why, not an op
// error, so the CLI tells it from a daemon that lacks the op.
func (d *Daemon) handlePRStatus(ctx context.Context, req *localPRStatusRequest) (any, error) {
	if req == nil || !validNodePRStatusArgs(req.Args) || !machineIDPattern.MatchString(req.Machine) || req.Machine == hubOperatorMachineID {
		return nil, &codedError{ExitUsage, errInvalidPRStatusRequest}
	}
	if d.cfg.Hub.Client == nil {
		return localPRStatusResult{Why: "local node has no hub connection"}, nil
	}
	hub := &NodeReapClient{hub: nodeHubClient{ctx: ctx, client: d.cfg.Hub.Client}}
	var result nodePRStatusResult
	err := hub.command(ctx, req.Machine, nodeCommandPRStatus, req.Args, &result)
	if why := prStatusHubFailure(err, req.Machine, result); why != "" {
		return localPRStatusResult{Why: why}, nil
	}
	return localPRStatusResult{Result: &result}, nil
}

package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"time"
)

// nodeReapClientTimeout outlasts the hub's own wait for a node's answer.
const nodeReapClientTimeout = hubNodeCommandWait + 5*time.Second

// ErrNodeReapRefused is a hub 401 or 403: the credential is unknown, or it is
// not a service whose scopes name this op on this machine.
var ErrNodeReapRefused = errors.New("hub refused the reap credential")

// NodeReapError is any other non-200 hub answer, carrying its closed error
// code ("node_unavailable", "node_timeout", "done_failed", ...).
type NodeReapError struct {
	Status int
	Code   string
}

func (e *NodeReapError) Error() string { return fmt.Sprintf("hub %d %s", e.Status, e.Code) }

// NodeReapClient is the typed caller of pane.read and job.done
// (docs/node-commands.md). It sends a service credential through the same
// guarded boundary as the operator commands, so only the approved hub origin
// ever receives it.
type NodeReapClient struct {
	hub *hubOperatorClient
}

// NewNodeReapClient binds a service client id and bearer to the hub origin.
// The Cloudflare Access pair is optional: pass both values or neither.
func NewNodeReapClient(hubURL, clientID, token, cfAccessClientID, cfAccessClientSecret string) (*NodeReapClient, error) {
	cf := hubCFAccessEnv{ClientID: cfAccessClientID, ClientSecret: cfAccessClientSecret}
	if (cf.ClientID == "") != (cf.ClientSecret == "") || (cf.ClientID != "" && (!validHubCFAccessValue(cf.ClientID) || !validHubCFAccessValue(cf.ClientSecret))) {
		return nil, errors.New("invalid Cloudflare Access pair")
	}
	return newNodeReapClient(hubURL, hubTokenEnv{MachineID: clientID, Token: token}, cf, hubCLIDeps{})
}

func newNodeReapClient(hubURL string, env hubTokenEnv, cf hubCFAccessEnv, deps hubCLIDeps) (*NodeReapClient, error) {
	if !machineIDPattern.MatchString(env.MachineID) || env.MachineID == hubOperatorMachineID || !validHubToken(env.Token) {
		return nil, errors.New("reap credential must be a service client id and token")
	}
	hub, err := newHubOperatorClient(hubURL, env.Token, cf, deps, nodeReapClientTimeout)
	if err != nil {
		return nil, err
	}
	hub.machineID = env.MachineID
	return &NodeReapClient{hub: hub}, nil
}

// PaneRead reports one pane on machine. name and cwd are the caller's snapshot
// of the pane; a pane that no longer matches answers pane-changed or pane-gone
// and returns nothing else. tailLines is 0 for no screen tail, at most 200.
func (client *NodeReapClient) PaneRead(ctx context.Context, machine, paneID, name, cwd string, tailLines int) (PaneReadResult, error) {
	var result PaneReadResult
	err := client.command(ctx, machine, nodeCommandPaneRead, nodePaneReadArgs{PaneID: paneID, Name: name, Cwd: cwd, TailLines: tailLines}, &result)
	return result, err
}

// JobDone runs `panewire done <job> --report <report>` on machine. report is the
// path pane.capture answered: <dir>/report.md or <dir>/pane-screen.txt in that
// job's directory.
func (client *NodeReapClient) JobDone(ctx context.Context, machine, job, report string) (JobDoneResult, error) {
	var result JobDoneResult
	err := client.command(ctx, machine, nodeCommandJobDone, nodeJobDoneArgs{Job: job, Report: report}, &result)
	return result, err
}

func (client *NodeReapClient) command(ctx context.Context, machine, op string, args, out any) error {
	encoded, err := json.Marshal(struct {
		Op   string `json:"op"`
		Args any    `json:"args"`
	}{Op: op, Args: args})
	if err != nil {
		return err
	}
	response, err := client.hub.do(ctx, http.MethodPost, "/v1/nodes/"+machine+"/command", nil, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	defer response.Body.Close()
	contents, err := io.ReadAll(io.LimitReader(response.Body, hubNodeCommandMaxBytes))
	if err != nil {
		return err
	}
	var body struct {
		Result json.RawMessage `json:"result"`
		Error  string          `json:"error"`
	}
	decodeErr := json.Unmarshal(contents, &body)
	switch {
	case response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden:
		return ErrNodeReapRefused
	case response.StatusCode != http.StatusOK:
		return &NodeReapError{Status: response.StatusCode, Code: body.Error}
	case decodeErr != nil || len(body.Result) == 0:
		return errors.New("hub answered without a result")
	}
	return json.Unmarshal(body.Result, out)
}

// runNodePaneReadCLI is `panewire node pane-read`; it prints the result as JSON.
func runNodePaneReadCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire node pane-read", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	connect := nodeReapFlags(flags)
	paneID := flags.String("pane-id", "", "pane id on that machine")
	name := flags.String("name", "", "expected pane name")
	cwd := flags.String("cwd", "", "expected pane working directory")
	tailLines := flags.Int("tail-lines", 0, "trailing screen lines to return, at most 200")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return ExitUsage
	}
	if _, valid := decodeNodeCommandArgs(nodeCommandPaneRead, mustNodeReapJSON(nodePaneReadArgs{PaneID: *paneID, Name: *name, Cwd: *cwd, TailLines: *tailLines})); !valid {
		return ExitUsage
	}
	return connect.run(stdout, stderr, deps, func(ctx context.Context, client *NodeReapClient, machine string) (any, error) {
		return client.PaneRead(ctx, machine, *paneID, *name, *cwd, *tailLines)
	})
}

// runNodeJobDoneCLI is `panewire node job-done`; it prints the result as JSON.
func runNodeJobDoneCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire node job-done", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	connect := nodeReapFlags(flags)
	job := flags.String("job", "", "job id")
	report := flags.String("report", "", "report path pane.capture answered")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return ExitUsage
	}
	if _, valid := decodeNodeCommandArgs(nodeCommandJobDone, mustNodeReapJSON(nodeJobDoneArgs{Job: *job, Report: *report})); !valid {
		return ExitUsage
	}
	return connect.run(stdout, stderr, deps, func(ctx context.Context, client *NodeReapClient, machine string) (any, error) {
		return client.JobDone(ctx, machine, *job, *report)
	})
}

func mustNodeReapJSON(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return encoded
}

type nodeReapConnect struct {
	hubURL, tokenPath, cfPath, machine *string
}

func nodeReapFlags(flags *flag.FlagSet) nodeReapConnect {
	return nodeReapConnect{
		hubURL:    flags.String("hub-url", "", "HTTPS hub base URL"),
		tokenPath: flags.String("hub-token-env", "", "mode-0600 service HUB_MACHINE_ID/HUB_TOKEN env file"),
		cfPath:    flags.String("hub-cf-env", "", "optional mode-0600 CF Access env file"),
		machine:   flags.String("machine", "", "machine the pane or job is on"),
	}
}

func (connect nodeReapConnect) run(stdout, stderr io.Writer, deps hubCLIDeps, call func(context.Context, *NodeReapClient, string) (any, error)) int {
	if *connect.hubURL == "" || *connect.tokenPath == "" || !machineIDPattern.MatchString(*connect.machine) || *connect.machine == hubOperatorMachineID {
		return ExitUsage
	}
	env, err := loadHubTokenEnv(*connect.tokenPath)
	if err != nil || env.MachineID == hubOperatorMachineID {
		fmt.Fprintln(stderr, "node reap rejected: invalid service token env")
		return ExitConditionInvalid
	}
	var cf hubCFAccessEnv
	if *connect.cfPath != "" {
		if cf, err = loadHubCFAccessEnv(*connect.cfPath); err != nil {
			fmt.Fprintln(stderr, "node reap rejected: invalid Cloudflare Access env")
			return ExitConditionInvalid
		}
	}
	client, err := newNodeReapClient(*connect.hubURL, env, cf, deps)
	if err != nil {
		fmt.Fprintln(stderr, "node reap rejected: invalid hub URL")
		return ExitConditionInvalid
	}
	ctx, cancel := context.WithTimeout(context.Background(), nodeReapClientTimeout)
	defer cancel()
	result, err := call(ctx, client, *connect.machine)
	var hubErr *NodeReapError
	switch {
	case errors.Is(err, ErrNodeReapRefused):
		fmt.Fprintln(stderr, "node reap refused: the credential does not cover this op and machine")
		return ExitConditionInvalid
	case errors.As(err, &hubErr):
		fmt.Fprintln(stderr, "node reap failed:", hubErr.Code)
		return ExitDaemonUnavailable
	case err != nil:
		fmt.Fprintln(stderr, "node reap unavailable")
		return ExitDaemonUnavailable
	}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		return ExitInternal
	}
	return ExitOK
}

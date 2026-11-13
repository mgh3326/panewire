package panewire

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

// `panewire jobs mine` and `panewire jobs close` are the lane's own view of
// its jobs on every machine (docs/jobs-own.md). They default to the local node
// daemon and its node credential, exactly as `panewire inbox` does, so a
// session that spawned panes on another host needs no operator token file.

const ownJobsUsage = `  panewire jobs mine --lane LANE [--json] ` + ownJobsConnectionUsage + `
  panewire jobs close --lane LANE --job JOB [--force] [--json] ` + ownJobsConnectionUsage + `
Lists or closes the jobs owned by LANE on every machine. LANE defaults to
PANEWIRE_LANE. Both default to the local node daemon and its node credential;
--hub-url and --hub-token-env reach the hub directly. close asks the job's
machine to close its pane with reap's safety rules and retires the lane route
that pointed at it: never a pane that is not the job's, never a job spawned
with --keep, and a pane that is still working or waiting at a prompt needs
--force.`

const ownJobsConnectionUsage = "[--hub-url URL --hub-token-env FILE [--hub-cf-env FILE]]"

// ownJobsCLITimeout bounds one own-jobs request. A close waits for the
// executing node, so it gets the node command bound plus the transport.
const ownJobsCLITimeout = hubNodeCommandWait + 15*time.Second

// ownJobsRequest is the daemon socket request and the CLI's own arguments.
// Command is "mine" or "close".
type ownJobsRequest struct {
	Command string `json:"command"`
	Lane    string `json:"lane,omitempty"`
	Job     string `json:"job,omitempty"`
	Force   bool   `json:"force,omitempty"`
	JSON    bool   `json:"json,omitempty"`
}

func validOwnJobsRequest(req ownJobsRequest) bool {
	switch req.Command {
	case "mine":
		return LaneNamePattern.MatchString(req.Lane) && req.Job == "" && !req.Force
	case "close":
		return LaneNamePattern.MatchString(req.Lane) && hubJobIDPattern.MatchString(req.Job)
	}
	return false
}

// runOwnJobsCLI parses and runs one `jobs mine` or `jobs close` invocation.
func runOwnJobsCLI(command string, args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire jobs "+command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	lane := flags.String("lane", os.Getenv("PANEWIRE_LANE"), "owner lane")
	job := flags.String("job", "", "job id")
	force := flags.Bool("force", false, "close a pane that is still working or blocked")
	jsonOutput := flags.Bool("json", false, "print hub response")
	hubURL := flags.String("hub-url", "", "HTTPS hub base URL")
	tokenEnvPath := flags.String("hub-token-env", "", "mode-0600 node HUB_MACHINE_ID/HUB_TOKEN env file")
	cfEnvPath := flags.String("hub-cf-env", "", "optional mode-0600 CF_ACCESS_CLIENT_ID/CF_ACCESS_CLIENT_SECRET env file")
	if err := flags.Parse(args); err != nil {
		return writeHubCLIUsage(stderr, jobsFlagReason(err), jobsUsage)
	}
	if flags.NArg() != 0 {
		return writeHubCLIUsage(stderr, "jobs: unexpected positional argument", jobsUsage)
	}
	if !LaneNamePattern.MatchString(*lane) {
		return writeHubCLIUsage(stderr, "jobs "+command+": valid --lane is required", jobsUsage)
	}
	if command == "mine" && *force {
		return writeHubCLIUsage(stderr, "jobs mine: --force belongs to jobs close", jobsUsage)
	}
	if command == "close" && !hubJobIDPattern.MatchString(*job) {
		return writeHubCLIUsage(stderr, "jobs close: valid --job is required", jobsUsage)
	}
	req := ownJobsRequest{Command: command, Lane: *lane, Job: *job, Force: *force, JSON: *jsonOutput}
	if *hubURL == "" && *tokenEnvPath == "" {
		return runOwnJobsLocal(req, stdout, stderr, deps.SocketPath)
	}
	if *hubURL == "" || *tokenEnvPath == "" {
		return writeHubCLIUsage(stderr, "jobs "+command+": --hub-url and --hub-token-env are both required", jobsUsage)
	}
	env, err := loadHubTokenEnv(*tokenEnvPath)
	if err != nil {
		fmt.Fprintln(stderr, "jobs rejected: invalid hub token env")
		return ExitConditionInvalid
	}
	var cf hubCFAccessEnv
	if *cfEnvPath != "" {
		cf, err = loadHubCFAccessEnv(*cfEnvPath)
		if err != nil {
			fmt.Fprintln(stderr, "jobs rejected: invalid Cloudflare Access env")
			return ExitConditionInvalid
		}
	}
	client, err := newHubOperatorClient(*hubURL, env.Token, cf, deps, ownJobsCLITimeout)
	if err != nil {
		fmt.Fprintln(stderr, "jobs rejected: invalid hub URL")
		return ExitConditionInvalid
	}
	if env.MachineID != hubOperatorMachineID {
		client.machineID = env.MachineID
	}
	return executeOwnJobs(client, stdout, stderr, req)
}

// executeOwnJobs runs one request over any hub transport, so the direct and
// daemon routes print and exit identically.
func executeOwnJobs(client inboxHTTPClient, stdout, stderr io.Writer, req ownJobsRequest) int {
	ctx, cancel := context.WithTimeout(context.Background(), ownJobsCLITimeout)
	defer cancel()
	if req.Command == "mine" {
		response, err := client.do(ctx, http.MethodGet, "/v1/jobs/mine", url.Values{"lane": {req.Lane}}, nil)
		if err != nil {
			fmt.Fprintln(stderr, "jobs unavailable")
			return ExitInternal
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return ownJobsRefusal(stderr, response, req.Command)
		}
		raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if err != nil || !json.Valid(raw) {
			fmt.Fprintln(stderr, "jobs: invalid hub response")
			return ExitInternal
		}
		if req.JSON {
			_, _ = stdout.Write(raw)
			return ExitOK
		}
		var body struct {
			Jobs []hubOwnJob `json:"jobs"`
		}
		if json.Unmarshal(raw, &body) != nil {
			fmt.Fprintln(stderr, "jobs: invalid hub response")
			return ExitInternal
		}
		fmt.Fprintln(stdout, "JOB_ID\tMACHINE\tPANE\tSTATE\tTASK\tLAST_SEEN")
		for _, job := range body.Jobs {
			fmt.Fprintf(stdout, "%s\t%s\t%s\t%s\t%s\t%s\n", job.JobID, job.Machine, dash(job.Pane), job.State, dash(job.Task), dash(job.LastSeen))
		}
		return ExitOK
	}
	payload, _ := json.Marshal(hubJobReapRequest{Lane: req.Lane, Job: req.Job, Force: req.Force})
	response, err := client.do(ctx, http.MethodPost, "/v1/jobs/reap", nil, bytes.NewReader(payload))
	if err != nil {
		fmt.Fprintln(stderr, "jobs unavailable")
		return ExitInternal
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return ownJobsRefusal(stderr, response, req.Command)
	}
	var result hubJobReapResult
	raw, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil || json.Unmarshal(raw, &result) != nil || result.Outcome == "" {
		fmt.Fprintln(stderr, "jobs: invalid hub response")
		return ExitInternal
	}
	if req.JSON {
		_, _ = stdout.Write(append(raw, '\n'))
		return ExitOK
	}
	line := fmt.Sprintf("OK job=%s machine=%s pane=%s outcome=%s", result.Job, result.Machine, dash(result.Pane), result.Outcome)
	for _, lane := range result.Released {
		line += " released=" + lane
	}
	fmt.Fprintln(stdout, line)
	return ExitOK
}

// ownJobsRefusal reports a hub refusal with the reason it names, so a nonzero
// exit is never silent and never guessed.
func ownJobsRefusal(stderr io.Writer, response *http.Response, command string) int {
	var body struct {
		Refusal string `json:"refusal"`
		Failure string `json:"failure"`
		Error   string `json:"error"`
	}
	raw, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	_ = json.Unmarshal(raw, &body)
	code := body.Refusal
	if code == "" {
		code = body.Error
	}
	if code == "" {
		code = body.Failure
	}
	if code == "" {
		switch response.StatusCode {
		case http.StatusUnauthorized:
			code = "hub_unauthorized"
		case http.StatusForbidden:
			code = "hub_forbidden"
		case http.StatusNotFound:
			// A hub that does not serve the route answers 404 with no reason
			// code; a job it does not know answers 404 with job_unknown.
			code = "hub_route_missing"
		default:
			code = "http_" + strconv.Itoa(response.StatusCode)
		}
	}
	reason := ownJobsReason(code)
	if response.StatusCode >= http.StatusInternalServerError {
		fmt.Fprintf(stderr, "jobs unavailable: %s (%s)\n", reason, code)
		return ExitInternal
	}
	fmt.Fprintf(stderr, "jobs %s refused: %s (%s)\n", command, reason, code)
	return jobCloseRefused
}

// ownJobsReason is the operator-readable form of a closed reason code. An
// unknown code is printed as is, never replaced by a guess.
func ownJobsReason(code string) string {
	switch code {
	case nodeJobReapJobWorking:
		return "the job is still working; use --force to close it"
	case nodeJobReapPaneBlocked:
		return "the pane is waiting at a prompt; use --force to close it"
	case nodeJobReapProtected:
		return "the job was spawned with --keep; its pane is protected"
	case nodeJobReapPaneChanged:
		return "the pane is no longer the job's"
	case nodeJobReapNotAJob:
		return "the job's machine has no directory for it; the hub's record is stale"
	case nodeJobReapOwnerMismatch, "owner_mismatch":
		return "the job belongs to another lane"
	case "lane_not_routed_to_caller", "hub_forbidden":
		return "the lane is not routed to this machine"
	case "hub_unauthorized":
		return "the hub refused this node's credential"
	case "hub_route_missing":
		return "the hub does not serve this command; upgrade the hub"
	case "job_unknown":
		return "the hub does not know the job"
	case "node_offline":
		return "the job's machine is not connected"
	case "close_unconfirmed":
		return "the job's machine did not answer; the pane may already be closed"
	case "invalid_request":
		return "the hub rejected the request"
	case "invalid_args":
		return "the job's node does not know job.reap; upgrade the nodes before the hub"
	}
	return code
}

// runOwnJobsLocal forwards one request to the local node daemon, which holds
// the node credential.
func runOwnJobsLocal(req ownJobsRequest, stdout, stderr io.Writer, socket string) int {
	if socket == "" {
		socket = socketPathFromEnv()
	}
	conn, err := net.DialTimeout("unix", socket, 5*time.Second)
	if err != nil {
		fmt.Fprintln(stderr, "jobs: daemon unavailable")
		return ExitDaemonUnavailable
	}
	defer conn.Close()
	timer := time.AfterFunc(ownJobsCLITimeout, func() { _ = conn.Close() })
	defer timer.Stop()
	if err := json.NewEncoder(conn).Encode(localRequest{Op: "jobs.own", OwnJobs: &req, TimeoutMS: ownJobsCLITimeout.Milliseconds()}); err != nil {
		fmt.Fprintln(stderr, "jobs: daemon unavailable")
		return ExitDaemonUnavailable
	}
	scan := bufio.NewScanner(conn)
	scan.Buffer(make([]byte, 4096), 64<<20)
	if !scan.Scan() {
		fmt.Fprintln(stderr, "jobs: daemon unavailable")
		return ExitDaemonUnavailable
	}
	var resp struct {
		OK     bool            `json:"ok"`
		Code   int             `json:"code"`
		Error  string          `json:"error"`
		Result localTextResult `json:"result"`
	}
	if json.Unmarshal(scan.Bytes(), &resp) != nil {
		fmt.Fprintln(stderr, "jobs: invalid daemon response")
		return ExitInternal
	}
	if !resp.OK {
		fmt.Fprintln(stderr, resp.Error)
		return resp.Code
	}
	_, _ = io.WriteString(stdout, resp.Result.Stdout)
	_, _ = io.WriteString(stderr, resp.Result.Stderr)
	return resp.Result.Code
}

// handleOwnJobs runs one own-jobs request with the daemon's node credential.
func (d *Daemon) handleOwnJobs(ctx context.Context, req *ownJobsRequest) (any, error) {
	if req == nil || !validOwnJobsRequest(*req) {
		return nil, &codedError{ExitUsage, errors.New("jobs: invalid request")}
	}
	if d.cfg.Hub.Client == nil {
		return nil, &codedError{ExitDaemonUnavailable, errors.New("jobs: daemon has no hub client")}
	}
	var out, errOut bytes.Buffer
	// A close is a write: an uncertain answer must not be replayed at the
	// second endpoint, since the first may already have closed the pane.
	client := nodeHubClient{ctx: ctx, client: d.cfg.Hub.Client, noFailover: req.Command == "close"}
	code := executeOwnJobs(client, &out, &errOut, *req)
	return localTextResult{Code: code, Stdout: out.String(), Stderr: errOut.String()}, nil
}

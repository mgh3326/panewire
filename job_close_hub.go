package panewire

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const nodeCommandJobClose = "job.close"
const jobCloseHubSource = "panewire job close via hub"

type hubJobCloseRequest struct {
	Job      string `json:"job"`
	Lane     string `json:"lane"`
	Outcome  string `json:"outcome"`
	Reason   string `json:"reason"`
	Override bool   `json:"override"`
}
type hubJobCloseResult struct {
	Record  jobCloseRecord `json:"record"`
	Name    string         `json:"name"`
	Refusal string         `json:"refusal"`
	Failure string         `json:"failure"`
}

func (r hubJobCloseRequest) options() (jobCloseOptions, error) {
	args := []string{r.Job, "--lane", r.Lane, "--outcome", r.Outcome, "--reason", r.Reason}
	if r.Override {
		args = append(args, "--operator-override")
	}
	return parseJobCloseArgs(args)
}
func (c *jobCLI) closeRemote(o jobCloseOptions) int {
	if o.hubURL == "" {
		o.hubURL = os.Getenv("PANEWIRE_HUB_URL")
	}
	if o.tokenEnv == "" {
		o.tokenEnv = os.Getenv("PANEWIRE_HUB_TOKEN_ENV")
	}
	if o.cfEnv == "" {
		o.cfEnv = os.Getenv("PANEWIRE_HUB_CF_ENV")
	}
	if o.hubURL == "" && o.tokenEnv == "" {
		return c.closeThroughHub(o)
	}
	if o.hubURL == "" || o.tokenEnv == "" {
		return c.exit(jobDie("close: --hub-url and --hub-token-env are required"))
	}
	env, err := loadHubTokenEnv(o.tokenEnv)
	if err != nil {
		return c.exit(&jobExit{code: ExitConditionInvalid, message: "close: invalid hub token env"})
	}
	var cf hubCFAccessEnv
	if o.cfEnv != "" {
		cf, err = loadHubCFAccessEnv(o.cfEnv)
		if err != nil {
			return c.exit(&jobExit{code: ExitConditionInvalid, message: "close: invalid Cloudflare env"})
		}
	}
	client, err := newHubOperatorClient(o.hubURL, env.Token, cf, hubCLIDeps{}, nodeReapClientTimeout)
	if err != nil {
		return c.exit(&jobExit{code: ExitConditionInvalid, message: "close: invalid hub URL"})
	}
	if env.MachineID != hubOperatorMachineID {
		client.machineID = env.MachineID
	}
	req := hubJobCloseRequest{o.job, o.lane, o.outcome, o.reason, o.override}
	result, err := requestHubJobClose(context.Background(), client, req)
	if err != nil {
		return c.exit(&jobExit{code: ExitInternal, message: "close: close_unconfirmed"})
	}
	return c.printHubClose(o, result)
}

func (c *jobCLI) closeThroughHub(o jobCloseOptions) int {
	req := hubJobCloseRequest{o.job, o.lane, o.outcome, o.reason, o.override}
	conn, err := net.DialTimeout("unix", c.socket, 5*time.Second)
	if err != nil {
		return c.exit(jobDie("close: no events directory for job %s", o.job))
	}
	defer conn.Close()
	timeout := nodeReapClientTimeout + 5*time.Second
	timer := time.AfterFunc(timeout, func() { _ = conn.Close() })
	defer timer.Stop()
	if json.NewEncoder(conn).Encode(localRequest{Op: "job.close", JobClose: &req, TimeoutMS: timeout.Milliseconds()}) != nil {
		return c.exit(&jobExit{code: ExitInternal, message: "close: close_unconfirmed"})
	}
	scan := bufio.NewScanner(conn)
	if !scan.Scan() {
		return c.exit(&jobExit{code: ExitInternal, message: "close: close_unconfirmed"})
	}
	var response struct {
		OK     bool              `json:"ok"`
		Code   int               `json:"code"`
		Error  string            `json:"error"`
		Result hubJobCloseResult `json:"result"`
	}
	if json.Unmarshal(scan.Bytes(), &response) != nil {
		return c.exit(jobDie("close: invalid daemon response"))
	}
	if !response.OK {
		if response.Code == 0 {
			return c.exit(jobDie("close: invalid daemon response"))
		}
		return c.exit(&jobExit{code: response.Code, message: response.Error})
	}
	return c.printHubClose(o, response.Result)
}

func (c *jobCLI) printHubClose(o jobCloseOptions, result hubJobCloseResult) int {
	if result.Failure != "" {
		return c.exit(&jobExit{code: ExitInternal, message: "close: " + result.Failure})
	}
	if result.Refusal != "" {
		return c.exit(&jobExit{code: jobCloseRefused, message: "close refused: " + result.Refusal})
	}
	record := result.Record
	if result.Name == "" || record.Kind != jobCloseKind || record.JobID != o.job {
		return c.exit(jobDie("close: no events directory for job %s", o.job))
	}
	line := fmt.Sprintf("OK job=%s kind=%s outcome=%s closed_by=%s owner_lane=%s", record.JobID, record.Kind, record.Outcome, record.ClosedBy, record.OwnerLane)
	if record.Override != "" {
		line += " override=" + record.Override
	}
	fmt.Fprintf(c.stdout, "%s record=%s\n", line, result.Name)
	return ExitOK
}

func requestHubJobClose(ctx context.Context, client inboxHTTPClient, req hubJobCloseRequest) (hubJobCloseResult, error) {
	data, _ := json.Marshal(req)
	response, err := client.do(ctx, http.MethodPost, "/v1/jobs/close", nil, bytes.NewReader(data))
	if err != nil {
		return hubJobCloseResult{}, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized {
		return hubJobCloseResult{Failure: "hub_unauthorized"}, nil
	}
	var result hubJobCloseResult
	if json.NewDecoder(io.LimitReader(response.Body, hubNodeCommandMaxBytes)).Decode(&result) != nil {
		if response.StatusCode == http.StatusForbidden {
			return hubJobCloseResult{Failure: "hub_forbidden"}, nil
		}
		return result, errors.New("invalid hub response")
	}
	if response.StatusCode != http.StatusOK && result.Refusal == "" && result.Failure == "" {
		return result, errors.New("hub request failed")
	}
	return result, nil
}

func (d *Daemon) handleJobClose(ctx context.Context, req *hubJobCloseRequest) (any, error) {
	if req == nil {
		return nil, &codedError{ExitUsage, errors.New("close: invalid request")}
	}
	if _, err := req.options(); err != nil {
		return nil, &codedError{ExitUsage, errors.New("close: invalid request")}
	}
	if d.cfg.Hub.Client == nil {
		return nil, &codedError{ExitDaemonUnavailable, errors.New("close: daemon has no hub connection")}
	}
	result, err := requestHubJobClose(ctx, nodeHubClient{ctx: ctx, client: d.cfg.Hub.Client, noFailover: true}, *req)
	if err != nil {
		return hubJobCloseResult{Failure: "close_unconfirmed"}, nil
	}
	return result, nil
}
func (h *HubServer) handleJobClose(w http.ResponseWriter, r *http.Request) {
	caller := h.authenticate(r)
	if caller.kind == hubCallerNone {
		hubUnauthorized(w)
		return
	}
	var req hubJobCloseRequest
	if !decodeHubJSON(w, r, hubNodeCommandMaxBytes, &req) {
		return
	}
	if _, err := req.options(); err != nil {
		writeJSON(w, http.StatusBadRequest, hubJobCloseResult{Refusal: "invalid_request"})
		return
	}
	if req.Override && caller.kind != hubCallerOperator {
		writeJSON(w, http.StatusForbidden, hubJobCloseResult{Refusal: "override_requires_operator"})
		return
	}
	switch caller.kind {
	case hubCallerNode:
		route, found := loadReportRelayRoutes(h.reportRelayPath)[req.Lane]
		if !found || route.Machine != caller.id {
			writeJSON(w, http.StatusForbidden, hubJobCloseResult{Refusal: "lane_not_routed_to_caller"})
			return
		}
	case hubCallerService:
		if !hubGrantsAllow(caller.grants, hubScopeJobsClose, req.Lane) {
			writeJSON(w, http.StatusForbidden, hubJobCloseResult{Refusal: "scope_forbidden"})
			return
		}
	}
	h.mu.Lock()
	job := h.jobs[req.Job]
	var node, owner string
	if job != nil {
		node, owner = job.Node, job.OwnerLane
	}
	h.mu.Unlock()
	if job == nil {
		writeJSON(w, http.StatusNotFound, hubJobCloseResult{Refusal: "job_unknown"})
		return
	}
	if !req.Override && (owner == "" || owner != req.Lane) {
		writeJSON(w, http.StatusForbidden, hubJobCloseResult{Refusal: "owner_mismatch"})
		return
	}
	raw, _ := json.Marshal(req)
	pending, id, state, sent := h.sendNodeCommand(node, nodeCommandJobClose, raw)
	if !sent {
		if state == "send_failed" {
			writeJSON(w, http.StatusServiceUnavailable, hubJobCloseResult{Failure: "close_unconfirmed"})
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, hubJobCloseResult{Refusal: "node_offline"})
		return
	}
	timer := time.NewTimer(hubNodeWait(h.nodeCommandWait, hubNodeCommandWait))
	defer timer.Stop()
	result := h.awaitNodeCommand(r, id, pending, timer.C)
	if result.Error == "node_lost" || result.Error == "node_timeout" {
		writeJSON(w, http.StatusServiceUnavailable, hubJobCloseResult{Failure: "close_unconfirmed"})
		return
	}
	if result.Error != "" {
		writeJSON(w, http.StatusServiceUnavailable, hubJobCloseResult{Refusal: result.Error})
		return
	}
	var closeResult hubJobCloseResult
	if json.Unmarshal(result.Result, &closeResult) != nil {
		writeJSON(w, http.StatusBadGateway, hubJobCloseResult{Refusal: "invalid_node_response"})
		return
	}
	writeJSON(w, http.StatusOK, closeResult)
}
func (client *HubClient) nodeJobClose(req hubJobCloseRequest) (any, string) {
	options, err := req.options()
	if err != nil {
		return nil, "invalid_args"
	}
	if client.jobsInboxRoot == "" {
		return nil, "jobs_root_unset"
	}
	dir := filepath.Join(client.jobsInboxRoot, "jobs", options.job)
	events := filepath.Join(dir, "events")
	if info, err := os.Stat(events); err != nil || !info.IsDir() {
		return hubJobCloseResult{Refusal: "job_unknown"}, ""
	}
	c := &jobCLI{now: client.nowUTC}
	record, name, err := c.publishJobClose(dir, events, options, jobCloseHubSource)
	if err != nil {
		var failure *jobExit
		if errors.As(err, &failure) && failure.code == jobCloseRefused {
			return hubJobCloseResult{Refusal: strings.TrimPrefix(failure.message, "close refused: ")}, ""
		}
		return hubJobCloseResult{Failure: "close_failed"}, ""
	}
	return hubJobCloseResult{Record: record, Name: name}, ""
}

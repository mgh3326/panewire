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
}

func (r hubJobCloseRequest) options() (jobCloseOptions, error) {
	args := []string{r.Job, "--lane", r.Lane, "--outcome", r.Outcome, "--reason", r.Reason}
	if r.Override {
		args = append(args, "--operator-override")
	}
	return parseJobCloseArgs(args)
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
		return c.exit(jobDie("close: daemon unavailable"))
	}
	scan := bufio.NewScanner(conn)
	if !scan.Scan() {
		return c.exit(jobDie("close: daemon unavailable"))
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
	if response.Result.Refusal != "" {
		return c.exit(&jobExit{code: jobCloseRefused, message: "close refused: " + response.Result.Refusal})
	}
	record := response.Result.Record
	if response.Result.Name == "" || record.Kind != jobCloseKind || record.JobID != o.job {
		return c.exit(jobDie("close: no events directory for job %s", o.job))
	}
	line := fmt.Sprintf("OK job=%s kind=%s outcome=%s closed_by=%s owner_lane=%s", record.JobID, record.Kind, record.Outcome, record.ClosedBy, record.OwnerLane)
	if record.Override != "" {
		line += " override=" + record.Override
	}
	fmt.Fprintf(c.stdout, "%s record=%s\n", line, response.Result.Name)
	return ExitOK
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
	data, _ := json.Marshal(req)
	response, err := (nodeHubClient{ctx: ctx, client: d.cfg.Hub.Client}).do(ctx, http.MethodPost, "/v1/jobs/close", nil, bytes.NewReader(data))
	if err != nil {
		return nil, &codedError{ExitDaemonUnavailable, errors.New("close: hub unavailable")}
	}
	defer response.Body.Close()
	var result hubJobCloseResult
	if json.NewDecoder(io.LimitReader(response.Body, hubNodeCommandMaxBytes)).Decode(&result) != nil {
		return nil, &codedError{ExitInternal, errors.New("close: invalid hub response")}
	}
	if response.StatusCode != http.StatusOK && result.Refusal == "" {
		return nil, &codedError{ExitDaemonUnavailable, errors.New("close: hub request failed")}
	}
	return result, nil
}
func (h *HubServer) handleJobClose(w http.ResponseWriter, r *http.Request) {
	if h.authenticate(r).kind != hubCallerNode {
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
	pending, id, _, sent := h.sendNodeCommand(node, nodeCommandJobClose, raw)
	if !sent {
		writeJSON(w, http.StatusServiceUnavailable, hubJobCloseResult{Refusal: "node_offline"})
		return
	}
	timer := time.NewTimer(hubNodeWait(h.nodeCommandWait, hubNodeCommandWait))
	defer timer.Stop()
	result := h.awaitNodeCommand(r, id, pending, timer.C)
	if result.Error == "node_lost" {
		result.Error = "node_offline"
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
		return hubJobCloseResult{Refusal: "close_refused"}, ""
	}
	return hubJobCloseResult{Record: record, Name: name}, ""
}

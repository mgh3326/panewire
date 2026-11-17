package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
)

const consultWorkerRefusal = "consult request refused: ask your design lane by ESC with an hk packet"

type localConsultRequest struct {
	Request hubConsultRequest `json:"request"`
	JSON    bool              `json:"json"`
	Session string            `json:"session,omitempty"`
}

func resolveConsultPane(pid int, parents func(int) (int, error), shells map[string]int) string {
	seen := map[int]bool{}
	for pid > 0 && !seen[pid] {
		seen[pid] = true
		for pane, shell := range shells {
			if pid == shell {
				return pane
			}
		}
		next, err := parents(pid)
		if err != nil {
			break
		}
		pid = next
	}
	return ""
}

func processParent(pid int) (int, error) {
	out, err := exec.Command("ps", "-o", "ppid=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(out)))
}

func consultPane(ctx context.Context, run relayCommandRunner, pid int, parent func(int) (int, error)) (string, error) {
	raw, err := run(ctx, "pane", "list")
	if err != nil {
		return "", err
	}
	var top map[string]json.RawMessage
	if err = json.Unmarshal(raw, &top); err != nil {
		return "", err
	}
	if v := top["result"]; v != nil {
		if err = json.Unmarshal(v, &top); err != nil {
			return "", err
		}
	}
	var panes []struct {
		ID string `json:"pane_id"`
	}
	if err = json.Unmarshal(top["panes"], &panes); err != nil || panes == nil {
		return "", errors.New("cannot read herdr pane list")
	}
	shells := make(map[string]int)
	for _, p := range panes {
		if p.ID == "" {
			return "", errors.New("invalid herdr pane list")
		}
		raw, err = run(ctx, "pane", "process-info", "--pane", p.ID)
		if err != nil {
			return "", err
		}
		top = nil
		if err = json.Unmarshal(raw, &top); err != nil {
			return "", err
		}
		if v := top["result"]; v != nil {
			if err = json.Unmarshal(v, &top); err != nil {
				return "", err
			}
		}
		var info struct {
			Shell int `json:"shell_pid"`
		}
		if err = json.Unmarshal(top["process_info"], &info); err != nil || info.Shell <= 0 {
			return "", errors.New("cannot read pane process info")
		}
		shells[p.ID] = info.Shell
	}
	pane := resolveConsultPane(pid, parent, shells)
	if pane == "" {
		return "", errors.New("not inside a herdr pane")
	}
	return pane, nil
}

func (d *Daemon) handleConsult(ctx context.Context, c net.Conn, req *localConsultRequest) (any, error) {
	if req == nil {
		return nil, &codedError{ExitUsage, errors.New("consult request: invalid request")}
	}
	client := d.cfg.Hub.Client
	if client == nil {
		return nil, &codedError{ExitDaemonUnavailable, errors.New("consult request: daemon has no hub client")}
	}
	pid, err := localPeerPID(c)
	if err != nil {
		return nil, &codedError{ExitConditionInvalid, fmt.Errorf("consult request: cannot identify local caller")}
	}
	if req.Session != "" && !consultRequesterPattern.MatchString(req.Session) {
		return nil, &codedError{ExitUsage, errors.New("consult request: invalid herdr session")}
	}
	pane, err := consultPane(ctx, client.nodeHerdrRunnerForSession(req.Session), pid, processParent)
	if err != nil {
		return nil, &codedError{ExitConditionInvalid, fmt.Errorf("consult request: %w", err)}
	}
	journal, err := client.acquireSpawnJournal()
	if err != nil {
		return nil, &codedError{ExitInternal, errors.New("consult request: spawn journal unavailable")}
	}
	defer client.releaseSpawnJournal()
	// A still-executing spawn may have created a pane before writing its result.
	// Refuse until its journal entry settles so that window cannot admit it.
	executing, err := journal.executing(ctx)
	if err != nil {
		return nil, &codedError{ExitInternal, errors.New("consult request: spawn journal unavailable")}
	}
	if len(executing) != 0 {
		return nil, &codedError{ExitConditionInvalid, errors.New(consultWorkerRefusal)}
	}
	rows, err := journal.db.QueryContext(ctx, `SELECT result FROM spawn_inbox WHERE status='completed'`)
	if err != nil {
		return nil, &codedError{ExitInternal, errors.New("consult request: spawn journal unavailable")}
	}
	worker := false
	for rows.Next() {
		var raw string
		var result hubSpawnResult
		if rows.Scan(&raw) != nil || json.Unmarshal([]byte(raw), &result) != nil {
			rows.Close()
			return nil, &codedError{ExitInternal, errors.New("consult request: spawn journal unreadable")}
		}
		if result.Pane == pane {
			worker = true
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, &codedError{ExitInternal, errors.New("consult request: spawn journal unavailable")}
	}
	if worker {
		return nil, &codedError{ExitConditionInvalid, errors.New(consultWorkerRefusal)}
	}
	request := req.Request
	request.CallerPane = pane
	var out, errOut bytes.Buffer
	code := executeConsult(ctx, nodeHubClient{ctx: ctx, client: client, noFailover: true}, request, req.JSON, &out, &errOut)
	return localTextResult{Code: code, Stdout: out.String(), Stderr: errOut.String()}, nil
}

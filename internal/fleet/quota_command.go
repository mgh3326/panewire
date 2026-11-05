package fleet

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"
)

func runQuotaCommand(ctx context.Context, runner Runner, cmd Command, what string, timeout time.Duration) Result {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	r := runner.Run(ctx, cmd)
	for _, line := range strings.Split(r.Err, "\n") {
		if line != "" && !strings.HasPrefix(line, "warning:") {
			slog.Debug("pacelever diagnostic", "line", line)
		}
	}
	if ctx.Err() == context.DeadlineExceeded || r.Code == exitTimeout {
		return Result{Code: exitTimeout, Out: what + ": timed out after " + timeout.String()}
	}
	return r
}
func checkQuota(r Result, what string) error {
	if r.Code == 0 {
		return nil
	}
	if r.Code == exitTimeout {
		return fail(r.Code, "%s", r.Out)
	}
	var body struct {
		Reason string `json:"reason"`
		Error  string `json:"error"`
	}
	_ = json.Unmarshal([]byte(r.Out), &body)
	reason := body.Reason
	if reason == "" {
		reason = body.Error
	}
	if reason == "" {
		reason = "dependency failed"
	}
	return fail(r.Code, "%s: %s", what, reason)
}

func checkQuotaPlain(r Result, what string) error {
	if r.Code == 0 {
		return nil
	}
	if r.Code == exitTimeout {
		return fail(r.Code, "%s", r.Out)
	}
	summary := ""
	for _, line := range strings.Split(r.Err, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "warning:") {
			summary = line
			break
		}
	}
	return fail(r.Code, "%s: exit %d: %s", what, r.Code, summary)
}

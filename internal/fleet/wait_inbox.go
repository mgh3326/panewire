package fleet

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"time"

	"github.com/mgh3326/panewire/internal/daemonop"
)

// waitInboxDaemonBudget is the daemon's own request budget, sent as
// timeout_ms: the budget `inbox list` gives a node-daemon call (hk 4344). The
// socket read keeps laneDaemonReadGrace above it, so the daemon's answer
// arrives instead of the read timing out.
const waitInboxDaemonBudget = 15 * time.Second

// waitInboxRoute is how a wait reads the owner lane inbox. It starts on the
// daemon route and stays on the hub route — the direct connection this host's
// config names, which needs the token the wait may not have — for the rest of
// the wait once no daemon answers, so a poll never redials a daemon that is not
// running and the fallback warns once instead of on every poll.
type waitInboxRoute int

const (
	waitInboxViaDaemon waitInboxRoute = iota
	waitInboxViaHub
)

// waitInbox reads the owner lane's inbox as `panewire inbox list --lane L
// --json` reports it, through the local node daemon first (hk 3706): the
// daemon holds the node credential, so a design session without the hub token
// still reads its lane. A session that carries its own lane credential keeps
// that credential, as lane registration does (laneDaemonServes): the node
// identity is not substitutable for it. The hub route needs the credential this
// host's config names, so it is also the fallback when the daemon is absent.
// Either way the wait warns at most once.
func (a *App) waitInbox(ctx context.Context, lane string, route *waitInboxRoute, warned *bool) Result {
	if *route == waitInboxViaDaemon && !a.laneDaemonServes() {
		*route = waitInboxViaHub
	}
	if *route == waitInboxViaDaemon {
		if r, answered := a.inboxThroughDaemon(ctx, lane); answered {
			if r.Code != 0 && !*warned {
				*warned = true
				a.warn("lane inbox read failed through the local node daemon; watching local events only")
			}
			return r
		}
		// No daemon took the read: fall back to the configured hub connection,
		// the route this wait used before hk 3706, and report it in one line.
		// The route switches once, so this line cannot repeat.
		*route = waitInboxViaHub
		r := a.inboxThroughHub(ctx, lane)
		*warned = true
		if r.Code != 0 {
			a.warn("lane inbox read failed; local node daemon unavailable; watching local events only")
		} else {
			a.warn("local node daemon unavailable; reading the lane inbox through the configured hub connection")
		}
		return r
	}
	r := a.inboxThroughHub(ctx, lane)
	if r.Code != 0 && !*warned {
		*warned = true
		a.warn("lane inbox read failed; watching local events only")
	}
	return r
}

// inboxThroughHub runs `inbox list` with the connection this host's config
// names. It is the route a wait had before hk 3706.
func (a *App) inboxThroughHub(ctx context.Context, lane string) Result {
	args := append([]string{"inbox", "list", "--lane", lane, "--json"}, a.controlHubArgs(true)...)
	return a.call(ctx, "panewire", args...)
}

// inboxThroughDaemon runs one `inbox list --json` through the local node
// daemon's socket, the route `panewire inbox list` takes by default. answered
// is false when no daemon took the request: no socket, a daemon without a hub
// client, or a request the daemon never accepted. A daemon that accepted the
// request and then failed is an answer, so the wait does not read again with
// another credential.
func (a *App) inboxThroughDaemon(ctx context.Context, lane string) (Result, bool) {
	conn, err := net.DialTimeout("unix", a.laneDaemonSocket(), laneDaemonDialTimeout)
	if err != nil {
		return Result{}, false
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	_ = conn.SetDeadline(time.Now().Add(waitInboxDaemonBudget + laneDaemonReadGrace))
	request := struct {
		Op        string          `json:"op"`
		Inbox     *daemonop.Inbox `json:"inbox"`
		TimeoutMS int64           `json:"timeout_ms"`
	}{
		Op:        daemonop.InboxOp,
		Inbox:     &daemonop.Inbox{Command: "list", Lane: lane, JSON: true, Timeout: waitInboxDaemonBudget},
		TimeoutMS: waitInboxDaemonBudget.Milliseconds(),
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return Result{}, false
	}
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 4096), 64<<20)
	if !scanner.Scan() {
		return Result{Code: exitFailure, Err: "inbox: the daemon accepted the read and did not answer"}, true
	}
	var response struct {
		OK     bool   `json:"ok"`
		Code   int    `json:"code"`
		Error  string `json:"error"`
		Result struct {
			Code   int    `json:"code"`
			Stdout string `json:"stdout"`
			Stderr string `json:"stderr"`
		} `json:"result"`
	}
	if json.Unmarshal(scanner.Bytes(), &response) != nil {
		return Result{Code: exitFailure, Err: "inbox: the daemon answered invalid JSON"}, true
	}
	if !response.OK {
		// A daemon that cannot serve the op is as absent as a missing socket:
		// the configured hub connection is the fallback.
		if response.Code == daemonop.ExitDaemonUnavailable {
			return Result{}, false
		}
		code := response.Code
		if code == 0 {
			code = exitFailure
		}
		return Result{Code: code, Err: response.Error}, true
	}
	return Result{Out: response.Result.Stdout, Err: response.Result.Stderr, Code: response.Result.Code}, true
}

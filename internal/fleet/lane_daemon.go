package fleet

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/daemonop"
)

// errLaneDaemonAbsent means no local daemon served the registration, so the
// fleet-config route is the fallback.
var errLaneDaemonAbsent = errors.New("lane daemon unavailable")

const (
	laneDaemonDialTimeout = 2 * time.Second
	laneDaemonCallTimeout = 30 * time.Second
	// The daemon bounds its own hub call by the request's timeout_ms, so the
	// socket read waits a little longer and reports the daemon's answer
	// instead of timing out into the fleet-config fallback.
	laneDaemonReadGrace = 5 * time.Second
)

// laneDaemonSocket is the daemon socket this host would answer on: the
// PANEWIRE_SOCKET override, else the daemon's default under the app's home.
// Tests point Home at a temp dir, so they never reach a real daemon.
func (a *App) laneDaemonSocket() string {
	if path := os.Getenv(daemonop.SocketPathEnv); path != "" {
		return path
	}
	return daemonop.DefaultSocketPath(a.Home)
}

// laneDaemonServes reports whether the daemon's node identity is this spawn's
// lane identity. A session with its own lane credential registers with that
// credential, not with the daemon's, so it keeps the fleet-config route.
func (a *App) laneDaemonServes() bool {
	tokens, ok := a.Config.Hub["session_lanes_token_envs"].(map[string]any)
	return !ok || str(tokens, a.herdrSession()) == ""
}

// registerLaneThroughDaemon asks the local daemon to register the lane with the
// node hub client it already holds. errLaneDaemonAbsent is returned when no
// socket answers or the daemon has no hub client, leaving the fleet-config
// `lanes add` route as the fallback. Once the request is written the daemon may
// have registered the lane, so a missing or unreadable answer is a failure
// rather than a second attempt with another credential.
func (a *App) registerLaneThroughDaemon(ctx context.Context, put lanePutRequest) (laneRegistration, error) {
	if !a.laneDaemonServes() {
		return laneRegistration{}, errLaneDaemonAbsent
	}
	conn, err := net.DialTimeout("unix", a.laneDaemonSocket(), laneDaemonDialTimeout)
	if err != nil {
		return laneRegistration{}, errLaneDaemonAbsent
	}
	defer conn.Close()
	_ = conn.SetDeadline(laneDaemonDeadline(ctx, time.Now()))
	request := struct {
		Op string `json:"op"`
		daemonop.LanesPut
		TimeoutMS int64 `json:"timeout_ms"`
	}{
		Op:        daemonop.LanesPutOp,
		LanesPut:  daemonop.LanesPut{Lane: put.Lane, Machine: put.Machine, Pane: put.Pane, Parent: put.Parent, Takeover: put.Takeover},
		TimeoutMS: laneDaemonCallTimeout.Milliseconds(),
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return laneRegistration{}, errLaneDaemonAbsent
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return laneRegistration{}, fail(exitFailure, "lanes: the daemon accepted the registration but did not answer")
	}
	var response struct {
		OK     bool            `json:"ok"`
		Code   int             `json:"code"`
		Error  string          `json:"error"`
		Result json.RawMessage `json:"result"`
	}
	if json.Unmarshal([]byte(line), &response) != nil {
		return laneRegistration{}, fail(exitFailure, "lanes: the daemon accepted the registration and answered invalid JSON")
	}
	if response.OK {
		return put.answer(response.Result), nil
	}
	if response.Code == daemonop.ExitDaemonUnavailable {
		return laneRegistration{}, errLaneDaemonAbsent
	}
	detail := strings.TrimSpace(response.Error)
	if detail == "" {
		detail = "lanes unavailable"
	}
	return laneRegistration{}, fail(response.Code, "lanes: %s", detail)
}

// laneDaemonDeadline is the socket read deadline. The daemon bounds its hub
// call by the request's timeout_ms, so the read waits at least that long even
// when the caller's context is shorter: a shorter deadline would abandon a
// slow but successful registration and re-run it on the fleet-config route.
func laneDaemonDeadline(ctx context.Context, now time.Time) time.Time {
	deadline := now.Add(laneDaemonCallTimeout + laneDaemonReadGrace)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.After(deadline) {
		deadline = ctxDeadline
	}
	return deadline
}

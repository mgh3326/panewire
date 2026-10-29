package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"
)

// HubWakeConfig is the operator-owned routing table behind `panewire node
// wake`. It names which authenticated node may be asked to emit a Wake-on-LAN
// packet for each target machine. Packet contents — the target MAC and the
// UDP broadcast destination — live only in that node's own --wake-targets
// file; the wire event carries no destination, MAC, argument, or command.
type HubWakeConfig struct {
	Targets []HubWakeTarget `json:"targets"`
}

type HubWakeTarget struct {
	Machine string `json:"machine"`
	Via     string `json:"via"`
}

func (target HubWakeTarget) valid() bool {
	return machineIDPattern.MatchString(target.Machine) && machineIDPattern.MatchString(target.Via) &&
		target.Machine != hubOperatorMachineID && target.Via != hubOperatorMachineID && target.Machine != target.Via
}

func ParseHubWakeConfig(data []byte) (map[string]HubWakeTarget, error) {
	var config HubWakeConfig
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return nil, errors.New("wake config is invalid")
	}
	if len(config.Targets) == 0 || len(config.Targets) > 128 {
		return nil, errors.New("wake config is invalid")
	}
	targets := make(map[string]HubWakeTarget, len(config.Targets))
	for _, target := range config.Targets {
		if !target.valid() {
			return nil, errors.New("wake config is invalid")
		}
		if _, duplicate := targets[target.Machine]; duplicate {
			return nil, errors.New("wake config is invalid")
		}
		targets[target.Machine] = target
	}
	return targets, nil
}

func LoadHubWakeConfig(path string) (map[string]HubWakeTarget, time.Time, error) {
	if path == "" {
		return nil, time.Time{}, errors.New("wake config path is required")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, time.Time{}, errors.New("wake config must be a regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, time.Time{}, errors.New("read wake config")
	}
	targets, err := ParseHubWakeConfig(data)
	if err != nil {
		return nil, time.Time{}, err
	}
	return targets, info.ModTime(), nil
}

// NodeWakeConfig is the waker node's own allowlist. A hub wake event can only
// select one of these entries; the MAC and broadcast destination are never
// taken from the wire.
type NodeWakeConfig struct {
	Targets []NodeWakeTarget `json:"targets"`
}

type NodeWakeTarget struct {
	Machine   string `json:"machine"`
	MAC       string `json:"mac"`
	Broadcast string `json:"broadcast"`
}

func (target NodeWakeTarget) valid() bool {
	return machineIDPattern.MatchString(target.Machine) && target.Machine != hubOperatorMachineID && validNodeWakeBroadcast(target.Broadcast)
}

// validNodeWakeBroadcast accepts any UDPv4 host:port destination the local
// operator configured. Directed broadcasts and last-known unicast addresses
// are both legitimate Wake-on-LAN destinations, so only shape is checked.
func validNodeWakeBroadcast(raw string) bool {
	address, err := net.ResolveUDPAddr("udp4", raw)
	return err == nil && address.IP != nil && address.IP.To4() != nil && address.Port > 0 && address.String() == raw
}

func ParseNodeWakeConfig(data []byte) (map[string]NodeWakeTarget, error) {
	var config NodeWakeConfig
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return nil, errors.New("wake targets are invalid")
	}
	if len(config.Targets) == 0 || len(config.Targets) > 128 {
		return nil, errors.New("wake targets are invalid")
	}
	targets := make(map[string]NodeWakeTarget, len(config.Targets))
	for _, target := range config.Targets {
		if !target.valid() {
			return nil, errors.New("wake targets are invalid")
		}
		mac, err := parseHubFailoverWakeMAC(target.MAC)
		if err != nil {
			return nil, errors.New("wake targets are invalid")
		}
		if _, duplicate := targets[target.Machine]; duplicate {
			return nil, errors.New("wake targets are invalid")
		}
		targets[target.Machine] = NodeWakeTarget{Machine: target.Machine, MAC: mac.String(), Broadcast: target.Broadcast}
	}
	return targets, nil
}

func LoadNodeWakeConfig(path string) (map[string]NodeWakeTarget, error) {
	if path == "" {
		return nil, errors.New("wake targets path is required")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("wake targets must be a regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("read wake targets")
	}
	return ParseNodeWakeConfig(data)
}

// hubWakeRequestEvent is the fixed three-field shape the hub sends only to the
// configured waker node. The packet it produces is entirely node-local config.
type hubWakeRequestEvent struct {
	Type      string    `json:"type"`
	Machine   string    `json:"machine"`
	EmittedAt time.Time `json:"emitted_at"`
}

const (
	hubWakeDefaultTimeout   = 120 * time.Second
	hubWakeMaxTimeout       = 10 * time.Minute
	hubWakeRedispatch       = 15 * time.Second
	hubWakeConfirmationPoll = 20 * time.Millisecond
)

type hubWakeResult struct {
	Machine          string `json:"machine"`
	Via              string `json:"via"`
	State            string `json:"state"`
	AlreadyConnected bool   `json:"already_connected,omitempty"`
	WaitedMS         int64  `json:"waited_ms"`
}

// reloadWakeConfigLocked hot-loads the wake routing file exactly the way the
// burst policy reloads: a rejected edit keeps the last good table.
func (h *HubServer) reloadWakeConfigLocked() {
	if h.wakeConfigPath == "" {
		return
	}
	info, err := os.Stat(h.wakeConfigPath)
	if err != nil || !info.Mode().IsRegular() {
		return
	}
	targets, modTime, err := LoadHubWakeConfig(h.wakeConfigPath)
	if err != nil {
		h.logger.Warn("wake config reload rejected")
		return
	}
	h.wakeTargets, h.wakeConfigModTime = targets, modTime
}

// wakeHeartbeatFreshLocked bounds "already awake" by heartbeat recency: a
// state of connected with a heartbeat older than three keepalive intervals is
// a dead connection the sweep has not reaped yet, so the wake proceeds.
func (h *HubServer) wakeHeartbeatFreshLocked(record *hubNodeRecord, now time.Time) bool {
	return !record.lastHeartbeat.IsZero() && now.Sub(record.lastHeartbeat) <= 3*h.keepaliveInterval
}

// requestNodeWake asks the configured waker node to emit one magic packet for
// the target, then waits for the target's own authenticated heartbeat to
// return after the dispatch began. Packet delivery itself is confirmed only
// by that heartbeat; the event is redispatched on a slow cadence because UDP
// loss is ordinary.
func (h *HubServer) requestNodeWake(ctx context.Context, machine string) (hubWakeResult, string) {
	started := h.now().UTC()
	h.mu.Lock()
	h.reloadWakeConfigLocked()
	target, known := h.wakeTargets[machine]
	if !known {
		h.mu.Unlock()
		return hubWakeResult{}, "unknown_target"
	}
	if record := h.nodes[machine]; record != nil && record.agent != nil && record.state == "connected" && h.wakeHeartbeatFreshLocked(record, started) {
		h.mu.Unlock()
		return hubWakeResult{Machine: machine, Via: target.Via, State: "connected", AlreadyConnected: true}, ""
	}
	waker := h.nodes[target.Via]
	if waker == nil || waker.agent == nil || waker.state != "connected" {
		h.mu.Unlock()
		return hubWakeResult{}, "waker_unavailable"
	}
	agent := waker.agent
	h.mu.Unlock()
	agent.queueWake(hubWakeRequestEvent{Type: "wake", Machine: machine, EmittedAt: started})
	tick := time.NewTicker(hubWakeConfirmationPoll)
	defer tick.Stop()
	redispatch := time.NewTicker(hubWakeRedispatch)
	defer redispatch.Stop()
	for {
		h.mu.Lock()
		record := h.nodes[machine]
		up := record != nil && record.agent != nil && record.state == "connected" && !record.lastHeartbeat.Before(started)
		h.mu.Unlock()
		if up {
			return hubWakeResult{Machine: machine, Via: target.Via, State: "connected", WaitedMS: h.now().Sub(started).Milliseconds()}, ""
		}
		select {
		case <-ctx.Done():
			return hubWakeResult{}, "wake_timeout"
		case <-redispatch.C:
			agent.queueWake(hubWakeRequestEvent{Type: "wake", Machine: machine, EmittedAt: h.now().UTC()})
		case <-tick.C:
		}
	}
}

func (h *HubServer) handleNodeWake(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	var body struct {
		Machine string `json:"machine"`
		Timeout string `json:"timeout,omitempty"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 4096))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&body) != nil || decoder.Decode(&struct{}{}) != io.EOF || !machineIDPattern.MatchString(body.Machine) || body.Machine == hubOperatorMachineID {
		http.Error(writer, "invalid wake request", http.StatusBadRequest)
		return
	}
	timeout := hubWakeDefaultTimeout
	if body.Timeout != "" {
		parsed, err := time.ParseDuration(body.Timeout)
		if err != nil || parsed <= 0 || parsed > hubWakeMaxTimeout {
			http.Error(writer, "invalid wake request", http.StatusBadRequest)
			return
		}
		timeout = parsed
	}
	ctx, cancel := context.WithTimeout(request.Context(), timeout)
	defer cancel()
	result, reason := h.requestNodeWake(ctx, body.Machine)
	writer.Header().Set("Content-Type", "application/json")
	if reason != "" {
		status := http.StatusGatewayTimeout
		switch reason {
		case "unknown_target":
			status = http.StatusNotFound
		case "waker_unavailable":
			status = http.StatusServiceUnavailable
		}
		writer.WriteHeader(status)
		_ = json.NewEncoder(writer).Encode(map[string]string{"reason": reason})
		return
	}
	_ = json.NewEncoder(writer).Encode(result)
}

// handleHubWake is the node side of `panewire node wake`. The event may only
// select a machine this node was locally configured to wake; every packet
// field then comes from that configuration.
func (client *HubClient) handleHubWake(ctx context.Context, message hubOutboundMessage) {
	key := message.Machine + ":" + message.EmittedAt.Format(time.RFC3339Nano)
	client.wakeMu.Lock()
	if client.wakeSeen == nil {
		client.wakeSeen = make(map[string]time.Time)
	}
	if _, seen := client.wakeSeen[key]; seen {
		client.wakeMu.Unlock()
		return
	}
	client.wakeSeen[key] = message.EmittedAt
	if len(client.wakeSeen) > 256 {
		cutoff := message.EmittedAt.Add(-time.Hour)
		for stale, at := range client.wakeSeen {
			if at.Before(cutoff) {
				delete(client.wakeSeen, stale)
			}
		}
	}
	target, known := client.wakeTargets[message.Machine]
	client.wakeMu.Unlock()
	if !known {
		client.warnMessage("wake target unavailable")
		return
	}
	mac, err := parseHubFailoverWakeMAC(target.MAC)
	if err != nil {
		client.warnMessage("wake target unavailable")
		return
	}
	if err := sendHubFailoverWakePacket(ctx, target.Broadcast, mac); err != nil {
		client.warnMessage("wake packet unavailable")
	}
}

func runNodeCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	if len(args) == 0 || args[0] != "wake" {
		return ExitUsage
	}
	return runNodeWakeCLI(args[1:], stdout, stderr, deps)
}

// runNodeWakeCLI is the operator credential path for `panewire node wake`. The
// hub confirms with the target's heartbeat, so this call may legitimately take
// minutes; the transport timeout is always a little beyond the request's own
// wait bound.
func runNodeWakeCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire node wake", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	hubURL := flags.String("hub-url", "", "HTTPS hub base URL")
	tokenPath := flags.String("hub-token-env", "", "mode-0600 operator HUB_MACHINE_ID/HUB_TOKEN env file")
	cfPath := flags.String("hub-cf-env", "", "optional mode-0600 CF Access env file")
	timeout := flags.Duration("timeout", hubWakeDefaultTimeout, "wake confirmation timeout")
	if flags.Parse(args) != nil || flags.NArg() != 1 || *hubURL == "" || *tokenPath == "" || *timeout <= 0 || *timeout > hubWakeMaxTimeout {
		return ExitUsage
	}
	machine := flags.Arg(0)
	if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID {
		return ExitUsage
	}
	env, err := loadHubTokenEnv(*tokenPath)
	if err != nil || env.MachineID != hubOperatorMachineID {
		fmt.Fprintln(stderr, "node wake rejected: invalid operator token env")
		return ExitConditionInvalid
	}
	var cf hubCFAccessEnv
	if *cfPath != "" {
		cf, err = loadHubCFAccessEnv(*cfPath)
		if err != nil {
			fmt.Fprintln(stderr, "node wake rejected: invalid Cloudflare Access env")
			return ExitConditionInvalid
		}
	}
	encoded, _ := json.Marshal(struct {
		Machine string `json:"machine"`
		Timeout string `json:"timeout"`
	}{machine, timeout.String()})
	client, err := newHubOperatorClient(*hubURL, env.Token, cf, deps, *timeout+15*time.Second)
	if err != nil {
		fmt.Fprintln(stderr, "node wake rejected: invalid hub URL")
		return ExitConditionInvalid
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout+15*time.Second)
	defer cancel()
	response, err := client.do(ctx, http.MethodPost, "/v1/nodes/wake", nil, bytes.NewReader(encoded))
	if err != nil {
		fmt.Fprintln(stderr, "node wake unavailable")
		return ExitTimeout
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if response.StatusCode != http.StatusOK {
		_, _ = stdout.Write(data)
		fmt.Fprintln(stderr, "node wake rejected")
		if response.StatusCode == http.StatusGatewayTimeout {
			return ExitTimeout
		}
		return ExitConditionInvalid
	}
	_, _ = stdout.Write(data)
	return ExitOK
}

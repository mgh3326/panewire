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
	"path/filepath"
	"time"
)

// BurstPolicy is the file-backed, operator-owned source of truth for burst
// activation. Durations are expressed as whole minutes to make emergency
// adjustment safe and obvious.
type BurstPolicy struct {
	SourceMachine   string  `json:"source_machine"`
	SwapGB          float64 `json:"swap_gb"`
	Load5           float64 `json:"load5"`
	Consecutive     int     `json:"consecutive"`
	WakeVia         string  `json:"wake_via"`
	WakeMAC         string  `json:"wake_mac"`
	TargetMachine   string  `json:"target_machine"`
	IdleMinutes     int     `json:"idle_minutes"`
	CooldownMinutes int     `json:"cooldown_minutes"`
}

func (policy BurstPolicy) valid() bool {
	if !machineIDPattern.MatchString(policy.SourceMachine) || !machineIDPattern.MatchString(policy.WakeVia) || !machineIDPattern.MatchString(policy.TargetMachine) ||
		policy.SourceMachine == hubOperatorMachineID || policy.WakeVia == hubOperatorMachineID || policy.TargetMachine == hubOperatorMachineID ||
		policy.SwapGB <= 0 || policy.Load5 <= 0 || policy.Consecutive < 1 || policy.Consecutive > 10000 || policy.IdleMinutes < 1 || policy.IdleMinutes > 10080 || policy.CooldownMinutes < 0 || policy.CooldownMinutes > 10080 {
		return false
	}
	mac, err := net.ParseMAC(policy.WakeMAC)
	return err == nil && len(mac) == 6
}

func ParseBurstPolicy(data []byte) (BurstPolicy, error) {
	var policy BurstPolicy
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&policy); err != nil || decoder.Decode(&struct{}{}) != io.EOF || !policy.valid() {
		return BurstPolicy{}, errors.New("burst policy is invalid")
	}
	return policy, nil
}

func LoadBurstPolicy(path string) (BurstPolicy, time.Time, error) {
	if path == "" {
		return BurstPolicy{}, time.Time{}, errors.New("burst policy path is required")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return BurstPolicy{}, time.Time{}, errors.New("burst policy must be a regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return BurstPolicy{}, time.Time{}, errors.New("read burst policy")
	}
	policy, err := ParseBurstPolicy(data)
	if err != nil {
		return BurstPolicy{}, time.Time{}, err
	}
	return policy, info.ModTime(), nil
}

func writeBurstPolicy(path string, policy BurstPolicy) error {
	if path == "" || !policy.valid() {
		return errors.New("burst policy is invalid")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("burst policy must be a regular file")
	}
	data, err := json.MarshalIndent(policy, "", "  ")
	if err != nil {
		return errors.New("burst policy encoding failed")
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".burst-policy-")
	if err != nil {
		return errors.New("burst policy write failed")
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(info.Mode().Perm()); err == nil {
		_, err = temporary.Write(append(data, '\n'))
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil || os.Rename(temporaryName, path) != nil {
		return errors.New("burst policy write failed")
	}
	return nil
}

func formatBurstPolicy(policy BurstPolicy) string {
	data, _ := json.MarshalIndent(policy, "", "  ")
	return string(data) + "\n"
}

func runBurstCLI(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return ExitUsage
	}
	flags := flag.NewFlagSet("panewire burst "+args[0], flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("burst-policy", "", "regular JSON burst policy file")
	hubURL := flags.String("hub-url", "", "optional HTTPS hub URL for live judgment state")
	tokenEnv := flags.String("hub-token-env", "", "operator HUB_MACHINE_ID/HUB_TOKEN env file")
	cfEnv := flags.String("hub-cf-env", "", "optional Cloudflare Access env file")
	swap := flags.Float64("swap-gb", -1, "source swap threshold in GB")
	load5 := flags.Float64("load5", -1, "source load5 threshold")
	consecutive := flags.Int("consecutive", -1, "required consecutive source reports")
	idle := flags.Int("idle-min", -1, "target idle duration in minutes")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 || ((*path == "") == (*hubURL == "")) || (*hubURL != "" && *tokenEnv == "") {
		return ExitUsage
	}
	switch args[0] {
	case "show":
		if *swap >= 0 || *load5 >= 0 || *consecutive >= 0 || *idle >= 0 {
			return ExitUsage
		}
		if *hubURL != "" {
			return runBurstShowLive(*hubURL, *tokenEnv, *cfEnv, stdout, stderr)
		}
		policy, _, err := LoadBurstPolicy(*path)
		if err != nil {
			fmt.Fprintln(stderr, "burst policy rejected")
			return ExitConditionInvalid
		}
		_, _ = io.WriteString(stdout, formatBurstPolicy(policy))
		return ExitOK
	case "set":
		if *path == "" {
			return ExitUsage
		}
		if *swap < 0 && *load5 < 0 && *consecutive < 0 && *idle < 0 {
			return ExitUsage
		}
		policy, _, err := LoadBurstPolicy(*path)
		if err != nil {
			fmt.Fprintln(stderr, "burst policy rejected")
			return ExitConditionInvalid
		}
		if *swap >= 0 {
			policy.SwapGB = *swap
		}
		if *load5 >= 0 {
			policy.Load5 = *load5
		}
		if *consecutive >= 0 {
			policy.Consecutive = *consecutive
		}
		if *idle >= 0 {
			policy.IdleMinutes = *idle
		}
		if err := writeBurstPolicy(*path, policy); err != nil {
			fmt.Fprintln(stderr, "burst policy write failed")
			return ExitConditionInvalid
		}
		_, _ = io.WriteString(stdout, formatBurstPolicy(policy))
		return ExitOK
	default:
		return ExitUsage
	}
}

func runBurstShowLive(rawURL, tokenPath, cfPath string, stdout, stderr io.Writer) int {
	env, err := loadHubTokenEnv(tokenPath)
	if err != nil || env.MachineID != hubOperatorMachineID {
		fmt.Fprintln(stderr, "burst show rejected: invalid operator token env")
		return ExitConditionInvalid
	}
	endpoint, err := hubHTTPSEndpoint(rawURL, "/v1/burst", false)
	if err != nil {
		fmt.Fprintln(stderr, "burst show rejected: invalid hub URL")
		return ExitConditionInvalid
	}
	request, err := http.NewRequest(http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return ExitInternal
	}
	request.Header.Set(hubAuthorizationHeader, "Bearer "+env.Token)
	if cfPath != "" {
		cf, err := loadHubCFAccessEnv(cfPath)
		if err != nil {
			fmt.Fprintln(stderr, "burst show rejected: invalid Cloudflare Access env")
			return ExitConditionInvalid
		}
		request.Header.Set("CF-Access-Client-Id", cf.ClientID)
		request.Header.Set("CF-Access-Client-Secret", cf.ClientSecret)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		fmt.Fprintln(stderr, "burst show unavailable")
		return ExitInternal
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		fmt.Fprintln(stderr, "burst show unavailable")
		return ExitInternal
	}
	_, _ = io.Copy(stdout, io.LimitReader(response.Body, 64<<10))
	return ExitOK
}

type hubBurstEvent struct {
	Type      string    `json:"type"`
	Machine   string    `json:"machine"`
	Phase     string    `json:"phase"`
	EmittedAt time.Time `json:"emitted_at"`
	WakeMAC   string    `json:"wake_mac,omitempty"`
}

func validHubBurstPhase(phase string) bool {
	return phase == hubFailoverPhaseUp || phase == hubFailoverPhaseDown
}

type hubBurstState struct {
	SourceRuns, IdleRuns        int
	IdleSince, LastUp, LastDown time.Time
	UpCompleted                 bool
	LastLoad                    HubHostLoad
}

func (h *HubServer) reloadBurstPolicyLocked() {
	if h.burstPolicyPath == "" {
		return
	}
	info, err := os.Stat(h.burstPolicyPath)
	if err != nil || !info.Mode().IsRegular() || info.ModTime().Equal(h.burstPolicyModTime) {
		return
	}
	policy, modTime, err := LoadBurstPolicy(h.burstPolicyPath)
	if err != nil {
		h.logger.Warn("burst policy reload rejected")
		h.burstPolicyModTime = info.ModTime()
		return
	}
	h.burstPolicy, h.burstPolicyModTime = policy, modTime
}

func (h *HubServer) observeBurstLocked(now time.Time, machineID string, load HubHostLoad) []hubBurstEvent {
	if h.burstPolicyPath == "" {
		return nil
	}
	h.reloadBurstPolicyLocked()
	policy := h.burstPolicy
	state := h.burstState
	if machineID == policy.SourceMachine {
		state.LastLoad = load
		if load.SwapUsedGB >= policy.SwapGB || load.Load5 >= policy.Load5 {
			state.SourceRuns++
		} else {
			state.SourceRuns = 0
		}
		if state.SourceRuns >= policy.Consecutive && (state.LastUp.IsZero() || now.Sub(state.LastUp) >= time.Duration(policy.CooldownMinutes)*time.Minute) {
			state.LastUp, state.UpCompleted = now, false
			return []hubBurstEvent{{Type: "burst", Machine: policy.TargetMachine, Phase: hubFailoverPhaseUp, EmittedAt: now, WakeMAC: policy.WakeMAC}}
		}
	}
	if machineID == policy.TargetMachine {
		state.LastLoad = load
		if load.WorkerProcs == 0 {
			if state.IdleRuns == 0 {
				state.IdleSince = now
			}
			state.IdleRuns++
		} else {
			state.IdleRuns = 0
			state.IdleSince = time.Time{}
		}
		// Heartbeats are normally ten seconds. Use elapsed time to make policy
		// correct when operators choose a different interval.
		if load.WorkerProcs == 0 && !state.IdleSince.IsZero() && now.Sub(state.IdleSince) >= time.Duration(policy.IdleMinutes)*time.Minute && (state.LastDown.IsZero() || now.Sub(state.LastDown) >= time.Duration(policy.CooldownMinutes)*time.Minute) {
			state.LastDown = now
			return []hubBurstEvent{{Type: "burst", Machine: policy.TargetMachine, Phase: hubFailoverPhaseDown, EmittedAt: now}}
		}
	}
	return nil
}

func (h *HubServer) burstStatus() (BurstPolicy, hubBurstState, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reloadBurstPolicyLocked()
	return h.burstPolicy, *h.burstState, h.burstPolicyPath != ""
}
func (h *HubServer) dispatchBurst(event hubBurstEvent) {
	h.recordUIEvent("burst", event.Phase, event.Machine, event.EmittedAt)
	h.mu.Lock()
	policy := h.burstPolicy
	record := h.nodes[policy.WakeVia]
	if event.Phase == hubFailoverPhaseDown {
		record = h.nodes[event.Machine]
	}
	load := h.burstState.LastLoad
	h.mu.Unlock()
	if record != nil && record.agent != nil {
		record.agent.queueBurst(event)
	}
	if notifier, ok := h.notifier.(interface {
		SendBurst(context.Context, string) error
	}); ok {
		go func() {
			_ = notifier.SendBurst(context.Background(), fmt.Sprintf("burst %s target=%s load5=%.2f swap_gb=%.2f workers=%d", event.Phase, event.Machine, load.Load5, load.SwapUsedGB, load.WorkerProcs))
		}()
	}
}

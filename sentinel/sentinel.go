// Package sentinel contains the L2 peer-monitoring state machine.  It has no
// Supabase or Telegram dependency: adapters own wire formats and credentials.
package sentinel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	DefaultVersion                 = "sentinel-r5-v1"
	DefaultHeartbeatInterval       = time.Minute
	DefaultWatchInterval           = 2 * time.Minute
	DefaultStaleThreshold          = 5 * time.Minute
	DefaultConsecutiveObservations = 2
	DefaultAlertWindow             = 15 * time.Minute
	DefaultClaimTTL                = 90 * time.Second
)

var (
	machineIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)
	checkNamePattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,63}$`)
	versionPattern   = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
)

// CheckStatus is deliberately closed.  Command output never leaves the node;
// only one of these three values can enter a heartbeat.
type CheckStatus string

const (
	CheckOK   CheckStatus = "ok"
	CheckFail CheckStatus = "fail"
	CheckSkip CheckStatus = "skip"
)

func (s CheckStatus) valid() bool {
	return s == CheckOK || s == CheckFail || s == CheckSkip
}

// Heartbeat is the complete remote observation.  It intentionally excludes
// command output, arguments, environment, and any other local-only values.
type Heartbeat struct {
	MachineID string
	SeenAt    time.Time
	Checks    map[string]CheckStatus
	Version   string
}

// Alert describes only the allowed notification fields.
type Alert struct {
	Recovery  bool
	MachineID string
	Reason    string
	LastSeen  time.Time
	Checks    map[string]CheckStatus
}

// AlertClaim is the durable, shared lease used to select one peer as the
// sender.  AlertWindow is a bucket start, not a retention duration.
type AlertClaim struct {
	IncidentKey string
	AlertWindow time.Time
	TTL         time.Duration
}

// Remote is the transport-facing boundary.  Implementations are responsible
// for authenticated upsert, RLS-visible reads, and atomic claim semantics.
type Remote interface {
	UpsertHeartbeat(context.Context, Heartbeat) error
	ListHeartbeats(context.Context) ([]Heartbeat, error)
	ClaimAlert(context.Context, AlertClaim) (bool, error)
	MarkAlertDelivered(context.Context, AlertClaim) error
}

// Notifier is intentionally small so Telegram remains an adapter concern.
type Notifier interface {
	Send(context.Context, Alert) error
}

// Check is a local-only executable self-check.  Argv is never serialized.
type Check struct {
	Name    string
	Argv    []string
	Timeout time.Duration
}

// Node describes the locally configured health contract for one machine.
type Node struct {
	MachineID string
	Checks    []Check
	Threshold time.Duration
}

// Settings is parsed from an explicit JSON file.  Nodes includes the local
// node and every peer to be evaluated by each watcher.
type Settings struct {
	Version                 string
	Nodes                   map[string]Node
	ConsecutiveObservations int
	AlertWindow             time.Duration
	ClaimTTL                time.Duration
}

type rawConfig struct {
	Version                 string             `json:"version"`
	Nodes                   map[string]rawNode `json:"nodes"`
	Checks                  []rawCheck         `json:"checks"`
	Peers                   []rawPeer          `json:"peers"`
	Threshold               string             `json:"threshold"`
	StaleAfter              string             `json:"stale_after"`
	ConsecutiveObservations int                `json:"consecutive_observations"`
	FlapObservations        int                `json:"flap_observations"`
	AlertWindow             string             `json:"alert_window"`
	ClaimTTL                string             `json:"claim_ttl"`
}

type rawNode struct {
	Checks     []rawCheck `json:"checks"`
	Threshold  string     `json:"threshold"`
	StaleAfter string     `json:"stale_after"`
}

type rawPeer struct {
	MachineID  string `json:"machine_id"`
	Threshold  string `json:"threshold"`
	StaleAfter string `json:"stale_after"`
}

type rawCheck struct {
	Name    string   `json:"name"`
	Argv    []string `json:"argv"`
	Timeout string   `json:"timeout"`
}

// LoadConfig reads an explicit, regular JSON file.  There is intentionally no
// default path, so a daemon can never probe a user's panewire configuration.
func LoadConfig(path, localMachineID string) (Settings, error) {
	if path == "" {
		return Settings{}, errors.New("sentinel config path is required")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return Settings{}, errors.New("sentinel config must be a regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Settings{}, errors.New("read sentinel config")
	}
	return ParseConfig(data, localMachineID)
}

// ParseConfig validates the JSON configuration without retaining the input
// bytes.  Besides the node-map form, top-level checks/peers are accepted for a
// compact single-node configuration.
func ParseConfig(data []byte, localMachineID string) (Settings, error) {
	if !machineIDPattern.MatchString(localMachineID) {
		return Settings{}, errors.New("sentinel local machine ID is invalid")
	}
	var raw rawConfig
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		return Settings{}, errors.New("sentinel config JSON is invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Settings{}, errors.New("sentinel config JSON has trailing data")
	}
	if raw.Nodes == nil {
		raw.Nodes = make(map[string]rawNode)
	}
	if _, exists := raw.Nodes[localMachineID]; !exists && len(raw.Checks) > 0 {
		raw.Nodes[localMachineID] = rawNode{Checks: raw.Checks, Threshold: raw.Threshold, StaleAfter: raw.StaleAfter}
	}
	for _, peer := range raw.Peers {
		if peer.MachineID == "" {
			return Settings{}, errors.New("sentinel peer machine ID is invalid")
		}
		if _, exists := raw.Nodes[peer.MachineID]; !exists {
			raw.Nodes[peer.MachineID] = rawNode{Threshold: peer.Threshold, StaleAfter: peer.StaleAfter}
		}
	}
	if _, exists := raw.Nodes[localMachineID]; !exists {
		return Settings{}, errors.New("sentinel config has no local node")
	}

	version := raw.Version
	if version == "" {
		version = DefaultVersion
	}
	if !versionPattern.MatchString(version) {
		return Settings{}, errors.New("sentinel version is invalid")
	}
	consecutive := raw.ConsecutiveObservations
	if consecutive == 0 {
		consecutive = raw.FlapObservations
	}
	if consecutive == 0 {
		consecutive = DefaultConsecutiveObservations
	}
	if consecutive < 1 || consecutive > 16 {
		return Settings{}, errors.New("sentinel consecutive observations are invalid")
	}
	alertWindow, err := parseDuration(raw.AlertWindow, DefaultAlertWindow, time.Minute, 24*time.Hour)
	if err != nil {
		return Settings{}, errors.New("sentinel alert window is invalid")
	}
	claimTTL, err := parseDuration(raw.ClaimTTL, DefaultClaimTTL, 5*time.Second, 10*time.Minute)
	if err != nil {
		return Settings{}, errors.New("sentinel claim TTL is invalid")
	}

	nodes := make(map[string]Node, len(raw.Nodes))
	for machineID, node := range raw.Nodes {
		if !machineIDPattern.MatchString(machineID) {
			return Settings{}, errors.New("sentinel node machine ID is invalid")
		}
		thresholdText := node.Threshold
		if thresholdText == "" {
			thresholdText = node.StaleAfter
		}
		if thresholdText == "" {
			thresholdText = raw.Threshold
		}
		if thresholdText == "" {
			thresholdText = raw.StaleAfter
		}
		threshold, err := parseDuration(thresholdText, DefaultStaleThreshold, 5*time.Second, 24*time.Hour)
		if err != nil {
			return Settings{}, errors.New("sentinel node threshold is invalid")
		}
		checks, err := parseChecks(node.Checks)
		if err != nil {
			return Settings{}, err
		}
		nodes[machineID] = Node{MachineID: machineID, Checks: checks, Threshold: threshold}
	}
	return Settings{
		Version:                 version,
		Nodes:                   nodes,
		ConsecutiveObservations: consecutive,
		AlertWindow:             alertWindow,
		ClaimTTL:                claimTTL,
	}, nil
}

func parseDuration(value string, fallback, minimum, maximum time.Duration) (time.Duration, error) {
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, errors.New("duration")
	}
	return parsed, nil
}

func parseChecks(raw []rawCheck) ([]Check, error) {
	checks := make([]Check, 0, len(raw))
	names := make(map[string]struct{}, len(raw))
	for _, check := range raw {
		if !checkNamePattern.MatchString(check.Name) {
			return nil, errors.New("sentinel check name is invalid")
		}
		if _, exists := names[check.Name]; exists {
			return nil, errors.New("sentinel check names must be unique")
		}
		names[check.Name] = struct{}{}
		if len(check.Argv) == 0 || len(check.Argv) > 32 {
			return nil, errors.New("sentinel check argv is invalid")
		}
		argv := make([]string, len(check.Argv))
		for index, arg := range check.Argv {
			if arg == "" || len(arg) > 1024 || strings.IndexByte(arg, 0) >= 0 {
				return nil, errors.New("sentinel check argv is invalid")
			}
			argv[index] = arg
		}
		timeout, err := parseDuration(check.Timeout, 10*time.Second, time.Millisecond, 5*time.Minute)
		if err != nil {
			return nil, errors.New("sentinel check timeout is invalid")
		}
		checks = append(checks, Check{Name: check.Name, Argv: argv, Timeout: timeout})
	}
	return checks, nil
}

// ValidateHeartbeat rejects malformed or open-vocabulary remote data before it
// can affect a watcher or notification.
func ValidateHeartbeat(heartbeat Heartbeat) error {
	if !machineIDPattern.MatchString(heartbeat.MachineID) || heartbeat.SeenAt.IsZero() || !versionPattern.MatchString(heartbeat.Version) {
		return errors.New("sentinel heartbeat is invalid")
	}
	for name, status := range heartbeat.Checks {
		if !checkNamePattern.MatchString(name) || !status.valid() {
			return errors.New("sentinel heartbeat checks are invalid")
		}
	}
	return nil
}

// ChecksSummary is safe for status and Telegram output: names and values are
// validated closed-vocabulary identifiers, sorted, and never include output.
func ChecksSummary(checks map[string]CheckStatus) string {
	if len(checks) == 0 {
		return "none"
	}
	names := make([]string, 0, len(checks))
	for name, status := range checks {
		if checkNamePattern.MatchString(name) && status.valid() {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return "none"
	}
	sort.Strings(names)
	var builder strings.Builder
	for index, name := range names {
		if index > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(name)
		builder.WriteByte('=')
		builder.WriteString(string(checks[name]))
	}
	return builder.String()
}

type pendingAlert struct {
	alert        Alert
	claim        AlertClaim
	retryAt      time.Time
	ownedFailure bool
	sent         bool
}

type observation struct {
	candidateRuns int
	clearRuns     int
	active        bool
	incident      *pendingAlert
	recovery      *pendingAlert
}

// Service owns local command execution and the in-memory debounce state.
// Durable de-duplication deliberately lives in Remote, shared by all nodes.
type Service struct {
	machineID string
	settings  Settings
	remote    Remote
	notifier  Notifier
	now       func() time.Time
	execute   func(context.Context, []string) error
	warn      func(string)

	evaluateMu   sync.Mutex
	observations map[string]*observation
}

type ServiceConfig struct {
	MachineID string
	Settings  Settings
	Remote    Remote
	Notifier  Notifier
	Now       func() time.Time
	Execute   func(context.Context, []string) error
	Warn      func(string)
}

func NewService(config ServiceConfig) (*Service, error) {
	if !machineIDPattern.MatchString(config.MachineID) {
		return nil, errors.New("sentinel machine ID is invalid")
	}
	if config.Remote == nil {
		return nil, errors.New("sentinel remote is required")
	}
	if _, exists := config.Settings.Nodes[config.MachineID]; !exists || !validSettings(config.Settings) || config.Settings.ConsecutiveObservations < 1 || config.Settings.AlertWindow <= 0 || config.Settings.ClaimTTL <= 0 {
		return nil, errors.New("sentinel settings are invalid")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Execute == nil {
		config.Execute = executeCheck
	}
	if config.Warn == nil {
		config.Warn = func(string) {}
	}
	return &Service{
		machineID: config.MachineID, settings: config.Settings, remote: config.Remote, notifier: config.Notifier,
		now: config.Now, execute: config.Execute, warn: config.Warn, observations: make(map[string]*observation),
	}, nil
}

func validSettings(settings Settings) bool {
	if !versionPattern.MatchString(settings.Version) {
		return false
	}
	for machineID, node := range settings.Nodes {
		if !machineIDPattern.MatchString(machineID) || node.MachineID != machineID || node.Threshold <= 0 {
			return false
		}
		seen := make(map[string]struct{}, len(node.Checks))
		for _, check := range node.Checks {
			if !checkNamePattern.MatchString(check.Name) || len(check.Argv) == 0 || check.Timeout <= 0 {
				return false
			}
			if _, exists := seen[check.Name]; exists {
				return false
			}
			seen[check.Name] = struct{}{}
		}
	}
	return true
}

// EmitHeartbeat runs local checks with independent timeouts and publishes only
// their closed result vocabulary.  Command stdout/stderr is always discarded.
func (s *Service) EmitHeartbeat(ctx context.Context) error {
	node := s.settings.Nodes[s.machineID]
	checks := make(map[string]CheckStatus, len(node.Checks))
	for _, check := range node.Checks {
		checkContext, cancel := context.WithTimeout(ctx, check.Timeout)
		err := s.execute(checkContext, append([]string(nil), check.Argv...))
		cancel()
		if err == nil {
			checks[check.Name] = CheckOK
		} else {
			checks[check.Name] = CheckFail
		}
	}
	heartbeat := Heartbeat{MachineID: s.machineID, SeenAt: s.now().UTC(), Checks: checks, Version: s.settings.Version}
	if err := ValidateHeartbeat(heartbeat); err != nil {
		return err
	}
	if err := s.remote.UpsertHeartbeat(ctx, heartbeat); err != nil {
		return errors.New("sentinel heartbeat upsert failed")
	}
	return nil
}

func executeCheck(ctx context.Context, argv []string) error {
	if len(argv) == 0 {
		return errors.New("empty argv")
	}
	command := exec.CommandContext(ctx, argv[0], argv[1:]...)
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	return command.Run()
}

// Evaluate applies per-node stale and failed-check conditions.  Both entering
// an incident and returning to health need consecutive observations; the
// shared alert claim handles races between independent watcher daemons.
func (s *Service) Evaluate(ctx context.Context) error {
	s.evaluateMu.Lock()
	defer s.evaluateMu.Unlock()
	heartbeats, err := s.remote.ListHeartbeats(ctx)
	if err != nil {
		return errors.New("sentinel heartbeat list failed")
	}
	byMachine := make(map[string]Heartbeat, len(heartbeats))
	for _, heartbeat := range heartbeats {
		if ValidateHeartbeat(heartbeat) != nil {
			// The database check should make this impossible.  Ignore a malformed
			// row rather than copy any untrusted value into an alert.
			continue
		}
		if previous, exists := byMachine[heartbeat.MachineID]; !exists || heartbeat.SeenAt.After(previous.SeenAt) {
			byMachine[heartbeat.MachineID] = heartbeat
		}
	}
	now := s.now().UTC()
	machines := make([]string, 0, len(s.settings.Nodes))
	for machineID := range s.settings.Nodes {
		if machineID != s.machineID {
			machines = append(machines, machineID)
		}
	}
	sort.Strings(machines)
	for _, machineID := range machines {
		node := s.settings.Nodes[machineID]
		heartbeat, exists := byMachine[machineID]
		stale := !exists || now.Sub(heartbeat.SeenAt) > node.Threshold
		failed := !stale && containsFailure(heartbeat.Checks)
		s.observe(ctx, now, machineID, "stale", stale, heartbeat)
		s.observe(ctx, now, machineID, "checks_fail", failed, heartbeat)
	}
	return nil
}

func containsFailure(checks map[string]CheckStatus) bool {
	for _, status := range checks {
		if status == CheckFail {
			return true
		}
	}
	return false
}

func (s *Service) observe(ctx context.Context, now time.Time, machineID, reason string, candidate bool, heartbeat Heartbeat) {
	key := machineID + "|" + reason
	state := s.observations[key]
	if state == nil {
		state = &observation{}
		s.observations[key] = state
	}
	if candidate {
		state.clearRuns = 0
		state.recovery = nil
		if !state.active {
			state.candidateRuns++
			if state.candidateRuns >= s.settings.ConsecutiveObservations {
				state.active = true
				state.candidateRuns = 0
				state.incident = s.newPending(now, Alert{MachineID: machineID, Reason: reason, LastSeen: heartbeat.SeenAt, Checks: cloneChecks(heartbeat.Checks)})
			}
		}
	} else {
		state.candidateRuns = 0
		state.incident = nil
		if state.active {
			state.clearRuns++
			if state.clearRuns >= s.settings.ConsecutiveObservations {
				state.active = false
				state.clearRuns = 0
				state.recovery = s.newPending(now, Alert{Recovery: true, MachineID: machineID, Reason: reason, LastSeen: heartbeat.SeenAt, Checks: cloneChecks(heartbeat.Checks)})
			}
		}
	}
	if state.incident != nil && s.tryNotify(ctx, now, state.incident) {
		state.incident = nil
	}
	if state.recovery != nil && s.tryNotify(ctx, now, state.recovery) {
		state.recovery = nil
	}
}

func (s *Service) newPending(now time.Time, alert Alert) *pendingAlert {
	kind := "incident"
	if alert.Recovery {
		kind = "recovery"
	}
	return &pendingAlert{
		alert: alert,
		claim: AlertClaim{
			IncidentKey: "sentinel:" + kind + ":" + alert.MachineID + ":" + alert.Reason,
			AlertWindow: now.Truncate(s.settings.AlertWindow),
			TTL:         s.settings.ClaimTTL,
		},
	}
}

// tryNotify returns true only when no more local work remains.  A Telegram
// failure leaves the claim unacknowledged, waits for its TTL, and then retries;
// a successful send retries only the delivery mark, never sends again.
func (s *Service) tryNotify(ctx context.Context, now time.Time, pending *pendingAlert) bool {
	if now.Before(pending.retryAt) {
		return false
	}
	if pending.sent {
		if err := s.remote.MarkAlertDelivered(ctx, pending.claim); err != nil {
			pending.retryAt = now.Add(retryDelay(s.settings.ClaimTTL))
			s.warn("sentinel alert delivery mark failed")
			return false
		}
		return true
	}
	claimed, err := s.remote.ClaimAlert(ctx, pending.claim)
	if err != nil {
		pending.retryAt = now.Add(retryDelay(s.settings.ClaimTTL))
		s.warn("sentinel alert claim failed")
		return false
	}
	if !claimed {
		if pending.ownedFailure {
			pending.retryAt = now.Add(s.settings.ClaimTTL)
			return false
		}
		return true
	}
	if s.notifier == nil {
		pending.ownedFailure = true
		pending.retryAt = now.Add(s.settings.ClaimTTL)
		s.warn("sentinel notification is not configured")
		return false
	}
	if err := s.notifier.Send(ctx, pending.alert); err != nil {
		pending.ownedFailure = true
		pending.retryAt = now.Add(s.settings.ClaimTTL)
		s.warn("sentinel Telegram notification failed")
		return false
	}
	pending.sent = true
	if err := s.remote.MarkAlertDelivered(ctx, pending.claim); err != nil {
		pending.retryAt = now.Add(retryDelay(s.settings.ClaimTTL))
		s.warn("sentinel alert delivery mark failed")
		return false
	}
	return true
}

func retryDelay(ttl time.Duration) time.Duration {
	if ttl < 30*time.Second {
		return ttl
	}
	return 30 * time.Second
}

func cloneChecks(checks map[string]CheckStatus) map[string]CheckStatus {
	if len(checks) == 0 {
		return map[string]CheckStatus{}
	}
	result := make(map[string]CheckStatus, len(checks))
	for name, status := range checks {
		result[name] = status
	}
	return result
}

// FormatAlert is intentionally restrictive: it emits only machine, reason,
// last seen, and the closed check summary required by the sentinel protocol.
func FormatAlert(alert Alert) string {
	state := "ALERT"
	reason := alert.Reason
	if alert.Recovery {
		state = "RECOVERY"
		reason = "recovered " + reason
	}
	machineID := alert.MachineID
	if !machineIDPattern.MatchString(machineID) {
		machineID = "unknown"
	}
	if alert.Reason != "stale" && alert.Reason != "checks_fail" {
		if alert.Recovery {
			reason = "recovered unknown"
		} else {
			reason = "unknown"
		}
	}
	lastSeen := "never"
	if !alert.LastSeen.IsZero() {
		lastSeen = alert.LastSeen.UTC().Format(time.RFC3339)
	}
	return fmt.Sprintf("[PANEWIRE SENTINEL %s]\nmachine: %s\nreason: %s\nlast seen: %s\nchecks: %s", state, machineID, reason, lastSeen, ChecksSummary(alert.Checks))
}

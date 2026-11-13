package panewire

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/configkeys"
	"gopkg.in/yaml.v3"
)

//go:embed delivery-policy.yaml
var defaultDeliveryPolicy []byte

// deliveryTagShape keeps a tag's edges on word characters so the compiled
// \b…\b boundary means whole-word; interior punctuation like "needs_decision"
// stays legal.
var deliveryTagShape = regexp.MustCompile(`^[0-9A-Za-z_](?:.*[0-9A-Za-z_])?$`)

// Safety tags outrank inbox delivery. READY can be deferred by a lane that
// explicitly opts into scheduler-owned summaries.
var mustActDeliveryTags = regexp.MustCompile(`(?i)\b(?:BLOCKER|READY|ESC|BOUNCE|URGENT|needs_decision)\b`)

// hubNotifyTypes is the closed vocabulary a producer declares with
// `panewire emit --type`. The same set validates the wire field and the keys
// of a policy's types: block, so a type that cannot be declared can never be
// configured.
var hubNotifyTypes = map[string]bool{"decision": true, "order": true, "merged": true, "closed": true, "idle-wake": true, "progress": true, "deploy-approval": true, "watch-hold": true, "culprit-hold": true, "automation-failed": true, "ready": true, "blocked": true, "findings": true, "report": true, "ack": true, "info": true}

// defaultNotifyTypeClasses is the built-in type→class map. A policy file
// overrides individual entries under types:; the rest stay built in.
var defaultNotifyTypeClasses = map[string]string{"merged": "inbox", "closed": "inbox", "idle-wake": "inbox", "progress": "inbox", "deploy-approval": "interrupt", "watch-hold": "interrupt", "culprit-hold": "interrupt", "automation-failed": "interrupt", "decision": "interrupt", "order": "interrupt", "ready": "interrupt", "blocked": "interrupt", "findings": "interrupt", "report": "inbox", "ack": "inbox", "info": "inbox"}

// mustAckNotifyTypes are the declared types a receiving session must
// acknowledge explicitly (`panewire inbox ack`): a READY, an ESC decision, a
// BLOCKED notice or a review-findings delivery. Injecting one into a pane, or
// reading it, does not retire it; it stays unread until acked, whatever class
// the policy gives it. Idle-wake and progress events declare none of these
// and keep their existing read-on-insert behaviour.
var mustAckNotifyTypes = map[string]bool{"ready": true, "decision": true, "blocked": true, "findings": true}

// defaultInboxAgentSupport is the built-in receiving-CLI capability map: only
// harnesses whose agent can background-wait on `panewire inbox wait` are
// listed supported. The policy file's agents: block overrides individual
// entries — flipping one CLI is a one-line change.
var defaultInboxAgentSupport = map[string]string{"claude": "supported"}

var hubAgentCLIPattern = regexp.MustCompile(`^[0-9A-Za-z_.-]{1,64}$`)

func validNotifyType(value string) bool {
	return value == "" || hubNotifyTypes[value]
}

type deliveryRule struct {
	Match  string   `yaml:"match"`
	Tags   []string `yaml:"tags"`
	Kind   string   `yaml:"kind"`
	Replay bool     `yaml:"replay"`
	Class  string   `yaml:"class"`
	re     *regexp.Regexp
	tags   []*regexp.Regexp
}

type deliveryHoldPolicy struct {
	MaxHold      string `yaml:"max_hold"`
	HandoffBytes int    `yaml:"handoff_bytes"`
	HandoffItems int    `yaml:"handoff_items"`
}

type inboxSummaryPolicy struct {
	DigestPrefix string `yaml:"digest_prefix"`
	MaxChars     int    `yaml:"max_chars"`
}

type deliveryLanePolicy struct {
	InboxSummary     *inboxSummaryPolicy `yaml:"inbox_summary"`
	IdlePromote      *bool               `yaml:"idle_promote"`
	HoldWhileWorking *deliveryHoldPolicy `yaml:"hold_while_working"`
	Default          string              `yaml:"default"`
	Deadlines        map[string]string   `yaml:"deadlines"`
	DigestWindow     string              `yaml:"digest_window"`
	Types            map[string]string   `yaml:"types"`
	Rules            []deliveryRule      `yaml:"rules"`
}

type inboxRetentionPolicy struct {
	Days    int `yaml:"days"`
	PerLane int `yaml:"per_lane"`
}

// defaultDesignLanePattern is the built-in design-lane class: every lane named
// *-design. An explicit relay.design_lanes list replaces it, and an explicit
// empty list disables the class.
const defaultDesignLanePattern = "*-design"

// deskPolicyLane keeps the operator desk out of the design-lane class whatever
// an operator pattern matches: the desk's external scheduler depends on the
// inbox summary and on idle-wake delivery.
const deskPolicyLane = "operator-desk"

// defaultDeskLanePatterns is the built-in desk-lane class: the personal desk
// and the per-fleet desks named <prefix>-operator-desk (work-operator-desk).
// An explicit relay.desk_lanes list replaces it, and an explicit empty list
// disables the class.
var defaultDeskLanePatterns = []string{deskPolicyLane, "*-" + deskPolicyLane}

// defaultInboxWakeWindow is how long an inbox-only lane's one-line wake
// coalesces later arrivals (relay.inbox_wake_window).
const defaultInboxWakeWindow = 5 * time.Minute

// defaultDrainWakeInterval is the least gap between two drain lines for one
// design lane (relay.drain_wake_interval).
const defaultDrainWakeInterval = time.Hour

// consultDoneMarker opens the text of a consult answer's notice, as rstack
// skills/ops/references/consult.md §2 has the answerer send it:
// `--type ready --text '[consult-done] <gist> | <answer path>'`.
const consultDoneMarker = "[consult-done]"

// deliveryRelayPolicy is the hub-side relay classification. A design-session
// lane (handoffkeep-design, panewire-design, mergewake-design, …) receives
// decisions and failures only; routine progress, idle-wakes and the inbox
// summary are suppressed for it (hk 3410).
//
// Design and desk lanes are inbox-only (hk 3602): a notification is filed in
// the lane's inbox and never typed into the pane; the hub types one coalesced
// line instead (inbox_wake.go). WorkerInboxTrial puts matching worker lanes in
// the same class until WorkerInboxTrialUntil, after which it is off by itself.
type deliveryRelayPolicy struct {
	DesignLanes           []string `yaml:"design_lanes"`
	DeskLanes             []string `yaml:"desk_lanes"`
	InboxWakeWindow       string   `yaml:"inbox_wake_window"`
	DrainWakeInterval     string   `yaml:"drain_wake_interval"`
	DrainWakeThreshold    int      `yaml:"drain_wake_threshold"`
	drainWakeInterval     time.Duration
	WorkerInboxTrial      []string `yaml:"worker_inbox_trial"`
	WorkerInboxTrialUntil string   `yaml:"worker_inbox_trial_until"`
	wakeWindow            time.Duration
	trialUntil            time.Time
}

type deliveryPolicy struct {
	Retention          inboxRetentionPolicy `yaml:"retention"`
	Relay              deliveryRelayPolicy  `yaml:"relay"`
	deliveryLanePolicy `yaml:",inline"`
	// Agents maps the receiving pane's reported agent CLI (harness) to
	// supported|unsupported. Receivers without a supported entry — unknown,
	// unlisted, or unreported harnesses — get no inbox deferral.
	Agents map[string]string             `yaml:"agents"`
	Lanes  map[string]deliveryLanePolicy `yaml:"lanes"`
	// ignoredKeys are the keys of the decoded policy that this release does not
	// read; hub startup logs them.
	ignoredKeys []string
	legacy      bool
}

func validDeliveryClass(class string) bool {
	return class == "" || class == "interrupt" || class == "inbox"
}

// validDesignLanePattern accepts an exact lane name or a shell-style glob in
// path.Match syntax. The empty pattern, control bytes and a malformed glob are
// refused at load rather than silently matching nothing.
func validDesignLanePattern(pattern string) bool {
	if pattern == "" || len(pattern) > 128 || strings.ContainsAny(pattern, "\x00\r\n") {
		return false
	}
	_, err := path.Match(pattern, "")
	return err == nil
}

// designLane reports whether the lane is a design-session lane: an exact name
// or glob from relay.design_lanes. The desk is never one, whatever an operator
// pattern would match.
func (p *deliveryPolicy) designLane(lane string) bool {
	if lane == "" || lane == deskPolicyLane || p.deskLane(lane) {
		return false
	}
	return laneMatchesAny(p.Relay.DesignLanes, lane)
}

// deskLane reports whether the lane is a desk lane: an exact name or glob from
// relay.desk_lanes (default operator-desk and *-operator-desk).
func (p *deliveryPolicy) deskLane(lane string) bool {
	return lane != "" && laneMatchesAny(p.Relay.DeskLanes, lane)
}

// workerInboxTrial reports whether a worker lane is in the one-week inbox-only
// trial at now: it matches relay.worker_inbox_trial and now is before
// relay.worker_inbox_trial_until. Past that instant the trial is off.
func (p *deliveryPolicy) workerInboxTrial(lane string, now time.Time) bool {
	return lane != "" && len(p.Relay.WorkerInboxTrial) > 0 && now.Before(p.Relay.trialUntil) && laneMatchesAny(p.Relay.WorkerInboxTrial, lane)
}

// inboxOnlyClass names the lane's inbox-only class at now -- "design", "desk"
// or "trial" -- or "" for a lane that keeps pane injection (hk 3602).
func (p *deliveryPolicy) inboxOnlyClass(lane string, now time.Time) string {
	switch {
	case p.legacy:
		return ""
	case p.deskLane(lane):
		return "desk"
	case p.designLane(lane):
		return "design"
	case p.workerInboxTrial(lane, now):
		return "trial"
	}
	return ""
}

func laneMatchesAny(patterns []string, lane string) bool {
	for _, pattern := range patterns {
		if matched, err := path.Match(pattern, lane); err == nil && matched {
			return true
		}
	}
	return false
}

// designRoutineWorkerTypes are the declared types of a worker's routine
// result: nothing in them asks the design session to act.
var designRoutineWorkerTypes = map[string]bool{"progress": true, "report": true, "info": true, "merged": true, "ack": true, "closed": true}

// designQuiet reports whether a design lane keeps this event in its inbox
// without a pane injection (hk 3426): a lane.event declared ready, the
// completion of a job that has a PR or a task, and (hk 3434) a worker's
// routine lane.event from its own pane -- a type in designRoutineWorkerTypes,
// or untyped text with none of the action tags neverHoldTags matches (ESC,
// BLOCKER, BOUNCE, URGENT, needs_decision; an untyped READY is a completion
// record). mergewake reviews and merges from the PR, and the job's hk record
// keeps the rest, so the design session has nothing to do with any of them.
// Every other type a worker declares (decision, blocked, findings, order,
// automation-failed, deploy-approval, watch-hold, culprit-hold, and any type
// added later) is classified as before. The completion of a job with neither
// a PR nor a task -- a consult answer the session waits for -- is delivered as
// before. hasTask is consulted only for a design-lane completion that names
// no PR, fromWorker only for a lane.event that would otherwise be quiet. Once
// quiet, an explicit class, replay and a worker-directive route do not bring
// an event back (receiveInbox). A lane.event whose text starts with
// consultDoneMarker is the reply the design session asked for and waits for
// in its pane (hk 3434), so it is delivered whatever its type.
func (p *deliveryPolicy) designQuiet(lane, kind string, e hubJobEventPayload, hasTask, fromWorker func() bool) bool {
	if !p.designLane(lane) {
		return false
	}
	switch kind {
	case "lane.event":
		switch {
		case strings.HasPrefix(e.Text, consultDoneMarker):
			return false
		case e.NotifyType == "ready":
			return true
		case e.NotifyType == "":
			return !neverHoldTags.MatchString(deliveryEventText(e)) && fromWorker()
		}
		return designRoutineWorkerTypes[e.NotifyType] && fromWorker()
	case "job.completed":
		return e.PR != "" || hasTask()
	}
	return false
}

func loadDeliveryPolicy(path string) (*deliveryPolicy, error) {
	raw := defaultDeliveryPolicy
	if path != "default" && path != "off" {
		var err error
		raw, err = os.ReadFile(path)
		if err != nil {
			return nil, err
		}
	}
	p, err := parseDeliveryPolicy(raw)
	if p != nil {
		p.legacy = path == "off"
	}
	return p, err
}

func parseDeliveryPolicy(raw []byte) (*deliveryPolicy, error) {
	if len(raw) > 1<<20 {
		return nil, fmt.Errorf("delivery policy too large")
	}
	var p deliveryPolicy
	d := yaml.NewDecoder(bytes.NewReader(raw))
	if err := d.Decode(&p); err != nil {
		return nil, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("expected one policy document")
	}
	ignored, err := configkeys.YAML(raw, &p)
	if err != nil {
		return nil, err
	}
	p.ignoredKeys = ignored
	if p.Retention.Days < 0 || p.Retention.Days > 36500 || p.Retention.PerLane < 0 {
		return nil, fmt.Errorf("retention needs days in 1..36500 and positive per_lane (zero uses defaults)")
	}
	if p.Retention.Days == 0 {
		p.Retention.Days = 30
	}
	if p.Retention.PerLane == 0 {
		p.Retention.PerLane = 2000
	}
	if p.Default == "" {
		return nil, fmt.Errorf("delivery policy needs a default")
	}
	if p.Relay.DesignLanes == nil {
		p.Relay.DesignLanes = []string{defaultDesignLanePattern}
	}
	for _, pattern := range p.Relay.DesignLanes {
		if !validDesignLanePattern(pattern) {
			return nil, fmt.Errorf("invalid relay.design_lanes pattern")
		}
	}
	if p.Relay.DeskLanes == nil {
		p.Relay.DeskLanes = append([]string(nil), defaultDeskLanePatterns...)
	}
	for _, pattern := range p.Relay.DeskLanes {
		if !validDesignLanePattern(pattern) {
			return nil, fmt.Errorf("invalid relay.desk_lanes pattern")
		}
	}
	p.Relay.wakeWindow = defaultInboxWakeWindow
	if p.Relay.InboxWakeWindow != "" {
		window, err := time.ParseDuration(p.Relay.InboxWakeWindow)
		if err != nil || window < 10*time.Second || window > time.Hour {
			return nil, fmt.Errorf("relay.inbox_wake_window must be 10s..1h")
		}
		p.Relay.wakeWindow = window
	}
	p.Relay.drainWakeInterval = defaultDrainWakeInterval
	if p.Relay.DrainWakeInterval != "" {
		interval, err := time.ParseDuration(p.Relay.DrainWakeInterval)
		if err != nil || interval < 10*time.Minute || interval > 24*time.Hour {
			return nil, fmt.Errorf("relay.drain_wake_interval must be 10m..24h")
		}
		p.Relay.drainWakeInterval = interval
	}
	if p.Relay.DrainWakeThreshold < 0 || p.Relay.DrainWakeThreshold > 10 {
		return nil, fmt.Errorf("relay.drain_wake_threshold must be 0..10")
	}
	for _, pattern := range p.Relay.WorkerInboxTrial {
		if !validDesignLanePattern(pattern) {
			return nil, fmt.Errorf("invalid relay.worker_inbox_trial pattern")
		}
	}
	if len(p.Relay.WorkerInboxTrial) > 0 || p.Relay.WorkerInboxTrialUntil != "" {
		// The trial carries its own end: a lane list without one, or an end
		// without a list, is refused rather than left running forever.
		until, err := time.Parse(time.RFC3339, p.Relay.WorkerInboxTrialUntil)
		if err != nil || len(p.Relay.WorkerInboxTrial) == 0 {
			return nil, fmt.Errorf("relay.worker_inbox_trial needs lane patterns and an RFC3339 relay.worker_inbox_trial_until")
		}
		p.Relay.trialUntil = until
	}
	validate := func(l *deliveryLanePolicy) error {
		if v := l.InboxSummary; v != nil {
			if l.IdlePromote != nil && !*l.IdlePromote {
				return fmt.Errorf("idle_promote: false cannot be combined with inbox_summary")
			}
			if v.MaxChars == 0 {
				v.MaxChars = 300
			}
			if v.MaxChars < 240 || v.MaxChars > 300 || !strings.HasPrefix(v.DigestPrefix, "inbox/") || strings.ContainsAny(v.DigestPrefix, "\r\n ?#") || len(v.DigestPrefix) > 120 {
				return fmt.Errorf("inbox_summary needs digest_prefix inbox/... and max_chars in 240..300")
			}
		}
		if v := l.HoldWhileWorking; v != nil {
			duration, err := time.ParseDuration(v.MaxHold)
			if err != nil || duration <= 0 || v.HandoffBytes < 0 || v.HandoffItems < 0 {
				return fmt.Errorf("hold_while_working needs positive max_hold and nonnegative handoff thresholds")
			}
		}
		if !validDeliveryClass(l.Default) {
			return fmt.Errorf("invalid delivery default")
		}
		if l.DigestWindow != "" {
			v, err := time.ParseDuration(l.DigestWindow)
			if err != nil || v < 0 || v > time.Minute {
				return fmt.Errorf("digest_window must be 0s..1m")
			}
		}
		for class, value := range l.Deadlines {
			v, err := time.ParseDuration(value)
			if class == "" || !validDeliveryClass(class) || err != nil || v < 0 {
				return fmt.Errorf("invalid class deadline")
			}
		}
		for name, class := range l.Types {
			if !hubNotifyTypes[name] || (class != "inbox" && class != "interrupt") {
				return fmt.Errorf("invalid type map entry")
			}
		}
		for i := range l.Rules {
			r := &l.Rules[i]
			if r.Class == "" || !validDeliveryClass(r.Class) || (r.Kind == "" && r.Match == "" && len(r.Tags) == 0 && !r.Replay) {
				return fmt.Errorf("invalid delivery rule")
			}
			if r.Kind != "" && !emitRelayKinds[r.Kind] {
				return fmt.Errorf("invalid rule kind")
			}
			if r.Match != "" {
				var err error
				r.re, err = regexp.Compile(r.Match)
				if err != nil {
					return err
				}
			}
			for _, tag := range r.Tags {
				// Tags are whole-word, case-insensitive literals: READY never
				// matches READYING and ESC never matches ESCAPE, so the
				// must-act vocabulary cannot be set off by look-alikes.
				// Word-char edges are what \b needs to mean that.
				if !deliveryTagShape.MatchString(tag) {
					return fmt.Errorf("invalid delivery tag")
				}
				re, err := regexp.Compile(`(?i)\b` + regexp.QuoteMeta(tag) + `\b`)
				if err != nil {
					return err
				}
				r.tags = append(r.tags, re)
			}
		}
		return nil
	}
	if p.IdlePromote != nil {
		return nil, fmt.Errorf("idle_promote is only supported per lane")
	}
	if err := validate(&p.deliveryLanePolicy); err != nil {
		return nil, err
	}
	// The built-in type map always applies; a policy file overrides
	// individual entries under types:. agents: merges the same way over the
	// built-in support map.
	types := make(map[string]string, len(defaultNotifyTypeClasses)+len(p.Types))
	for name, class := range defaultNotifyTypeClasses {
		types[name] = class
	}
	for name, class := range p.Types {
		types[name] = class
	}
	p.Types = types
	for cli, support := range p.Agents {
		if !hubAgentCLIPattern.MatchString(cli) || (support != "supported" && support != "unsupported") {
			return nil, fmt.Errorf("invalid agents entry")
		}
	}
	agents := make(map[string]string, len(defaultInboxAgentSupport)+len(p.Agents))
	for cli, support := range defaultInboxAgentSupport {
		agents[cli] = support
	}
	for cli, support := range p.Agents {
		agents[cli] = support
	}
	p.Agents = agents
	for lane, l := range p.Lanes {
		if !validLaneReference(lane) {
			return nil, fmt.Errorf("invalid policy lane")
		}
		if err := validate(&l); err != nil {
			return nil, err
		}
		p.Lanes[lane] = l
	}
	return &p, nil
}

func (p *deliveryPolicy) classify(lane, kind string, e hubJobEventPayload, replay bool) string {
	if p.legacy || p.mustAct(lane, e) || (p.Lanes[lane].InboxSummary != nil && (kind == "job.escalate" || kind == "job.lost" || kind == "job.revoked")) {
		return "interrupt"
	}
	if e.DeliveryClass != "" {
		return e.DeliveryClass
	}
	// A sender-declared type beats every prose heuristic: chat panes and the
	// regex rules only ever see untyped events.
	if class := p.typeClass(lane, e.NotifyType); class != "" {
		return class
	}
	local := p.Lanes[lane]
	text := deliveryEventText(e)
	for _, rules := range [][]deliveryRule{local.Rules, p.Rules} {
		for _, r := range rules {
			if r.Kind != "" && r.Kind != kind {
				continue
			}
			if r.Replay && !replay && !e.Replay {
				continue
			}
			if r.re != nil && !r.re.MatchString(text) {
				continue
			}
			tagged := len(r.tags) == 0
			for _, tag := range r.tags {
				if tag.MatchString(text) {
					tagged = true
					break
				}
			}
			if !tagged {
				continue
			}
			return r.Class
		}
	}
	if local.Default != "" {
		return local.Default
	}
	return p.Default
}

// agentSupported reports whether the receiving pane's reported CLI is allowed
// inbox deferral. Absent entries are unsupported — a CLI must be named
// supported to opt into the type/owner/idle timing rules.
func (p *deliveryPolicy) agentSupported(cli string) bool {
	return p.Agents[cli] == "supported"
}

// typeClass resolves a declared notification type through the receiving
// lane's types: override first, then the policy-wide map (built-ins merged).
// An empty or unmapped type falls through to the rule set.
func (p *deliveryPolicy) typeClass(lane, notifyType string) string {
	if notifyType == "" {
		return ""
	}
	if local, ok := p.Lanes[lane]; ok {
		if class, ok := local.Types[notifyType]; ok {
			return class
		}
	}
	return p.Types[notifyType]
}

func deliveryEventText(e hubJobEventPayload) string {
	return e.Text + "\n" + e.ReportLastLine + "\n" + e.Question + "\n" + e.Reason
}

func (p *deliveryPolicy) mustAct(lane string, e hubJobEventPayload) bool {
	if p.Lanes[lane].InboxSummary != nil {
		switch e.NotifyType {
		case "decision", "order", "deploy-approval", "watch-hold", "culprit-hold", "automation-failed":
			return true
		}
	}
	text := deliveryEventText(e)
	// The desk reviews READY asynchronously. Other lanes retain the existing
	// READY safety tag. ESC and the remaining safety tags always interrupt.
	desk := p.Lanes[lane].InboxSummary != nil
	if mustActDeliveryTags.MatchString(text) && (!desk || neverHoldTags.MatchString(text)) {
		return true
	}
	for _, rules := range [][]deliveryRule{p.Lanes[lane].Rules, p.Rules} {
		for _, rule := range rules {
			if rule.Class != "interrupt" {
				continue
			}
			for _, tag := range rule.tags {
				if desk && tag.String() == `(?i)\bREADY\b` {
					continue
				}
				if tag.MatchString(text) {
					return true
				}
			}
		}
	}
	return false
}

func (p *deliveryPolicy) timing(lane, class string) (deadline, window time.Duration) {
	deadline, _ = time.ParseDuration(p.Deadlines[class])
	window, _ = time.ParseDuration(p.DigestWindow)
	if l, ok := p.Lanes[lane]; ok {
		if v, ok := l.Deadlines[class]; ok {
			deadline, _ = time.ParseDuration(v)
		}
		if l.DigestWindow != "" {
			window, _ = time.ParseDuration(l.DigestWindow)
		}
	}
	if p.legacy {
		window = 0
	}
	return
}

// Keep the legacy envelope byte-for-byte when the optional class is absent.
func relayPayloadDeliveryClass(payload []byte, class string) []byte {
	return relayPayloadOptionalString(payload, "delivery_class", class)
}

// relayPayloadNotifyType adds the optional sender-declared notification type
// with the same byte-for-byte legacy envelope rule as the delivery class.
func relayPayloadNotifyType(payload []byte, notifyType string) []byte {
	return relayPayloadOptionalString(payload, "notify_type", notifyType)
}

func relayPayloadOptionalString(payload []byte, key, value string) []byte {
	if value == "" {
		return payload
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil {
		return payload
	}
	fields[key], _ = json.Marshal(value)
	raw, _ := json.Marshal(fields)
	return raw
}

func decodeDeliveryClass(fields map[string]json.RawMessage, event *hubJobEventPayload) bool {
	raw, ok := fields["delivery_class"]
	return !ok || (json.Unmarshal(raw, &event.DeliveryClass) == nil && validDeliveryClass(event.DeliveryClass))
}

func decodeNotifyType(fields map[string]json.RawMessage, event *hubJobEventPayload) bool {
	raw, ok := fields["notify_type"]
	return !ok || (json.Unmarshal(raw, &event.NotifyType) == nil && validNotifyType(event.NotifyType))
}

func (p *deliveryPolicy) holdPolicy(lane string) *deliveryHoldPolicy {
	if p.legacy {
		return nil
	}
	if local := p.Lanes[lane].HoldWhileWorking; local != nil {
		return local
	}
	return p.HoldWhileWorking
}

// neverHoldTags are the must-act tags without READY: a working hold never
// keeps them, and designQuiet never quiets a worker's untyped text with one.
var neverHoldTags = regexp.MustCompile(`(?i)\b(?:BLOCKER|URGENT|ESC|BOUNCE|needs_decision)\b`)

func holdableEvent(kind string, e hubJobEventPayload) bool {
	return !e.noDigest && e.NotifyType != "decision" && e.NotifyType != "order" &&
		kind != "job.escalate" && kind != "job.lost" && kind != "job.revoked" &&
		!neverHoldTags.MatchString(deliveryEventText(e))
}

package panewire

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

//go:embed delivery-policy.yaml
var defaultDeliveryPolicy []byte

// deliveryTagShape keeps a tag's edges on word characters so the compiled
// \b…\b boundary means whole-word; interior punctuation like "needs_decision"
// stays legal.
var deliveryTagShape = regexp.MustCompile(`^[0-9A-Za-z_](?:.*[0-9A-Za-z_])?$`)

// These safety tags cannot be demoted by a custom inbox policy or an emitter.
var mustActDeliveryTags = regexp.MustCompile(`(?i)\b(?:BLOCKER|READY|ESC|BOUNCE|URGENT|needs_decision)\b`)

// hubNotifyTypes is the closed vocabulary a producer declares with
// `panewire emit --type`. The same set validates the wire field and the keys
// of a policy's types: block, so a type that cannot be declared can never be
// configured.
var hubNotifyTypes = map[string]bool{"decision": true, "order": true, "ready": true, "report": true, "ack": true, "info": true}

// defaultNotifyTypeClasses is the built-in type→class map. A policy file
// overrides individual entries under types:; the rest stay built in.
var defaultNotifyTypeClasses = map[string]string{"decision": "interrupt", "order": "interrupt", "ready": "interrupt", "report": "inbox", "ack": "inbox", "info": "inbox"}

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

type deliveryLanePolicy struct {
	Default      string            `yaml:"default"`
	Deadlines    map[string]string `yaml:"deadlines"`
	DigestWindow string            `yaml:"digest_window"`
	Types        map[string]string `yaml:"types"`
	Rules        []deliveryRule    `yaml:"rules"`
}

type deliveryPolicy struct {
	deliveryLanePolicy `yaml:",inline"`
	// Agents maps the receiving pane's reported agent CLI (harness) to
	// supported|unsupported. Receivers without a supported entry — unknown,
	// unlisted, or unreported harnesses — get no inbox deferral.
	Agents map[string]string             `yaml:"agents"`
	Lanes  map[string]deliveryLanePolicy `yaml:"lanes"`
	legacy bool
}

func validDeliveryClass(class string) bool {
	return class == "" || class == "interrupt" || class == "inbox"
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
	d.KnownFields(true)
	if err := d.Decode(&p); err != nil {
		return nil, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("expected one policy document")
	}
	if p.Default == "" {
		return nil, fmt.Errorf("delivery policy needs a default")
	}
	validate := func(l *deliveryLanePolicy) error {
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
		if !validReportRelayLaneName(lane) {
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
	if p.legacy || p.mustAct(lane, e) {
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
	// Operator chat is an intentional turn, including declarative instructions.
	if chatRelayEventPattern.MatchString(e.EventID) {
		return "interrupt"
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
	text := deliveryEventText(e)
	if mustActDeliveryTags.MatchString(text) {
		return true
	}
	// Additional interrupt tags in either scope also outrank all inbox rules,
	// even when another rule would otherwise match first.
	for _, rules := range [][]deliveryRule{p.Lanes[lane].Rules, p.Rules} {
		for _, rule := range rules {
			if rule.Class != "interrupt" {
				continue
			}
			for _, tag := range rule.tags {
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

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

type deliveryRule struct {
	Match  string `yaml:"match"`
	Kind   string `yaml:"kind"`
	Replay bool   `yaml:"replay"`
	Class  string `yaml:"class"`
	re     *regexp.Regexp
}

type deliveryLanePolicy struct {
	Default      string            `yaml:"default"`
	Deadlines    map[string]string `yaml:"deadlines"`
	DigestWindow string            `yaml:"digest_window"`
	Rules        []deliveryRule    `yaml:"rules"`
}

type deliveryPolicy struct {
	deliveryLanePolicy `yaml:",inline"`
	Lanes              map[string]deliveryLanePolicy `yaml:"lanes"`
	legacy             bool
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
		for i := range l.Rules {
			r := &l.Rules[i]
			if r.Class == "" || !validDeliveryClass(r.Class) || (r.Kind == "" && r.Match == "" && !r.Replay) {
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
		}
		return nil
	}
	if err := validate(&p.deliveryLanePolicy); err != nil {
		return nil, err
	}
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
	if e.DeliveryClass != "" {
		return e.DeliveryClass
	}
	// Operator chat is an intentional turn, including declarative instructions.
	if chatRelayEventPattern.MatchString(e.EventID) || p.legacy {
		return "interrupt"
	}
	local := p.Lanes[lane]
	text := e.Text + "\n" + e.ReportLastLine + "\n" + e.Question + "\n" + e.Reason
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
			return r.Class
		}
	}
	if local.Default != "" {
		return local.Default
	}
	return p.Default
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
	if class == "" {
		return payload
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil {
		return payload
	}
	fields["delivery_class"], _ = json.Marshal(class)
	raw, _ := json.Marshal(fields)
	return raw
}
func decodeDeliveryClass(fields map[string]json.RawMessage, event *hubJobEventPayload) bool {
	raw, ok := fields["delivery_class"]
	return !ok || (json.Unmarshal(raw, &event.DeliveryClass) == nil && validDeliveryClass(event.DeliveryClass))
}

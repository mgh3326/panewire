package panewire

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
)

// The targets file is the operator-edited mapping from an opaque target id —
// the only thing berry may name — to a lane or a chat conversation. Callers
// can never supply a lane, URL, route or shell string; the file is the whole
// routing truth. It must be a regular mode-0600 file: anything else refuses
// startup and fails every lookup closed.

var assistantTargetIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
var assistantConversationPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

type assistantTarget struct {
	ID           string `json:"id"`
	Kind         string `json:"kind"` // "lane" or "conversation"
	Lane         string `json:"lane,omitempty"`
	Conversation string `json:"conversation,omitempty"`
	// DeliverLane is the operator-mapped lane a deliver call posts to. Lane
	// targets deliver to their own lane; conversation targets carry no lane
	// of their own, so deliver_lane is the only way they are deliverable —
	// and the only way a conversation's lane is ever revealed to a write.
	DeliverLane string `json:"deliver_lane,omitempty"`
	Description string `json:"description,omitempty"`
}

// assistantTargetsFile is the on-disk schema.
type assistantTargetsFile struct {
	Targets map[string]struct {
		Kind         string `json:"kind"`
		Lane         string `json:"lane,omitempty"`
		Conversation string `json:"conversation,omitempty"`
		DeliverLane  string `json:"deliver_lane,omitempty"`
		Description  string `json:"description,omitempty"`
	} `json:"targets"`
}

// loadAssistantTargets stats and parses the mapping file. The mode check
// runs on every call, not just startup: an operator chmod must revoke the
// surface, not leave stale access in place.
func loadAssistantTargets(path string) (map[string]assistantTarget, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 {
		return nil, configError("targets file must be a regular mode-0600 file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, configError("targets file unreadable")
	}
	var file assistantTargetsFile
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&file); err != nil {
		return nil, configError("targets file is not valid json")
	}
	if len(file.Targets) == 0 {
		return nil, configError("targets file declares no targets")
	}
	targets := make(map[string]assistantTarget, len(file.Targets))
	for id, entry := range file.Targets {
		if !assistantTargetIDPattern.MatchString(id) {
			return nil, configError("targets file holds an invalid target id")
		}
		target := assistantTarget{ID: id, Kind: entry.Kind, Lane: entry.Lane, Conversation: entry.Conversation, DeliverLane: entry.DeliverLane, Description: entry.Description}
		switch entry.Kind {
		case "lane":
			if !validReportRelayLaneName(entry.Lane) || entry.Conversation != "" || entry.DeliverLane != "" {
				return nil, configError("targets file holds an invalid lane target")
			}
		case "conversation":
			if !assistantConversationPattern.MatchString(entry.Conversation) || entry.Lane != "" {
				return nil, configError("targets file holds an invalid conversation target")
			}
			// The hub ingress accepts only the agent-label lane spelling;
			// an unpostable deliver_lane is a config error, not a runtime
			// surprise.
			if entry.DeliverLane != "" && !hubAgentLabelPattern.MatchString(entry.DeliverLane) {
				return nil, configError("targets file holds an invalid conversation deliver lane")
			}
		default:
			return nil, configError("targets file holds an unknown target kind")
		}
		if len(entry.Description) > 500 || strings.ContainsAny(entry.Description, "\x00\r\n") {
			return nil, configError("targets file holds an invalid description")
		}
		targets[id] = target
	}
	return targets, nil
}

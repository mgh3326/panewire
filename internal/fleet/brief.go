package fleet

import (
	_ "embed"
	"fmt"
	"os"
	"strings"
)

//go:embed brief-policy.json
var defaultBriefPolicy []byte

type BriefPolicy struct {
	Version  int               `json:"version"`
	Default  string            `json:"default"`
	Blocks   map[string]string `json:"blocks"`
	Hosts    map[string]string `json:"hosts"`
	Profiles map[string]string `json:"profiles"`
}

func (a *App) prepareBrief(s Spawn, seat Seat, launch Launch, body string) (string, error) {
	b := defaultBriefPolicy
	path := os.Getenv("PANEWIRE_BRIEF_POLICY")
	if path == "" {
		path = str(a.Config.Local, "brief_policy")
	}
	if path != "" {
		var e error
		b, e = os.ReadFile(expand(path))
		if e != nil {
			return "", e
		}
	}
	var p BriefPolicy
	if e := decode(b, &p); e != nil {
		return "", e
	}
	if p.Version != 1 {
		return "", fail(exitConfig, "unsupported brief policy version")
	}
	key := p.Default
	if v := p.Hosts[a.machine()]; v != "" {
		key = v
	}
	if v := p.Profiles[s.Profile]; v != "" {
		key = v
	}
	if v := str(a.Config.Local, "brief_preamble"); v != "" {
		key = v
	}
	block := p.Blocks[key]
	if block == "" {
		return "", fail(exitConfig, "missing brief preamble %q", key)
	}
	profile := "unknown"
	if seat.Canon != "" {
		profile = launchProfile(seat.Canon, launch.Effort)
	}
	trailers := ""
	if s.Task != "" {
		var lines []string
		for _, field := range []struct{ name, value string }{
			{"Mergewake-Author-Family", seat.Family},
			{"Mergewake-Author-Profile", profile},
			{"Mergewake-Task", s.Task},
			{"Mergewake-Grade", s.TaskGrade},
			{"Mergewake-Tier", s.T},
		} {
			if field.value != "" && field.value != "unknown" {
				lines = append(lines, field.name+": "+field.value)
			}
		}
		trailers = "Mergewake trailers (paste at the end of the PR body):\n" + strings.Join(lines, "\n") + "\n\n"
	}
	return fmt.Sprintf("panewire brief-policy: v%d / %s\n%s\nLaunch identity: family=%s launch_profile=%s (use these for the Mergewake-Author-Family and Mergewake-Author-Profile trailers)\n\n%s%s", p.Version, key, block, firstNonempty(seat.Family, "unknown"), profile, trailers, body), nil
}

// Package releasecomponents names the panewire components a release changed,
// so that a rollout restarts only the daemons whose code moved. The release
// workflow feeds it the paths changed since the previous release and
// publishes its components.json next to the binaries. The path map and its
// rules are documented in docs/release-components.md.
package releasecomponents

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
)

// Component names. A release restarts a daemon only when its component is
// listed. The CLI is part of every release and is never listed.
const (
	Hub  = "hub"
	Node = "node"
)

// PathMapVersion identifies the rules in classify. Bump it when a rule
// changes, so a rollout can tell which map named a release's components.
const PathMapVersion = 1

// Manifest is the components.json published with each release.
type Manifest struct {
	Version      string   `json:"version"`
	Previous     string   `json:"previous,omitempty"`
	Basis        string   `json:"basis"`
	Components   []string `json:"components"`
	CLIOnly      bool     `json:"cli_only"`
	Summary      string   `json:"summary"`
	ChangedFiles int      `json:"changed_files"`
	PathMap      int      `json:"path_map"`
}

type mask uint8

const (
	maskHub mask = 1 << iota
	maskNode
)

// classify returns the daemon components a changed path can affect. An empty
// mask means the path shapes no shipped binary: documentation, tests and
// testdata only. Every other path maps to both components, the conservative
// default: CI, scripts and build settings change how the daemon binaries are
// built (the version stamp, the flags, the toolchain), so they restart both.
func classify(changed string) mask {
	p := path.Clean(strings.ReplaceAll(changed, "\\", "/"))
	switch {
	case strings.HasSuffix(p, "_test.go"), strings.HasSuffix(p, ".md"):
		return 0
	case hasDirPrefix(p, "docs/", "testdata/"):
		return 0
	case p == "hub_ui.html":
		// Served by the hub's own HTTP handler (embedded in hub.go).
		return maskHub
	case hasDirPrefix(p, "deploy/") && strings.HasPrefix(path.Base(p), "dev.panewire.panewired"):
		// The node daemon's LaunchAgent template.
		return maskNode
	}
	return maskHub | maskNode
}

func hasDirPrefix(p string, prefixes ...string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}

// Build maps the changed paths to the release manifest. unknown reports that
// the diff could not be computed (no previous release, or git failed): every
// component is then listed.
func Build(version, previous string, changed []string, unknown bool) Manifest {
	m := Manifest{Version: version, Previous: previous, PathMap: PathMapVersion, Components: []string{}}
	if unknown {
		m.Basis = "unknown"
		m.Components = []string{Hub, Node}
	} else {
		m.Basis = "diff"
		m.ChangedFiles = len(changed)
		var got mask
		for _, p := range changed {
			got |= classify(p)
		}
		if got&maskHub != 0 {
			m.Components = append(m.Components, Hub)
		}
		if got&maskNode != 0 {
			m.Components = append(m.Components, Node)
		}
	}
	sort.Strings(m.Components)
	m.CLIOnly = len(m.Components) == 0
	m.Summary = summarize(m)
	return m
}

func summarize(m Manifest) string {
	switch {
	case m.Basis == "unknown":
		return "unknown baseline, so every daemon restarts: hub, node"
	case m.CLIOnly:
		return "cli-only: no daemon component changed since " + m.Previous
	default:
		return "changed since " + m.Previous + ": " + strings.Join(m.Components, ", ")
	}
}

// Run is the release-components command: it reads changed paths, one per
// line, from stdin and writes components.json to stdout. With -summary it
// prints the one-line summary instead.
func Run(args []string, stdin io.Reader, stdout io.Writer) error {
	fs := flag.NewFlagSet("release-components", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	version := fs.String("version", "", "release tag the manifest describes (required)")
	previous := fs.String("previous", "", "previous release tag the paths are diffed from")
	unknown := fs.Bool("unknown", false, "the diff is unavailable: list every component")
	summary := fs.Bool("summary", false, "print the one-line summary instead of the JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *version == "" {
		return fmt.Errorf("-version is required")
	}
	var changed []string
	if !*unknown {
		scanner := bufio.NewScanner(stdin)
		for scanner.Scan() {
			if line := strings.TrimSpace(scanner.Text()); line != "" {
				changed = append(changed, line)
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
	}
	m := Build(*version, *previous, changed, *unknown)
	if *summary {
		_, err := fmt.Fprintln(stdout, m.Summary)
		return err
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(m)
}

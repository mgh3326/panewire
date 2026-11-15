package releasecomponents

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func TestPathMapComponents(t *testing.T) {
	cases := []struct {
		name    string
		changed []string
		want    []string
		cliOnly bool
	}{
		{"hub-only UI page", []string{"hub_ui.html"}, []string{Hub}, false},
		{"node-only LaunchAgent template", []string{"deploy/dev.panewire.panewired.plist"}, []string{Node}, false},
		{"shared root Go file", []string{"hub_inbox.go"}, []string{Hub, Node}, false},
		{"shared internal package", []string{"internal/fleet/admission.go"}, []string{Hub, Node}, false},
		{"unknown path defaults to both", []string{"some_new_file.go"}, []string{Hub, Node}, false},
		{"hub and node together", []string{"hub_ui.html", "node_command.go"}, []string{Hub, Node}, false},
		{"docs only is cli-only", []string{"docs/releasing.md", "README.md"}, []string{}, true},
		{"build scripts shape the daemon binaries", []string{"scripts/build-release.sh"}, []string{Hub, Node}, false},
		{"CI workflows and actions are conservative", []string{".github/actions/setup-go-cached/action.yml"}, []string{Hub, Node}, false},
		{"docs and testdata are cli-only", []string{"docs/a.md", "testdata/job_golden/x.json"}, []string{}, true},
		{"tests only are cli-only", []string{"hub_inbox_test.go"}, []string{}, true},
		{"no changed paths is cli-only", nil, []string{}, true},
		{"path separators are normalised", []string{`docs\x.md`}, []string{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := Build("v0.0.300", "v0.0.299", tc.changed, false)
			if !slices.Equal(m.Components, tc.want) {
				t.Fatalf("components=%v, want %v", m.Components, tc.want)
			}
			if m.CLIOnly != tc.cliOnly {
				t.Fatalf("cli_only=%v, want %v", m.CLIOnly, tc.cliOnly)
			}
		})
	}
}

func TestUnknownBaselineListsEveryComponent(t *testing.T) {
	m := Build("v0.0.300", "", nil, true)
	if !slices.Equal(m.Components, []string{Hub, Node}) || m.Basis != "unknown" || m.CLIOnly {
		t.Fatalf("unknown baseline manifest = %+v", m)
	}
	if !strings.Contains(m.Summary, "every daemon restarts") {
		t.Fatalf("summary=%q", m.Summary)
	}
}

func TestSummaryNamesTheReleaseChange(t *testing.T) {
	hub := Build("v0.0.300", "v0.0.299", []string{"hub_ui.html"}, false)
	if hub.Summary != "changed since v0.0.299: hub" {
		t.Fatalf("hub summary=%q", hub.Summary)
	}
	cli := Build("v0.0.300", "v0.0.299", []string{"docs/a.md"}, false)
	if cli.Summary != "cli-only: no daemon component changed since v0.0.299" {
		t.Fatalf("cli summary=%q", cli.Summary)
	}
}

func TestRunWritesManifestJSON(t *testing.T) {
	var out bytes.Buffer
	in := strings.NewReader("hub_ui.html\n\ndocs/x.md\n")
	if err := Run([]string{"-version", "v0.0.300", "-previous", "v0.0.299"}, in, &out); err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err := json.Unmarshal(out.Bytes(), &m); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, out.String())
	}
	if !slices.Equal(m.Components, []string{Hub}) || m.ChangedFiles != 2 || m.PathMap != PathMapVersion {
		t.Fatalf("manifest = %+v", m)
	}
}

func TestRunUnknownIgnoresStdinAndRequiresVersion(t *testing.T) {
	var out bytes.Buffer
	if err := Run([]string{"-version", "v0.0.300", "-unknown", "-summary"}, strings.NewReader("docs/x.md\n"), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "unknown baseline, so every daemon restarts: hub, node\n" {
		t.Fatalf("summary=%q", got)
	}
	if err := Run(nil, strings.NewReader(""), &out); err == nil {
		t.Fatal("missing -version must fail")
	}
}

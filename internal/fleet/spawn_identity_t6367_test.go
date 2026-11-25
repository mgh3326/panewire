package fleet

import (
	"testing"

	"github.com/mgh3326/panewire/internal/spawnargs"
)

// ParseSpawnIdentity names a spawn exactly as the launcher does.
//
// This is defence in depth for older hubs: spawnargs.Refusal stops the
// repeated, --flag=value, single-dash and empty-value forms below on the wire
// today (the node refuses them on decode, the hub on submit). The rows marked
// wire pass that rule, so a node reads them in production.
func TestSpawnIdentityMatchesLauncherParse_t6367(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want SpawnIdentity
		// wire: spawnargs.Refusal admits the argv for an operator.
		wire bool
		// refused: the launcher reads the argv and then refuses a value.
		refused bool
	}{
		{name: "job and label", args: []string{"--job", "x", "-l", "y"}, want: SpawnIdentity{Job: "x", Label: "y"}, wire: true},
		{name: "label only", args: []string{"-l", "y"}, want: SpawnIdentity{Job: "y", Label: "y"}, wire: true},
		{name: "job only", args: []string{"--job", "x"}, want: SpawnIdentity{Job: "x"}, wire: true},
		{name: "repeated lane", args: []string{"-L", "mock", "-L", "live", "-l", "y"}, want: SpawnIdentity{Job: "y", Label: "y", ExecutionLane: "live"}},
		{name: "repeated label", args: []string{"-l", "a", "-l", "b"}, want: SpawnIdentity{Job: "b", Label: "b"}},
		{name: "repeated job", args: []string{"--job", "a", "--job", "b", "-l", "y"}, want: SpawnIdentity{Job: "b", Label: "y"}},
		{name: "equals form", args: []string{"--job=x", "-l=y"}, want: SpawnIdentity{Job: "x", Label: "y"}},
		{name: "single and double dash", args: []string{"-job", "x", "--l", "y", "--L", "mock"}, want: SpawnIdentity{Job: "x", Label: "y", ExecutionLane: "mock"}},
		{name: "empty job falls back to the label", args: []string{"--job", "", "-l", "y"}, want: SpawnIdentity{Job: "y", Label: "y"}},
		{name: "booleans before flags", args: []string{"--keep", "--job-dup-ok", "--task-hk-bypass", "--allow-escalation", "--job", "x", "-l", "y"}, want: SpawnIdentity{Job: "x", Label: "y"}, wire: true},
		{name: "purpose value is -L", args: []string{"--purpose", "-L", "-l", "y"}, want: SpawnIdentity{Job: "y", Label: "y"}, wire: true},
		{name: "purpose value is -l", args: []string{"--purpose", "-l", "-l", "y"}, want: SpawnIdentity{Job: "y", Label: "y"}, wire: true},
		{name: "purpose value is --job", args: []string{"--purpose", "--job", "-l", "y"}, want: SpawnIdentity{Job: "y", Label: "y"}, wire: true},
		{name: "purpose value is --job=forged", args: []string{"--purpose", "--job=forged", "--job", "x", "-l", "y"}, want: SpawnIdentity{Job: "x", Label: "y"}, wire: true},
		{name: "directory", args: []string{"-c", "/work/a", "-l", "y"}, want: SpawnIdentity{Job: "y", Label: "y", Dir: "/work/a"}, wire: true},
		{name: "--lane is not the execution lane", args: []string{"--lane", "owner-lane", "-l", "y"}, want: SpawnIdentity{Job: "y", Label: "y"}, wire: true},
		{name: "nothing named", args: []string{"-m", "auto"}, want: SpawnIdentity{}, wire: true},
		{name: "value the launcher refuses", args: []string{"--role", "implementer", "--review-pass", "2", "--job", "x", "-l", "y"}, want: SpawnIdentity{Job: "x", Label: "y"}, wire: true, refused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if refusal := spawnargs.Refusal(tc.args, true); (refusal == "") != tc.wire {
				t.Fatalf("wire refusal %q, want admitted=%v", refusal, tc.wire)
			}
			got, err := ParseSpawnIdentity(tc.args)
			if err != nil || got != tc.want {
				t.Fatalf("ParseSpawnIdentity=%+v err=%v, want %+v", got, err, tc.want)
			}
			launched, err := parseSpawn(tc.args)
			if tc.refused {
				if err == nil {
					t.Fatal("the launcher accepted this argv; the row no longer proves anything")
				}
				return
			}
			if err != nil {
				t.Fatalf("launcher parse: %v", err)
			}
			if parsed := (SpawnIdentity{Job: launched.Job, Label: launched.Label, Dir: launched.CWD, ExecutionLane: launched.ExecutionLane}); parsed != got {
				t.Fatalf("launcher reads %+v, identity %+v", parsed, got)
			}
			// The node's binding replaces the directory and keeps the names.
			bound := bindNodeSpawn(launched, LaunchDir{Path: "/node/cwd"}, "/node/brief", "workspace")
			if bound.Job != got.Job || bound.Label != got.Label || bound.ExecutionLane != got.ExecutionLane || bound.CWD != "/node/cwd" {
				t.Fatalf("node binding names job=%q label=%q lane=%q cwd=%q, identity %+v", bound.Job, bound.Label, bound.ExecutionLane, bound.CWD, got)
			}
		})
	}
}

// An argv the launcher cannot read names nothing.
func TestSpawnIdentityRejectsWhatTheLauncherCannotRead_t6367(t *testing.T) {
	for name, args := range map[string][]string{
		"missing value":    {"-l", "y", "--job"},
		"stray argument":   {"--job", "a", "stray", "--job", "b"},
		"unknown flag":     {"--job", "a", "--no-such-flag", "x"},
		"value after bool": {"--keep", "true", "--job", "a"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseSpawn(args); err == nil {
				t.Fatal("the launcher accepted this argv")
			}
			if got, err := ParseSpawnIdentity(args); err == nil || got != (SpawnIdentity{}) {
				t.Fatalf("ParseSpawnIdentity=%+v err=%v, want an error and no names", got, err)
			}
		})
	}
}

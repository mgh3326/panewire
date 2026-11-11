package panewire

import "testing"

// hk 3973: the node refuses the same --e6-arm forms submission refuses. An
// effortless rung's marker is canon@ (nothing after the @); the seat gate, not
// the node, decides whether the rung it names is armed.
func TestHubSpawnArgsE6ArmEffortlessRung_t3973(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want bool
	}{
		{"effortless arm", []string{"-m", "devin-ds41-max", "--e6-arm", "devin-ds41-max@"}, true},
		{"effort arm", []string{"-m", "devin-ds41-max", "--e6-arm", "devin-ds41-max@high"}, true},
		{"foreign canon", []string{"-m", "devin-ds41-max", "--e6-arm", "other@"}, false},
		{"no @", []string{"-m", "devin-ds41-max", "--e6-arm", "devin-ds41-max"}, false},
		{"second @", []string{"-m", "devin-ds41-max", "--e6-arm", "devin-ds41-max@@"}, false},
		{"whitespace", []string{"-m", "devin-ds41-max", "--e6-arm", "devin-ds41-max@ high"}, false},
		{"arm without -m", []string{"--e6-arm", "devin-ds41-max@"}, false},
	} {
		if got := validHubSpawnArgs(tc.args); got != tc.want {
			t.Errorf("%s: validHubSpawnArgs(%q)=%v want %v", tc.name, tc.args, got, tc.want)
		}
	}
}

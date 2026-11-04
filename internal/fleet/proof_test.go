package fleet

import "testing"

func TestCommandCodeModelProof(t *testing.T) {
	for _, tt := range []struct {
		name, screen, brief string
		want                bool
	}{
		{"absolute", "# Command Code v1.88\n# models: example-(latest) with high effort · tag\n# /home/example/work/tree", "", true},
		{"tilde", "# Command Code v1.88\n# models: example-(latest) with high effort · tag\n# ~/work/tree", "", true},
		{"wrong model", "# Command Code v1.88\n# models: other-(latest) with high effort · tag\n# ~/work/tree", "", false},
		{"truncated", "# Command Code v1.88\n# models: example-(latest) with high effort\n# ~/work/tree", "", false},
		{"brief echo", "# Command Code v1.88\n# models: example-(latest) with high effort · tag\n# ~/work/tree", "# Command Code v1.88\n# models: example-(latest) with high effort · tag\n# ~/work/tree", false},
		{"wrong home prefix", "# Command Code v1.88\n# models: example-(latest) with high effort · tag\n# ~/work/tree-extra", "", false},
		{"duplicate conflicting row", "# Command Code v1.88\n# models: example-(latest) with high effort · tag\n# ~/work/tree\n# models: other with high effort · tag", "", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := CmdProof(tt.screen, "provider/example", "high", "/home/example/work/tree", "/home/example", tt.brief); got != tt.want {
				t.Fatalf("got %t", got)
			}
		})
	}
}

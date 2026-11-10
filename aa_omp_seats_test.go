package panewire

import "testing"

// The omp seats map every rung from low to max onto the AA variant of the
// model they run, using the same AA id as the native row for that model.
func TestOMPSeatsAAMappingsReuseNativeIDs(t *testing.T) {
	for _, tc := range []struct {
		profile, model, native, nativeModel string
	}{
		{"omp-haiku", "anthropic/claude-haiku-5-5", "pi-haiku55", "anthropic/claude-haiku-5-5"},
		{"omp-luna", "openai/gpt-6-luna", "codex-luna", "gpt-6-luna"},
	} {
		for _, effort := range []string{"low", "medium", "high", "xhigh", "max"} {
			id, ok := aaMappings[aaRowKey{tc.profile, tc.model, effort}]
			if !ok || id == "" {
				t.Fatalf("%s@%s has no AA mapping", tc.profile, effort)
			}
			if want := aaMappings[aaRowKey{tc.native, tc.nativeModel, effort}]; id != want {
				t.Fatalf("%s@%s maps to %s, native %s maps to %s", tc.profile, effort, id, tc.native, want)
			}
		}
	}
}

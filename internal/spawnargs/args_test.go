package spawnargs

import (
	"strings"
	"testing"
)

// A refusal names the flag and the rule it broke, so the operator can correct
// the request the node dropped.
func TestRefusalNamesFlagAndRule(t *testing.T) {
	for name, test := range map[string]struct {
		args     []string
		operator bool
		want     string
	}{
		"operator request hash":  {[]string{"--operator-request", "hk:doc/x#y"}, true, "--operator-request value has a character outside [A-Za-z0-9._:/@-]"},
		"duplicate flag":         {[]string{"--job", "a", "--job", "b"}, true, "duplicate --job"},
		"unknown flag":           {[]string{"--nonsense", "x"}, true, "--nonsense is not a flag the node accepts"},
		"missing value":          {[]string{"--job"}, true, "--job needs a value"},
		"empty value":            {[]string{"--job", ""}, true, "--job value is empty"},
		"control character":      {[]string{"--job", "a\nb"}, true, "--job value contains a control character"},
		"too long":               {[]string{"--job", strings.Repeat("a", 201)}, true, "--job value is longer than 200 bytes"},
		"tags shape":             {[]string{"--tags", "a b"}, true, "--tags value must be comma-separated values of [A-Za-z0-9._:/@-], at most 16"},
		"grade shape":            {[]string{"--grade", "A++"}, true, "--grade value must be C, B, A, A+, S, S+ or SS"},
		"excluded host shape":    {[]string{"--exclude-host", "a b"}, true, "--exclude-host value must be comma-separated values of [A-Za-z0-9._:/@-]"},
		"role shape":             {[]string{"--role", "captain"}, true, "--role value must be implementer or reviewer"},
		"old worker role":        {[]string{"--role", "worker"}, true, "--role value must be implementer or reviewer"},
		"old tester role":        {[]string{"--role", "tester"}, true, "--role value must be implementer or reviewer"},
		"old counsel role":       {[]string{"--role", "counsel"}, true, "--role value must be implementer or reviewer"},
		"author family shape":    {[]string{"--author-family", "acme"}, true, "--author-family value must be anthropic"},
		"old builder family":     {[]string{"--builder-family", "anthropic"}, true, "--builder-family is not a flag the node accepts"},
		"routing role removed":   {[]string{"--routing-role", "consult"}, true, "--routing-role is not a flag the node accepts"},
		"review pass shape":      {[]string{"--review-pass", "10"}, true, "--review-pass value must be a single digit from 1 to 9"},
		"builder job shape":      {[]string{"--builder-job", "a/b"}, true, "--builder-job value must be 1-128 characters"},
		"report relative shape":  {[]string{"--report-relative", "a/../b"}, true, "--report-relative value must be a relative path"},
		"operator only flag":     {[]string{"--operator-request", "hk:task/1"}, false, "--operator-request is not a flag the node accepts"},
		"operator flag accepted": {[]string{"--operator-request", "hk:task/1"}, true, ""},
		"consult needs operator": {[]string{"--role", "consult"}, false, "--role value must be implementer or reviewer"},
		"consult for operator":   {[]string{"--role", "consult"}, true, ""},
		"implementer for node":   {[]string{"--role", "implementer"}, false, ""},
		"path over the name cap": {[]string{"-c", "/" + strings.Repeat("a", 200)}, true, ""},
		"path needs operator":    {[]string{"-c", "/work/project"}, false, "-c is not a flag the node accepts"},
		"free text override":     {[]string{"--override-reason", "hk 3965 verified"}, true, ""},
		"free text workspace":    {[]string{"-w", "worker pool"}, true, ""},
	} {
		t.Run(name, func(t *testing.T) {
			got := Refusal(test.args, test.operator)
			if test.want == "" {
				if got != "" {
					t.Fatalf("Refusal(%q)=%q, want acceptance", test.args, got)
				}
				return
			}
			if !strings.Contains(got, test.want) {
				t.Fatalf("Refusal(%q)=%q, want it to name %q", test.args, got, test.want)
			}
		})
	}
}

func TestValidDocKey(t *testing.T) {
	for key, want := range map[string]bool{
		"briefs/job-a.md": true,
		"job-a/report.md": true,
		"/briefs/a.md":    false,
		"briefs/":         false,
		"a//b":            false,
		"./a":             false,
		"a/../b":          false,
		"":                false,
		"a:b":             false,
	} {
		if got := ValidDocKey(key); got != want {
			t.Errorf("ValidDocKey(%q)=%v, want %v", key, got, want)
		}
	}
}

func TestGradeSSSpawnArgs(t *testing.T) {
	for _, value := range []string{"SS"} {
		if got := Refusal([]string{"--grade", value}, true); got != "" {
			t.Errorf("--grade %q refused: %s", value, got)
		}
	}
	for _, value := range []string{"A++", "Z", "SS\n"} {
		if got := Refusal([]string{"--grade", value}, true); got == "" {
			t.Errorf("--grade %q was accepted", value)
		}
	}
}

package spawnargs

import (
	"strings"
	"testing"
)

// hk 4959: a family panewire hands out is a name the node accepts for
// --author-family, and a name it refuses is not handed out.

func TestAuthorFamilyListAcceptedByNode(t *testing.T) {
	for _, family := range AuthorFamilies() {
		for _, operator := range []bool{false, true} {
			if got := Refusal([]string{"--author-family", family}, operator); got != "" {
				t.Errorf("--author-family %s refused (operator=%v): %s", family, operator, got)
			}
		}
	}
}

func TestAuthorFamilyRefusesNamesOffTheList(t *testing.T) {
	for _, family := range []string{"acme", "Cognition", "cognitions", "devin", "moonshot"} {
		got := Refusal([]string{"--author-family", family}, true)
		if !strings.Contains(got, "--author-family value must be anthropic, cognition, deepseek") {
			t.Errorf("--author-family %s: refusal %q does not name the list", family, got)
		}
	}
}

func TestAuthorFamilyListIsUniqueAndSorted(t *testing.T) {
	seen := map[string]bool{}
	prev := ""
	for _, family := range AuthorFamilies() {
		if seen[family] {
			t.Errorf("family %q listed twice", family)
		}
		if family <= prev {
			t.Errorf("family %q is out of order after %q", family, prev)
		}
		seen[family] = true
		prev = family
	}
}

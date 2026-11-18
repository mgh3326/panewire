package spawnargs

import (
	"slices"
	"strings"
)

// authorFamilies is the one list of author families: each family a seat is
// handed out with (its "family" in internal/fleet/policy.json, which the brief
// writes as the Mergewake-Author-Family trailer) and each name the node accepts
// for --author-family. Add a family here before a seat may carry it, so a name
// panewire hands out is never one it refuses (hk 4959). xai and mistral predate
// the policy and stay accepted. The list is sorted; a test holds that order.
var authorFamilies = []string{"anthropic", "cognition", "deepseek", "google", "meta", "minimax", "mistral", "mixed", "openai", "other", "qwen", "upstage", "xai", "zhipu"}

// AuthorFamilies returns the list in order. The caller gets a copy.
func AuthorFamilies() []string {
	return slices.Clone(authorFamilies)
}

// ValidAuthorFamily reports whether the node accepts name for --author-family.
func ValidAuthorFamily(name string) bool {
	return slices.Contains(authorFamilies, name)
}

// authorFamilyDescription names the list for a refusal: "a, b or z".
func authorFamilyDescription() string {
	last := len(authorFamilies) - 1
	return strings.Join(authorFamilies[:last], ", ") + " or " + authorFamilies[last]
}

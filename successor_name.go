package panewire

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strconv"
	"strings"

	"github.com/mgh3326/panewire/internal/fleet"
)

// successorNameMax is the length herdr allows an agent name: "[a-z][a-z0-9_-]{0,31}",
// 1 to 32 characters (herdr agent --help, and its "agent name must start with a
// lowercase letter ... (1-32 characters)" refusal). A successor's job id is kept
// within it, so the -l label built from the same helper can always fit it.
const successorNameMax = 32

// successorChain is the trailing run of replacement markers of a job id or
// label: -l<n> for a ladder step, -q<n> for a quota replacement, -h<n> for a
// hand-over. quotaReplaceChain and escalateAuthorChain count replacements from
// it, so a shortened name keeps the whole run.
var successorChain = regexp.MustCompile(`(?:-[lhq][0-9]+)*$`)

// successorName builds the successor of old for replacement kind (q, l or h)
// number n. A name that fits successorNameMax is "<old>-<kind><n>". A longer one
// keeps old's trailing marker chain and the new marker whole, and cuts the base
// before the chain to a prefix plus the first 8 hex digits of sha256(old):
// "<prefix>-<hash>-l1-q1". The result is deterministic, unique per (old, kind,
// n), and a further replacement of it is shortened the same way. ok is false
// when the result is not a valid job id (fleet.ValidJobID). A -l label must
// also pass fleet.ValidAgentName, which the callers that set one check.
func successorName(old, kind string, n int) (name string, ok bool) {
	tail := "-" + kind + strconv.Itoa(n)
	if full := old + tail; len(full) <= successorNameMax {
		return full, fleet.ValidJobID(full)
	}
	chain := successorChain.FindString(old)
	head := strings.TrimSuffix(old, chain)
	sum := sha256.Sum256([]byte(old))
	hash := "-" + hex.EncodeToString(sum[:4])
	marks := chain + tail
	budget := successorNameMax - len(hash) - len(marks)
	if budget < 1 {
		return "", false
	}
	base := strings.TrimRight(utf8PrefixWithin(head, budget), "-_")
	name = base + hash + marks
	return name, fleet.ValidJobID(name)
}

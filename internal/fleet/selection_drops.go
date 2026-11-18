package fleet

import (
	"slices"
	"strings"
)

// SelectionDrop is one SWE-2 candidate a placed selection dropped: the reason
// the selection gave and the Devin account it ran on. The hub counts them per
// UTC day into the daily loops row (docs/loops.md, selection_drops), so a
// design session can see how often the per-account and pool caps dropped
// SWE-2 (decision 427).
type SelectionDrop struct {
	Reason  string
	Account string
}

// selectionDropReasons are the drop reasons the daily count reads.
var selectionDropReasons = []string{reasonPoolAtCap, "free_limit", "pool_cooling"}

// selectionDrops lists the SWE-2 candidates the selection record dropped for a
// counted reason, one entry per candidate. An admitted or merely ranked-below
// candidate is never listed, and neither is a non-SWE-2 drop.
func selectionDrops(sel map[string]any) []SelectionDrop {
	cands, _ := sel["candidates"].([]any)
	var out []SelectionDrop
	for _, raw := range cands {
		row, _ := raw.(map[string]any)
		if !strings.HasPrefix(str(row, "outcome"), "dropped:") {
			continue
		}
		reason := str(row, "reason")
		if !slices.Contains(selectionDropReasons, reason) || !IsFreeRung(str(row, "profile")) {
			continue
		}
		account := str(row, "account_key")
		if account == "" {
			account = "unknown"
		}
		out = append(out, SelectionDrop{Reason: reason, Account: account})
	}
	return out
}

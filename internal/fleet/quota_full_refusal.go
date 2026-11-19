package fleet

import (
	"context"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/quota/devin"
	"github.com/mgh3326/panewire/internal/quotastop"
)

// QuotaFull is what a node's quota gate said when it refused a spawn because
// the chosen seat's quota is full: a free limit, an exhausted window or a
// credit balance below one. The hub moves an automatic request on to its next
// candidate on this refusal (docs/hub-spawn-operations.md, "A full quota moves
// an automatic request on", hk 5358).
type QuotaFull struct {
	// Reason is the gate's own reason, after "quota gate: ".
	Reason string
	// Free is a SWE-2 free-bucket limit: the free band, not the paid window.
	Free bool
	// Reset is when the limit ends; zero when the refusal names none (a
	// window or a credit balance has no end time in the gate's text).
	Reset time.Time
}

// quotaFullReasons are the gate reasons (admission_verdict.go) that say the
// seat's quota is full. A refusal the request, the catalog or the policy
// decided ("pool excluded by policy", "e6_arm_required") and an unmeasured
// reading ("measurement unavailable") are not full quota and stay refusals.
var quotaFullReasons = []string{freeLimitReason, "quota exhausted window=", "credit_exhausted balance="}

const (
	quotaGateRefusalPrefix = "quota gate: "
	freeLimitReason        = "free limit exceeded until "
)

// QuotaFullRefusal reads a spawn refusal's text. ok is false for any refusal
// that is not a full quota. The reset is read from the text alone, so the hub
// and the node derive it the same way across a deploy skew.
func QuotaFullRefusal(text string) (full QuotaFull, ok bool) {
	for _, line := range strings.Split(text, "\n") {
		_, reason, found := strings.Cut(line, quotaGateRefusalPrefix)
		if !found {
			continue
		}
		reason = strings.TrimSpace(reason)
		for _, want := range quotaFullReasons {
			rest, match := strings.CutPrefix(reason, want)
			if !match {
				continue
			}
			full = QuotaFull{Reason: reason}
			if want == freeLimitReason {
				full.Free = true
				if reset, err := time.Parse(time.RFC3339, strings.TrimSpace(strings.Fields(rest + " ")[0])); err == nil {
					full.Reset = reset.UTC()
				}
			}
			return full, true
		}
	}
	return QuotaFull{}, false
}

// recordRefusedFreeLimit keeps what a free-limit refusal teaches: the limit's
// end time for this host's Devin account. The refusal is the gate's own reading
// of that limit, so the record is the same one a quota-stopped pane writes
// (RecordFreeLimit), published at once into the account's shared reading
// (publishFreeLimit), and every host's selection skips the free band until the
// reset instead of walking into the same refusal. It runs on a refusal only,
// so a spawn that succeeds pays nothing (hk 5358).
func (a *App) recordRefusedFreeLimit(ctx context.Context, job string, refusal error) {
	full, ok := QuotaFullRefusal(refusal.Error())
	if !ok || !full.Free || full.Reset.IsZero() || !full.Reset.After(a.Now()) || ctx.Err() != nil {
		return
	}
	stop := quotastop.Stop{Pool: "devin-free", Reset: full.Reset}
	if err := RecordFreeLimit(devin.Config{Home: a.Home, Now: a.Now}, job, stop); err != nil {
		a.warn("free-limit refusal not recorded for job %s: %v", job, err)
		return
	}
	a.publishFreeLimit(ctx, job)
}

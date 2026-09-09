package panewire

// HubQuotaGateOutcome is the closed vocabulary consumed by the wrk adapter.
// Only Unavailable permits the existing local-only fallback.
type HubQuotaGateOutcome string

const (
	HubQuotaGateAllow          HubQuotaGateOutcome = "allow"
	HubQuotaGateDeny           HubQuotaGateOutcome = "deny"
	HubQuotaGateUnavailable    HubQuotaGateOutcome = "unavailable"
	HubQuotaGateAuthentication HubQuotaGateOutcome = "authentication_error"
	HubQuotaGateMalformed      HubQuotaGateOutcome = "fail_closed"
)

type WrkQuotaGateResult string

const (
	WrkQuotaGateAllow          WrkQuotaGateResult = "allow"
	WrkQuotaGateDeny           WrkQuotaGateResult = "deny"
	WrkQuotaGateUnknown        WrkQuotaGateResult = "unknown"
	WrkQuotaGateAuthentication WrkQuotaGateResult = "authentication_error"
	WrkQuotaGateFailClosed     WrkQuotaGateResult = "fail_closed"
)

// ResolveWrkQuotaGate is the panewire-side reference contract for the adapter
// owned by wrk. localExit is scopefuel gate's stable 0/3/4 exit vocabulary.
// A hub allow can therefore never override a local role or quota denial.
func ResolveWrkQuotaGate(hub HubQuotaGateOutcome, localExit int) WrkQuotaGateResult {
	switch hub {
	case HubQuotaGateAuthentication:
		return WrkQuotaGateAuthentication
	case HubQuotaGateMalformed:
		return WrkQuotaGateFailClosed
	case HubQuotaGateDeny:
		return WrkQuotaGateDeny
	case HubQuotaGateAllow, HubQuotaGateUnavailable:
		switch localExit {
		case 0:
			return WrkQuotaGateAllow
		case 3:
			return WrkQuotaGateDeny
		case 4:
			return WrkQuotaGateUnknown
		default:
			return WrkQuotaGateFailClosed
		}
	default:
		return WrkQuotaGateFailClosed
	}
}

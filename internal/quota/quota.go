// Package quota holds the vocabulary every provider measurement and the shared
// quota store use: the buckets a measurement yields and the error kinds a
// failure reports. The kind names are pacelever's (kinds.go) so readers of its
// JSON, such as the fleet-state login rule, keep working unchanged.
package quota

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

// Error kinds. They are part of the quota contract: a producer and a reader
// compare these names, never a string literal.
const (
	ErrKindAuth                = "auth"          // 401/403 without a provider-specific hint
	ErrKindAuthFailed          = "auth_failed"   // 401/403 with a provider-specific login hint
	ErrKindEdgeRejected        = "edge_rejected" // an edge proxy refused the request (403 without JSON)
	ErrKindCredentials         = "credentials"   // no usable local credential
	ErrKindHTTP                = "http"          // any other non-200 response
	ErrKindNetwork             = "network"       // the request never got a response
	ErrKindParse               = "parse_error"   // a response or screen had no usable windows
	ErrKindProbe               = "probe_error"   // the CLI probe could not run
	ErrKindRateLimited         = "rate_limited"  // 429; RetryAfter drives the backoff
	ErrKindTimeout             = "timeout"       // the collection deadline passed
	ErrKindTokenExpired        = "token_expired" // the saved login is past its expiry
	ErrKindAccountMismatch     = "account_mismatch"
	ErrKindAccountUnknown      = "account_unknown"
	ErrKindProbeFailed         = "probe_failed"
	ErrKindSharedFailed        = "shared_failed" // an in-flight shared measurement ended without a publication
	ErrKindSharedPending       = "shared_pending"
	ErrKindSharedPublishFailed = "shared_publish_failed"
	ErrKindSharedReleaseFailed = "shared_release_failed"
	ErrKindSharedUnavailable   = "shared_unavailable"
	ErrKindSharedWait          = "shared_wait"
)

// Observation sources and bucket scopes, as pacelever names them.
const (
	SourceUsageAPI    = "usage-api"
	SourceUsageRPC    = "usage-rpc"
	SourceCLI         = "cli:/usage"
	SourceHandoffkeep = "handoffkeep"

	ScopeAccount = "account"
	ScopeModel   = "model"
)

// Scope says what a bucket limits: the whole account or one model.
type Scope struct {
	Kind string
	Name string
}

// Bucket is one measured window. UsedPct is nil for a window the provider
// reported without a value; ResetsAt is RFC 3339 or empty.
type Bucket struct {
	Window   string
	UsedPct  *float64
	ResetsAt string
	Scope    Scope
}

// Observation is one successful measurement. AccountFP is the 16-hex account
// fingerprint, set only when the login is one the account may be shared from.
type Observation struct {
	Source    string
	Plan      string
	Buckets   []Bucket
	AccountFP string
}

// Error is a measurement failure. It carries a kind and a fixed, safe
// sentence, never a response body, URL or credential.
type Error struct {
	Kind    string
	Message string
	// RetryAfter is the server-declared wait of a rate_limited failure.
	RetryAfter time.Duration
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Kind, e.Message) }

// Fail is an Error of the given kind.
func Fail(kind, message string) *Error { return &Error{Kind: kind, Message: message} }

// KindOf is err's error kind, or "" when err is not a quota Error.
func KindOf(err error) string {
	if e, ok := err.(*Error); ok {
		return e.Kind
	}
	return ""
}

// ValidPct reports whether p is a measured percentage in 0..100.
func ValidPct(p *float64) bool {
	return p != nil && !math.IsNaN(*p) && !math.IsInf(*p, 0) && *p >= 0 && *p <= 100
}

// Pct is v as a percentage pointer, or nil when v is not in 0..100.
func Pct(v float64) *float64 {
	if !ValidPct(&v) {
		return nil
	}
	return &v
}

// ParseFinite parses a decimal number and refuses NaN and ±Inf.
func ParseFinite(s string) (float64, bool) {
	n, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
		return 0, false
	}
	return n, true
}

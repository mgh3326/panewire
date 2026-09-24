package panewire

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Account-scoped quota v2 (task #578). The wire and hub rules follow contract
// quota-v2.r3 (hk review/2026-09-24/578-contract-r3.1, §2 and §7). This is an additive store next
// to the legacy machine-keyed quota surfaces (heartbeat R29 nodeQuota and the
// R19 /v1/quota/{machine} cache). Nothing here reads or writes legacy state and
// nothing in the legacy paths reads v2 state: there is no automatic mixing.
//
// Roles (advice hk 2558 Q2): the hub stores operator enrollments
// (account_bindings) and appends observations that an authenticated node
// measured through its own current binding. It never interprets quota values;
// admission stays in scopefuel's evaluator. Stage 1 has no lease, cooldown or
// manual entries.
const (
	quotaV2ObservationSchema  = "quota-observation/v2"
	quotaV2ContractRev        = "quota-v2.r3"
	quotaV2BindingsSchema     = "quota-bindings/v2"
	quotaV2ObservationsSchema = "quota-observations/v2"
	quotaV2StoreSchema        = "panewire.quota-v2-store/v2" // v2: contract quota-v2.r3 envelopes

	// The body cap is a transport limit, not an envelope rule: the envelope
	// has no bucket-count limit (§2), so the hub adds none.
	quotaV2MaxBodyBytes              = 64 << 10
	quotaV2MaxBindings               = 1024
	quotaV2MaxObservationsPerAccount = 128
	quotaV2MaxBindingValidity        = 30 * 24 * time.Hour
)

var (
	// account_ref is an opaque random id. The fixed shape rejects emails,
	// paths and tokens outright instead of trying to recognise them.
	quotaV2AccountRefPattern = regexp.MustCompile(`^acct_[0-9a-z]{8,40}$`)
	quotaV2RefPattern        = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
	quotaV2ProviderPattern   = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,63}$`)
	quotaV2MachinePattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)
	quotaV2VersionPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+/:-]{0,127}$`)
	quotaV2WindowPattern     = regexp.MustCompile(`^(?:[a-z0-9][a-z0-9._-]{0,31}|\?)$`)
	quotaV2InstancePattern   = regexp.MustCompile(`^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$`)
	// RFC3339 with an explicit offset (hh ≤ 23, mm ≤ 59); the same text rule as scopefuel.
	quotaV2TimePattern   = regexp.MustCompile(`^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$`)
	quotaV2Statuses      = map[string]bool{"success": true, "partial": true, "rate_limited": true, "auth_error": true, "parse_error": true, "transport_error": true}
	quotaV2Horizons      = map[string]bool{"now": true, "week": true, "month": true}
	quotaV2ScopeKinds    = map[string]bool{"account": true, "model": true, "group": true}
	quotaV2IdentityBases = map[string]bool{"operator_attested": true, "provider_subject_verified": true}
	// §4.3 error_ref values; the part before ":" must equal status.
	quotaV2ErrorRefs = map[string]bool{
		"rate_limited:http_429": true, "auth_error:http_401": true, "auth_error:http_403": true,
		"transport_error:http_5xx": true, "transport_error:network": true, "transport_error:timeout": true,
		"parse_error:http_4xx": true, "parse_error:schema": true, "parse_error:required_missing": true,
		"parse_error:duplicate_limit": true, "parse_error:outcome_missing": true, "parse_error:http_unexpected": true,
		"partial:required_missing": true,
	}
	// §2.0: every field is required; only these may hold null.
	quotaV2EnvelopeFields = map[string]bool{
		"schema": false, "contract_rev": false, "observation_id": false, "provider": false, "account_ref": false,
		"entitlement_ref": false, "source_machine": false, "source_slot_ref": false, "source_binding_revision": false,
		"collector_version": false, "measured_at": false, "received_at": true, "status": false, "error_ref": true,
		"unshared_limit_count": false, "buckets": false, "lease_epoch": true,
	}
	quotaV2BucketFields = map[string]bool{
		"limit_id": false, "label": true, "scope": false, "horizon": false, "window": false,
		"window_instance": false, "used_pct": true, "reset_at": true, "observed_at": true,
	}
	quotaV2ScopeFields = map[string]bool{"kind": false, "ref": true}
)

type QuotaV2Scope struct {
	Kind string  `json:"kind"`
	Ref  *string `json:"ref"`
}

type QuotaV2Bucket struct {
	LimitID        string       `json:"limit_id"`
	Label          *string      `json:"label"`
	Scope          QuotaV2Scope `json:"scope"`
	Horizon        string       `json:"horizon"`
	Window         string       `json:"window"`
	WindowInstance string       `json:"window_instance"`
	UsedPct        *float64     `json:"used_pct"`
	ResetAt        *string      `json:"reset_at"`
	ObservedAt     *string      `json:"observed_at"`
}

// QuotaV2Observation is the quota-observation/v2 envelope (§2). measured_at is
// when the provider response completed; received_at is stamped by the hub and
// never replaces it. The POST form carries received_at null; the stored form
// (hub store and GET) carries it non-null.
type QuotaV2Observation struct {
	Schema                string          `json:"schema"`
	ContractRev           string          `json:"contract_rev"`
	ObservationID         string          `json:"observation_id"`
	Provider              string          `json:"provider"`
	AccountRef            string          `json:"account_ref"`
	EntitlementRef        string          `json:"entitlement_ref"`
	SourceMachine         string          `json:"source_machine"`
	SourceSlotRef         string          `json:"source_slot_ref"`
	SourceBindingRevision int64           `json:"source_binding_revision"`
	CollectorVersion      string          `json:"collector_version"`
	MeasuredAt            string          `json:"measured_at"`
	ReceivedAt            *string         `json:"received_at"`
	Status                string          `json:"status"`
	ErrorRef              *string         `json:"error_ref"`
	UnsharedLimitCount    int64           `json:"unshared_limit_count"`
	Buckets               []QuotaV2Bucket `json:"buckets"`
	LeaseEpoch            *int64          `json:"lease_epoch"`
}

// QuotaV2Binding ties one node login slot to one registered account. Label is
// a display alias only: it is never part of a key or an authorization check.
type QuotaV2Binding struct {
	AccountRef      string  `json:"account_ref"`
	Provider        string  `json:"provider"`
	MachineID       string  `json:"machine_id"`
	LocalSlotRef    string  `json:"local_slot_ref"`
	EntitlementRef  string  `json:"entitlement_ref"`
	IdentityBasis   string  `json:"identity_basis"`
	BindingRevision int64   `json:"binding_revision"`
	VerifiedAt      string  `json:"verified_at"`
	ValidUntil      string  `json:"valid_until"`
	VerifierReceipt *string `json:"verifier_receipt"`
	Label           *string `json:"label"`
}

type quotaV2BindingRequest struct {
	AccountRef      string  `json:"account_ref"`
	Provider        string  `json:"provider"`
	MachineID       string  `json:"machine_id"`
	LocalSlotRef    string  `json:"local_slot_ref"`
	EntitlementRef  string  `json:"entitlement_ref"`
	IdentityBasis   string  `json:"identity_basis"`
	ValidUntil      string  `json:"valid_until"`
	VerifierReceipt *string `json:"verifier_receipt"`
	Label           *string `json:"label"`
}

type quotaV2StoreFile struct {
	Schema       string               `json:"schema"`
	LastRevision int64                `json:"last_revision"`
	Bindings     []QuotaV2Binding     `json:"bindings"`
	Observations []QuotaV2Observation `json:"observations"`
}

// hubQuotaV2Store has its own lock so v2 traffic never contends with the
// presence/relay state guarded by HubServer.mu.
type hubQuotaV2Store struct {
	mu sync.Mutex
	// unavailable, when set, closes every v2 route with 503: v2 needs a
	// durable store (bindings and revisions must survive a restart) and a
	// distinct bearer per principal (a shared secret would let a node act as
	// the operator or as another node).
	unavailable  string
	path         string
	clockSkew    time.Duration // Δ_hub (§7.2, §7.3)
	lastRevision int64
	bindings     []QuotaV2Binding
	observations map[string][]QuotaV2Observation
}

// quotaV2StoreLoad reads observations raw so the stored form is checked with
// the same key-set rule as a POST (§2.0), not with Go's lenient decoding.
type quotaV2StoreLoad struct {
	Schema       string            `json:"schema"`
	LastRevision int64             `json:"last_revision"`
	Bindings     []QuotaV2Binding  `json:"bindings"`
	Observations []json.RawMessage `json:"observations"`
}

func quotaV2AccountKey(provider, accountRef string) string {
	return provider + "\x00" + accountRef
}

func newHubQuotaV2Store(path string, tokens map[string]string, clockSkew time.Duration) (*hubQuotaV2Store, error) {
	store := &hubQuotaV2Store{path: path, clockSkew: clockSkew, observations: make(map[string][]QuotaV2Observation)}
	secrets := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		if _, duplicate := secrets[token]; duplicate {
			store.unavailable = "bearer_not_distinct"
		}
		secrets[token] = struct{}{}
	}
	if path == "" {
		if store.unavailable == "" {
			store.unavailable = "store_not_configured"
		}
		return store, nil
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	// Bindings and verifier receipts are not for other local users: an existing
	// store must be a regular mode-0600 file, the same mode persistLocked writes.
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return nil, errors.New("quota v2 store must be a regular mode-0600 file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var file quotaV2StoreLoad
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&file) != nil || file.Schema != quotaV2StoreSchema || file.LastRevision < 0 {
		return nil, errors.New("quota v2 store is invalid")
	}
	seen := make(map[string]struct{}, len(file.Bindings))
	for _, binding := range file.Bindings {
		if !validQuotaV2StoredBinding(binding) || binding.BindingRevision > file.LastRevision {
			return nil, errors.New("quota v2 store is invalid")
		}
		key := quotaV2SlotKey(binding.MachineID, binding.Provider, binding.LocalSlotRef)
		if _, duplicate := seen[key]; duplicate {
			return nil, errors.New("quota v2 store is invalid")
		}
		seen[key] = struct{}{}
	}
	ids := make(map[string]struct{}, len(file.Observations))
	for _, rawObservation := range file.Observations {
		// §7.7: stored observations are the stored form with received_at set.
		observation, err := decodeQuotaV2Observation(rawObservation, quotaV2FormStored)
		if err != nil || observation.ReceivedAt == nil {
			return nil, errors.New("quota v2 store is invalid")
		}
		if _, duplicate := ids[observation.ObservationID]; duplicate {
			return nil, errors.New("quota v2 store is invalid")
		}
		ids[observation.ObservationID] = struct{}{}
		key := quotaV2AccountKey(observation.Provider, observation.AccountRef)
		store.observations[key] = append(store.observations[key], observation)
	}
	store.lastRevision, store.bindings = file.LastRevision, file.Bindings
	return store, nil
}

func quotaV2SlotKey(machineID, provider, slot string) string {
	return machineID + "\x00" + provider + "\x00" + slot
}

// persistLocked writes the whole store atomically. Callers mutate only after
// it succeeds, so a failed write never leaves memory ahead of disk.
func (store *hubQuotaV2Store) persistLocked(lastRevision int64, bindings []QuotaV2Binding, observations map[string][]QuotaV2Observation) error {
	if store.path == "" {
		return nil
	}
	file := quotaV2StoreFile{Schema: quotaV2StoreSchema, LastRevision: lastRevision, Bindings: bindings, Observations: []QuotaV2Observation{}}
	if file.Bindings == nil {
		file.Bindings = []QuotaV2Binding{}
	}
	keys := make([]string, 0, len(observations))
	for key := range observations {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		file.Observations = append(file.Observations, observations[key]...)
	}
	data, err := json.Marshal(file)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(store.path), ".quota-v2-*.tmp")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(name, store.path)
}

// quotaV2Time parses RFC3339 with an explicit offset, with the same text and
// range rule as scopefuel's parse_time: ASCII, offset hh ≤ 23 and mm ≤ 59, and
// a year 1..9999 both as written and in UTC.
func quotaV2Time(value string) (time.Time, bool) {
	if !quotaV2TimePattern.MatchString(value) || strings.HasPrefix(value, "0000") {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, false
	}
	if year := parsed.UTC().Year(); year < 1 || year > 9999 {
		return time.Time{}, false
	}
	return parsed, true
}

func validQuotaV2Time(value string) bool {
	_, ok := quotaV2Time(value)
	return ok
}

func validQuotaV2Text(value *string, limit int) bool {
	return value == nil || (*value != "" && len(*value) <= limit && !strings.ContainsAny(*value, "\x00\r\n\t"))
}

// quotaV2Label is the §2 text rule for label and scope.ref: UTF-8, 1..128
// bytes, no C0, DEL or C1 control character. Nothing is truncated or repaired.
func quotaV2Label(value string) bool {
	if value == "" || len(value) > 128 || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r <= 0x1F || r == 0x7F || (r >= 0x80 && r <= 0x9F) {
			return false
		}
	}
	return true
}

func validQuotaV2StoredBinding(binding QuotaV2Binding) bool {
	return quotaV2AccountRefPattern.MatchString(binding.AccountRef) &&
		hubQuotaPoolPattern.MatchString(binding.Provider) &&
		machineIDPattern.MatchString(binding.MachineID) && binding.MachineID != hubOperatorMachineID &&
		quotaV2RefPattern.MatchString(binding.LocalSlotRef) &&
		quotaV2RefPattern.MatchString(binding.EntitlementRef) &&
		quotaV2IdentityBases[binding.IdentityBasis] &&
		binding.BindingRevision > 0 &&
		validQuotaV2Time(binding.VerifiedAt) && validQuotaV2Time(binding.ValidUntil) &&
		validQuotaV2Text(binding.VerifierReceipt, 256) && validQuotaV2Text(binding.Label, 64)
}

const (
	quotaV2FormPost   = "post"
	quotaV2FormStored = "stored"
)

// quotaV2Keys checks one JSON object against a §2 field table: exactly these
// keys, and null only where the table allows it (absent is not null).
func quotaV2Keys(raw json.RawMessage, fields map[string]bool) (map[string]json.RawMessage, bool) {
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil || len(object) != len(fields) {
		return nil, false
	}
	for key, value := range object {
		nullable, known := fields[key]
		if !known || (!nullable && bytes.Equal(bytes.TrimSpace(value), []byte("null"))) {
			return nil, false
		}
	}
	return object, true
}

// quotaV2LoneSurrogate reports a \u escape of a UTF-16 surrogate that is not
// part of a high+low pair. encoding/json would silently turn it into U+FFFD;
// §2 text is UTF-8 and is never repaired, so such a body is rejected instead.
func quotaV2LoneSurrogate(raw []byte) bool {
	inString, pendingHigh := false, false
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if !inString {
			inString = c == '"'
			continue
		}
		if c != '\\' || i+1 >= len(raw) || raw[i+1] != 'u' {
			if pendingHigh {
				return true
			}
			if c == '"' {
				inString = false
			} else if c == '\\' {
				i++ // a two-character escape such as \" or \\
			}
			continue
		}
		if i+5 >= len(raw) {
			return false // truncated escape: the decoder rejects it
		}
		code, err := strconv.ParseUint(string(raw[i+2:i+6]), 16, 16)
		if err != nil {
			return false // bad hex: the decoder rejects it
		}
		i += 5
		switch {
		case code >= 0xD800 && code <= 0xDBFF:
			if pendingHigh {
				return true
			}
			pendingHigh = true
		case code >= 0xDC00 && code <= 0xDFFF:
			if !pendingHigh {
				return true
			}
			pendingHigh = false
		case pendingHigh:
			return true
		}
	}
	return pendingHigh
}

// decodeQuotaV2Observation reads one envelope in the given form (§2): the raw
// text (UTF-8, no lone surrogate escape), the key sets, then a strict typed
// decode, then the field rules.
func decodeQuotaV2Observation(raw []byte, form string) (QuotaV2Observation, error) {
	invalid := errors.New("invalid envelope")
	if !utf8.Valid(raw) || quotaV2LoneSurrogate(raw) {
		return QuotaV2Observation{}, invalid
	}
	object, ok := quotaV2Keys(raw, quotaV2EnvelopeFields)
	if !ok {
		return QuotaV2Observation{}, invalid
	}
	var buckets []json.RawMessage
	if json.Unmarshal(object["buckets"], &buckets) != nil || buckets == nil {
		return QuotaV2Observation{}, invalid
	}
	for _, bucket := range buckets {
		fields, ok := quotaV2Keys(bucket, quotaV2BucketFields)
		if !ok {
			return QuotaV2Observation{}, invalid
		}
		if _, ok := quotaV2Keys(fields["scope"], quotaV2ScopeFields); !ok {
			return QuotaV2Observation{}, invalid
		}
	}
	var observation QuotaV2Observation
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&observation) != nil {
		return QuotaV2Observation{}, invalid
	}
	if err := validateQuotaV2Envelope(observation, form); err != nil {
		return QuotaV2Observation{}, err
	}
	return observation, nil
}

// validateQuotaV2Envelope checks the §2 field rules of a decoded envelope.
// Identity and authority checks happen against the binding registry in
// recordObservation.
func validateQuotaV2Envelope(observation QuotaV2Observation, form string) error {
	invalid := errors.New("invalid envelope")
	measuredAt, measuredOK := quotaV2Time(observation.MeasuredAt)
	if observation.Schema != quotaV2ObservationSchema || observation.ContractRev != quotaV2ContractRev ||
		!quotaV2RefPattern.MatchString(observation.ObservationID) ||
		!quotaV2ProviderPattern.MatchString(observation.Provider) ||
		!quotaV2AccountRefPattern.MatchString(observation.AccountRef) ||
		!quotaV2RefPattern.MatchString(observation.EntitlementRef) ||
		!quotaV2MachinePattern.MatchString(observation.SourceMachine) ||
		!quotaV2RefPattern.MatchString(observation.SourceSlotRef) ||
		observation.SourceBindingRevision < 1 ||
		!quotaV2VersionPattern.MatchString(observation.CollectorVersion) ||
		!measuredOK || !quotaV2Statuses[observation.Status] ||
		observation.UnsharedLimitCount < 0 || observation.LeaseEpoch != nil || observation.Buckets == nil {
		return invalid
	}
	switch {
	case form == quotaV2FormPost && observation.ReceivedAt != nil:
		return invalid
	case observation.ReceivedAt != nil && !validQuotaV2Time(*observation.ReceivedAt):
		return invalid
	}
	if observation.Status == "success" {
		if observation.ErrorRef != nil {
			return invalid
		}
	} else if observation.ErrorRef == nil || !quotaV2ErrorRefs[*observation.ErrorRef] ||
		strings.SplitN(*observation.ErrorRef, ":", 2)[0] != observation.Status {
		return invalid
	}
	measuring := observation.Status == "success" || observation.Status == "partial"
	if measuring != (len(observation.Buckets) > 0) {
		// A failed probe carries no values: its last success is a separate,
		// earlier observation and is never re-stamped by the failure.
		return invalid
	}
	// The key is limit_id, not (provider, account, window): an account-wide 7d
	// limit and a model 7d limit coexist in one envelope (legacy R29 rejects
	// that shape as a duplicate; v2 must not).
	limits := make(map[string]struct{}, len(observation.Buckets))
	for _, bucket := range observation.Buckets {
		if !quotaV2RefPattern.MatchString(bucket.LimitID) || (bucket.Label != nil && !quotaV2Label(*bucket.Label)) ||
			!quotaV2Horizons[bucket.Horizon] || !quotaV2WindowPattern.MatchString(bucket.Window) ||
			!quotaV2ScopeKinds[bucket.Scope.Kind] || (bucket.Scope.Kind == "account") != (bucket.Scope.Ref == nil) ||
			(bucket.Scope.Ref != nil && !quotaV2Label(*bucket.Scope.Ref)) {
			return errors.New("invalid bucket")
		}
		if bucket.WindowInstance != "unknown" && (!quotaV2InstancePattern.MatchString(bucket.WindowInstance) || !validQuotaV2Time(bucket.WindowInstance)) {
			return errors.New("invalid bucket")
		}
		if bucket.UsedPct != nil && (math.IsNaN(*bucket.UsedPct) || math.IsInf(*bucket.UsedPct, 0) || *bucket.UsedPct < 0 || *bucket.UsedPct > 100) {
			return errors.New("invalid bucket")
		}
		if bucket.ResetAt != nil && !validQuotaV2Time(*bucket.ResetAt) {
			return errors.New("invalid bucket")
		}
		if bucket.ObservedAt != nil {
			observedAt, ok := quotaV2Time(*bucket.ObservedAt)
			if !ok || observedAt.After(measuredAt) {
				return errors.New("invalid bucket")
			}
		}
		if _, duplicate := limits[bucket.LimitID]; duplicate {
			return errors.New("duplicate limit_id")
		}
		limits[bucket.LimitID] = struct{}{}
	}
	return nil
}

// quotaV2Times is TS(o) (§5.3): measured_at and every non-null observed_at.
func quotaV2Times(observation QuotaV2Observation) []time.Time {
	measuredAt, _ := quotaV2Time(observation.MeasuredAt)
	times := []time.Time{measuredAt}
	for _, bucket := range observation.Buckets {
		if bucket.ObservedAt != nil {
			observedAt, _ := quotaV2Time(*bucket.ObservedAt)
			times = append(times, observedAt)
		}
	}
	return times
}

// bindingWindow is a binding's effect window [valid_from, valid_until), with
// valid_from = verified_at (§5.4).
func bindingWindow(binding QuotaV2Binding) (time.Time, time.Time, bool) {
	validFrom, fromOK := quotaV2Time(binding.VerifiedAt)
	validUntil, untilOK := quotaV2Time(binding.ValidUntil)
	return validFrom, validUntil, fromOK && untilOK && validFrom.Before(validUntil)
}

// bindingFor lists the machine's bindings for the account that are current at now.
func (store *hubQuotaV2Store) bindingFor(machineID, provider, accountRef string, now time.Time) []QuotaV2Binding {
	out := []QuotaV2Binding{}
	for _, binding := range store.bindings {
		if binding.MachineID != machineID || binding.Provider != provider || binding.AccountRef != accountRef {
			continue
		}
		validFrom, validUntil, ok := bindingWindow(binding)
		if !ok || now.Before(validFrom) || !now.Before(validUntil) {
			continue
		}
		out = append(out, binding)
	}
	return out
}

func (store *hubQuotaV2Store) putBinding(request quotaV2BindingRequest, now time.Time) (QuotaV2Binding, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	// Revisions are global and time-seeded so a rebind never reuses a number,
	// even after an in-memory hub restarts.
	revision := store.lastRevision + 1
	if seed := now.UnixMilli(); seed > revision {
		revision = seed
	}
	binding := QuotaV2Binding{
		AccountRef: request.AccountRef, Provider: request.Provider, MachineID: request.MachineID, LocalSlotRef: request.LocalSlotRef,
		EntitlementRef: request.EntitlementRef, IdentityBasis: request.IdentityBasis, BindingRevision: revision,
		VerifiedAt: now.UTC().Format(time.RFC3339), ValidUntil: request.ValidUntil, VerifierReceipt: request.VerifierReceipt, Label: request.Label,
	}
	next := make([]QuotaV2Binding, 0, len(store.bindings)+1)
	slot := quotaV2SlotKey(request.MachineID, request.Provider, request.LocalSlotRef)
	for _, existing := range store.bindings {
		if quotaV2SlotKey(existing.MachineID, existing.Provider, existing.LocalSlotRef) != slot {
			next = append(next, existing)
		}
	}
	if len(next) >= quotaV2MaxBindings {
		return QuotaV2Binding{}, errors.New("binding limit reached")
	}
	next = append(next, binding)
	if err := store.persistLocked(revision, next, store.observations); err != nil {
		return QuotaV2Binding{}, err
	}
	store.lastRevision, store.bindings = revision, next
	return binding, nil
}

func (store *hubQuotaV2Store) deleteBinding(machineID, provider, slot string) (bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	next := make([]QuotaV2Binding, 0, len(store.bindings))
	for _, existing := range store.bindings {
		if quotaV2SlotKey(existing.MachineID, existing.Provider, existing.LocalSlotRef) != quotaV2SlotKey(machineID, provider, slot) {
			next = append(next, existing)
		}
	}
	if len(next) == len(store.bindings) {
		return false, nil
	}
	if err := store.persistLocked(store.lastRevision, next, store.observations); err != nil {
		return false, err
	}
	store.bindings = next
	return true, nil
}

var (
	errQuotaV2Forbidden = errors.New("forbidden")
	errQuotaV2Window    = errors.New("outside binding window")
	errQuotaV2Conflict  = errors.New("conflict")
	errQuotaV2Invalid   = errors.New("invalid")
	errQuotaV2Future    = errors.New("future")
)

// recordObservation appends one automatic observation, checking in the §7.5
// order after the POST form: future (§7.3) → source machine → a current
// binding of this exact slot (§7.1) → every time inside that binding's window
// widened by Δ_hub (§7.2) → observation_id idempotency.
func (store *hubQuotaV2Store) recordObservation(machineID string, observation QuotaV2Observation, now time.Time) (QuotaV2Observation, bool, error) {
	if validateQuotaV2Envelope(observation, quotaV2FormPost) != nil {
		return QuotaV2Observation{}, false, errQuotaV2Invalid
	}
	times := quotaV2Times(observation)
	for _, t := range times {
		if t.Add(-store.clockSkew).After(now) { // t = now is accepted
			return QuotaV2Observation{}, false, errQuotaV2Future
		}
	}
	if observation.SourceMachine != machineID {
		return QuotaV2Observation{}, false, errQuotaV2Forbidden
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	var matched *QuotaV2Binding
	for _, binding := range store.bindingFor(machineID, observation.Provider, observation.AccountRef, now) {
		if binding.LocalSlotRef == observation.SourceSlotRef && binding.BindingRevision == observation.SourceBindingRevision && binding.EntitlementRef == observation.EntitlementRef {
			matched = &binding
			break
		}
	}
	if matched == nil {
		return QuotaV2Observation{}, false, errQuotaV2Forbidden
	}
	validFrom, validUntil, _ := bindingWindow(*matched)
	for _, t := range times {
		if t.Add(-store.clockSkew).Before(validFrom) || !t.Add(store.clockSkew).Before(validUntil) {
			return QuotaV2Observation{}, false, errQuotaV2Window
		}
	}
	key := quotaV2AccountKey(observation.Provider, observation.AccountRef)
	for _, rows := range store.observations {
		for _, existing := range rows {
			if existing.ObservationID != observation.ObservationID {
				continue
			}
			candidate := existing
			candidate.ReceivedAt = nil
			if equalQuotaV2Observation(candidate, observation) {
				return existing, false, nil
			}
			return QuotaV2Observation{}, false, errQuotaV2Conflict
		}
	}
	received := now.UTC().Format(time.RFC3339)
	stored := observation
	stored.ReceivedAt = &received
	rows := append(append([]QuotaV2Observation{}, store.observations[key]...), stored)
	if len(rows) > quotaV2MaxObservationsPerAccount {
		rows = rows[len(rows)-quotaV2MaxObservationsPerAccount:]
	}
	next := make(map[string][]QuotaV2Observation, len(store.observations)+1)
	for existingKey, existingRows := range store.observations {
		next[existingKey] = existingRows
	}
	next[key] = rows
	if err := store.persistLocked(store.lastRevision, store.bindings, next); err != nil {
		return QuotaV2Observation{}, false, err
	}
	store.observations = next
	return stored, true, nil
}

func equalQuotaV2Observation(left, right QuotaV2Observation) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func (store *hubQuotaV2Store) listBindings(machineID string) []QuotaV2Binding {
	store.mu.Lock()
	defer store.mu.Unlock()
	out := []QuotaV2Binding{}
	for _, binding := range store.bindings {
		if machineID == "" || binding.MachineID == machineID {
			out = append(out, binding)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].MachineID != out[j].MachineID {
			return out[i].MachineID < out[j].MachineID
		}
		if out[i].Provider != out[j].Provider {
			return out[i].Provider < out[j].Provider
		}
		return out[i].LocalSlotRef < out[j].LocalSlotRef
	})
	return out
}

// listObservations is a pure read. It never triggers a probe or a refresh.
func (store *hubQuotaV2Store) listObservations(provider, accountRef string) []QuotaV2Observation {
	store.mu.Lock()
	defer store.mu.Unlock()
	rows := append([]QuotaV2Observation{}, store.observations[quotaV2AccountKey(provider, accountRef)]...)
	sort.SliceStable(rows, func(i, j int) bool {
		left, _ := quotaV2Time(rows[i].MeasuredAt)
		right, _ := quotaV2Time(rows[j].MeasuredAt)
		return left.Before(right)
	})
	return rows
}

func (store *hubQuotaV2Store) nodeMayRead(machineID, provider, accountRef string, now time.Time) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	return len(store.bindingFor(machineID, provider, accountRef, now)) > 0
}

func decodeQuotaV2Body(r *http.Request, target any) bool {
	decoder := json.NewDecoder(io.LimitReader(r.Body, quotaV2MaxBodyBytes+1))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		return false
	}
	var extra json.RawMessage
	return decoder.Decode(&extra) == io.EOF
}

func writeQuotaV2JSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func quotaV2Error(w http.ResponseWriter, status int, code string) {
	writeQuotaV2JSON(w, status, map[string]string{"error": code})
}

// quotaV2Closed answers 503 before any authorization when v2 is not safely
// configured, so no role check runs against a shared secret.
func (h *HubServer) quotaV2Closed(w http.ResponseWriter) bool {
	if h.quotaV2 == nil {
		quotaV2Error(w, http.StatusServiceUnavailable, "store_not_configured")
		return true
	}
	if h.quotaV2.unavailable != "" {
		quotaV2Error(w, http.StatusServiceUnavailable, h.quotaV2.unavailable)
		return true
	}
	return false
}

// handleQuotaV2BindingPut is the enrollment path: operator token only.
func (h *HubServer) handleQuotaV2BindingPut(w http.ResponseWriter, r *http.Request) {
	if h.quotaV2Closed(w) {
		return
	}
	if !h.authorizeOperator(r) {
		hubUnauthorized(w)
		return
	}
	var request quotaV2BindingRequest
	if !decodeQuotaV2Body(r, &request) {
		quotaV2Error(w, http.StatusBadRequest, "invalid_binding")
		return
	}
	now := h.now()
	validUntil, err := time.Parse(time.RFC3339, request.ValidUntil)
	if _, known := h.tokens[request.MachineID]; err != nil || !known || !now.Before(validUntil) || validUntil.Sub(now) > quotaV2MaxBindingValidity {
		quotaV2Error(w, http.StatusBadRequest, "invalid_binding")
		return
	}
	candidate := QuotaV2Binding{AccountRef: request.AccountRef, Provider: request.Provider, MachineID: request.MachineID, LocalSlotRef: request.LocalSlotRef, EntitlementRef: request.EntitlementRef, IdentityBasis: request.IdentityBasis, BindingRevision: 1, VerifiedAt: request.ValidUntil, ValidUntil: request.ValidUntil, VerifierReceipt: request.VerifierReceipt, Label: request.Label}
	if !validQuotaV2StoredBinding(candidate) {
		quotaV2Error(w, http.StatusBadRequest, "invalid_binding")
		return
	}
	binding, err := h.quotaV2.putBinding(request, now)
	if err != nil {
		quotaV2Error(w, http.StatusServiceUnavailable, "store_unavailable")
		return
	}
	writeQuotaV2JSON(w, http.StatusOK, binding)
}

func (h *HubServer) handleQuotaV2BindingDelete(w http.ResponseWriter, r *http.Request) {
	if h.quotaV2Closed(w) {
		return
	}
	if !h.authorizeOperator(r) {
		hubUnauthorized(w)
		return
	}
	query := r.URL.Query()
	removed, err := h.quotaV2.deleteBinding(query.Get("machine_id"), query.Get("provider"), query.Get("local_slot_ref"))
	switch {
	case err != nil:
		quotaV2Error(w, http.StatusServiceUnavailable, "store_unavailable")
	case !removed:
		quotaV2Error(w, http.StatusNotFound, "binding_not_found")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// handleQuotaV2BindingList: the operator sees every binding; a node sees only
// its own machine's bindings.
func (h *HubServer) handleQuotaV2BindingList(w http.ResponseWriter, r *http.Request) {
	if h.quotaV2Closed(w) {
		return
	}
	var machineID *string
	if !h.authorizeOperator(r) {
		node, ok := h.authorizeAgent(r)
		if !ok {
			hubUnauthorized(w)
			return
		}
		machineID = &node
	}
	filter := ""
	if machineID != nil {
		filter = *machineID
	}
	writeQuotaV2JSON(w, http.StatusOK, struct {
		Schema    string           `json:"schema"`
		MachineID *string          `json:"machine_id"`
		Bindings  []QuotaV2Binding `json:"bindings"`
	}{Schema: quotaV2BindingsSchema, MachineID: machineID, Bindings: h.quotaV2.listBindings(filter)})
}

// handleQuotaV2ObservationPost accepts only node-authenticated automatic
// observations. The operator token is not a node and cannot write here.
func (h *HubServer) handleQuotaV2ObservationPost(w http.ResponseWriter, r *http.Request) {
	if h.quotaV2Closed(w) {
		return
	}
	machineID, ok := h.authorizeAgent(r)
	if !ok {
		hubUnauthorized(w)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, quotaV2MaxBodyBytes+1))
	if err != nil || len(raw) > quotaV2MaxBodyBytes {
		quotaV2Error(w, http.StatusBadRequest, "invalid_envelope")
		return
	}
	observation, err := decodeQuotaV2Observation(raw, quotaV2FormPost)
	if err != nil {
		quotaV2Error(w, http.StatusBadRequest, "invalid_envelope")
		return
	}
	stored, created, err := h.quotaV2.recordObservation(machineID, observation, h.now())
	switch {
	case errors.Is(err, errQuotaV2Invalid):
		quotaV2Error(w, http.StatusBadRequest, "invalid_envelope")
	case errors.Is(err, errQuotaV2Future):
		quotaV2Error(w, http.StatusBadRequest, "measured_in_future")
	case errors.Is(err, errQuotaV2Forbidden):
		quotaV2Error(w, http.StatusForbidden, "binding_mismatch")
	case errors.Is(err, errQuotaV2Window):
		quotaV2Error(w, http.StatusForbidden, "outside_binding_window")
	case errors.Is(err, errQuotaV2Conflict):
		quotaV2Error(w, http.StatusConflict, "observation_id_conflict")
	case err != nil:
		quotaV2Error(w, http.StatusServiceUnavailable, "store_unavailable")
	case created:
		writeQuotaV2JSON(w, http.StatusCreated, stored)
	default:
		writeQuotaV2JSON(w, http.StatusOK, stored)
	}
}

// handleQuotaV2ObservationList is a pure read. The operator may read any
// account; a node may read only an account it currently holds a binding for.
func (h *HubServer) handleQuotaV2ObservationList(w http.ResponseWriter, r *http.Request) {
	if h.quotaV2Closed(w) {
		return
	}
	query := r.URL.Query()
	provider, accountRef := query.Get("provider"), query.Get("account_ref")
	operator := h.authorizeOperator(r)
	machineID, node := "", false
	if !operator {
		machineID, node = h.authorizeAgent(r)
		if !node {
			hubUnauthorized(w)
			return
		}
	}
	if !hubQuotaPoolPattern.MatchString(provider) || !quotaV2AccountRefPattern.MatchString(accountRef) {
		quotaV2Error(w, http.StatusBadRequest, "invalid_query")
		return
	}
	if node && !h.quotaV2.nodeMayRead(machineID, provider, accountRef, h.now()) {
		quotaV2Error(w, http.StatusForbidden, "binding_mismatch")
		return
	}
	writeQuotaV2JSON(w, http.StatusOK, struct {
		Schema       string               `json:"schema"`
		Provider     string               `json:"provider"`
		AccountRef   string               `json:"account_ref"`
		Observations []QuotaV2Observation `json:"observations"`
	}{Schema: quotaV2ObservationsSchema, Provider: provider, AccountRef: accountRef, Observations: h.quotaV2.listObservations(provider, accountRef)})
}

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
	"strings"
	"sync"
	"time"
)

// Account-scoped quota v2 (task #578, stage 1). This is an additive store next
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
	quotaV2BindingsSchema     = "quota-bindings/v2"
	quotaV2ObservationsSchema = "quota-observations/v2"
	quotaV2StoreSchema        = "panewire.quota-v2-store/v1"

	quotaV2MaxBodyBytes              = 64 << 10
	quotaV2MaxBuckets                = 64
	quotaV2MaxBindings               = 1024
	quotaV2MaxObservationsPerAccount = 128
	quotaV2MaxFutureSkew             = 5 * time.Minute
	quotaV2MaxBindingValidity        = 30 * 24 * time.Hour
)

var (
	// account_ref is an opaque random id. The fixed shape rejects emails,
	// paths and tokens outright instead of trying to recognise them.
	quotaV2AccountRefPattern = regexp.MustCompile(`^acct_[0-9a-z]{8,40}$`)
	quotaV2RefPattern        = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
	quotaV2VersionPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+/:-]{0,127}$`)
	quotaV2InstancePattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:+-]{0,63}$`)
	quotaV2Statuses          = map[string]bool{"success": true, "partial": true, "rate_limited": true, "auth_error": true, "parse_error": true, "transport_error": true}
	quotaV2Horizons          = map[string]bool{"now": true, "week": true, "month": true}
	quotaV2IdentityBases     = map[string]bool{"operator_attested": true, "provider_subject_verified": true}
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

// QuotaV2Observation is the quota-observation/v2 envelope. measured_at is the
// time the value was observed at the provider; received_at is stamped by the
// hub and never replaces it.
type QuotaV2Observation struct {
	Schema                string          `json:"schema"`
	ObservationID         string          `json:"observation_id"`
	Provider              string          `json:"provider"`
	AccountRef            string          `json:"account_ref"`
	EntitlementRef        string          `json:"entitlement_ref"`
	SourceMachine         string          `json:"source_machine"`
	SourceBindingRevision int64           `json:"source_binding_revision"`
	CollectorVersion      string          `json:"collector_version"`
	MeasuredAt            string          `json:"measured_at"`
	ReceivedAt            *string         `json:"received_at"`
	Status                string          `json:"status"`
	Buckets               []QuotaV2Bucket `json:"buckets"`
	ErrorRef              *string         `json:"error_ref"`
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
	mu           sync.Mutex
	path         string
	lastRevision int64
	bindings     []QuotaV2Binding
	observations map[string][]QuotaV2Observation
}

func quotaV2AccountKey(provider, accountRef string) string {
	return provider + "\x00" + accountRef
}

func newHubQuotaV2Store(path string) (*hubQuotaV2Store, error) {
	store := &hubQuotaV2Store{path: path, observations: make(map[string][]QuotaV2Observation)}
	if path == "" {
		return store, nil
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, err
	}
	var file quotaV2StoreFile
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
	for _, observation := range file.Observations {
		if validateQuotaV2Envelope(observation) != nil || observation.ReceivedAt == nil || !validQuotaV2Time(*observation.ReceivedAt) {
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

func validQuotaV2Time(value string) bool {
	_, err := time.Parse(time.RFC3339, value)
	return err == nil
}

func validQuotaV2Text(value *string, limit int) bool {
	return value == nil || (*value != "" && len(*value) <= limit && !strings.ContainsAny(*value, "\x00\r\n\t"))
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

// validateQuotaV2Envelope checks shape only. Identity and authority checks
// happen against the binding registry in recordObservation.
func validateQuotaV2Envelope(observation QuotaV2Observation) error {
	if observation.Schema != quotaV2ObservationSchema ||
		!quotaV2RefPattern.MatchString(observation.ObservationID) ||
		!hubQuotaPoolPattern.MatchString(observation.Provider) ||
		!quotaV2AccountRefPattern.MatchString(observation.AccountRef) ||
		!quotaV2RefPattern.MatchString(observation.EntitlementRef) ||
		!machineIDPattern.MatchString(observation.SourceMachine) ||
		observation.SourceBindingRevision <= 0 ||
		!quotaV2VersionPattern.MatchString(observation.CollectorVersion) ||
		!validQuotaV2Time(observation.MeasuredAt) ||
		!quotaV2Statuses[observation.Status] ||
		observation.Buckets == nil || len(observation.Buckets) > quotaV2MaxBuckets {
		return errors.New("invalid envelope")
	}
	if observation.ReceivedAt != nil && !validQuotaV2Time(*observation.ReceivedAt) {
		return errors.New("invalid envelope")
	}
	if observation.ErrorRef != nil && !quotaV2RefPattern.MatchString(*observation.ErrorRef) {
		return errors.New("invalid envelope")
	}
	if observation.LeaseEpoch != nil && *observation.LeaseEpoch <= 0 {
		return errors.New("invalid envelope")
	}
	measuring := observation.Status == "success" || observation.Status == "partial"
	if measuring != (len(observation.Buckets) > 0) {
		// A failed probe carries no values: its last success is a separate,
		// earlier observation and is never re-stamped by the failure.
		return errors.New("invalid envelope")
	}
	// The key is limit_id, not (provider, account, window): an account-wide 7d
	// limit and a model 7d limit coexist in one envelope (legacy R29 rejects
	// that shape as a duplicate; v2 must not).
	limits := make(map[string]struct{}, len(observation.Buckets))
	for _, bucket := range observation.Buckets {
		if !quotaV2RefPattern.MatchString(bucket.LimitID) || !validQuotaV2Text(bucket.Label, 128) ||
			!quotaV2Horizons[bucket.Horizon] || !hubQuotaWindowPattern.MatchString(bucket.Window) ||
			!quotaV2InstancePattern.MatchString(bucket.WindowInstance) {
			return errors.New("invalid bucket")
		}
		switch bucket.Scope.Kind {
		case "account":
			if bucket.Scope.Ref != nil {
				return errors.New("invalid bucket")
			}
		case "model", "group":
			if bucket.Scope.Ref == nil || !validQuotaV2Text(bucket.Scope.Ref, 128) {
				return errors.New("invalid bucket")
			}
		default:
			return errors.New("invalid bucket")
		}
		if bucket.UsedPct != nil && (math.IsNaN(*bucket.UsedPct) || math.IsInf(*bucket.UsedPct, 0) || *bucket.UsedPct < 0 || *bucket.UsedPct > 100) {
			return errors.New("invalid bucket")
		}
		if (bucket.ResetAt != nil && !validQuotaV2Time(*bucket.ResetAt)) || (bucket.ObservedAt != nil && !validQuotaV2Time(*bucket.ObservedAt)) {
			return errors.New("invalid bucket")
		}
		if _, duplicate := limits[bucket.LimitID]; duplicate {
			return errors.New("duplicate limit_id")
		}
		limits[bucket.LimitID] = struct{}{}
	}
	return nil
}

func (store *hubQuotaV2Store) bindingFor(machineID, provider, accountRef string, now time.Time) []QuotaV2Binding {
	out := []QuotaV2Binding{}
	for _, binding := range store.bindings {
		if binding.MachineID != machineID || binding.Provider != provider || binding.AccountRef != accountRef {
			continue
		}
		validUntil, err := time.Parse(time.RFC3339, binding.ValidUntil)
		if err != nil || !now.Before(validUntil) {
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
	errQuotaV2Conflict  = errors.New("conflict")
	errQuotaV2Invalid   = errors.New("invalid")
)

// recordObservation appends one automatic observation. The authenticated node
// may write only through its own binding that is current right now: machine,
// provider, account_ref, entitlement and the exact binding revision must all
// match, and the value must have been measured after that binding began.
func (store *hubQuotaV2Store) recordObservation(machineID string, observation QuotaV2Observation, now time.Time) (QuotaV2Observation, bool, error) {
	if validateQuotaV2Envelope(observation) != nil || observation.ReceivedAt != nil {
		return QuotaV2Observation{}, false, errQuotaV2Invalid
	}
	measuredAt, _ := time.Parse(time.RFC3339, observation.MeasuredAt)
	if measuredAt.After(now.Add(quotaV2MaxFutureSkew)) {
		return QuotaV2Observation{}, false, errQuotaV2Invalid
	}
	if observation.SourceMachine != machineID {
		return QuotaV2Observation{}, false, errQuotaV2Forbidden
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	authorized := false
	for _, binding := range store.bindingFor(machineID, observation.Provider, observation.AccountRef, now) {
		verifiedAt, _ := time.Parse(time.RFC3339, binding.VerifiedAt)
		if binding.BindingRevision == observation.SourceBindingRevision && binding.EntitlementRef == observation.EntitlementRef && !measuredAt.Before(verifiedAt) {
			authorized = true
			break
		}
	}
	if !authorized {
		return QuotaV2Observation{}, false, errQuotaV2Forbidden
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
		left, _ := time.Parse(time.RFC3339, rows[i].MeasuredAt)
		right, _ := time.Parse(time.RFC3339, rows[j].MeasuredAt)
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

// handleQuotaV2BindingPut is the enrollment path: operator token only.
func (h *HubServer) handleQuotaV2BindingPut(w http.ResponseWriter, r *http.Request) {
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
	machineID, ok := h.authorizeAgent(r)
	if !ok {
		hubUnauthorized(w)
		return
	}
	var observation QuotaV2Observation
	if !decodeQuotaV2Body(r, &observation) {
		quotaV2Error(w, http.StatusBadRequest, "invalid_envelope")
		return
	}
	stored, created, err := h.quotaV2.recordObservation(machineID, observation, h.now())
	switch {
	case errors.Is(err, errQuotaV2Invalid):
		quotaV2Error(w, http.StatusBadRequest, "invalid_envelope")
	case errors.Is(err, errQuotaV2Forbidden):
		quotaV2Error(w, http.StatusForbidden, "binding_mismatch")
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

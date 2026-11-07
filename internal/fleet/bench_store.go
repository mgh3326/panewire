package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/durable"
)

// BenchStore writes and reads bench reps on hk's /v1/bench/reps, the store
// pacelever uses. A rep intent is saved locally before the request so a failed
// write keeps its origin id and retries without duplicating a successful one.
type BenchStore struct {
	hk          *hkQuota
	now         func() time.Time
	pendingPath string
}

// NewBenchStore builds the bench client over an explicit handoffkeep endpoint
// and credential, as NewJobStore does.
func NewBenchStore(baseURL, token, cfID, cfSecret string) (*BenchStore, error) {
	client, err := newNodeHKClient(baseURL, token, cfID, cfSecret)
	if err != nil {
		return nil, err
	}
	return &BenchStore{hk: client, now: time.Now}, nil
}

func (a *App) benchStore() (*BenchStore, error) {
	if a.BenchStore != nil {
		return a.BenchStore, nil
	}
	c, err := a.quotaClient()
	if err != nil {
		return nil, err
	}
	a.BenchStore = &BenchStore{hk: c, now: a.Now, pendingPath: a.repsPath()}
	return a.BenchStore, nil
}

func (a *App) repsPath() string {
	dir := a.StateDir
	if dir == "" {
		base := os.Getenv("XDG_STATE_HOME")
		if base == "" {
			base = filepath.Join(a.Home, ".local", "state")
		}
		dir = filepath.Join(base, "panewire")
	}
	return filepath.Join(dir, "reps.json")
}

func (s *BenchStore) localReps() ([]Rep, error) {
	if s.pendingPath == "" {
		return []Rep{}, nil
	}
	data, err := os.ReadFile(s.pendingPath)
	if errors.Is(err, os.ErrNotExist) {
		return []Rep{}, nil
	}
	if err != nil {
		return nil, err
	}
	var rows []Rep
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("invalid local reps")
	}
	return rows, nil
}

func (s *BenchStore) saveRep(r Rep) error {
	if s.pendingPath == "" {
		return nil
	}
	rows, err := s.localReps()
	if err != nil {
		return err
	}
	found := false
	for i := range rows {
		if rows[i].OriginID == r.OriginID {
			rows[i] = r
			found = true
			break
		}
	}
	if !found {
		rows = append(rows, r)
	}
	data, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.pendingPath), 0700); err != nil {
		return err
	}
	return durable.ReplaceFileAtomically(s.pendingPath, ".reps-*", data, 0600)
}

func (s *BenchStore) dropPending(origin int64) error {
	rows, err := s.localReps()
	if err != nil {
		return err
	}
	out := make([]Rep, 0, len(rows))
	found := false
	for _, r := range rows {
		if r.OriginID == origin && r.Pending && r.ID == 0 {
			found = true
			continue
		}
		out = append(out, r)
	}
	if !found {
		return fmt.Errorf("pending origin not found")
	}
	data, err := json.Marshal(out)
	if err != nil {
		return err
	}
	return durable.ReplaceFileAtomically(s.pendingPath, ".reps-*", data, 0600)
}

// AddRep validates and writes one mergewake-shaped rep. It mirrors pacelever's
// AddRep, including the write-ahead intent and the 17-key wire envelope.
func (s *BenchStore) AddRep(ctx context.Context, r Rep) (Rep, error) {
	if err := hydrateRepOutcomes(&r); err != nil {
		return r, err
	}
	if r.Profile == "" || r.ModelID == "" || r.TaskRef == "" || r.Rounds < 0 || r.BlockersFound < 0 ||
		(!hasRepOutcomeFields(r) && r.Completed == nil) || (r.Completed != nil && *r.Completed != 0 && *r.Completed != 1) {
		return r, fmt.Errorf("rep requires profile, model, task, valid rounds/blockers and completed 0/1")
	}
	if r.Grade != "" && (!validRepGrade(r.Grade) || r.Grade == "unrated") {
		return r, fmt.Errorf("invalid rep grade")
	}
	if r.Tier != "T0" && r.Tier != "T1" && r.Tier != "T2" && r.Tier != "T3" {
		return r, fmt.Errorf("invalid rep tier")
	}
	if r.Role != "impl" && r.Role != "verify" && r.Role != "fix" && r.Role != "orch" {
		return r, fmt.Errorf("invalid rep role")
	}
	if r.OriginID == 0 {
		id, err := newRepOriginID()
		if err != nil {
			return r, err
		}
		r.OriginID = id
	}
	if r.RecordedAt == "" {
		r.RecordedAt = s.now().UTC().Format(time.RFC3339Nano)
	}
	if _, err := time.Parse(time.RFC3339Nano, r.RecordedAt); err != nil {
		return r, fmt.Errorf("invalid rep timestamp")
	}
	r.Pending = true
	if err := s.saveRep(r); err != nil {
		return r, err
	}
	return s.retryRep(ctx, r)
}

func (s *BenchStore) retryRep(ctx context.Context, r Rep) (Rep, error) {
	var result struct {
		IDs []int64 `json:"ids"`
	}
	if err := s.hk.request(ctx, http.MethodPut, "/v1/bench/reps", map[string]any{"reps": []map[string]any{repWire(r)}}, &result); err != nil {
		return r, fmt.Errorf("rep saved pending: %w", err)
	}
	if len(result.IDs) != 1 || result.IDs[0] <= 0 {
		return r, fmt.Errorf("rep saved pending: server id missing")
	}
	r.ID = result.IDs[0]
	r.Pending = false
	if err := s.saveRep(r); err != nil {
		return r, err
	}
	return r, nil
}

// ReadReps lists the stored reps, merging any local pending intents. The source
// is "handoffkeep" (with "-truncated" at the 5000-row window), or "local".
func (s *BenchStore) ReadReps(ctx context.Context, profile string) ([]Rep, string, error) {
	var body struct {
		Reps []Rep `json:"reps"`
	}
	source := "handoffkeep"
	if err := s.hk.request(ctx, http.MethodGet, "/v1/bench/reps?limit=5000", nil, &body); err != nil {
		return nil, source, err
	}
	rows := make([]Rep, 0, len(body.Reps))
	seen := map[int64]bool{}
	for _, r := range body.Reps {
		if err := hydrateRepOutcomes(&r); err != nil {
			r.Pending = true
		}
		if profile != "" && r.Profile != profile {
			continue
		}
		rows = append(rows, r)
		seen[r.ID] = true
	}
	local, err := s.localReps()
	if err != nil {
		return nil, source, err
	}
	for _, r := range local {
		if err := hydrateRepOutcomes(&r); err != nil {
			r.Pending = true
		}
		if profile != "" && r.Profile != profile {
			continue
		}
		if r.ID > 0 && seen[r.ID] {
			continue
		}
		r.Pending = r.Pending || r.ID == 0
		rows = append(rows, r)
	}
	uniqueRepRows(rows)
	if len(body.Reps) >= 5000 {
		source += "-truncated"
	}
	return rows, source, nil
}

// ReadTaskAreas reads each task's area for the list --area filter. A missing
// task is "unlinked".
func (s *BenchStore) ReadTaskAreas(ctx context.Context, reps []Rep) (map[string]string, error) {
	out := map[string]string{}
	for _, r := range reps {
		if _, ok := out[r.TaskRef]; ok {
			continue
		}
		id, err := repTaskID(r.TaskRef)
		if err != nil {
			out[r.TaskRef] = "unlinked"
			continue
		}
		var task struct {
			Area *string `json:"area"`
		}
		err = s.hk.request(ctx, http.MethodGet, "/v1/tasks/"+url.PathEscape(strconv.FormatInt(id, 10)), nil, &task)
		var backend *BackendError
		if errors.As(err, &backend) && backend.Status == http.StatusNotFound {
			out[r.TaskRef] = "unlinked"
			continue
		}
		if err != nil {
			return nil, err
		}
		if task.Area == nil || strings.TrimSpace(*task.Area) == "" {
			out[r.TaskRef] = "unlinked"
		} else {
			out[r.TaskRef] = *task.Area
		}
	}
	return out, nil
}

func (s *BenchStore) PendingReps() ([]Rep, error) {
	rows, err := s.localReps()
	if err != nil {
		return nil, err
	}
	out := []Rep{}
	for _, r := range rows {
		if r.Pending && r.ID == 0 {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *BenchStore) DropPendingRep(origin int64) error { return s.dropPending(origin) }

// AddOutcome writes panewire's automatic outcome. The caller supplies a stable
// origin id, so a repeat is a no-op; a conflicting repeat is an existing record,
// not a duplicate.
func (s *BenchStore) AddOutcome(ctx context.Context, r Rep) (int64, bool, error) {
	if err := hydrateRepOutcomes(&r); err != nil {
		return 0, false, err
	}
	var result struct {
		IDs []int64 `json:"ids"`
	}
	err := s.hk.request(ctx, http.MethodPut, "/v1/bench/reps", map[string]any{"reps": []map[string]any{outcomeWire(r)}}, &result)
	if err != nil {
		var backend *BackendError
		if errors.As(err, &backend) && backend.Status == http.StatusConflict && backend.Code == "bench_rep_conflict" {
			return 0, false, nil
		}
		return 0, false, err
	}
	if len(result.IDs) != 1 || result.IDs[0] <= 0 {
		return 0, false, fmt.Errorf("bench outcome has no server id")
	}
	return result.IDs[0], true, nil
}

func repTaskID(ref string) (int64, error) {
	ref = strings.TrimSpace(ref)
	if id, err := strconv.ParseInt(ref, 10, 64); err == nil && id > 0 {
		return id, nil
	}
	lower := strings.ToLower(ref)
	lower = strings.TrimPrefix(lower, "hk:")
	lower = strings.TrimPrefix(lower, "hk")
	lower = strings.TrimPrefix(lower, "task/")
	id, err := strconv.ParseInt(lower, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("invalid task ref")
	}
	return id, nil
}

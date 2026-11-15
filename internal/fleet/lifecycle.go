package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/mgh3326/panewire/internal/arbiter"
)

func (a *App) lifecycle(ctx context.Context, kind string, args []string) error {
	return a.lifecycleWithOptions(ctx, kind, args, false, false)
}

// lifecycleWithOptions is lifecycle for the completion watcher: watchResult
// attaches the job-dir findings.json (hk 2927), late marks a completion a
// re-armed watcher found (hk 2986).
func (a *App) lifecycleWithOptions(ctx context.Context, kind string, args []string, watchResult, late bool) error {
	if len(args) == 0 || !validID(args[0]) {
		return fail(exitUsage, "%s requires a job id", kind)
	}
	id := args[0]
	f := flags(kind)
	report := f.String("report", "", "")
	question := f.String("question", "", "")
	resultFile := f.String("result", "", "")
	if e := f.Parse(args[1:]); e != nil {
		return fail(exitUsage, "%v", e)
	}
	if f.NArg() > 0 {
		return fail(exitUsage, "unexpected arguments")
	}
	j, e := a.job(id)
	if e != nil {
		return e
	}
	if kind == "restore" {
		if *report != "" || *question != "" {
			return fail(exitUsage, "restore accepts only JOB")
		}
		return a.restore(ctx, j)
	}
	if kind == "escalate" && (*question == "" || j.Parent == "") {
		return fail(exitUsage, "escalate requires --question and a recorded parent lane")
	}
	var result json.RawMessage
	var rejected string
	if !watchResult && *resultFile != "" {
		if kind != "done" {
			return fail(exitUsage, "%s accepts no --result", kind)
		}
		result, e = readResult(*resultFile)
		if e != nil {
			return e
		}
	}
	if *report == "" && kind == "done" {
		*report = newestReport(filepath.Join(a.Jobs, id))
	}
	if kind == "done" && *report == "" {
		return fail(exitUsage, "done requires an existing report")
	}
	if watchResult {
		*resultFile, rejected = watcherResultFile(a.Jobs, id, *report)
		if *resultFile != "" {
			result, e = readResult(*resultFile)
			if e != nil {
				result, rejected = nil, "result failed validation"
			}
		}
	}
	strictResult := kind == "done" && !watchResult && result != nil
	body := []byte{}
	if *report != "" {
		if e = noSymlinks(*report); e != nil {
			return e
		}
		body, e = os.ReadFile(*report)
		if e != nil {
			return e
		}
	}
	lock, e := os.OpenFile(filepath.Join(a.Jobs, id, ".panewire-operation.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); e != nil {
		return e
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	j, e = a.job(id)
	if e != nil {
		return e
	}
	if watchResult && (j.Terminal || j.Reaped) {
		return nil
	}
	if rejected != "" {
		if e = a.event(ctx, id, "job.result_rejected", map[string]any{"reason": rejected}); e != nil {
			return e
		}
		a.warn("watcher result rejected: %s", rejected)
	}
	digest := reportDigest(body)
	eventKind := "job.completed"
	if kind == "escalate" {
		eventKind = "job.escalate"
	}
	// A report already announced (same bytes, same question) is the case where
	// every receipt exists; it is not a branch of its own. rounds and usedPath
	// pick the -rN snapshot name for a later answer at the same path.
	rounds, usedPath, announced := 0, false, false
	for _, ev := range j.Events {
		if ev.Kind == eventKind {
			rounds++
			if str(ev.Payload, "report_sha256") == digest && str(ev.Payload, "question") == *question {
				announced = true
			}
			if str(ev.Payload, "report") == *report {
				usedPath = true
			}
		}
	}
	if kind == "escalate" && announced {
		fmt.Fprintf(a.Out, "OK job=%s report=%s duplicate=yes\n", id, *report)
		return nil
	}
	if usedPath && !announced && *report != "" {
		ext := filepath.Ext(*report)
		stem := strings.TrimSuffix(*report, ext)
		for n := rounds + 1; ; n++ {
			path := fmt.Sprintf("%s-r%d%s", stem, n, ext)
			file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if os.IsExist(err) {
				continue
			}
			if err != nil {
				return err
			}
			_, err = file.Write(body)
			closeErr := file.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
			*report = path
			break
		}
	}
	if kind == "done" {
		return a.doneReport(ctx, j, doneInput{
			report: *report, body: body, digest: digest, question: *question, result: result,
			strict: strictResult, watcher: watchResult, late: late,
		})
	}
	return a.escalateReport(ctx, j, *report, *question, body, digest, late)
}

func readResult(path string) (json.RawMessage, error) {
	if e := noSymlinks(path); e != nil {
		return nil, e
	}
	// Read at most one byte past the limit: the watcher runs this on any
	// worker-written findings.json, which must not be loaded whole.
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	body, e := io.ReadAll(io.LimitReader(f, 64<<10+1))
	if e != nil {
		return nil, e
	}
	if len(body) > 64<<10 {
		return nil, fail(exitUsage, "--result exceeds 64 KiB")
	}
	var v any
	if e := json.Unmarshal(body, &v); e != nil {
		return nil, fail(exitUsage, "--result is not valid JSON: %v", e)
	}
	if _, ok := v.(map[string]any); !ok {
		return nil, fail(exitUsage, "--result must be a JSON object")
	}
	return json.RawMessage(body), nil
}

// watcherResultFile finds the result a finished pane filed in its job dir:
// findings.json, a reviewer's convention. The report's directory is checked
// first, where a reviewer writes it.
func watcherResultFile(jobs, id, report string) (string, string) {
	var candidates []string
	if report != "" {
		candidates = append(candidates, filepath.Join(filepath.Dir(report), "findings.json"))
	}
	candidates = append(candidates, filepath.Join(jobs, id, "findings.json"))
	seen := map[string]bool{}
	for _, path := range candidates {
		if seen[path] {
			continue
		}
		seen[path] = true
		info, e := os.Lstat(path)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil || !info.Mode().IsRegular() {
			return "", "result is not a regular file"
		}
		return path, ""
	}
	return "", ""
}

// sameJSONValue compares two decoded JSON values: key order, whitespace and
// number spelling do not matter.
func sameJSONValue(x, y any) bool {
	cx, ok := canonicalJSON(x)
	if !ok {
		return false
	}
	cy, ok := canonicalJSON(y)
	return ok && cx == cy
}

func canonicalJSON(v any) (string, bool) {
	b, e := json.Marshal(v)
	if e != nil {
		return "", false
	}
	var decoded any
	if e := json.Unmarshal(b, &decoded); e != nil {
		return "", false
	}
	b, e = json.Marshal(decoded)
	if e != nil {
		return "", false
	}
	return string(b), true
}

func (a *App) releaseQuota(ctx context.Context, j Job) error {
	pools, e := a.ledger().QuotaPools(ctx, j.ID)
	if e != nil {
		return ledgerFail(e, "status")
	}
	for _, pool := range pools {
		if pool == "" {
			return fail(exitStateConflict, "invalid quota record")
		}
		if e := a.ledger().ReleaseQuotaPool(ctx, j.ID, pool, false); e != nil {
			return ledgerFail(e, "release")
		}
	}
	return nil
}
func (a *App) restore(ctx context.Context, j Job) error {
	d, e := a.durable(ctx, j.ID)
	if e != nil {
		return e
	}
	if d.State != "released" {
		return fail(exitUsage, "restore requires released job")
	}
	agents, e := a.agents(ctx)
	if e != nil {
		return e
	}
	found := 0
	for _, ag := range agents {
		if ag.Pane == j.Pane {
			if ag.Tab != j.Tab || ag.Name != j.Label || !liveStatus(ag.Status) {
				return fail(exitUsage, "restore pane identity or liveness mismatch")
			}
			found++
		}
	}
	if found != 1 || j.Pane == "" || j.Tab == "" {
		return fail(exitUsage, "restore requires one verified existing pane")
	}
	role := "implementer"
	if j.Role == "builder" {
		role = "builder"
	}
	claim := arbiter.Claim{ReclaimReleased: true, Job: j.ID, Lane: j.Owner, AgentLabel: j.Label, T: j.T, Role: role, TaskID: j.Task}
	if role == "builder" {
		claim.ParentLane = j.Parent
	}
	if _, e = a.ledger().Claim(ctx, claim); e != nil {
		return ledgerFail(e, "claim")
	}
	if e = a.event(ctx, j.ID, "job.spawned", j.Receipt); e != nil {
		return fail(exitFailure, "job reclaimed; restoring receipt failed: %v", e)
	}
	if e = a.startWatch(j.ID); e != nil {
		return e
	}
	fmt.Fprintf(a.Out, "OK job=%s restored pane=%s tab=%s label=%s\n", j.ID, j.Pane, j.Tab, j.Label)
	return nil
}
func liveStatus(s string) bool { return s == "working" || s == "idle" || s == "done" }

// newestReport picks the most recently modified report*.md in dir. Equal
// modification times fall back to lexical name order so the choice is stable;
// files that vanish between the glob and the stat are skipped.
func newestReport(dir string) string {
	type candidate struct {
		path string
		mod  time.Time
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "report*.md"))
	var found []candidate
	for _, m := range matches {
		if st, e := os.Stat(m); e == nil {
			found = append(found, candidate{m, st.ModTime()})
		}
	}
	if len(found) == 0 {
		return ""
	}
	slices.SortStableFunc(found, func(x, y candidate) int {
		if c := y.mod.Compare(x.mod); c != 0 {
			return c
		}
		return strings.Compare(x.path, y.path)
	})
	return found[0].path
}

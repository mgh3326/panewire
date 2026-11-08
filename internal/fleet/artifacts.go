package fleet

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/mgh3326/handoffkeep/client"
)

//go:embed artifact-policy.json
var defaultArtifactPolicy []byte

type Content struct {
	Path       string `json:"path"`
	Bytes      int64  `json:"bytes"`
	SHA        string `json:"sha256"`
	State      string `json:"state,omitempty"`
	UploadedAt string `json:"uploaded_at,omitempty"`
	Mirror     bool   `json:"mirror_required,omitempty"`
}
type Archive struct {
	SourceHost  string    `json:"source_host,omitempty"`
	Source      string    `json:"source,omitempty"`
	DrivePath   string    `json:"drive_path"`
	Contents    []Content `json:"contents"`
	State       string    `json:"state"`
	ConfirmedAt string    `json:"drive_confirmed_at,omitempty"`
	Manifest    string    `json:"manifest"`
	ManifestSHA string    `json:"manifest_sha256"`
}
type ArtifactRecord struct {
	Schema  string    `json:"schema"`
	Name    string    `json:"name"`
	Host    string    `json:"host"`
	Job     string    `json:"job_id,omitempty"`
	Task    string    `json:"task_id,omitempty"`
	Archive Archive   `json:"archive"`
	Deleted []Content `json:"local_deleted,omitempty"`
}

func hashFile(path string) (int64, string, error) {
	f, e := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if e != nil {
		return 0, "", e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		return 0, "", e
	}
	if !info.Mode().IsRegular() {
		return 0, "", fail(exitUsage, "artifact is not a regular file")
	}
	h := sha256.New()
	n, e := io.Copy(h, f)
	return n, hex.EncodeToString(h.Sum(nil)), e
}
func glob(pattern, path string) bool {
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '*':
			if i+1 < len(pattern) && pattern[i+1] == '*' {
				if i+2 < len(pattern) && pattern[i+2] == '/' {
					b.WriteString("(?:.*/)?")
					i += 2
				} else {
					b.WriteString(".*")
					i++
				}
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		default:
			b.WriteString(regexp.QuoteMeta(string(pattern[i])))
		}
	}
	b.WriteString("$")
	return regexp.MustCompile(b.String()).MatchString(path)
}
func excluded(path string, patterns []string) bool {
	for _, part := range strings.Split(filepath.ToSlash(path), "/") {
		lower := strings.ToLower(part)
		if strings.HasPrefix(lower, ".env") || strings.Contains(lower, "credential") || strings.Contains(lower, "secret") || strings.HasSuffix(lower, ".pem") || strings.HasSuffix(lower, ".key") || lower == ".git" {
			return true
		}
	}
	for _, pat := range patterns {
		if glob(pat, path) || glob(pat, filepath.Base(path)) {
			return true
		}
	}
	return false
}
func inventory(root string, exclude []string) ([]Content, error) {
	var rows []Content
	e := filepath.WalkDir(root, func(path string, entry fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if path == root {
			return nil
		}
		rel, e := filepath.Rel(root, path)
		if e != nil {
			return e
		}
		if excluded(rel, exclude) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		info, e := entry.Info()
		if e != nil {
			return e
		}
		if !info.Mode().IsRegular() || strings.ContainsAny(rel, "\t\r\n") {
			return fail(exitUsage, "nonregular or unrepresentable artifact path")
		}
		n, h, e := hashFile(path)
		if e != nil {
			return e
		}
		rows = append(rows, Content{Path: filepath.ToSlash(rel), Bytes: n, SHA: h})
		return nil
	})
	return rows, e
}
func (a *App) artifacts(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fail(exitUsage, "artifacts requires move or sweep")
	}
	switch args[0] {
	case "move":
		return a.artifactMove(ctx, args[1:])
	case "sweep":
		return a.artifactSweep(ctx, args[1:])
	default:
		return fail(exitUsage, "unknown artifacts command")
	}
}
func (a *App) artifactMove(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fail(exitUsage, "move requires a source tree")
	}
	src := expand(args[0])
	f := flags("artifacts move")
	dest := f.String("dest", "", "")
	sourceHost := f.String("source-host", "", "")
	apply := f.Bool("apply", false, "")
	if e := f.Parse(args[1:]); e != nil {
		return fail(exitUsage, "%v", e)
	}
	if f.NArg() > 0 {
		return fail(exitUsage, "unexpected move arguments")
	}
	root := expand(str(a.Config.Artifacts, "drive_folder"))
	target := expand(*dest)
	if !filepath.IsAbs(root) || !filepath.IsAbs(target) || !within(root, target) || *dest == "" {
		return fail(exitUsage, "--dest must be under artifacts.drive_folder")
	}
	if e := noSymlinks(target); e != nil {
		return e
	}
	if src == target || within(src, target) || within(target, src) {
		return fail(exitUsage, "source and destination trees overlap")
	}
	exclude := stringsValue(a.Config.Artifacts, "exclude")
	if len(exclude) == 0 {
		return fail(exitConfig, "artifacts.exclude is required")
	}
	if *sourceHost != "" {
		return a.remoteArtifactMove(ctx, src, target, *sourceHost, *apply)
	}
	if !filepath.IsAbs(src) {
		var e error
		src, e = filepath.Abs(src)
		if e != nil {
			return e
		}
	}
	if e := noSymlinks(src); e != nil {
		return e
	}
	rows, e := inventory(src, exclude)
	if e != nil {
		return e
	}
	if len(rows) == 0 {
		return fail(exitUsage, "empty artifact inventory")
	}
	return a.moveInventory(ctx, src, target, root, rows, *apply, "")
}
func (a *App) moveInventory(ctx context.Context, src, target, root string, rows []Content, apply bool, remoteHost string) error {
	if len(rows) == 0 {
		return fail(exitUsage, "empty artifact inventory")
	}
	rel, e := filepath.Rel(root, target)
	if e != nil {
		return e
	}
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) < 2 || !validID(parts[0]) {
		return fail(exitUsage, "dest must be drive_folder/host/name")
	}
	fmt.Fprintf(a.Out, "artifact-move source=%s dest=%s files=%d apply=%t\n", src, target, len(rows), apply)
	if !apply {
		for _, row := range rows {
			fmt.Fprintf(a.Out, "copy path=%s bytes=%d sha256=%s\n", row.Path, row.Bytes, row.SHA)
		}
		return nil
	}
	record := ArtifactRecord{Schema: "fleet.artifact.v2", Host: parts[0], Name: strings.Join(parts[1:], "/"), Archive: Archive{Source: src, SourceHost: remoteHost, DrivePath: target, State: "running"}}
	key := "artifacts/" + parts[0] + "/" + record.Name
	batchFiles := integer(a.Config.Artifacts, "batch_files", 2000)
	batchBytes := integer(a.Config.Artifacts, "batch_bytes", 3<<30)
	if batchFiles <= 0 || batchBytes <= 0 {
		return fail(exitConfig, "invalid artifact batch limits")
	}
	for i := 0; i < len(rows); {
		end := i
		size := int64(0)
		for end < len(rows) && int64(end-i) < batchFiles && (size < batchBytes || end == i) {
			if end > i && size+rows[end].Bytes > batchBytes {
				break
			}
			size += rows[end].Bytes
			end++
		}
		sourceDir := src
		scratch := ""
		if remoteHost != "" {
			scratch, e = a.pullBatch(ctx, remoteHost, src, rows[i:end], size)
			if e != nil {
				return e
			}
			defer os.RemoveAll(scratch)
			sourceDir = scratch
		}
		var paths []string
		for _, row := range rows[i:end] {
			source := filepath.Join(sourceDir, filepath.FromSlash(row.Path))
			destination := filepath.Join(target, filepath.FromSlash(row.Path))
			if e = noSymlinks(destination); e != nil {
				return e
			}
			if e = os.MkdirAll(filepath.Dir(destination), 0700); e != nil {
				return e
			}
			if e = copyVerified(source, destination, row); e != nil {
				return e
			}
			paths = append(paths, destination)
		}
		if e = a.confirmUpload(ctx, paths); e != nil {
			return e
		}
		for k := i; k < end; k++ {
			rows[k].State = "uploaded"
			rows[k].UploadedAt = a.Now().UTC().Format(time.RFC3339)
		}
		record.Archive.Contents = append(record.Archive.Contents, rows[i:end]...)
		if e = a.putRecord(ctx, key, record); e != nil {
			return e
		}
		// Evict Drive copies only after the server has retained confirmation.
		helper := expand(str(a.Config.Artifacts, "fpbulk"))
		r := a.Runner.Run(ctx, Command{Name: helper, Args: []string{"evict"}, Input: strings.Join(paths, "\n") + "\n"})
		if e = check(r, "Drive eviction"); e != nil {
			return e
		}
		status := a.Runner.Run(ctx, Command{Name: helper, Args: []string{"status"}, Input: strings.Join(paths, "\n") + "\n"})
		if e = check(status, "Drive eviction status"); e != nil {
			return e
		}
		evicted := map[string]bool{}
		for _, line := range strings.Split(status.Out, "\n") {
			flag, path, ok := strings.Cut(line, "\t")
			if ok {
				evicted[path] = flag == "10"
			}
		}
		for _, path := range paths {
			if !evicted[path] {
				return fail(exitEvictionUnconfirmed, "Drive copy eviction unconfirmed; originals retained")
			}
		}
		if scratch != "" {
			if e = os.RemoveAll(scratch); e != nil {
				return e
			}
		}
		i = end
	}
	var manifest strings.Builder
	manifest.WriteString("path\tsize\tsha256\n")
	for _, row := range rows {
		fmt.Fprintf(&manifest, "%s\t%d\t%s\n", row.Path, row.Bytes, row.SHA)
	}
	manifestPath := target + ".manifest.tsv"
	if e = noSymlinks(manifestPath); e != nil {
		return e
	}
	if e = os.WriteFile(manifestPath, []byte(manifest.String()), 0600); e != nil {
		return e
	}
	_, sha, e := hashFile(manifestPath)
	if e != nil {
		return e
	}
	record.Archive.Manifest = filepath.Base(manifestPath)
	record.Archive.ManifestSHA = sha
	record.Archive.State = "drive_confirmed"
	record.Archive.ConfirmedAt = a.Now().UTC().Format(time.RFC3339)
	if e = a.putRecord(ctx, key, record); e != nil {
		return e
	}
	fmt.Fprintf(a.Out, "OK artifact=%s files=%d originals=retained\n", key, len(rows))
	return nil
}
func copyVerified(src, dest string, row Content) error {
	if e := noSymlinks(src); e != nil {
		return e
	}
	n, hash, e := hashFile(src)
	if e != nil {
		return e
	}
	if n != row.Bytes || hash != row.SHA {
		return fail(exitStateConflict, "source changed during copy")
	}
	if _, e = os.Lstat(dest); e == nil {
		n, h, e := hashFile(dest)
		if e != nil || n != row.Bytes || h != row.SHA {
			return fail(exitStateConflict, "destination conflict")
		}
		return nil
	} else if !os.IsNotExist(e) {
		return e
	}
	in, e := os.Open(src)
	if e != nil {
		return e
	}
	defer in.Close()
	out, e := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	_, err := io.Copy(out, in)
	closeErr := out.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	n, h, e := hashFile(dest)
	if e != nil {
		return e
	}
	if n != row.Bytes || h != row.SHA {
		return fail(exitStateConflict, "copied file hash mismatch")
	}
	return nil
}
func (a *App) confirmUpload(ctx context.Context, paths []string) error {
	helper := expand(str(a.Config.Artifacts, "fpbulk"))
	logs := expand(str(a.Config.Artifacts, "drivefs_log_dir"))
	if helper == "" || logs == "" {
		return fail(exitConfig, "upload confirmation needs fpbulk and drivefs_log_dir")
	}
	gap := time.Duration(integer(a.Config.Artifacts, "confirm_gap_seconds", 120)) * time.Second
	if gap < 120*time.Second {
		return fail(exitConfig, "confirmation gap must be at least 120 seconds")
	}
	timeout := time.Duration(integer(a.Config.Artifacts, "confirm_timeout_seconds", 21600)) * time.Second
	poll := time.Duration(integer(a.Config.Artifacts, "poll_seconds", 30)) * time.Second
	if poll <= 0 || timeout < gap {
		return fail(exitConfig, "invalid confirmation timing")
	}
	stale := time.Duration(integer(a.Config.Artifacts, "stale_minutes", 10)) * time.Minute
	deadline := a.Now().Add(timeout)
	var first time.Time
	for {
		app := str(a.Config.Artifacts, "app_check")
		if app == "" {
			app = "pgrep -x 'Google Drive'"
		}
		r := a.Runner.Run(ctx, Command{Name: "sh", Args: []string{"-c", app}})
		fresh := false
		entries, _ := os.ReadDir(logs)
		for _, ent := range entries {
			if info, e := ent.Info(); e == nil && !ent.IsDir() && a.Now().Sub(info.ModTime()) <= stale && info.ModTime().Sub(a.Now()) <= time.Minute {
				fresh = true
			}
		}
		status := a.Runner.Run(ctx, Command{Name: helper, Args: []string{"status"}, Input: strings.Join(paths, "\n") + "\n"})
		uploaded := map[string]bool{}
		for _, line := range strings.Split(status.Out, "\n") {
			flag, path, ok := strings.Cut(line, "\t")
			if ok {
				uploaded[path] = len(flag) == 2 && flag[0] == '1' && (flag[1] == '0' || flag[1] == '1')
			}
		}
		all := r.Code == 0 && fresh && status.Code == 0
		for _, path := range paths {
			all = all && uploaded[path]
		}
		if all {
			if !first.IsZero() && a.Now().Sub(first) >= gap {
				return nil
			}
			if first.IsZero() {
				first = a.Now()
			}
		} else {
			first = time.Time{}
		}
		if !a.Now().Before(deadline) {
			return fail(exitUploadTimeout, "upload confirmation timed out; copies retained")
		}
		if e := a.Sleep(ctx, poll); e != nil {
			return e
		}
	}
}

// putRecord writes the artifact record over hk HTTP with the svc-panewire
// credential (hk 3607): kind artifact, no job, the encoded record as the body,
// as `handoffkeep doc put --kind artifact --file` sent it, under the upload
// bound. A failure reads as a.must's did.
func (a *App) putRecord(ctx context.Context, key string, record any) error {
	b, e := encode(record)
	if e != nil {
		return e
	}
	return hkCheck(hkPutDocument(ctx, client.Document{Key: key, Kind: "artifact", Body: string(b)}), "handoffkeep")
}
func (a *App) remoteArtifactMove(ctx context.Context, src, dest, host string, apply bool) error {
	h, ok := a.Config.Hosts[host]
	if !ok || str(h, "ssh") == "" || !filepath.IsAbs(src) {
		return fail(exitConfig, "remote source needs configured SSH host and absolute source")
	}
	request := struct {
		Root    string
		Exclude []string
	}{src, stringsValue(a.Config.Artifacts, "exclude")}
	b, _ := encode(request)
	r := a.Runner.Run(ctx, Command{Name: "ssh", Args: []string{"-o", "BatchMode=yes", "--", str(h, "ssh"), "panewire _artifact-inventory"}, Input: string(b)})
	if e := check(r, "remote artifact inventory"); e != nil {
		return e
	}
	var rows []Content
	if e := json.Unmarshal([]byte(r.Out), &rows); e != nil {
		return e
	}
	for _, row := range rows {
		if !within(src, filepath.Join(src, row.Path)) || row.Bytes < 0 || len(row.SHA) != 64 || excluded(row.Path, request.Exclude) {
			return fail(exitConfig, "invalid remote artifact inventory")
		}
	}
	return a.moveInventory(ctx, src, dest, expand(str(a.Config.Artifacts, "drive_folder")), rows, apply, host)
}
func (a *App) artifactInventory() error {
	var request struct {
		Root    string
		Exclude []string
	}
	b, e := io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
	if e != nil {
		return e
	}
	if e = decode(b, &request); e != nil {
		return e
	}
	if !filepath.IsAbs(request.Root) {
		return fail(exitUsage, "inventory root must be absolute")
	}
	if e = noSymlinks(request.Root); e != nil {
		return e
	}
	rows, e := inventory(request.Root, request.Exclude)
	if e != nil {
		return e
	}
	return writeJSON(a.Out, rows)
}
func (a *App) pullBatch(ctx context.Context, host, src string, rows []Content, size int64) (string, error) {
	base := expand(str(a.Config.Artifacts, "scratch_dir"))
	if base == "" {
		base = filepath.Join(a.Home, ".local", "share", "fleet-upload", "scratch")
	}
	if e := noSymlinks(base); e != nil {
		return "", e
	}
	if e := os.MkdirAll(base, 0700); e != nil {
		return "", e
	}
	var stat syscall.Statfs_t
	if e := syscall.Statfs(base, &stat); e != nil {
		return "", e
	}
	free := uint64(stat.Bavail) * uint64(stat.Bsize)
	floor := integer(a.Config.Artifacts, "free_floor_bytes", 20<<30)
	if floor < 0 || free < uint64(size)+uint64(floor) {
		return "", fail(exitStateConflict, "artifact scratch free-space floor would be crossed")
	}
	scratch, e := os.MkdirTemp(base, "panewire-pull-*")
	if e != nil {
		return "", e
	}
	var names strings.Builder
	for _, row := range rows {
		names.WriteString(row.Path)
		names.WriteByte(0)
	}
	args := []string{"-a", "--no-links", "--protect-args", "--from0", "--files-from=-", "--", str(a.Config.Hosts[host], "ssh") + ":" + src + "/", scratch + "/"}
	r := a.Runner.Run(ctx, Command{Name: "rsync", Args: args, Input: names.String()})
	if e = check(r, "artifact batch pull"); e != nil {
		os.RemoveAll(scratch)
		return "", e
	}
	return scratch, nil
}

type ArtifactPolicy struct {
	Schema string                     `json:"schema"`
	Roots  map[string]json.RawMessage `json:"roots"`
	Rows   []ArtifactRule             `json:"rows"`
}
type ArtifactRule struct {
	Class, Root, Glob, Host, Job string
	KeepNewest                   int  `json:"keep_newest"`
	Mirror                       bool `json:"mirror_required"`
}

func (a *App) artifactSweep(ctx context.Context, args []string) error {
	f := flags("sweep")
	job := f.String("job", "", "")
	ageArg := f.String("age", "7d", "")
	policyPath := f.String("policy", "", "")
	apply := f.Bool("apply", false, "")
	if e := f.Parse(args); e != nil {
		return fail(exitUsage, "%v", e)
	}
	if f.NArg() > 0 {
		return fail(exitUsage, "unexpected sweep arguments")
	}
	age, e := duration(*ageArg)
	if e != nil || age < 0 {
		return fail(exitUsage, "invalid sweep age")
	}
	b := defaultArtifactPolicy
	if *policyPath != "" {
		b, e = os.ReadFile(*policyPath)
		if e != nil {
			return e
		}
	}
	var pol ArtifactPolicy
	if e = decode(b, &pol); e != nil || pol.Schema != "fleet.artifact-policy.v1" {
		return fail(exitConfig, "invalid artifact policy")
	}
	hatch := str(a.Config.Artifacts, "local_delete_at")
	if hatch == "" {
		hatch = "drive_confirmed"
	}
	if hatch != "drive_confirmed" && hatch != "uploaded" {
		return fail(exitConfig, "invalid local_delete_at")
	}
	agents, e := a.agents(ctx)
	if e != nil {
		return e
	}
	jobs, e := os.ReadDir(a.Jobs)
	if e != nil {
		return e
	}
	for _, entry := range jobs {
		if !entry.IsDir() || *job != "" && entry.Name() != *job {
			continue
		}
		j, e := a.job(entry.Name())
		if e != nil {
			return e
		}
		if !j.Terminal || j.Keep || a.Now().Sub(j.At) < age {
			continue
		}
		alive := false
		for _, ag := range agents {
			if ag.Pane == j.Pane {
				alive = true
			}
		}
		if alive {
			continue
		}
		if a.watchActive(j.ID) {
			continue
		}
		// hk HTTP with the svc-panewire credential (hk 3607); each answer is
		// read as the hk CLI's output was, and any hk failure skips the job
		// or the document as a failed CLI call did.
		b, e := hkTaskBody(ctx, j.Task)
		if e != nil {
			continue
		}
		var task struct{ State, Project string }
		if json.Unmarshal(b, &task) != nil || !slices.Contains([]string{"merged", "dropped"}, task.State) || task.Project == "" {
			continue
		}
		prefix := "artifacts/" + task.Project + "/" + j.Task + "/" + j.ID
		b, e = hkDocumentListBody(ctx, prefix)
		if e != nil {
			continue
		}
		var listing struct {
			Documents []struct{ Key string } `json:"documents"`
		}
		if json.Unmarshal(b, &listing) != nil {
			return fail(exitConfig, "invalid artifact doc listing")
		}
		for _, doc := range listing.Documents {
			// The prefix is a LIKE pattern: a _ or % in it matches more.
			if doc.Key != prefix && !strings.HasPrefix(doc.Key, prefix+"/") {
				continue
			}
			b, e = hkDocumentBody(ctx, doc.Key)
			if e != nil {
				continue
			}
			var d struct{ Body string }
			if json.Unmarshal(b, &d) != nil {
				continue
			}
			var rec ArtifactRecord
			if json.Unmarshal([]byte(d.Body), &rec) != nil || rec.Schema != "fleet.artifact.v2" || rec.Host != a.machine() || rec.Job != "" && rec.Job != j.ID || rec.Task != "" && rec.Task != j.Task {
				continue
			}
			if e = a.sweepRecord(ctx, j, doc.Key, rec, pol, hatch, *apply); e != nil {
				return e
			}
		}
	}
	return nil
}
func (a *App) sweepRecord(ctx context.Context, j Job, key string, rec ArtifactRecord, pol ArtifactPolicy, hatch string, apply bool) error {
	if rec.Archive.SourceHost != "" && rec.Archive.SourceHost != a.machine() {
		return nil
	}
	src := rec.Archive.Source
	if !filepath.IsAbs(src) || !within(filepath.Join(a.Jobs, j.ID), src) {
		return nil
	}
	if e := noSymlinks(src); e != nil {
		return e
	}
	for _, row := range rec.Archive.Contents {
		if filepath.IsAbs(row.Path) || !within(src, filepath.Join(src, row.Path)) {
			continue
		}
		path := filepath.Join(src, row.Path)
		class := ""
		mirror := row.Mirror
		for _, rule := range pol.Rows {
			var root struct{ Path, Kind string }
			raw, ok := pol.Roots[rule.Root]
			if !ok {
				return fail(exitConfig, "unknown artifact policy root")
			}
			if json.Unmarshal(raw, &root) != nil {
				var rel string
				if json.Unmarshal(raw, &rel) != nil {
					return fail(exitConfig, "invalid artifact policy root")
				}
				root.Path = rel
				root.Kind = "per-job"
			}
			workRoot := filepath.Dir(filepath.Dir(a.Jobs))
			base := filepath.Join(workRoot, root.Path)
			if !within(workRoot, base) {
				return fail(exitConfig, "artifact root escapes work root")
			}
			if root.Kind == "per-job" {
				base = filepath.Join(base, j.ID)
			}
			rel, e := filepath.Rel(base, path)
			if e != nil || strings.HasPrefix(rel, "..") {
				continue
			}
			if rule.Job != "" && rule.Job != j.ID || rule.Host != "" && rule.Host != a.machine() {
				continue
			}
			if glob(rule.Glob, filepath.ToSlash(rel)) {
				class = rule.Class
				mirror = mirror || rule.Mirror
				break
			}
		}
		if class != "evidence" && class != "final-build" {
			continue
		}
		if rec.Archive.State != "drive_confirmed" && rec.Archive.ConfirmedAt == "" {
			if hatch != "uploaded" || class == "evidence" || mirror || row.UploadedAt == "" {
				continue
			}
		}
		if excluded(row.Path, stringsValue(a.Config.Artifacts, "exclude")) {
			continue
		}
		if e := noSymlinks(path); e != nil {
			continue
		}
		n, h, e := hashFile(path)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return e
		}
		if n != row.Bytes || h != row.SHA {
			continue
		}
		fmt.Fprintf(a.Out, "artifact-sweep job=%s path=%s class=%s apply=%t\n", j.ID, path, class, apply)
		if !apply {
			continue
		}
		latest, e := a.job(j.ID)
		if e != nil {
			return e
		}
		if !latest.Terminal || latest.Keep {
			return fail(exitStateConflict, "job changed during artifact sweep")
		}
		n, h, e = hashFile(path)
		if e != nil || n != row.Bytes || h != row.SHA {
			return fail(exitStateConflict, "artifact changed before deletion")
		}
		rec.Deleted = append(rec.Deleted, row)
		if e = a.putRecord(ctx, key, rec); e != nil {
			return e
		}
		if e = a.sweepStillSafe(ctx, j); e != nil {
			return e
		}
		if e = noSymlinks(path); e != nil {
			return e
		}
		n, h, e = hashFile(path)
		if e != nil || n != row.Bytes || h != row.SHA {
			return fail(exitStateConflict, "artifact changed while recording deletion")
		}
		if e = os.Remove(path); e != nil {
			return e
		}
	}
	return nil
}
func (a *App) watchActive(id string) bool {
	path := filepath.Join(a.Jobs, id, ".panewire-watch.lock")
	f, e := secureOpen(path, false, a.warn)
	if os.IsNotExist(e) {
		_, e = os.Stat(filepath.Join(a.Jobs, id, "completion-sentinel.pid"))
		return e == nil
	}
	if e != nil {
		return true
	}
	defer f.Close()
	return !tryLock(f)
}
func (a *App) sweepStillSafe(ctx context.Context, j Job) error {
	latest, e := a.job(j.ID)
	if e != nil {
		return e
	}
	if !latest.Terminal || latest.Keep || a.watchActive(j.ID) {
		return fail(exitStateConflict, "job or watcher changed during sweep")
	}
	agents, e := a.agents(ctx)
	if e != nil {
		return e
	}
	for _, ag := range agents {
		if ag.Pane == latest.Pane {
			return fail(exitStateConflict, "pane present during sweep")
		}
	}
	b, e := hkTaskBody(ctx, j.Task)
	if e != nil {
		return hkCheck(e, "sweep task recheck")
	}
	var t struct{ State string }
	if json.Unmarshal(b, &t) != nil || !slices.Contains([]string{"merged", "dropped"}, t.State) {
		return fail(exitStateConflict, "task reopened during sweep")
	}
	return nil
}

package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type contextStringFlags []string

func (v *contextStringFlags) String() string         { return strings.Join(*v, ",") }
func (v *contextStringFlags) Set(value string) error { *v = append(*v, value); return nil }

type contextCLIAuth struct{ hubURL, tokenEnv, cfEnv string }

func addContextAuth(flags *flag.FlagSet) *contextCLIAuth {
	a := &contextCLIAuth{}
	flags.StringVar(&a.hubURL, "hub-url", "", "HTTPS hub base URL")
	flags.StringVar(&a.tokenEnv, "hub-token-env", "", "mode-0600 HUB_MACHINE_ID/HUB_TOKEN env file")
	flags.StringVar(&a.cfEnv, "hub-cf-env", "", "optional mode-0600 CF Access env file")
	return a
}
func (a contextCLIAuth) request(ctx context.Context, method, path string, body io.Reader, deps hubCLIDeps) (*http.Response, error) {
	if a.hubURL == "" || a.tokenEnv == "" {
		return nil, fmt.Errorf("missing hub credentials")
	}
	env, err := loadHubTokenEnv(a.tokenEnv)
	if err != nil {
		return nil, err
	}
	endpoint, err := hubHTTPSEndpoint(a.hubURL, path, deps.AllowInsecureForTests)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return nil, err
	}
	request.Header.Set(hubAuthorizationHeader, "Bearer "+env.Token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if a.cfEnv != "" {
		cf, err := loadHubCFAccessEnv(a.cfEnv)
		if err != nil {
			return nil, err
		}
		request.Header.Set("CF-Access-Client-Id", cf.ClientID)
		request.Header.Set("CF-Access-Client-Secret", cf.ClientSecret)
	}
	client := deps.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return client.Do(request)
}
func contextRequestTimeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 15*time.Second)
}
func contextCLIError(stderr io.Writer, label string, response *http.Response, err error) int {
	if response != nil {
		defer response.Body.Close()
		var e struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&e)
		if e.Error != "" {
			fmt.Fprintf(stderr, "%s rejected: %s\n", label, e.Error)
		} else {
			fmt.Fprintf(stderr, "%s unavailable\n", label)
		}
		return ExitConditionInvalid
	}
	if err != nil {
		fmt.Fprintf(stderr, "%s unavailable\n", label)
	}
	return ExitInternal
}

func runContextCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	if len(args) == 0 {
		return ExitUsage
	}
	switch args[0] {
	case "checkpoint":
		return runContextCheckpoint(args[1:], stdout, stderr, deps)
	case "recent":
		return runContextRecent(args[1:], stdout, stderr, deps)
	case "search":
		return runContextSearch(args[1:], stdout, stderr, deps)
	default:
		return ExitUsage
	}
}

func runContextSearch(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire ctx search", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	auth := addContextAuth(flags)
	session := flags.String("session", "", "session")
	kind := flags.String("kind", "", "kind")
	scope := flags.String("scope", "all", "docs, ctx, or all")
	limit := flags.Int("limit", 20, "maximum results")
	if flags.Parse(args) != nil || flags.NArg() != 1 {
		return ExitUsage
	}
	query := flags.Arg(0)
	endpoint := "/v1/context/search?q=" + urlQuery(query) + "&scope=" + urlQuery(*scope) + "&limit=" + fmt.Sprint(*limit)
	if *session != "" {
		endpoint += "&session=" + urlQuery(*session)
	}
	if *kind != "" {
		endpoint += "&kind=" + urlQuery(*kind)
	}
	ctx, cancel := contextRequestTimeout()
	defer cancel()
	response, err := auth.request(ctx, http.MethodGet, endpoint, nil, deps)
	if err != nil || response.StatusCode != http.StatusOK {
		return contextCLIError(stderr, "ctx search", response, err)
	}
	defer response.Body.Close()
	var result struct {
		Results []ContextSearchResult `json:"results"`
	}
	if json.NewDecoder(response.Body).Decode(&result) != nil {
		return ExitInternal
	}
	for _, item := range result.Results {
		fmt.Fprintf(stdout, "%s\t%s\t%s\t%s\n%s\n", item.CreatedAt.Format(time.RFC3339), item.Session, item.Kind, item.Title, item.Snippet)
	}
	return ExitOK
}
func runContextCheckpoint(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire ctx checkpoint", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	auth := addContextAuth(flags)
	session := flags.String("session", "", "session name")
	kind := flags.String("kind", "checkpoint", "checkpoint kind")
	title := flags.String("title", "", "title")
	body := flags.String("body", "", "body")
	file := flags.String("file", "", "body file")
	var refs contextStringFlags
	flags.Var(&refs, "ref", "reference k=v (repeatable)")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *session == "" || *title == "" || (*body != "" && *file != "") {
		return ExitUsage
	}
	if *file != "" {
		data, err := os.ReadFile(*file)
		if err != nil {
			fmt.Fprintln(stderr, "ctx checkpoint rejected: unreadable body file")
			return ExitConditionInvalid
		}
		*body = string(data)
	}
	refMap := map[string]string{}
	for _, ref := range refs {
		k, v, ok := strings.Cut(ref, "=")
		if !ok || k == "" || v == "" {
			return ExitUsage
		}
		refMap[k] = v
	}
	payload, err := json.Marshal(map[string]any{"session": *session, "kind": *kind, "title": *title, "body": *body, "refs": refMap})
	if err != nil {
		return ExitInternal
	}
	ctx, cancel := contextRequestTimeout()
	defer cancel()
	response, err := auth.request(ctx, http.MethodPost, "/v1/context/checkpoints", bytes.NewReader(payload), deps)
	if err != nil || response.StatusCode != http.StatusCreated {
		return contextCLIError(stderr, "ctx checkpoint", response, err)
	}
	defer response.Body.Close()
	var item ContextCheckpoint
	if json.NewDecoder(response.Body).Decode(&item) != nil {
		return ExitInternal
	}
	fmt.Fprintf(stdout, "checkpoint %d recorded for %s\n", item.ID, item.Session)
	return ExitOK
}
func runContextRecent(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire ctx recent", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	auth := addContextAuth(flags)
	session := flags.String("session", "", "session name")
	kind := flags.String("kind", "", "kind")
	limit := flags.Int("limit", 3, "maximum records")
	asJSON := flags.Bool("json", false, "JSON output")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *session == "" {
		return ExitUsage
	}
	path := "/v1/context/checkpoints?session=" + urlQuery(*session) + "&limit=" + fmt.Sprint(*limit)
	if *kind != "" {
		path += "&kind=" + urlQuery(*kind)
	}
	ctx, cancel := contextRequestTimeout()
	defer cancel()
	response, err := auth.request(ctx, http.MethodGet, path, nil, deps)
	if err != nil || response.StatusCode != http.StatusOK {
		return contextCLIError(stderr, "ctx recent", response, err)
	}
	defer response.Body.Close()
	var result struct {
		Checkpoints []ContextCheckpoint `json:"checkpoints"`
	}
	if json.NewDecoder(response.Body).Decode(&result) != nil {
		return ExitInternal
	}
	if *asJSON {
		_ = json.NewEncoder(stdout).Encode(result)
		return ExitOK
	}
	for _, item := range result.Checkpoints {
		fmt.Fprintf(stdout, "## %s — %s\n\n%s\n\n- kind: %s\n- created_by: %s\n- created_at: %s\n", item.Session, item.Title, item.Body, item.Kind, item.CreatedBy, item.CreatedAt.Format(time.RFC3339))
		if len(item.Refs) > 0 {
			keys := make([]string, 0, len(item.Refs))
			for k := range item.Refs {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Fprintf(stdout, "- ref %s: %s\n", k, item.Refs[k])
			}
		}
		fmt.Fprintln(stdout)
	}
	return ExitOK
}
func urlQuery(value string) string {
	return strings.NewReplacer("%", "%25", " ", "%20", "&", "%26", "?", "%3F", "/", "%2F", "#", "%23").Replace(value)
}

func runMemoryCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	if len(args) == 0 {
		return ExitUsage
	}
	switch args[0] {
	case "push":
		return runMemoryPush(args[1:], stdout, stderr, deps)
	case "pull":
		return runMemoryPull(args[1:], stdout, stderr, deps)
	case "list":
		return runMemoryList(args[1:], stdout, stderr, deps)
	default:
		return ExitUsage
	}
}
func memoryFlags(name string, args []string) (*flag.FlagSet, *contextCLIAuth, *string, *string, *bool, bool) {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	auth := addContextAuth(flags)
	agent := flags.String("agent", "", "agent")
	dir := flags.String("dir", "", "memory directory")
	apply := flags.Bool("apply", false, "write changes")
	return flags, auth, agent, dir, apply, flags.Parse(args) == nil && flags.NArg() == 0
}
func runMemoryPush(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	_, auth, agent, dir, apply, ok := memoryFlags("panewire memory push", args)
	if !ok || *agent == "" || *dir == "" {
		return ExitUsage
	}
	items, err := readMemoryDir(*agent, *dir)
	if err != nil {
		fmt.Fprintln(stderr, "memory push rejected: invalid memory directory")
		return ExitConditionInvalid
	}
	if !*apply {
		fmt.Fprintf(stdout, "dry-run: would push %d memory records\n", len(items))
		return ExitOK
	}
	for _, item := range items {
		payload, _ := json.Marshal(map[string]string{"description": item.Description, "type": item.MemoryType, "content": item.Content})
		ctx, cancel := contextRequestTimeout()
		response, requestErr := auth.request(ctx, http.MethodPut, "/v1/context/memory/"+urlQuery(*agent)+"/"+urlQuery(item.Name), bytes.NewReader(payload), deps)
		cancel()
		if requestErr != nil || response.StatusCode != http.StatusOK {
			return contextCLIError(stderr, "memory push", response, requestErr)
		}
		response.Body.Close()
	}
	fmt.Fprintf(stdout, "pushed %d memory records\n", len(items))
	return ExitOK
}
func runMemoryPull(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	_, auth, agent, dir, apply, ok := memoryFlags("panewire memory pull", args)
	if !ok || *agent == "" || *dir == "" {
		return ExitUsage
	}
	ctx, cancel := contextRequestTimeout()
	response, err := auth.request(ctx, http.MethodGet, "/v1/context/memory/"+urlQuery(*agent), nil, deps)
	defer cancel()
	if err != nil || response.StatusCode != http.StatusOK {
		return contextCLIError(stderr, "memory pull", response, err)
	}
	defer response.Body.Close()
	var list struct {
		Memory []ContextMemory `json:"memory"`
	}
	if json.NewDecoder(response.Body).Decode(&list) != nil {
		return ExitInternal
	}
	for index := range list.Memory {
		ctx, cancel := contextRequestTimeout()
		detail, detailErr := auth.request(ctx, http.MethodGet, "/v1/context/memory/"+urlQuery(*agent)+"/"+urlQuery(list.Memory[index].Name), nil, deps)
		cancel()
		if detailErr != nil || detail.StatusCode != http.StatusOK {
			return contextCLIError(stderr, "memory pull", detail, detailErr)
		}
		if json.NewDecoder(detail.Body).Decode(&list.Memory[index]) != nil {
			detail.Body.Close()
			return ExitInternal
		}
		detail.Body.Close()
	}
	if !*apply {
		fmt.Fprintf(stdout, "dry-run: would materialize %d memory records\n", len(list.Memory))
		return ExitOK
	}
	if err := writeMemoryDir(*dir, list.Memory); err != nil {
		fmt.Fprintln(stderr, "memory pull unavailable")
		return ExitInternal
	}
	fmt.Fprintf(stdout, "materialized %d memory records\n", len(list.Memory))
	return ExitOK
}
func runMemoryList(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire memory list", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	auth := addContextAuth(flags)
	agent := flags.String("agent", "", "agent")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *agent == "" {
		return ExitUsage
	}
	ctx, cancel := contextRequestTimeout()
	defer cancel()
	response, err := auth.request(ctx, http.MethodGet, "/v1/context/memory/"+urlQuery(*agent), nil, deps)
	if err != nil || response.StatusCode != http.StatusOK {
		return contextCLIError(stderr, "memory list", response, err)
	}
	defer response.Body.Close()
	var list struct {
		Memory []ContextMemory `json:"memory"`
	}
	if json.NewDecoder(response.Body).Decode(&list) != nil {
		return ExitInternal
	}
	for _, item := range list.Memory {
		fmt.Fprintf(stdout, "%s\t%s\t%s\t%s\n", item.Name, item.MemoryType, item.UpdatedAt.Format(time.RFC3339), item.Description)
	}
	return ExitOK
}

func readMemoryDir(agent, dir string) ([]ContextMemory, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var items []ContextMemory
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".md" || entry.Name() == "MEMORY.md" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		item, err := parseMemoryFile(agent, strings.TrimSuffix(entry.Name(), ".md"), string(data))
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	return items, nil
}
func parseMemoryFile(agent, fallback, raw string) (ContextMemory, error) {
	if !strings.HasPrefix(raw, "---\n") {
		return ContextMemory{}, fmt.Errorf("missing frontmatter")
	}
	end := strings.Index(raw[4:], "\n---\n")
	if end < 0 {
		return ContextMemory{}, fmt.Errorf("missing frontmatter end")
	}
	front := raw[4 : 4+end]
	content := raw[4+end+5:]
	item := ContextMemory{Agent: agent, Name: fallback, MemoryType: "project", Content: content}
	inMetadata := false
	for _, line := range strings.Split(front, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if key == "metadata" {
			inMetadata = true
			continue
		}
		if inMetadata && key == "type" {
			item.MemoryType = value
			continue
		}
		inMetadata = false
		switch key {
		case "name":
			item.Name = value
		case "description":
			item.Description = value
		}
	}
	if !validContextName(item.Name) || !memoryTypes[item.MemoryType] || !validContextText(item.Description) || !validContextText(item.Content) {
		return item, fmt.Errorf("invalid memory")
	}
	return item, nil
}
func writeMemoryDir(dir string, items []ContextMemory) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	var index strings.Builder
	for _, item := range items {
		if !validContextName(item.Name) {
			return fmt.Errorf("invalid memory name")
		}
		body := fmt.Sprintf("---\nname: %s\ndescription: %s\nmetadata:\n  type: %s\n---\n%s", item.Name, item.Description, item.MemoryType, item.Content)
		if err := os.WriteFile(filepath.Join(dir, item.Name+".md"), []byte(body), 0600); err != nil {
			return err
		}
		fmt.Fprintf(&index, "- [%s](%s.md) — %s\n", item.Name, item.Name, item.Description)
	}
	return os.WriteFile(filepath.Join(dir, "MEMORY.md"), []byte(index.String()), 0600)
}

func runDocumentCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	if len(args) == 0 {
		return ExitUsage
	}
	switch args[0] {
	case "put":
		return runDocumentPut(args[1:], stdout, stderr, deps)
	case "get":
		return runDocumentGet(args[1:], stdout, stderr, deps)
	case "list":
		return runDocumentList(args[1:], stdout, stderr, deps)
	case "import":
		return runDocumentImport(args[1:], stdout, stderr, deps)
	default:
		return ExitUsage
	}
}
func runDocumentPut(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire doc put", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	auth := addContextAuth(flags)
	key := flags.String("key", "", "inbox-relative document key")
	file := flags.String("file", "", "document file")
	kind := flags.String("kind", "other", "document kind")
	session := flags.String("session", "", "session")
	job := flags.String("job", "", "job")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *key == "" || *file == "" {
		return ExitUsage
	}
	body, err := os.ReadFile(*file)
	if err != nil {
		fmt.Fprintln(stderr, "doc put rejected: unreadable file")
		return ExitConditionInvalid
	}
	return putDocument(auth, *key, *kind, *session, *job, string(body), stdout, stderr, deps)
}
func putDocument(auth *contextCLIAuth, key, kind, session, job, body string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	changed, document, code := putDocumentResult(auth, key, kind, session, job, body, stderr, deps)
	if code != ExitOK {
		return code
	}
	if changed {
		fmt.Fprintf(stdout, "stored %s\n", document.Key)
	} else {
		fmt.Fprintf(stdout, "unchanged %s\n", document.Key)
	}
	return ExitOK
}
func putDocumentResult(auth *contextCLIAuth, key, kind, session, job, body string, stderr io.Writer, deps hubCLIDeps) (bool, ContextDocument, int) {
	payload, err := json.Marshal(map[string]string{"kind": kind, "session": session, "job": job, "body": body})
	if err != nil {
		return false, ContextDocument{}, ExitInternal
	}
	ctx, cancel := contextRequestTimeout()
	defer cancel()
	response, err := auth.request(ctx, http.MethodPut, "/v1/context/docs/"+urlQuery(key), bytes.NewReader(payload), deps)
	if err != nil || response.StatusCode != http.StatusOK {
		return false, ContextDocument{}, contextCLIError(stderr, "doc put", response, err)
	}
	defer response.Body.Close()
	var result struct {
		Document ContextDocument `json:"document"`
		Changed  bool            `json:"changed"`
	}
	if json.NewDecoder(response.Body).Decode(&result) != nil {
		return false, ContextDocument{}, ExitInternal
	}
	return result.Changed, result.Document, ExitOK
}
func runDocumentGet(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire doc get", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	auth := addContextAuth(flags)
	if flags.Parse(args) != nil || flags.NArg() != 1 {
		return ExitUsage
	}
	ctx, cancel := contextRequestTimeout()
	defer cancel()
	response, err := auth.request(ctx, http.MethodGet, "/v1/context/docs/"+urlQuery(flags.Arg(0)), nil, deps)
	if err != nil || response.StatusCode != http.StatusOK {
		return contextCLIError(stderr, "doc get", response, err)
	}
	defer response.Body.Close()
	var item ContextDocument
	if json.NewDecoder(response.Body).Decode(&item) != nil {
		return ExitInternal
	}
	_, _ = io.WriteString(stdout, item.Body)
	return ExitOK
}
func runDocumentList(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire doc list", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	auth := addContextAuth(flags)
	prefix := flags.String("prefix", "", "key prefix")
	kind := flags.String("kind", "", "kind")
	session := flags.String("session", "", "session")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return ExitUsage
	}
	endpoint := "/v1/context/docs?prefix=" + urlQuery(*prefix) + "&kind=" + urlQuery(*kind) + "&session=" + urlQuery(*session)
	ctx, cancel := contextRequestTimeout()
	defer cancel()
	response, err := auth.request(ctx, http.MethodGet, endpoint, nil, deps)
	if err != nil || response.StatusCode != http.StatusOK {
		return contextCLIError(stderr, "doc list", response, err)
	}
	defer response.Body.Close()
	var result struct {
		Documents []ContextDocument `json:"documents"`
	}
	if json.NewDecoder(response.Body).Decode(&result) != nil {
		return ExitInternal
	}
	for _, item := range result.Documents {
		fmt.Fprintf(stdout, "%s\t%s\t%s\t%s\n", item.Key, item.Kind, item.Session, item.SHA256)
	}
	return ExitOK
}

type documentImportSummary struct {
	Files     int
	Rejected  []string
	Kinds     map[string]int
	Stored    int
	Unchanged int
}

func runDocumentImport(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire doc import", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	auth := addContextAuth(flags)
	dir := flags.String("dir", "", "inbox root")
	glob := flags.String("glob", "**/*.md", "relative document glob")
	apply := flags.Bool("apply", false, "store documents")
	session := flags.String("session", "", "optional session override")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *dir == "" {
		return ExitUsage
	}
	summary := documentImportSummary{Kinds: map[string]int{}}
	err := filepath.WalkDir(*dir, func(filename string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		rel, err := filepath.Rel(*dir, filename)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if !documentGlobMatch(*glob, key) {
			return nil
		}
		summary.Files++
		kind, job := documentImportKind(key)
		summary.Kinds[kind]++
		body, err := os.ReadFile(filename)
		if err != nil {
			return err
		}
		if len(body) > contextDocumentMaxBytes || contextSecret(string(body)) != "" {
			summary.Rejected = append(summary.Rejected, key)
			return nil
		}
		if !*apply {
			return nil
		}
		changed, _, code := putDocumentResult(auth, key, kind, *session, job, string(body), stderr, deps)
		if code != ExitOK {
			return fmt.Errorf("document import failed")
		}
		if changed {
			summary.Stored++
		} else {
			summary.Unchanged++
		}
		return nil
	})
	if err != nil {
		fmt.Fprintln(stderr, "doc import unavailable")
		return ExitInternal
	}
	sort.Strings(summary.Rejected)
	fmt.Fprintf(stdout, "files=%d stored=%d unchanged=%d rejected=%d", summary.Files, summary.Stored, summary.Unchanged, len(summary.Rejected))
	keys := make([]string, 0, len(summary.Kinds))
	for kind := range summary.Kinds {
		keys = append(keys, kind)
	}
	sort.Strings(keys)
	for _, kind := range keys {
		fmt.Fprintf(stdout, " %s=%d", kind, summary.Kinds[kind])
	}
	fmt.Fprintln(stdout)
	for _, key := range summary.Rejected {
		fmt.Fprintf(stdout, "rejected %s\n", key)
	}
	return ExitOK
}
func documentImportKind(key string) (string, string) {
	base := path.Base(key)
	parts := strings.Split(key, "/")
	job := ""
	if len(parts) >= 3 && parts[0] == "jobs" {
		job = parts[1]
	}
	switch {
	case base == "brief.md":
		return "brief", job
	case base == "report.md":
		return "report", job
	case strings.HasPrefix(base, "answer-") && strings.HasSuffix(base, ".md"):
		return "answer", job
	default:
		return "note", job
	}
}
func documentGlobMatch(pattern, key string) bool {
	patternParts := strings.Split(strings.TrimPrefix(pattern, "./"), "/")
	keyParts := strings.Split(key, "/")
	var match func(int, int) bool
	match = func(i, j int) bool {
		if i == len(patternParts) {
			return j == len(keyParts)
		}
		if patternParts[i] == "**" {
			return match(i+1, j) || (j < len(keyParts) && match(i, j+1))
		}
		return j < len(keyParts) && func() bool { ok, _ := path.Match(patternParts[i], keyParts[j]); return ok }() && match(i+1, j+1)
	}
	return match(0, 0)
}

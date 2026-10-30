package panewire

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
)

// The job golden files are the frozen record contract for
// done/escalate/joined: the event records, the socket request, the
// handoffkeep call, the OK lines, the exit statuses and the failure logs.
// They were produced once by the retired spawner the command replaced and are
// now maintained by hand; a change that alters any byte must update the
// golden in the same commit.

type jobGoldenCase struct {
	Name string `json:"name"`
	// HostDependent cases shape non-ASCII text through tr/cut, whose
	// semantics differ between BSD and GNU; their golden was produced on
	// darwin and is compared there only.
	HostDependent bool              `json:"host_dependent"`
	Env           map[string]string `json:"env"`
	Handoffkeep   string            `json:"handoffkeep"`  // absent | ok | fail
	Socket        string            `json:"socket"`       // ok | reject | absent
	Files         map[string]string `json:"files"`        // relative path -> content
	FilesBase64   map[string]string `json:"files_base64"` // relative path -> content that is not valid UTF-8
	ReadOnly      []string          `json:"read_only"`    // relative directories made 0555 before the steps
	Steps         [][]string        `json:"steps"`
}

func loadJobGoldenCases(t *testing.T) []jobGoldenCase {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join("testdata", "job_golden", "cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []jobGoldenCase
	if err := json.Unmarshal(contents, &cases); err != nil {
		t.Fatal(err)
	}
	return cases
}

// jobFakeSocket stands in for panewired: it records each request line and
// answers ok, or rejects the way the daemon does.
type jobFakeSocket struct {
	listener net.Listener
	mu       sync.Mutex
	lines    []string
	wg       sync.WaitGroup
}

func startJobFakeSocket(t *testing.T, path, mode string) *jobFakeSocket {
	t.Helper()
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server := &jobFakeSocket{listener: listener}
	server.wg.Add(1)
	go func() {
		defer server.wg.Done()
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			scanner := bufio.NewScanner(connection)
			scanner.Buffer(make([]byte, 1<<20), 1<<20)
			if scanner.Scan() {
				server.mu.Lock()
				server.lines = append(server.lines, scanner.Text())
				server.mu.Unlock()
				if mode == "reject" {
					fmt.Fprintln(connection, `{"ok":false,"code":2,"error":"fixture rejection"}`)
				} else {
					fmt.Fprintln(connection, `{"ok":true,"code":0}`)
				}
			}
			_ = connection.Close()
		}
	}()
	return server
}

func (s *jobFakeSocket) stop() []string {
	_ = s.listener.Close()
	s.wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.lines...)
}

// jobGoldenWorkspace lays a case out under a short root (unix socket paths
// are limited to about 100 bytes) and returns the environment both paths run
// with. Nothing points at the operator's inbox, hub or panes.
func jobGoldenWorkspace(t *testing.T, c jobGoldenCase) (string, map[string]string) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "pwjob")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{}
	for name, content := range c.Files {
		files[name] = []byte(content)
	}
	for name, encoded := range c.FilesBase64 {
		content, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatal(err)
		}
		files[name] = content
	}
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, files[name], 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range c.ReadOnly {
		path := filepath.Join(root, name)
		if err := os.Chmod(path, 0o555); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(path, 0o755) })
	}
	if err := os.MkdirAll(filepath.Join(root, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	handoffkeep := filepath.Join(root, "bin", "absent-handoffkeep")
	if c.Handoffkeep == "ok" || c.Handoffkeep == "fail" {
		handoffkeep = filepath.Join(root, "bin", "handoffkeep")
		rc := "0"
		if c.Handoffkeep == "fail" {
			rc = "1"
		}
		script := fmt.Sprintf("#!/bin/sh\n{ for a in \"$@\"; do printf '%%s\\n' \"$a\"; done; printf -- '--\\n'; } >>%q\nexit %s\n", filepath.Join(root, "handoffkeep.log"), rc)
		if err := os.WriteFile(handoffkeep, []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	env := map[string]string{
		"ARBITER_INBOX_ROOT":  filepath.Join(root, "inbox", "jobs"),
		"HOSTNAME":            "fixture-host",
		"HANDOFFKEEP_BIN":     handoffkeep,
		"PANEWIRE_SOCKET":     filepath.Join(root, "s"),
		"PANEWIRE_INBOX_ROOT": filepath.Join(root, "unused-inbox"),
	}
	for key, value := range c.Env {
		env[key] = value
	}
	return root, env
}

var jobGoldenTimestamp = regexp.MustCompile(`(?m)^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z `)

// jobGoldenDump renders everything a path left behind, with the workspace root
// and wall-clock timestamps replaced so that two runs can be compared.
func jobGoldenDump(t *testing.T, root string, steps []string, socketLines []string) string {
	t.Helper()
	var out strings.Builder
	for _, step := range steps {
		out.WriteString(step)
	}
	var files []string
	_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		relative, _ := filepath.Rel(root, path)
		if relative == "s" || strings.HasPrefix(relative, "bin"+string(filepath.Separator)) {
			return nil
		}
		files = append(files, relative)
		return nil
	})
	sort.Strings(files)
	for _, name := range files {
		info, err := os.Stat(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		contents, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&out, "== file %s mode=%04o\n", name, info.Mode().Perm())
		out.Write(jobGoldenTimestamp.ReplaceAll(contents, []byte("<TS> ")))
		if len(contents) > 0 && contents[len(contents)-1] != '\n' {
			out.WriteString("\n<no newline at end>\n")
		}
	}
	for _, line := range socketLines {
		fmt.Fprintf(&out, "== socket\n%s\n", line)
	}
	return strings.ReplaceAll(out.String(), root, "@ROOT@")
}

var jobGoldenStderrPrefix = regexp.MustCompile(`(?m)^panewire job: `)

func jobGoldenStep(step []string, stdout, stderr []byte, rc int) string {
	errText := jobGoldenStderrPrefix.ReplaceAll(stderr, []byte("<cmd>: "))
	return fmt.Sprintf("== step %q rc=%d\n-- stdout\n%s-- stderr\n%s", step, rc, stdout, errText)
}

// runJobGoldenGo runs a case through `panewire job` in this process.
func runJobGoldenGo(t *testing.T, c jobGoldenCase) string {
	root, env := jobGoldenWorkspace(t, c)
	for key, value := range env {
		t.Setenv(key, value)
	}
	for _, key := range []string{"LC_ALL", "LC_CTYPE", "LANG"} {
		if _, ok := env[key]; !ok {
			t.Setenv(key, os.Getenv(key))
		}
	}
	var server *jobFakeSocket
	if c.Socket != "absent" {
		server = startJobFakeSocket(t, env["PANEWIRE_SOCKET"], c.Socket)
	}
	previous, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(previous) }()
	var steps []string
	for _, step := range c.Steps {
		args := make([]string, len(step))
		for i, arg := range step {
			args[i] = strings.ReplaceAll(arg, "@ROOT@", root)
		}
		var stdout, stderr bytes.Buffer
		rc := runJobCLI(args, &stdout, &stderr, CLIConfig{})
		steps = append(steps, jobGoldenStep(step, stdout.Bytes(), stderr.Bytes(), rc))
	}
	var lines []string
	if server != nil {
		lines = server.stop()
	}
	_ = os.Chdir(previous)
	return jobGoldenDump(t, root, steps, lines)
}

func jobGoldenPath(name string) string {
	return filepath.Join("testdata", "job_golden", name+".golden")
}

// jobGoldenComparable strips stderr: its prefix differs by design and the
// host tools' messages are not part of the record contract. Everything else
// is compared byte for byte.
func jobGoldenComparable(dump string) string {
	var out strings.Builder
	skipping := false
	for _, line := range strings.SplitAfter(dump, "\n") {
		if strings.HasPrefix(line, "-- stderr") {
			skipping = true
			continue
		}
		if strings.HasPrefix(line, "== ") {
			skipping = false
		}
		if !skipping {
			out.WriteString(line)
		}
	}
	return out.String()
}

func TestJobGolden(t *testing.T) {
	cases := loadJobGoldenCases(t)
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			if c.HostDependent && runtime.GOOS != "darwin" {
				t.Skip("non-ASCII shaping follows the host's tr/cut; golden produced on darwin")
			}
			if len(c.ReadOnly) > 0 && os.Geteuid() == 0 {
				t.Skip("root ignores directory permissions, so a read-only case cannot fail as recorded")
			}
			want, err := os.ReadFile(jobGoldenPath(c.Name))
			if err != nil {
				t.Fatal(err)
			}
			got := runJobGoldenGo(t, c)
			if jobGoldenComparable(got) != jobGoldenComparable(string(want)) {
				t.Errorf("panewire job differs from golden %s\n--- golden\n%s\n--- panewire job\n%s", jobGoldenPath(c.Name), want, got)
			}
		})
	}
}

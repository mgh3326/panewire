package agy

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

// The Antigravity app's local quota source, pacelever's agy_local.go: find the
// running language server, read --csrf_token and --extension_server_port from
// its command line, and POST Connect-RPC calls to its loopback port. No OAuth
// token, keychain read or credential write-back is involved, so it is strictly
// safer than the cloud source and is tried first. The CSRF token is sent only
// to 127.0.0.1 URLs built here — never to a host read from the process.
var (
	localNames = []string{"language_server", "language_server_macos", "language_server_macos_arm", "agy"}
	// A bare `agy` argv is the CLI, not the app's server: the match must carry
	// an antigravity marker, never just the binary name.
	localMatch = []*regexp.Regexp{
		regexp.MustCompile(`--app_data_dir.*antigravity`),
		regexp.MustCompile(`antigravity.*--app_data_dir`),
		regexp.MustCompile(`/antigravity/`),
		regexp.MustCompile(`\.antigravity/`),
	}
	localCSRF  = regexp.MustCompile(`--csrf_token[=\s]+([^\s]+)`)
	localPort  = regexp.MustCompile(`--extension_server_port[=\s]+([^\s]+)`)
	listenPort = regexp.MustCompile(`:(\d+)\s+\(LISTEN\)`)
)

var localPaths = []string{
	"/exa.language_server_pb.LanguageServerService/RetrieveUserQuotaSummary",
	"/exa.language_server_pb.LanguageServerService/GetUserStatus",
	"/exa.language_server_pb.LanguageServerService/GetCommandModelConfigs",
}

const (
	localRequestBody = `{"metadata":{"ideName":"antigravity","extensionName":"antigravity","ideVersion":"unknown","locale":"en"}}`
	localTimeout     = 8 * time.Second
	listTimeout      = 10 * time.Second
)

// Proc is a running process: its pid and command line.
type Proc struct {
	PID  int
	Argv string
}

// isServer reports whether a command line is the Antigravity app's server: it
// names one of the known binaries and carries an antigravity marker, so an
// unrelated language_server or an agy-named CLI is skipped.
func isServer(argv string) bool {
	lower := strings.ToLower(strings.ReplaceAll(argv, "\\", "/"))
	named := false
	for _, name := range localNames {
		if strings.Contains(lower, name) {
			named = true
			break
		}
	}
	if !named {
		return false
	}
	for _, pattern := range localMatch {
		if pattern.MatchString(lower) {
			return true
		}
	}
	return false
}

func argvValue(pattern *regexp.Regexp, argv string) string {
	match := pattern.FindStringSubmatch(argv)
	if len(match) < 2 {
		return ""
	}
	return match[1]
}

func extensionPort(argv string) int {
	port, err := strconv.Atoi(argvValue(localPort, argv))
	if err != nil || port < 1 || port > 65535 {
		return 0
	}
	return port
}

// processes lists candidate servers via `pgrep -a -lf`. A missing pgrep or an
// empty table is not an error: the local source simply does not answer.
func processes(ctx context.Context) []Proc {
	ctx, cancel := context.WithTimeout(ctx, listTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "pgrep", "-a", "-lf", strings.Join(localNames, "|")).Output()
	if err != nil {
		return nil
	}
	var procs []Proc
	for _, line := range strings.Split(string(out), "\n") {
		pid, rest, found := strings.Cut(strings.TrimSpace(line), " ")
		n, err := strconv.Atoi(pid)
		if !found || err != nil {
			continue
		}
		procs = append(procs, Proc{PID: n, Argv: strings.TrimSpace(rest)})
	}
	return procs
}

// listenPorts reads the process's listening TCP ports via lsof: the endpoints
// tried over TLS when the plain extension port does not answer.
func listenPorts(ctx context.Context, pid int) []int {
	ctx, cancel := context.WithTimeout(ctx, listTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "lsof", "-nP", "-iTCP", "-sTCP:LISTEN", "-a", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return nil
	}
	seen := map[int]bool{}
	var ports []int
	for _, match := range listenPort.FindAllStringSubmatch(string(out), -1) {
		if port, err := strconv.Atoi(match[1]); err == nil && port > 0 && port <= 65535 && !seen[port] {
			seen[port] = true
			ports = append(ports, port)
		}
	}
	sort.Ints(ports)
	return ports
}

// loopbackClient is used only for the 127.0.0.1 URLs this file builds. The app
// serves a self-signed certificate on its TLS port, so certificate checks are
// skipped for these loopback calls only. Redirects are not followed.
func loopbackClient() Doer {
	return &http.Client{
		Timeout:       localTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport:     &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec // loopback only
	}
}

// local asks the running app for quota. It reports false on any failure — no
// process, no CSRF token, every endpoint refused, an unrecognised answer — so
// the caller always falls back to the cloud source.
func (c Config) local(ctx context.Context) (quota.Observation, bool) {
	list := c.Processes
	if list == nil {
		list = processes
	}
	ports := c.ListenPorts
	if ports == nil {
		ports = listenPorts
	}
	procs := list(ctx)
	sort.Slice(procs, func(i, j int) bool { return procs[i].PID < procs[j].PID })
	for _, proc := range procs {
		if !isServer(proc.Argv) {
			continue
		}
		csrf := argvValue(localCSRF, proc.Argv)
		if csrf == "" {
			continue
		}
		client := c.LocalHTTP
		if client == nil {
			client = loopbackClient()
		}
		type endpoint struct {
			scheme string
			port   int
		}
		var tries []endpoint
		for _, port := range ports(ctx, proc.PID) {
			tries = append(tries, endpoint{"https", port})
		}
		if port := extensionPort(proc.Argv); port > 0 {
			tries = append(tries, endpoint{"http", port})
		}
		headers := map[string]string{
			"Content-Type":             "application/json",
			"X-Codeium-Csrf-Token":     csrf,
			"Connect-Protocol-Version": "1",
		}
		for _, try := range tries {
			for _, path := range localPaths {
				reply, err := doJSON(ctx, client, http.MethodPost, fmt.Sprintf("%s://127.0.0.1:%d%s", try.scheme, try.port, path), headers, []byte(localRequestBody))
				if err != nil || reply.status != http.StatusOK || reply.parseError {
					continue
				}
				if observed, err := parse(localShape(reply.body), nil); err == nil {
					return observed, true
				}
				// The app answered 200 with an unrecognised shape; that is a
				// local failure, not a reason to ask it again.
				return quota.Observation{}, false
			}
		}
	}
	return quota.Observation{}, false
}

// localShape normalizes the two per-model Connect-RPC answers
// (GetUserStatus's userStatus.clientModelConfigs and GetCommandModelConfigs's
// clientModelConfigs) into the models map parse already reads, carrying the
// plan name through the same paidTier field the cloud summary uses.
func localShape(body map[string]any) map[string]any {
	root := body
	if response := object(body["response"]); len(response) > 0 {
		root = response
	}
	if len(list(root["groups"])) > 0 || len(object(root["models"])) > 0 {
		return body
	}
	status := object(root["userStatus"])
	configs := list(object(status["cascadeModelConfigData"])["clientModelConfigs"])
	if len(configs) == 0 {
		configs = list(root["clientModelConfigs"])
	}
	if len(configs) == 0 {
		return body
	}
	models := map[string]any{}
	for _, value := range configs {
		entry := object(value)
		label := text(entry["label"])
		if label == "" {
			continue
		}
		models[label] = map[string]any{"displayName": label, "quotaInfo": entry["quotaInfo"]}
	}
	out := map[string]any{"models": models}
	if name := text(object(object(status["planStatus"])["planInfo"])["planName"]); name != "" {
		out["plan"] = map[string]any{"paidTier": map[string]any{"name": name}}
	}
	return out
}

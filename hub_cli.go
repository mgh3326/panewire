package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// loadHubAuthFile accepts the intentionally small server-side token format:
// HUB_TOKEN_operator plus one HUB_TOKEN_<machine_id> for each node. It shares
// the repository's no-symlink, exact-mode-0600 reader and never reflects a
// token value in an error.
func loadHubAuthFile(path string) (map[string]string, error) {
	values, err := loadMode0600Env(path)
	if err != nil {
		return nil, errors.New("hub auth file must be a regular mode-0600 file")
	}
	tokens := make(map[string]string, len(values))
	for key, token := range values {
		machineID, found := strings.CutPrefix(key, "HUB_TOKEN_")
		if !found || !machineIDPattern.MatchString(machineID) || !validHubToken(token) {
			return nil, errors.New("hub auth file is invalid")
		}
		if _, exists := tokens[machineID]; exists {
			return nil, errors.New("hub auth file is invalid")
		}
		tokens[machineID] = token
	}
	if !validHubToken(tokens[hubOperatorMachineID]) {
		return nil, errors.New("hub auth file is missing the operator token")
	}
	return tokens, nil
}

func hubListenAddress(raw string) (string, error) {
	host, portText, err := net.SplitHostPort(raw)
	if err != nil || host != "127.0.0.1" {
		return "", errors.New("hub listen address must be 127.0.0.1:PORT")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 0 || port > 65535 || strconv.Itoa(port) != portText {
		return "", errors.New("hub listen address must be 127.0.0.1:PORT")
	}
	return net.JoinHostPort(host, portText), nil
}

func newHubServerForCLI(args []string, logger *slog.Logger) (*HubServer, string, int, error) {
	flags := flag.NewFlagSet("panewire hub", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	listen := flags.String("listen", "127.0.0.1:9377", "loopback listen address")
	authPath := flags.String("hub-auth", "", "mode-0600 HUB_TOKEN_<machine_id> file")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return nil, "", ExitUsage, errors.New("invalid hub flags")
	}
	if *authPath == "" {
		return nil, "", ExitUsage, errors.New("hub auth file is required")
	}
	address, err := hubListenAddress(*listen)
	if err != nil {
		return nil, "", ExitConditionInvalid, err
	}
	tokens, err := loadHubAuthFile(*authPath)
	if err != nil {
		return nil, "", ExitConditionInvalid, errors.New("hub auth file is invalid")
	}
	hub, err := NewHubServer(HubServerConfig{Tokens: tokens, Logger: logger})
	if err != nil {
		return nil, "", ExitConditionInvalid, errors.New("hub auth configuration is invalid")
	}
	return hub, address, ExitOK, nil
}

func runHubCLI(args []string) int {
	hub, address, code, err := newHubServerForCLI(args, slog.Default())
	if err != nil {
		fmt.Fprintln(os.Stderr, "hub configuration rejected:", err)
		return code
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Fprintln(os.Stderr, "hub unavailable")
		return ExitInternal
	}
	defer listener.Close()
	server := &http.Server{
		Handler:           hub.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	go hub.RunMaintenance(ctx)
	go func() {
		<-ctx.Done()
		shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownContext)
	}()
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, "hub unavailable")
		return ExitInternal
	}
	return ExitOK
}

type hubTokenEnv struct {
	MachineID string
	Token     string
}

func loadHubTokenEnv(path string) (hubTokenEnv, error) {
	values, err := loadMode0600Env(path)
	if err != nil {
		return hubTokenEnv{}, errors.New("hub token env must be a regular mode-0600 file")
	}
	env := hubTokenEnv{MachineID: values["HUB_MACHINE_ID"], Token: values["HUB_TOKEN"]}
	if !machineIDPattern.MatchString(env.MachineID) || !validHubToken(env.Token) {
		return hubTokenEnv{}, errors.New("hub token env is invalid")
	}
	return env, nil
}

type hubCLIDeps struct {
	HTTPClient            *http.Client
	AllowInsecureForTests bool
}

func buildHubDaemonClient(rawURL, tokenEnvPath string, sentinelEnabled bool, deps daemonCLIDeps) (*HubClient, error) {
	env, err := loadHubTokenEnv(tokenEnvPath)
	if err != nil || env.MachineID == hubOperatorMachineID {
		return nil, errors.New("hub token env must contain a node credential")
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	client, err := NewHubClient(HubClientConfig{
		URL: rawURL, MachineID: env.MachineID, Token: env.Token, SentinelEnabled: sentinelEnabled,
		AllowInsecureForTests: deps.AllowInsecureForTests,
		Warn:                  func(message string) { logger.Warn(message) },
	})
	if err != nil {
		return nil, errors.New("hub client configuration is invalid")
	}
	return client, nil
}

func runHubStatusCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire hub-status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	hubURL := flags.String("hub-url", "", "HTTPS hub base URL")
	tokenEnvPath := flags.String("hub-token-env", "", "mode-0600 HUB_MACHINE_ID/HUB_TOKEN env file")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *hubURL == "" || *tokenEnvPath == "" {
		return ExitUsage
	}
	env, err := loadHubTokenEnv(*tokenEnvPath)
	if err != nil || env.MachineID != hubOperatorMachineID {
		fmt.Fprintln(stderr, "hub-status rejected: invalid operator token env")
		return ExitConditionInvalid
	}
	endpoint, err := hubHTTPSEndpoint(*hubURL, "/v1/nodes", deps.AllowInsecureForTests)
	if err != nil {
		fmt.Fprintln(stderr, "hub-status rejected: invalid hub URL")
		return ExitConditionInvalid
	}
	client := deps.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		fmt.Fprintln(stderr, "hub-status unavailable")
		return ExitInternal
	}
	request.Header.Set(hubAuthorizationHeader, "Bearer "+env.Token)
	response, err := client.Do(request)
	if err != nil {
		fmt.Fprintln(stderr, "hub-status unavailable")
		return ExitInternal
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		fmt.Fprintln(stderr, "hub-status unavailable")
		return ExitInternal
	}
	var body struct {
		Nodes []HubNode `json:"nodes"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&body); err != nil || !validHubStatusNodes(body.Nodes) {
		fmt.Fprintln(stderr, "hub-status unavailable")
		return ExitInternal
	}
	renderHubStatus(stdout, body.Nodes)
	return ExitOK
}

func hubHTTPSEndpoint(raw, endpoint string, allowInsecureForTests bool) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, errors.New("invalid hub URL")
	}
	switch parsed.Scheme {
	case "https", "wss":
		parsed.Scheme = "https"
	case "http", "ws":
		if !allowInsecureForTests {
			return nil, errors.New("invalid hub URL")
		}
		parsed.Scheme = "http"
	default:
		return nil, errors.New("invalid hub URL")
	}
	parsed.Path = endpoint
	return parsed, nil
}

func validHubStatusNodes(nodes []HubNode) bool {
	for _, node := range nodes {
		if !machineIDPattern.MatchString(node.MachineID) || (node.State != "connected" && node.State != "stale" && node.State != "disconnected") || node.LastPingMS < 0 || len(node.RemoteMeta) > 8 {
			return false
		}
		for key, value := range node.RemoteMeta {
			if key == "" || len(key) > 64 || len(value) > 512 {
				return false
			}
		}
	}
	return true
}

func renderHubStatus(writer io.Writer, nodes []HubNode) {
	rows := append([]HubNode(nil), nodes...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].MachineID < rows[j].MachineID })
	fmt.Fprintln(writer, "MACHINE\tSTATE\tCONNECTED_SINCE\tLAST_PING_MS\tREMOTE_META")
	for _, node := range rows {
		meta, _ := json.Marshal(node.RemoteMeta)
		fmt.Fprintf(writer, "%s\t%s\t%s\t%d\t%s\n", node.MachineID, node.State, node.ConnectedSince.UTC().Format(time.RFC3339), node.LastPingMS, meta)
	}
}

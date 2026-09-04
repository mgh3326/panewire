package panewire

import (
	"bytes"
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

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
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
	ip := net.ParseIP(host)
	if err != nil || ip == nil || (!ip.IsLoopback() && !isTailnetIPv4(ip)) {
		return "", errors.New("hub listen address must be loopback or tailnet IP:PORT")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 0 || port > 65535 || strconv.Itoa(port) != portText {
		return "", errors.New("hub listen address must be 127.0.0.1:PORT")
	}
	return net.JoinHostPort(host, portText), nil
}

func isTailnetIPv4(ip net.IP) bool {
	v4 := ip.To4()
	return v4 != nil && v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127
}

type hubListenValues []string

func (values *hubListenValues) String() string { return strings.Join(*values, ",") }
func (values *hubListenValues) Set(raw string) error {
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			return errors.New("hub listen address is required")
		}
		*values = append(*values, item)
	}
	return nil
}

type hubServerCLIDeps struct {
	TelegramHTTPClient    *http.Client
	TelegramBaseURL       string
	AllowInsecureForTests bool
	Now                   func() time.Time
}

func newHubServerForCLI(args []string, logger *slog.Logger) (*HubServer, string, int, error) {
	return newHubServerForCLIWithDeps(args, logger, hubServerCLIDeps{})
}

func newHubServerForCLIWithDeps(args []string, logger *slog.Logger, deps hubServerCLIDeps) (*HubServer, string, int, error) {
	flags := flag.NewFlagSet("panewire hub", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var listens hubListenValues
	flags.Var(&listens, "listen", "repeatable loopback or tailnet listen address")
	authPath := flags.String("hub-auth", "", "mode-0600 HUB_TOKEN_<machine_id> file")
	tgEnvPath := flags.String("hub-tg-env", "", "optional mode-0600 TG_BOT_TOKEN/TG_CHAT_ID env file")
	gracePeriod := flags.Duration("hub-grace", defaultHubGracePeriod, "continuous disconnected/stale grace period")
	alertNodesCSV := flags.String("alert-nodes", "", "comma-separated authenticated machine IDs to alert on")
	burstPolicyPath := flags.String("burst-policy", "", "explicit regular JSON burst policy file (hot-reloaded)")
	placementPolicyPath := flags.String("placement-policy", "/etc/panewire/placement.json", "operator-owned JSON placement policy (hot-reloaded)")
	uiAllowCFOnly := flags.Bool("ui-allow-cf-only", false, "serve /ui only to Cloudflare Access identities or loopback clients")
	reportRelayPath := flags.String("report-relay-routes", "/etc/panewire/report-relay.json", "operator-owned report relay route JSON")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return nil, "", ExitUsage, errors.New("invalid hub flags")
	}
	if *authPath == "" {
		return nil, "", ExitUsage, errors.New("hub auth file is required")
	}
	if len(listens) == 0 {
		listens = append(listens, "127.0.0.1:9377")
	}
	address, err := hubListenAddress(listens[0])
	if err != nil {
		return nil, "", ExitConditionInvalid, err
	}
	tokens, err := loadHubAuthFile(*authPath)
	if err != nil {
		return nil, "", ExitConditionInvalid, errors.New("hub auth file is invalid")
	}
	if *gracePeriod <= 0 {
		return nil, "", ExitConditionInvalid, errors.New("hub grace period must be positive")
	}
	var alertNodes map[string]struct{}
	alertNodesSet := false
	flags.Visit(func(flag *flag.Flag) {
		if flag.Name == "alert-nodes" {
			alertNodesSet = true
		}
	})
	if alertNodesSet {
		alertNodes, err = parseHubAlertNodes(*alertNodesCSV, tokens)
		if err != nil {
			return nil, "", ExitConditionInvalid, errors.New("hub alert nodes are invalid")
		}
	}
	var notifier HubNotifier
	if *tgEnvPath != "" {
		env, err := loadHubTelegramEnv(*tgEnvPath)
		if err != nil {
			return nil, "", ExitConditionInvalid, errors.New("hub Telegram env is invalid")
		}
		notifier, err = newHubTelegramNotifier(env, hubNotifierDeps{
			HTTPClient: deps.TelegramHTTPClient, BaseURL: deps.TelegramBaseURL, AllowInsecureForTests: deps.AllowInsecureForTests,
		})
		if err != nil {
			return nil, "", ExitConditionInvalid, errors.New("hub Telegram configuration is invalid")
		}
	}
	placementPath := *placementPolicyPath
	if _, err := os.Stat(placementPath); os.IsNotExist(err) {
		placementPath = ""
	}
	hub, err := NewHubServer(HubServerConfig{Tokens: tokens, AlertNodes: alertNodes, Now: deps.Now, GracePeriod: *gracePeriod, Notifier: notifier, Logger: logger, BurstPolicyPath: *burstPolicyPath, PlacementPolicyPath: placementPath, PrometheusURL: os.Getenv("PANEWIRE_PROM_URL"), PrometheusBearer: os.Getenv("PANEWIRE_PROM_BEARER"), PrometheusBasicUser: os.Getenv("PANEWIRE_PROM_BASIC_USER"), PrometheusBasicPass: os.Getenv("PANEWIRE_PROM_BASIC_PASS"), UIAllowCFOnly: *uiAllowCFOnly, ReportRelayPath: *reportRelayPath})
	if err != nil {
		return nil, "", ExitConditionInvalid, errors.New("hub auth configuration is invalid")
	}
	return hub, address, ExitOK, nil
}

// hubListenAddresses parses the same flag grammar used to construct the hub.
// It is separate so the historical constructor signature remains stable.
func hubListenAddresses(args []string) ([]string, error) {
	var values hubListenValues
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if arg == "--listen" {
			if index+1 >= len(args) || values.Set(args[index+1]) != nil {
				return nil, errors.New("invalid hub listen")
			}
			index++
		} else if value, found := strings.CutPrefix(arg, "--listen="); found {
			if values.Set(value) != nil {
				return nil, errors.New("invalid hub listen")
			}
		}
	}
	if len(values) == 0 {
		values = append(values, "127.0.0.1:9377")
	}
	seen := make(map[string]struct{}, len(values))
	addresses := make([]string, 0, len(values))
	for _, value := range values {
		address, err := hubListenAddress(value)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[address]; duplicate {
			return nil, errors.New("duplicate hub listen address")
		}
		seen[address] = struct{}{}
		addresses = append(addresses, address)
	}
	return addresses, nil
}

func parseHubAlertNodes(raw string, tokens map[string]string) (map[string]struct{}, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("alert nodes are required when the flag is set")
	}
	alertNodes := make(map[string]struct{})
	for _, item := range strings.Split(raw, ",") {
		machineID := strings.TrimSpace(item)
		if machineID == "" || machineID == hubOperatorMachineID || !machineIDPattern.MatchString(machineID) {
			return nil, errors.New("invalid alert node")
		}
		if _, exists := tokens[machineID]; !exists {
			return nil, errors.New("unknown alert node")
		}
		if _, duplicate := alertNodes[machineID]; duplicate {
			return nil, errors.New("duplicate alert node")
		}
		alertNodes[machineID] = struct{}{}
	}
	return alertNodes, nil
}

func runHubCLI(args []string) int {
	hub, _, code, err := newHubServerForCLI(args, slog.Default())
	if err != nil {
		fmt.Fprintln(os.Stderr, "hub configuration rejected:", err)
		return code
	}
	addresses, err := hubListenAddresses(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "hub configuration rejected:", err)
		return ExitConditionInvalid
	}
	listeners := make([]net.Listener, 0, len(addresses))
	for _, address := range addresses {
		listener, listenErr := net.Listen("tcp", address)
		if listenErr != nil {
			for _, open := range listeners {
				_ = open.Close()
			}
			fmt.Fprintln(os.Stderr, "hub unavailable")
			return ExitInternal
		}
		listeners = append(listeners, listener)
	}
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
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
	errs := make(chan error, len(listeners))
	for _, listener := range listeners {
		go func(listener net.Listener) { errs <- server.Serve(listener) }(listener)
	}
	if serveErr := <-errs; serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, "hub unavailable")
		return ExitInternal
	}
	return ExitOK
}

type hubTokenEnv struct {
	MachineID string
	Token     string
}

type hubCFAccessEnv struct {
	ClientID     string
	ClientSecret string
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

func loadHubCFAccessEnv(path string) (hubCFAccessEnv, error) {
	values, err := loadMode0600Env(path)
	if err != nil {
		return hubCFAccessEnv{}, errors.New("hub Cloudflare Access env must be a regular mode-0600 file")
	}
	env := hubCFAccessEnv{ClientID: values["CF_ACCESS_CLIENT_ID"], ClientSecret: values["CF_ACCESS_CLIENT_SECRET"]}
	if !validHubCFAccessValue(env.ClientID) || !validHubCFAccessValue(env.ClientSecret) {
		return hubCFAccessEnv{}, errors.New("hub Cloudflare Access env is invalid")
	}
	return env, nil
}

type hubCLIDeps struct {
	HTTPClient            *http.Client
	AllowInsecureForTests bool
}

// runHubEmitCLI sends one display-only note as an authenticated node and
// exits. It deliberately does not use HubClient: that sidecar owns a retrying
// heartbeat loop, while this command is an explicit one-shot operation.
func runHubEmitCLI(args []string, _ io.Writer, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire hub-emit", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	hubURL := flags.String("hub-url", "", "WSS hub base URL")
	tokenEnvPath := flags.String("hub-token-env", "", "mode-0600 HUB_MACHINE_ID/HUB_TOKEN env file")
	cfEnvPath := flags.String("hub-cf-env", "", "optional mode-0600 CF_ACCESS_CLIENT_ID/CF_ACCESS_CLIENT_SECRET env file")
	text := flags.String("text", "", "note text")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *hubURL == "" || *tokenEnvPath == "" || !hubEmitTextProvided(flags) {
		return ExitUsage
	}
	if !validHubNoteText(*text) {
		fmt.Fprintln(stderr, "hub-emit rejected: invalid text")
		return ExitConditionInvalid
	}
	env, err := loadHubTokenEnv(*tokenEnvPath)
	if err != nil || env.MachineID == hubOperatorMachineID {
		fmt.Fprintln(stderr, "hub-emit rejected: invalid node token env")
		return ExitConditionInvalid
	}
	var cfAccess hubCFAccessEnv
	if *cfEnvPath != "" {
		cfAccess, err = loadHubCFAccessEnv(*cfEnvPath)
		if err != nil {
			fmt.Fprintln(stderr, "hub-emit rejected: invalid Cloudflare Access env")
			return ExitConditionInvalid
		}
	}
	endpoint, err := hubWSEndpoint(*hubURL, deps.AllowInsecureForTests)
	if err != nil {
		fmt.Fprintln(stderr, "hub-emit rejected: invalid hub URL")
		return ExitConditionInvalid
	}
	headers := make(http.Header)
	headers.Set(hubMachineIDHeader, env.MachineID)
	headers.Set(hubAuthorizationHeader, "Bearer "+env.Token)
	if cfAccess.ClientID != "" {
		headers.Set("CF-Access-Client-Id", cfAccess.ClientID)
		headers.Set("CF-Access-Client-Secret", cfAccess.ClientSecret)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		fmt.Fprintln(stderr, "hub-emit unavailable")
		return ExitInternal
	}
	defer connection.Close(websocket.StatusNormalClosure, "")
	if err := wsjson.Write(ctx, connection, struct {
		Type      string `json:"type"`
		MachineID string `json:"machine_id"`
		Version   string `json:"version"`
		Transient bool   `json:"transient"`
	}{Type: "hello", MachineID: env.MachineID, Version: "panewire-r9", Transient: true}); err != nil {
		fmt.Fprintln(stderr, "hub-emit unavailable")
		return ExitInternal
	}
	payload, err := json.Marshal(hubNotePayload{Text: *text})
	if err != nil {
		fmt.Fprintln(stderr, "hub-emit unavailable")
		return ExitInternal
	}
	if err := wsjson.Write(ctx, connection, hubClientWireEvent(hubClientEvent{Kind: "note", Payload: payload})); err != nil {
		fmt.Fprintln(stderr, "hub-emit unavailable")
		return ExitInternal
	}
	return ExitOK
}

func hubEmitTextProvided(flags *flag.FlagSet) bool {
	provided := false
	flags.Visit(func(item *flag.Flag) {
		if item.Name == "text" {
			provided = true
		}
	})
	return provided
}

func buildHubDaemonClient(rawURL, tokenEnvPath, cfEnvPath string, checks []HubCheck, accepting bool, failoverWakeOn, failoverWakeMAC string, deps daemonCLIDeps) (*HubClient, error) {
	return buildHubDaemonClientWithBurst(rawURL, tokenEnvPath, cfEnvPath, checks, accepting, failoverWakeOn, failoverWakeMAC, "", false, deps)
}

func buildHubDaemonClientWithBurst(rawURL, tokenEnvPath, cfEnvPath string, checks []HubCheck, accepting bool, failoverWakeOn, failoverWakeMAC, burstWakeMAC string, burstPoweroffAllowed bool, deps daemonCLIDeps) (*HubClient, error) {
	env, err := loadHubTokenEnv(tokenEnvPath)
	if err != nil || env.MachineID == hubOperatorMachineID {
		return nil, errors.New("hub token env must contain a node credential")
	}
	var cfAccess hubCFAccessEnv
	if cfEnvPath != "" {
		cfAccess, err = loadHubCFAccessEnv(cfEnvPath)
		if err != nil {
			return nil, errors.New("hub Cloudflare Access env is invalid")
		}
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	client, err := NewHubClient(HubClientConfig{
		URL: rawURL, MachineID: env.MachineID, Token: env.Token, CFAccessClientID: cfAccess.ClientID, CFAccessClientSecret: cfAccess.ClientSecret, Accepting: accepting,
		FailoverWakeOn: failoverWakeOn, FailoverWakeMAC: failoverWakeMAC, BurstWakeMAC: burstWakeMAC, BurstPoweroffAllowed: burstPoweroffAllowed, Checks: checks,
		Version:               deps.Version,
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
	cfEnvPath := flags.String("hub-cf-env", "", "optional mode-0600 CF_ACCESS_CLIENT_ID/CF_ACCESS_CLIENT_SECRET env file")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *hubURL == "" || *tokenEnvPath == "" {
		return ExitUsage
	}
	env, err := loadHubTokenEnv(*tokenEnvPath)
	if err != nil || env.MachineID != hubOperatorMachineID {
		fmt.Fprintln(stderr, "hub-status rejected: invalid operator token env")
		return ExitConditionInvalid
	}
	var cfAccess hubCFAccessEnv
	if *cfEnvPath != "" {
		cfAccess, err = loadHubCFAccessEnv(*cfEnvPath)
		if err != nil {
			fmt.Fprintln(stderr, "hub-status rejected: invalid Cloudflare Access env")
			return ExitConditionInvalid
		}
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
	if cfAccess.ClientID != "" {
		request.Header.Set("CF-Access-Client-Id", cfAccess.ClientID)
		request.Header.Set("CF-Access-Client-Secret", cfAccess.ClientSecret)
	}
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
		if !machineIDPattern.MatchString(node.MachineID) || (node.AlertClass != "watched" && node.AlertClass != "presence-only") || (node.State != "connected" && node.State != "stale" && node.State != "disconnected") || node.LastPingMS < 0 || len(node.RemoteMeta) > 8 {
			return false
		}
		if node.LastNote != nil && (!validHubNoteText(node.LastNote.Text) || node.LastNote.ReceivedAt.IsZero()) {
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
	fmt.Fprintln(writer, "MACHINE\tCLASS\tSTATE\tACCEPTING\tCONNECTED_SINCE\tLAST_PING_MS\tLAST_NOTE\tREMOTE_META")
	for _, node := range rows {
		meta, _ := json.Marshal(node.RemoteMeta)
		fmt.Fprintf(writer, "%s\t%s\t%s\t%t\t%s\t%d\t%s\t%s\n", node.MachineID, node.AlertClass, node.State, node.Accepting, node.ConnectedSince.UTC().Format(time.RFC3339), node.LastPingMS, renderHubLastNote(node.LastNote, time.Now().UTC()), meta)
	}
}

func runJobsCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	if len(args) == 0 || (args[0] != "orphaned" && args[0] != "reassign") {
		return ExitUsage
	}
	flags := flag.NewFlagSet("panewire jobs "+args[0], flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	hubURL := flags.String("hub-url", "", "HTTPS hub base URL")
	tokenEnvPath := flags.String("hub-token-env", "", "mode-0600 operator HUB_MACHINE_ID/HUB_TOKEN env file")
	cfEnvPath := flags.String("hub-cf-env", "", "optional mode-0600 CF_ACCESS_CLIENT_ID/CF_ACCESS_CLIENT_SECRET env file")
	jobID := flags.String("job-id", "", "orphaned job ID")
	to := flags.String("to", "", "destination node identity")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 || *hubURL == "" || *tokenEnvPath == "" || (args[0] == "reassign" && (*jobID == "" || *to == "")) {
		return ExitUsage
	}
	env, err := loadHubTokenEnv(*tokenEnvPath)
	if err != nil || env.MachineID != hubOperatorMachineID {
		fmt.Fprintln(stderr, "jobs rejected: invalid operator token env")
		return ExitConditionInvalid
	}
	var cfAccess hubCFAccessEnv
	if *cfEnvPath != "" {
		cfAccess, err = loadHubCFAccessEnv(*cfEnvPath)
		if err != nil {
			fmt.Fprintln(stderr, "jobs rejected: invalid Cloudflare Access env")
			return ExitConditionInvalid
		}
	}
	path, method := "/v1/jobs/orphaned", http.MethodGet
	var body io.Reader
	if args[0] == "reassign" {
		if !hubJobIDPattern.MatchString(*jobID) || !machineIDPattern.MatchString(*to) || *to == hubOperatorMachineID {
			fmt.Fprintln(stderr, "jobs rejected: invalid reassignment")
			return ExitConditionInvalid
		}
		encoded, _ := json.Marshal(struct {
			JobID string `json:"job_id"`
			To    string `json:"to"`
		}{*jobID, *to})
		path, method, body = "/v1/jobs/reassign", http.MethodPost, bytes.NewReader(encoded)
	}
	endpoint, err := hubHTTPSEndpoint(*hubURL, path, deps.AllowInsecureForTests)
	if err != nil {
		fmt.Fprintln(stderr, "jobs rejected: invalid hub URL")
		return ExitConditionInvalid
	}
	client := deps.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return ExitInternal
	}
	request.Header.Set(hubAuthorizationHeader, "Bearer "+env.Token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if cfAccess.ClientID != "" {
		request.Header.Set("CF-Access-Client-Id", cfAccess.ClientID)
		request.Header.Set("CF-Access-Client-Secret", cfAccess.ClientSecret)
	}
	response, err := client.Do(request)
	if err != nil {
		fmt.Fprintln(stderr, "jobs unavailable")
		return ExitInternal
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		fmt.Fprintln(stderr, "jobs unavailable")
		return ExitConditionInvalid
	}
	if args[0] == "orphaned" {
		var result struct {
			Jobs []hubJobEventPayload `json:"jobs"`
		}
		if json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result) != nil {
			return ExitInternal
		}
		fmt.Fprintln(stdout, "JOB_ID\tNODE\tEPOCH\tLAST_SEEN\tRESUME_HINT")
		for _, job := range result.Jobs {
			fmt.Fprintf(stdout, "%s\t%s\t%d\t%s\t%s\n", job.JobID, job.Node, job.Epoch, job.LastSeen.UTC().Format(time.RFC3339), job.ResumeHint)
		}
		return ExitOK
	}
	var result hubJobEventPayload
	if json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&result) != nil || !hubJobIDPattern.MatchString(result.JobID) || !machineIDPattern.MatchString(result.From) || !machineIDPattern.MatchString(result.To) || result.Epoch == 0 {
		return ExitInternal
	}
	fmt.Fprintf(stdout, "%s\t%s\t%s\t%d\n", result.JobID, result.From, result.To, result.Epoch)
	return ExitOK
}

func renderHubLastNote(note *HubLastNote, now time.Time) string {
	if note == nil {
		return "-"
	}
	age := now.Sub(note.ReceivedAt)
	if age < 0 {
		age = 0
	}
	return fmt.Sprintf("%q (%s)", hubNoteSummary(note.Text), age.Round(time.Second))
}

func hubNoteSummary(text string) string {
	runes := []rune(text)
	if len(runes) <= 60 {
		return text
	}
	return string(runes[:60])
}

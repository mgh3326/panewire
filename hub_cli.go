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
	if err != nil || host != "127.0.0.1" {
		return "", errors.New("hub listen address must be 127.0.0.1:PORT")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 0 || port > 65535 || strconv.Itoa(port) != portText {
		return "", errors.New("hub listen address must be 127.0.0.1:PORT")
	}
	return net.JoinHostPort(host, portText), nil
}

// hubTailnetListenAddress permits only Tailscale's CGNAT range. This prevents
// a typo from turning the context API into a public listener.
func hubTailnetListenAddress(raw string) (string, error) {
	host, portText, err := net.SplitHostPort(raw)
	if err != nil {
		return "", errors.New("hub tailnet listen address must be 100.64.0.0/10:PORT")
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.To4() == nil || ip.To4()[0] != 100 || ip.To4()[1] < 64 || ip.To4()[1] > 127 {
		return "", errors.New("hub tailnet listen address must be 100.64.0.0/10:PORT")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 0 || port > 65535 || strconv.Itoa(port) != portText {
		return "", errors.New("hub tailnet listen address must be 100.64.0.0/10:PORT")
	}
	return net.JoinHostPort(ip.String(), portText), nil
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
	listen := flags.String("listen", "127.0.0.1:9377", "loopback listen address")
	listenTailnet := flags.String("listen-tailnet", "", "optional Tailscale 100.64.0.0/10 listen address")
	authPath := flags.String("hub-auth", "", "mode-0600 HUB_TOKEN_<machine_id> file")
	contextDBURL := flags.String("context-db-url", os.Getenv("PANEWIRE_CONTEXT_DB_URL"), "optional PostgreSQL context database URL (defaults PANEWIRE_CONTEXT_DB_URL)")
	tgEnvPath := flags.String("hub-tg-env", "", "optional mode-0600 TG_BOT_TOKEN/TG_CHAT_ID env file")
	gracePeriod := flags.Duration("hub-grace", defaultHubGracePeriod, "continuous disconnected/stale grace period")
	alertNodesCSV := flags.String("alert-nodes", "", "comma-separated authenticated machine IDs to alert on")
	burstPolicyPath := flags.String("burst-policy", "", "explicit regular JSON burst policy file (hot-reloaded)")
	uiAllowCFOnly := flags.Bool("ui-allow-cf-only", false, "serve /ui only to Cloudflare Access identities or loopback clients")
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
	var tailnetAddress string
	if *listenTailnet != "" {
		tailnetAddress, err = hubTailnetListenAddress(*listenTailnet)
		if err != nil {
			return nil, "", ExitConditionInvalid, err
		}
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
	var contextStore *ContextStore
	if *contextDBURL != "" {
		contextStore, err = OpenContextStore(*contextDBURL)
		if err != nil {
			if errors.Is(err, ErrContextTrigramUnavailable) {
				return nil, "", ExitConditionInvalid, errors.New("context PostgreSQL pg_trgm extension unavailable")
			}
			return nil, "", ExitConditionInvalid, errors.New("context PostgreSQL database is invalid")
		}
	}
	hub, err := NewHubServer(HubServerConfig{Tokens: tokens, ContextStore: contextStore, AlertNodes: alertNodes, Now: deps.Now, GracePeriod: *gracePeriod, Notifier: notifier, Logger: logger, BurstPolicyPath: *burstPolicyPath, UIAllowCFOnly: *uiAllowCFOnly})
	if err != nil {
		if contextStore != nil {
			_ = contextStore.Close()
		}
		return nil, "", ExitConditionInvalid, errors.New("hub auth configuration is invalid")
	}
	hub.tailnetListen = tailnetAddress
	return hub, address, ExitOK, nil
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
	hub, address, code, err := newHubServerForCLI(args, slog.Default())
	if err != nil {
		fmt.Fprintln(os.Stderr, "hub configuration rejected:", err)
		return code
	}
	if hub.contextStore != nil {
		defer hub.contextStore.Close()
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Fprintln(os.Stderr, "hub unavailable")
		return ExitInternal
	}
	defer listener.Close()
	var tailnetListener net.Listener
	if hub.tailnetListen != "" {
		tailnetListener, err = net.Listen("tcp", hub.tailnetListen)
		if err != nil {
			fmt.Fprintln(os.Stderr, "hub unavailable")
			return ExitInternal
		}
		defer tailnetListener.Close()
	}
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
	if tailnetListener != nil {
		go func() { _ = server.Serve(tailnetListener) }()
	}
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
	endpointURL, err := url.Parse(endpoint)
	if err != nil || endpointURL.IsAbs() || endpointURL.Host != "" || !strings.HasPrefix(endpointURL.Path, "/") {
		return nil, errors.New("invalid hub URL")
	}
	parsed.Path = endpointURL.Path
	parsed.RawQuery = endpointURL.RawQuery
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

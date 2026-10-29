package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const inboxUsage = `usage: panewire inbox list [--lane L] [--unread] [--json]
       panewire inbox read <id> [--lane L] [--json]
       panewire inbox ack <id>|--all [--lane L] [--json]
       panewire inbox count [--lane L] [--json]
       panewire inbox wait [--lane L] [--timeout D] [--json]
Connection: --hub-url URL --hub-token-env FILE [--hub-cf-env FILE]
Defaults: PANEWIRE_HUB_URL, PANEWIRE_HUB_TOKEN_ENV, PANEWIRE_HUB_CF_ENV,
          PANEWIRE_LANE. Node credentials require an owned --lane.
wait blocks until an unread item exists, prints it like read, and exits 0;
on timeout it prints nothing and exits 3.
read and ack <id> name one item: an ambient PANEWIRE_LANE does not scope them
for operator tokens — only an explicit --lane does.`

// inboxWaitDefaultTimeout is the client deadline when --timeout is absent.
// Each server request is bounded by hubInboxWaitMaxWait; the client re-issues
// until this deadline, so there is no tight polling.
const inboxWaitDefaultTimeout = 60 * time.Second

func runInboxCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, inboxUsage)
		return ExitUsage
	}
	command := args[0]
	if command == "--help" || command == "-h" {
		fmt.Fprintln(stdout, inboxUsage)
		return ExitOK
	}
	if command != "list" && command != "read" && command != "ack" && command != "count" && command != "wait" {
		return ExitUsage
	}
	lane, hubURL, tokenEnv, cfEnv := os.Getenv("PANEWIRE_LANE"), os.Getenv("PANEWIRE_HUB_URL"), os.Getenv("PANEWIRE_HUB_TOKEN_ENV"), os.Getenv("PANEWIRE_HUB_CF_ENV")
	unread, jsonMode, all := false, false, false
	laneExplicit := false
	timeout := inboxWaitDefaultTimeout
	id := int64(0)
	for i := 1; i < len(args); i++ {
		arg := args[i]
		switch arg {
		case "--json":
			jsonMode = true
		case "--unread":
			unread = true
		case "--all":
			all = true
		case "--lane", "--hub-url", "--hub-token-env", "--hub-cf-env", "--timeout":
			if i+1 == len(args) {
				return ExitUsage
			}
			i++
			v := args[i]
			switch arg {
			case "--lane":
				lane = v
				laneExplicit = true
			case "--hub-url":
				hubURL = v
			case "--hub-token-env":
				tokenEnv = v
			case "--hub-cf-env":
				cfEnv = v
			case "--timeout":
				parsed, err := time.ParseDuration(v)
				if err != nil || parsed <= 0 {
					fmt.Fprintln(stderr, "inbox: invalid --timeout")
					return ExitUsage
				}
				timeout = parsed
			}
		default:
			if id != 0 || strings.HasPrefix(arg, "-") {
				return ExitUsage
			}
			var err error
			id, err = strconv.ParseInt(arg, 10, 64)
			if err != nil || id < 1 {
				return ExitUsage
			}
		}
	}
	if (lane != "" && !validReportRelayLaneName(lane)) || (unread && command != "list") || (all && command != "ack") {
		fmt.Fprintln(stderr, inboxUsage)
		return ExitUsage
	}
	if ((command == "read" || command == "ack") && !all && id == 0) || ((command == "list" || command == "count" || command == "wait" || all) && id != 0) {
		fmt.Fprintln(stderr, inboxUsage)
		return ExitUsage
	}
	if command == "wait" && lane == "" {
		fmt.Fprintln(stderr, inboxUsage)
		return ExitUsage
	}
	if hubURL == "" || tokenEnv == "" {
		fmt.Fprintln(stderr, inboxUsage)
		return ExitUsage
	}
	env, err := loadHubTokenEnv(tokenEnv)
	if err != nil {
		fmt.Fprintln(stderr, "inbox: invalid token env")
		return ExitConditionInvalid
	}
	var cf hubCFAccessEnv
	if cfEnv != "" {
		cf, err = loadHubCFAccessEnv(cfEnv)
		if err != nil {
			fmt.Fprintln(stderr, "inbox: invalid Cloudflare env")
			return ExitConditionInvalid
		}
	}
	clientTimeout := 5 * time.Second
	if command == "wait" {
		// The long-poll holds one request open for up to a full server wait;
		// give the HTTP client the whole CLI deadline plus one server bound.
		clientTimeout = timeout + hubInboxWaitMaxWait + 15*time.Second
	}
	client, err := newHubOperatorClient(hubURL, env.Token, cf, deps, clientTimeout)
	if err != nil {
		fmt.Fprintln(stderr, "inbox: invalid hub URL")
		return ExitConditionInvalid
	}
	if env.MachineID != hubOperatorMachineID {
		client.machineID = env.MachineID
	}
	if command == "wait" {
		return runInboxWait(client, stdout, stderr, lane, timeout, jsonMode)
	}
	// read <id> and ack <id> name one stored item: for an operator token an
	// ambient PANEWIRE_LANE must not hide it behind a lane-scoped 404. The
	// scope still applies when --lane was passed explicitly, and node
	// credentials always keep the ambient default because the hub needs it
	// for authorization.
	itemScoped := command == "read" || (command == "ack" && !all)
	if itemScoped && env.MachineID == hubOperatorMachineID && !laneExplicit {
		lane = ""
	}
	query := url.Values{}
	if lane != "" {
		query.Set("lane", lane)
	}
	if unread {
		query.Set("unread", "true")
	}
	path, method := "/v1/inbox", http.MethodGet
	var body io.Reader
	if command == "count" {
		path += "/count"
	}
	if command == "read" {
		path += "/" + strconv.FormatInt(id, 10) + "/read"
		method = http.MethodPost
	}
	if command == "ack" {
		path += "/ack"
		method = http.MethodPost
		raw, _ := json.Marshal(struct {
			ID   int64  `json:"id"`
			All  bool   `json:"all"`
			Lane string `json:"lane"`
		}{id, all, lane})
		body = bytes.NewReader(raw)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := client.do(ctx, method, path, query, body)
	if err != nil {
		fmt.Fprintln(stderr, "inbox: hub unavailable")
		return ExitInternal
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		fmt.Fprintf(stderr, "inbox: hub returned HTTP %d\n", response.StatusCode)
		return ExitDeliveryFailure
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil || !json.Valid(raw) {
		fmt.Fprintln(stderr, "inbox: invalid hub response")
		return ExitInternal
	}
	if jsonMode {
		_, _ = stdout.Write(raw)
		return ExitOK
	}
	switch command {
	case "count":
		var counts map[string]int
		if json.Unmarshal(raw, &counts) != nil {
			fmt.Fprintln(stderr, "inbox: invalid hub response")
			return ExitInternal
		}
		if lane != "" {
			fmt.Fprintf(stdout, "inbox: %d unread\n", counts[lane])
			break
		}
		lanes := make([]string, 0, len(counts))
		for l := range counts {
			lanes = append(lanes, l)
		}
		sort.Strings(lanes)
		for _, l := range lanes {
			fmt.Fprintf(stdout, "%s inbox: %d unread\n", l, counts[l])
		}
	case "list":
		var items []laneInboxItem
		if json.Unmarshal(raw, &items) != nil {
			fmt.Fprintln(stderr, "inbox: invalid hub response")
			return ExitInternal
		}
		for _, item := range items {
			state := "unread"
			if item.ReadAt > 0 {
				state = "read"
			}
			if item.AckedAt > 0 {
				state = "acked"
			}
			fmt.Fprintf(stdout, "%d\t%s\t%s\t%s\t%s\n", item.ID, item.Lane, item.Class, state, truncateRelayText(strings.Join(strings.Fields(relayTextForKind(item.Kind, item.Event)), " "), 180))
		}
	case "read":
		var item laneInboxItem
		if json.Unmarshal(raw, &item) != nil {
			fmt.Fprintln(stderr, "inbox: invalid hub response")
			return ExitInternal
		}
		fmt.Fprintf(stdout, "#%d %s %s (%s)\n%s\n", item.ID, item.Lane, item.Class, item.Kind, relayTextForKind(item.Kind, item.Event))
	case "ack":
		var result struct {
			Acked int64 `json:"acked"`
		}
		if json.Unmarshal(raw, &result) != nil {
			fmt.Fprintln(stderr, "inbox: invalid hub response")
			return ExitInternal
		}
		fmt.Fprintf(stdout, "acked: %d\n", result.Acked)
	}
	return ExitOK
}

// runInboxWait long-polls /v1/inbox/wait until the lane reports an unread item
// or the CLI deadline runs out. Each request asks the hub to hold for at most
// hubInboxWaitMaxWait, so the loop is a continuation of one long wait rather
// than polling.
func runInboxWait(client *hubOperatorClient, stdout, stderr io.Writer, lane string, timeout time.Duration, jsonMode bool) int {
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return ExitTimeout
		}
		waitFor := remaining
		if waitFor > hubInboxWaitMaxWait {
			waitFor = hubInboxWaitMaxWait
		}
		query := url.Values{"lane": {lane}, "timeout": {waitFor.String()}}
		ctx, cancel := context.WithTimeout(context.Background(), waitFor+10*time.Second)
		response, err := client.do(ctx, http.MethodGet, "/v1/inbox/wait", query, nil)
		if err != nil {
			cancel()
			fmt.Fprintln(stderr, "inbox: hub unavailable")
			return ExitInternal
		}
		raw, readErr := io.ReadAll(io.LimitReader(response.Body, 8<<20))
		response.Body.Close()
		status := response.StatusCode
		cancel()
		if status != 200 {
			fmt.Fprintf(stderr, "inbox: hub returned HTTP %d\n", status)
			return ExitDeliveryFailure
		}
		var body struct {
			Item *laneInboxItem `json:"item"`
		}
		if readErr != nil || !json.Valid(raw) || json.Unmarshal(raw, &body) != nil {
			fmt.Fprintln(stderr, "inbox: invalid hub response")
			return ExitInternal
		}
		if body.Item == nil {
			continue
		}
		if jsonMode {
			encoded, _ := json.Marshal(body.Item)
			_, _ = stdout.Write(append(encoded, '\n'))
			return ExitOK
		}
		item := body.Item
		fmt.Fprintf(stdout, "#%d %s %s (%s)\n%s\n", item.ID, item.Lane, item.Class, item.Kind, relayTextForKind(item.Kind, item.Event))
		return ExitOK
	}
}

package panewire

import (
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

	"github.com/mgh3326/panewire/internal/hubretry"
)

const inboxUsage = `usage: panewire inbox list [--lane L] [--unread] [--json]
       panewire inbox read <id> [--lane L] [--json]
       panewire inbox ack <id>|--all [--lane L] [--json]
       panewire inbox count [--lane L] [--json]
       panewire inbox wait [--lane L] [--timeout D] [--json]
       panewire inbox status --event-id ID [--event-id ID ...] [--json]
Connection: defaults to the local node daemon; use --hub-url URL
            --hub-token-env FILE [--hub-cf-env FILE] for direct access.
Defaults: PANEWIRE_HUB_URL, PANEWIRE_HUB_TOKEN_ENV, PANEWIRE_HUB_CF_ENV,
          PANEWIRE_LANE. Node credentials need an owned --lane, except read <id> and
          ack <id>, which the hub scopes by the item's own lane.
wait blocks until an unread item exists, prints it like read, and exits 0;
on timeout it prints nothing and exits 3.
read and ack <id> name one item: an ambient PANEWIRE_LANE does not scope them
for operator tokens — only an explicit --lane does.
ready, decision, blocked and findings items stay unread until ack <id>:
injection into the pane and read do not retire them.
status answers, for each event id, the lane, the delivery stage its row
reached (queued, held, typed, delivered, unconfirmed, dropped or inbox_only),
the reason for an unconfirmed or dropped stage, the machine and pane, and
whether the event was acked: a node credential sees the events its own node
relayed, a service needs the inbox.ack_state scope. It exits 7 when any id has
no visible row.`

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
	if command != "list" && command != "read" && command != "ack" && command != "count" && command != "wait" && command != "status" {
		return ExitUsage
	}
	lane, hubURL, tokenEnv, cfEnv := os.Getenv("PANEWIRE_LANE"), os.Getenv("PANEWIRE_HUB_URL"), os.Getenv("PANEWIRE_HUB_TOKEN_ENV"), os.Getenv("PANEWIRE_HUB_CF_ENV")
	unread, jsonMode, all := false, false, false
	laneExplicit := false
	connectionExplicit := hubURL != "" || tokenEnv != ""
	timeout := inboxWaitDefaultTimeout
	id := int64(0)
	var eventIDs []string
	for i := 1; i < len(args); i++ {
		arg := args[i]
		switch arg {
		case "--json":
			jsonMode = true
		case "--unread":
			unread = true
		case "--all":
			all = true
		case "--lane", "--hub-url", "--hub-token-env", "--hub-cf-env", "--timeout", "--event-id":
			if i+1 == len(args) {
				return ExitUsage
			}
			i++
			v := args[i]
			switch arg {
			case "--lane":
				lane = v
				laneExplicit = true
			case "--event-id":
				eventIDs = append(eventIDs, v)
			case "--hub-url":
				connectionExplicit = true
				hubURL = v
			case "--hub-token-env":
				connectionExplicit = true
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
	if (command != "status" && len(eventIDs) > 0) || (command == "status" && (len(eventIDs) == 0 || len(eventIDs) > hubInboxAckStateMaxIDs || laneExplicit || id != 0)) {
		fmt.Fprintln(stderr, inboxUsage)
		return ExitUsage
	}
	for _, eventID := range eventIDs {
		if !validLaneEventID(eventID) {
			fmt.Fprintln(stderr, "inbox: invalid --event-id")
			return ExitUsage
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
	if command == "status" {
		// An ambient PANEWIRE_LANE means nothing to status (an explicit --lane
		// was rejected above); the hub scopes the answer by credential.
		lane = ""
	}
	if !connectionExplicit {
		return runInboxLocal(inboxRequest{Command: command, Lane: lane, ID: id, Unread: unread, All: all, JSON: jsonMode, Timeout: timeout, EventIDs: eventIDs}, stdout, stderr, deps.SocketPath)
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
		// Clamp the sum so a huge --timeout cannot overflow into a negative
		// duration.
		const waitSlack = hubInboxWaitMaxWait + 15*time.Second
		clientTimeout = time.Duration(1<<63 - 1)
		if timeout <= time.Duration(1<<63-1)-waitSlack {
			clientTimeout = timeout + waitSlack
		}
	}
	client, err := newHubOperatorClient(hubURL, env.Token, cf, deps, clientTimeout)
	if err != nil {
		fmt.Fprintln(stderr, "inbox: invalid hub URL")
		return ExitConditionInvalid
	}
	if env.MachineID != hubOperatorMachineID {
		client.machineID = env.MachineID
	}
	return executeInbox(client, stdout, stderr, inboxRequest{Command: command, Lane: lane, ID: id, Unread: unread, All: all, JSON: jsonMode, Timeout: timeout, EventIDs: eventIDs}, env.MachineID == hubOperatorMachineID && !laneExplicit, deps.HubRetry)
}

type inboxHTTPClient interface {
	do(context.Context, string, string, url.Values, io.Reader) (*http.Response, error)
}

// executeInbox runs one inbox command over client. Every command is idempotent,
// so each hub call may be sent again under policy (inbox_retry.go); the one
// stderr line that reports the retries is the command's own.
func executeInbox(client inboxHTTPClient, stdout, stderr io.Writer, req inboxRequest, unscopedOperator bool, policy hubretry.Policy) int {
	command, lane, id, unread, all, jsonMode, timeout, eventIDs := req.Command, req.Lane, req.ID, req.Unread, req.All, req.JSON, req.Timeout, req.EventIDs
	if command == "wait" {
		return runInboxWait(client, stdout, stderr, lane, timeout, jsonMode, policy)
	}
	// read <id> and ack <id> name one stored item: for an operator token an
	// ambient PANEWIRE_LANE must not hide it behind a lane-scoped 404. The
	// scope still applies when --lane was passed explicitly, and node
	// credentials always keep the ambient default because the hub needs it
	// for authorization.
	itemScoped := command == "read" || (command == "ack" && !all)
	if itemScoped && unscopedOperator {
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
	var payload []byte
	if command == "count" {
		path += "/count"
	}
	if command == "status" {
		path += "/ack-state"
		// The ambient PANEWIRE_LANE means nothing here: the hub scopes the
		// answer by credential, not by lane.
		query.Del("lane")
		for _, eventID := range eventIDs {
			query.Add("event_id", eventID)
		}
	}
	if command == "read" {
		path += "/" + strconv.FormatInt(id, 10) + "/read"
		method = http.MethodPost
	}
	if command == "ack" {
		path += "/ack"
		method = http.MethodPost
		encoded, _ := json.Marshal(struct {
			ID   int64  `json:"id"`
			All  bool   `json:"all"`
			Lane string `json:"lane"`
		}{id, all, lane})
		payload = encoded
	}
	answer, stats, err := inboxCall{method: method, path: path, query: query, body: payload, timeout: inboxAttemptTimeout}.send(context.Background(), client, policy)
	reportHubRetries(stderr, stats)
	if err != nil {
		fmt.Fprintln(stderr, "inbox: hub unavailable")
		return ExitInternal
	}
	// For status a 404 is an answer, not a failure: it lists the ids with no
	// visible row.
	if answer.status != 200 && !(command == "status" && answer.status == http.StatusNotFound) {
		fmt.Fprintf(stderr, "inbox: hub returned HTTP %d\n", answer.status)
		return ExitDeliveryFailure
	}
	raw := answer.body
	if !json.Valid(raw) {
		fmt.Fprintln(stderr, "inbox: invalid hub response")
		return ExitInternal
	}
	if command == "status" {
		return printInboxStatus(raw, stdout, stderr, jsonMode)
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
			switch {
			case item.SupersededAt > 0:
				state = "superseded"
			case item.AckedAt > 0:
				state = "acked"
			case item.MustAck:
				// Read or injected, it stays unread until acked.
				state = "unacked"
			case item.ReadAt > 0:
				state = "read"
			}
			fmt.Fprintf(stdout, "%d\t%s\t%s\t%s\t%s\n", item.ID, item.Lane, item.Class, state, truncateRelayText(strings.Join(strings.Fields(relayTextForKind(item.Kind, item.Event)), " "), 180))
		}
	case "read":
		var item laneInboxItem
		if json.Unmarshal(raw, &item) != nil {
			fmt.Fprintln(stderr, "inbox: invalid hub response")
			return ExitInternal
		}
		fmt.Fprintf(stdout, "#%d %s %s (%s)\n", item.ID, item.Lane, item.Class, item.Kind)
		if item.SupersededAt > 0 {
			fmt.Fprintf(stdout, "superseded by #%d\n", item.SupersededBy)
		}
		fmt.Fprintln(stdout, relayTextForKind(item.Kind, item.Event))
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
func runInboxWait(client inboxHTTPClient, stdout, stderr io.Writer, lane string, timeout time.Duration, jsonMode bool, policy hubretry.Policy) int {
	deadline := time.Now().Add(timeout)
	var retried hubretry.Stats
	defer func() { reportHubRetries(stderr, retried) }()
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
		// The long poll and its retries both stay inside the CLI deadline: no
		// attempt outlives --timeout, and a retry that would run past it is not
		// sent.
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		answer, stats, err := inboxCall{method: http.MethodGet, path: "/v1/inbox/wait", query: query, timeout: waitFor + 10*time.Second}.send(ctx, client, policy.Within(remaining))
		cancel()
		retried.Add(stats)
		if err != nil {
			if time.Until(deadline) <= 0 {
				return ExitTimeout
			}
			fmt.Fprintln(stderr, "inbox: hub unavailable")
			return ExitInternal
		}
		raw, status := answer.body, answer.status
		if status != 200 {
			fmt.Fprintf(stderr, "inbox: hub returned HTTP %d\n", status)
			return ExitDeliveryFailure
		}
		var body struct {
			Item *laneInboxItem `json:"item"`
		}
		var fields map[string]json.RawMessage
		if !json.Valid(raw) || json.Unmarshal(raw, &body) != nil || json.Unmarshal(raw, &fields) != nil {
			fmt.Fprintln(stderr, "inbox: invalid hub response")
			return ExitInternal
		}
		// A 200 without the item key is not a timeout answer — explicit null
		// means keep waiting, anything else means the hub speaks another shape.
		if _, ok := fields["item"]; !ok {
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
		fmt.Fprintf(stdout, "#%d %s %s (%s)\n", item.ID, item.Lane, item.Class, item.Kind)
		if item.SupersededAt > 0 {
			fmt.Fprintf(stdout, "superseded by #%d\n", item.SupersededBy)
		}
		fmt.Fprintln(stdout, relayTextForKind(item.Kind, item.Event))
		return ExitOK
	}
}

// printInboxStatus prints an ack-state answer and picks the exit code: 0 when
// every id has a row, ExitPartial when any has none. One line per event names
// the lane, the delivery stage that row reached, and whether it was acked.
func printInboxStatus(raw []byte, stdout, stderr io.Writer, jsonMode bool) int {
	var answer struct {
		Events   []laneInboxAckState `json:"events"`
		NotFound []string            `json:"not_found"`
	}
	if json.Unmarshal(raw, &answer) != nil {
		fmt.Fprintln(stderr, "inbox: invalid hub response")
		return ExitInternal
	}
	if jsonMode {
		_, _ = stdout.Write(raw)
	} else {
		for _, event := range answer.Events {
			acked := "no"
			if event.Acked {
				acked = "yes"
			}
			fmt.Fprintf(stdout, "event=%s lane=%s stage=%s reason=%s machine=%s pane=%s acked=%s\n", event.EventID, dash(event.Lane), dash(event.Stage), dash(event.Reason), dash(event.Machine), dash(event.Pane), acked)
		}
		for _, id := range answer.NotFound {
			fmt.Fprintf(stdout, "%s\tnot found\n", id)
		}
	}
	if len(answer.NotFound) > 0 {
		return ExitPartial
	}
	return ExitOK
}

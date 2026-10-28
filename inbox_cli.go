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
       panewire inbox read <id> [--json]
       panewire inbox ack <id>|--all [--lane L] [--json]
       panewire inbox count [--lane L] [--json]
Connection: --hub-url URL --hub-token-env FILE [--hub-cf-env FILE]
Defaults: PANEWIRE_HUB_URL, PANEWIRE_HUB_TOKEN_ENV, PANEWIRE_HUB_CF_ENV,
          PANEWIRE_LANE. Node credentials require an owned --lane.`

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
	if command != "list" && command != "read" && command != "ack" && command != "count" {
		return ExitUsage
	}
	lane, hubURL, tokenEnv, cfEnv := os.Getenv("PANEWIRE_LANE"), os.Getenv("PANEWIRE_HUB_URL"), os.Getenv("PANEWIRE_HUB_TOKEN_ENV"), os.Getenv("PANEWIRE_HUB_CF_ENV")
	unread, jsonMode, all := false, false, false
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
		case "--lane", "--hub-url", "--hub-token-env", "--hub-cf-env":
			if i+1 == len(args) {
				return ExitUsage
			}
			i++
			v := args[i]
			switch arg {
			case "--lane":
				lane = v
			case "--hub-url":
				hubURL = v
			case "--hub-token-env":
				tokenEnv = v
			case "--hub-cf-env":
				cfEnv = v
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
		return ExitUsage
	}
	if ((command == "read" || command == "ack") && !all && id == 0) || ((command == "list" || command == "count" || all) && id != 0) {
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
	client, err := newHubOperatorClient(hubURL, env.Token, cf, deps, 5*time.Second)
	if err != nil {
		fmt.Fprintln(stderr, "inbox: invalid hub URL")
		return ExitConditionInvalid
	}
	if env.MachineID != hubOperatorMachineID {
		client.machineID = env.MachineID
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
			return ExitInternal
		}
		fmt.Fprintf(stdout, "#%d %s %s (%s)\n%s\n", item.ID, item.Lane, item.Class, item.Kind, relayTextForKind(item.Kind, item.Event))
	case "ack":
		var result struct {
			Acked int64 `json:"acked"`
		}
		if json.Unmarshal(raw, &result) != nil {
			return ExitInternal
		}
		fmt.Fprintf(stdout, "acked: %d\n", result.Acked)
	}
	return ExitOK
}

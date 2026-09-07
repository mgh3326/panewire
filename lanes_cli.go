package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

const lanesCLIRequestTimeout = 15 * time.Second

type lanesCLIOptions struct {
	Command  string
	Lane     string
	Machine  string
	Pane     string
	Parent   string
	Sink     bool
	HubURL   string
	TokenEnv string
	CFEnv    string
	seen     map[string]bool
}

func runLanesCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	options, err := parseLanesCLI(args)
	if err != nil {
		return ExitUsage
	}
	if options.HubURL == "" || options.TokenEnv == "" {
		return ExitUsage
	}
	if !laneNamePattern.MatchString(options.Lane) && options.Command != "ls" {
		fmt.Fprintln(stderr, "lanes rejected: invalid lane")
		return ExitConditionInvalid
	}
	if options.Command == "add" {
		if options.Machine == hubOperatorMachineID || !machineIDPattern.MatchString(options.Machine) || !validLanePane(options.Pane) {
			fmt.Fprintln(stderr, "lanes rejected: invalid route")
			return ExitConditionInvalid
		}
		if options.Parent != "" && (!laneNamePattern.MatchString(options.Parent) || options.Parent == options.Lane) {
			fmt.Fprintln(stderr, "lanes rejected: invalid parent")
			return ExitConditionInvalid
		}
	}
	env, err := loadHubTokenEnv(options.TokenEnv)
	if err != nil || env.MachineID != hubOperatorMachineID {
		fmt.Fprintln(stderr, "lanes rejected: invalid operator token env")
		return ExitConditionInvalid
	}
	var cfAccess hubCFAccessEnv
	if options.CFEnv != "" {
		cfAccess, err = loadHubCFAccessEnv(options.CFEnv)
		if err != nil {
			fmt.Fprintln(stderr, "lanes rejected: invalid Cloudflare Access env")
			return ExitConditionInvalid
		}
	}

	path := "/v1/lanes"
	method := http.MethodGet
	var requestBody io.Reader
	if options.Command == "add" {
		path += "/" + options.Lane
		method = http.MethodPut
		encoded, marshalErr := json.Marshal(struct {
			Machine string `json:"machine"`
			Pane    string `json:"pane"`
			Parent  string `json:"parent,omitempty"`
			Sink    bool   `json:"sink,omitempty"`
		}{Machine: options.Machine, Pane: options.Pane, Parent: options.Parent, Sink: options.Sink})
		if marshalErr != nil {
			return ExitInternal
		}
		requestBody = bytes.NewReader(encoded)
	} else if options.Command == "rm" {
		path += "/" + options.Lane
		method = http.MethodDelete
	}
	endpoint, err := hubHTTPSEndpoint(options.HubURL, path, deps.AllowInsecureForTests)
	if err != nil {
		fmt.Fprintln(stderr, "lanes rejected: invalid hub URL")
		return ExitConditionInvalid
	}
	client := deps.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	ctx, cancel := context.WithTimeout(context.Background(), lanesCLIRequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), requestBody)
	if err != nil {
		fmt.Fprintln(stderr, "lanes unavailable")
		return ExitInternal
	}
	request.Header.Set(hubAuthorizationHeader, "Bearer "+env.Token)
	if requestBody != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if cfAccess.ClientID != "" {
		request.Header.Set("CF-Access-Client-Id", cfAccess.ClientID)
		request.Header.Set("CF-Access-Client-Secret", cfAccess.ClientSecret)
	}
	response, err := client.Do(request)
	if err != nil {
		fmt.Fprintln(stderr, "lanes unavailable")
		return ExitInternal
	}
	defer response.Body.Close()

	switch options.Command {
	case "add":
		if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
			return lanesCLIStatus(stderr, response.StatusCode)
		}
		var result hubLaneProjection
		if err := decodeLanesJSON(response.Body, &result); err != nil || !validHubLaneWriteProjection(result) || result.Lane != options.Lane {
			fmt.Fprintln(stderr, "lanes unavailable")
			return ExitInternal
		}
		_ = json.NewEncoder(stdout).Encode(result)
		return ExitOK
	case "rm":
		if response.StatusCode != http.StatusOK {
			return lanesCLIStatus(stderr, response.StatusCode)
		}
		var result hubLaneDeleteResponse
		if err := decodeLanesJSON(response.Body, &result); err != nil || result.Lane != options.Lane || !result.Removed {
			fmt.Fprintln(stderr, "lanes unavailable")
			return ExitInternal
		}
		_ = json.NewEncoder(stdout).Encode(result)
		return ExitOK
	case "ls":
		if response.StatusCode != http.StatusOK {
			return lanesCLIStatus(stderr, response.StatusCode)
		}
		var result struct {
			Lanes []hubLaneProjection `json:"lanes"`
		}
		if err := decodeLanesJSON(response.Body, &result); err != nil {
			fmt.Fprintln(stderr, "lanes unavailable")
			return ExitInternal
		}
		if result.Lanes == nil {
			result.Lanes = []hubLaneProjection{}
		}
		for _, lane := range result.Lanes {
			if !validHubLaneProjection(lane) {
				fmt.Fprintln(stderr, "lanes unavailable")
				return ExitInternal
			}
		}
		sort.Slice(result.Lanes, func(i, j int) bool { return result.Lanes[i].Lane < result.Lanes[j].Lane })
		_ = json.NewEncoder(stdout).Encode(result)
		return ExitOK
	default:
		return ExitUsage
	}
}

func parseLanesCLI(args []string) (lanesCLIOptions, error) {
	if len(args) == 0 {
		return lanesCLIOptions{}, errors.New("lanes command is required")
	}
	options := lanesCLIOptions{Command: args[0], seen: make(map[string]bool)}
	if options.Command != "add" && options.Command != "rm" && options.Command != "ls" {
		return lanesCLIOptions{}, errors.New("unknown lanes command")
	}
	positionals := make([]string, 0, 1)
	for index := 1; index < len(args); index++ {
		argument := args[index]
		if !strings.HasPrefix(argument, "-") {
			positionals = append(positionals, argument)
			continue
		}
		name, value, hasValue := strings.Cut(argument, "=")
		switch name {
		case "--sink":
			if options.Command != "add" || options.seen[name] {
				return lanesCLIOptions{}, errors.New("invalid sink flag")
			}
			options.seen[name] = true
			if !hasValue {
				options.Sink = true
				continue
			}
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return lanesCLIOptions{}, errors.New("invalid sink value")
			}
			options.Sink = parsed
		case "--hub-url", "--hub-token-env", "--hub-cf-env", "--machine", "--pane", "--parent":
			if options.seen[name] {
				return lanesCLIOptions{}, errors.New("duplicate lanes flag")
			}
			options.seen[name] = true
			if !hasValue {
				if index+1 >= len(args) {
					return lanesCLIOptions{}, errors.New("lanes flag value is required")
				}
				index++
				value = args[index]
			}
			switch name {
			case "--hub-url":
				options.HubURL = value
			case "--hub-token-env":
				options.TokenEnv = value
			case "--hub-cf-env":
				options.CFEnv = value
			case "--machine":
				options.Machine = value
			case "--pane":
				options.Pane = value
			case "--parent":
				options.Parent = value
			}
		default:
			return lanesCLIOptions{}, errors.New("unknown lanes flag")
		}
	}
	switch options.Command {
	case "add":
		if len(positionals) != 1 || !options.seen["--machine"] || !options.seen["--pane"] {
			return lanesCLIOptions{}, errors.New("lane and route flags are required")
		}
		options.Lane = positionals[0]
	case "rm":
		if len(positionals) != 1 {
			return lanesCLIOptions{}, errors.New("lane is required")
		}
		options.Lane = positionals[0]
	case "ls":
		if len(positionals) != 0 || options.seen["--machine"] || options.seen["--pane"] || options.seen["--parent"] || options.seen["--sink"] {
			return lanesCLIOptions{}, errors.New("invalid lanes list flags")
		}
	}
	return options, nil
}

func lanesCLIStatus(stderr io.Writer, status int) int {
	if status >= 400 && status < 500 {
		fmt.Fprintln(stderr, "lanes rejected by hub")
		return ExitConditionInvalid
	}
	fmt.Fprintln(stderr, "lanes unavailable")
	return ExitInternal
}

func decodeLanesJSON(reader io.Reader, target any) error {
	decoder := json.NewDecoder(io.LimitReader(reader, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func validHubLaneProjection(lane hubLaneProjection) bool {
	if !laneNamePattern.MatchString(lane.Lane) || lane.Parent != "" && !laneNamePattern.MatchString(lane.Parent) {
		return false
	}
	if lane.Sink {
		return lane.Machine == "" && lane.Pane == ""
	}
	return machineIDPattern.MatchString(lane.Machine) && lane.Machine != hubOperatorMachineID && lane.Pane != "" && len(lane.Pane) <= 128
}

func validHubLaneWriteProjection(lane hubLaneProjection) bool {
	return validHubLaneProjection(lane) && (lane.Sink || validLanePane(lane.Pane))
}

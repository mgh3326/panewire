package panewire

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const consultUsage = `Usage:
  panewire consult request --task N --question hk:doc/KEY
      [--author-job JOB | --author-family F] [--pr REPO#N --head SHA]
      [--requester LANE|TOOL] [--json]
      [` + hubCredentialUsage + `]

Asks the hub for a consult on a task. The hub picks the seat (one grade above
the task, another family than the author's), runs the consult and stores the
answer. The same (task, repo#pr, head) returns the existing consult.
By default the local node identifies the calling pane and its design lane.
For direct hub access provide --hub-url and --hub-token-env (or their
PANEWIRE_HUB_URL and PANEWIRE_HUB_TOKEN_ENV defaults).
See docs/consult-on-request.md.`

type hubConsultCLIResponse struct {
	ID         string `json:"id"`
	State      string `json:"state"`
	Job        string `json:"job"`
	AnswerDoc  string `json:"answer_doc"`
	Reason     string `json:"reason"`
	RequestID  string `json:"request_id"`
	Requester  string `json:"requester"`
	CreatedAt  string `json:"created_at"`
	AnsweredAt string `json:"answered_at"`
	Seat       *struct {
		Profile string `json:"profile"`
		Family  string `json:"family"`
		Grade   string `json:"grade"`
	} `json:"seat"`
}

func runConsultCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	if len(args) == 0 || args[0] != "request" {
		return writeHubCLIUsage(stderr, "consult: expected the request subcommand", consultUsage)
	}
	flags := flag.NewFlagSet("panewire consult request", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	hubURL := flags.String("hub-url", os.Getenv("PANEWIRE_HUB_URL"), "HTTPS hub base URL")
	tokenEnvPath := flags.String("hub-token-env", os.Getenv("PANEWIRE_HUB_TOKEN_ENV"), "mode-0600 HUB_MACHINE_ID/HUB_TOKEN env file")
	cfEnvPath := flags.String("hub-cf-env", os.Getenv("PANEWIRE_HUB_CF_ENV"), "optional mode-0600 CF_ACCESS_CLIENT_ID/CF_ACCESS_CLIENT_SECRET env file")
	task := flags.String("task", "", "task id")
	question := flags.String("question", "", "question document key, hk:doc/KEY")
	authorJob := flags.String("author-job", "", "the author's job id")
	authorFamily := flags.String("author-family", "", "the author's family when the job is not readable here")
	pr := flags.String("pr", "", "owner/repo#N")
	head := flags.String("head", "", "PR head commit id")
	requester := flags.String("requester", "", "requesting lane or tool id")
	asJSON := flags.Bool("json", false, "print the hub's JSON answer")
	if err := flags.Parse(args[1:]); err != nil {
		return writeHubCLIUsage(stderr, "consult request: invalid flag", consultUsage)
	}
	if flags.NArg() != 0 {
		return writeHubCLIUsage(stderr, "consult request: unexpected positional argument", consultUsage)
	}
	if *task == "" || *question == "" {
		return writeHubCLIUsage(stderr, "consult request: --task and --question are required", consultUsage)
	}
	body := hubConsultRequest{Task: *task, QuestionDoc: *question, Requester: *requester, AuthorJob: *authorJob, AuthorFamily: *authorFamily, PR: *pr, Head: *head}
	if *hubURL == "" && *tokenEnvPath == "" {
		return runConsultLocal(body, *asJSON, stdout, stderr, deps.SocketPath)
	}
	if *hubURL == "" || *tokenEnvPath == "" {
		return writeHubCLIUsage(stderr, "consult request: --hub-url and --hub-token-env are required", consultUsage)
	}
	env, err := loadHubTokenEnv(*tokenEnvPath)
	if err != nil {
		fmt.Fprintln(stderr, "consult request rejected: invalid hub token env")
		return ExitConditionInvalid
	}
	var cfAccess hubCFAccessEnv
	if *cfEnvPath != "" {
		if cfAccess, err = loadHubCFAccessEnv(*cfEnvPath); err != nil {
			fmt.Fprintln(stderr, "consult request rejected: invalid Cloudflare Access env")
			return ExitConditionInvalid
		}
	}
	asker := *requester
	if asker == "" {
		asker = os.Getenv("PANEWIRE_LANE")
	}
	if asker == "" {
		asker = env.MachineID
	}
	body.Requester = asker
	client, err := newHubOperatorClient(*hubURL, env.Token, cfAccess, deps, 30*time.Second)
	if err != nil {
		fmt.Fprintln(stderr, "consult request rejected: invalid hub URL")
		return ExitConditionInvalid
	}
	if env.MachineID != hubOperatorMachineID {
		client.machineID = env.MachineID
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return executeConsult(ctx, client, body, *asJSON, stdout, stderr)
}

func executeConsult(ctx context.Context, client inboxHTTPClient, body hubConsultRequest, asJSON bool, stdout, stderr io.Writer) int {
	encoded, _ := json.Marshal(body)
	response, err := client.do(ctx, http.MethodPost, "/v1/consults", url.Values(nil), bytes.NewReader(encoded))
	if err != nil {
		fmt.Fprintln(stderr, "consult request unavailable")
		return ExitInternal
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(response.Body, 1<<16))
	switch response.StatusCode {
	case http.StatusOK, http.StatusCreated:
	default:
		var refusal map[string]string
		_ = json.Unmarshal(raw, &refusal)
		switch response.StatusCode {
		case http.StatusBadRequest:
			fmt.Fprintf(stderr, "consult request refused: %s\n", firstNonEmptyString(refusal["reason"], refusal["error"]))
			return ExitUsage
		case http.StatusUnauthorized, http.StatusForbidden:
			if refusal["reason"] == "multiple lanes name this pane; specify --requester" {
				fmt.Fprintln(stderr, "consult request refused:", refusal["reason"])
				return ExitConditionInvalid
			}
			fmt.Fprintln(stderr, "consult request refused: hub credential is not admitted for consults; ask your design lane by ESC with an hk packet")
			return ExitConditionInvalid
		case http.StatusServiceUnavailable:
			fmt.Fprintf(stderr, "consult request unavailable: %s\n", firstNonEmptyString(refusal["error"], "hub store is unavailable"))
			return ExitInternal
		}
		fmt.Fprintf(stderr, "consult request unavailable: http %d\n", response.StatusCode)
		return ExitInternal
	}
	if asJSON {
		_, _ = stdout.Write(append(bytes.TrimSpace(raw), '\n'))
		return ExitOK
	}
	var view hubConsultCLIResponse
	if json.Unmarshal(raw, &view) != nil {
		fmt.Fprintln(stderr, "consult request unavailable: unreadable hub answer")
		return ExitInternal
	}
	seat := "none"
	if view.Seat != nil && view.Seat.Profile != "" {
		seat = view.Seat.Profile
		if view.Seat.Grade != "" {
			seat += "@" + view.Seat.Grade
		}
	}
	line := fmt.Sprintf("CONSULT id=%s state=%s seat=%s job=%s", view.ID, view.State, seat, firstNonEmptyString(view.Job, "none"))
	if view.AnswerDoc != "" {
		line += " answer_doc=" + view.AnswerDoc
	}
	if view.Reason != "" {
		line += " reason=" + strings.ReplaceAll(view.Reason, "\n", " ")
	}
	fmt.Fprintln(stdout, line)
	return ExitOK
}

func runConsultLocal(req hubConsultRequest, asJSON bool, stdout, stderr io.Writer, socket string) int {
	if socket == "" {
		socket = socketPathFromEnv()
	}
	conn, err := net.DialTimeout("unix", socket, 5*time.Second)
	if err != nil {
		fmt.Fprintln(stderr, "consult request: daemon unavailable")
		return ExitDaemonUnavailable
	}
	defer conn.Close()
	timer := time.AfterFunc(35*time.Second, func() { _ = conn.Close() })
	defer timer.Stop()
	if json.NewEncoder(conn).Encode(localRequest{Op: "consult.request", Consult: &localConsultRequest{Request: req, JSON: asJSON, Session: os.Getenv("HERDR_SESSION")}, TimeoutMS: 30000}) != nil {
		fmt.Fprintln(stderr, "consult request: daemon unavailable")
		return ExitDaemonUnavailable
	}
	scan := bufio.NewScanner(conn)
	if !scan.Scan() {
		fmt.Fprintln(stderr, "consult request: daemon unavailable")
		return ExitDaemonUnavailable
	}
	var resp struct {
		OK     bool            `json:"ok"`
		Code   int             `json:"code"`
		Error  string          `json:"error"`
		Result localTextResult `json:"result"`
	}
	if json.Unmarshal(scan.Bytes(), &resp) != nil {
		fmt.Fprintln(stderr, "consult request: invalid daemon response")
		return ExitInternal
	}
	if !resp.OK {
		fmt.Fprintln(stderr, resp.Error)
		return resp.Code
	}
	io.WriteString(stdout, resp.Result.Stdout)
	io.WriteString(stderr, resp.Result.Stderr)
	return resp.Result.Code
}

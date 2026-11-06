package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// `panewire pr status` and `panewire pr findings` read a PR's state from
// mergewake's last gate decision and handoffkeep's change record through the
// pr.status node op (docs/pr-status.md), so a worker spends no GitHub request
// and holds no GitHub token. gh is the fallback only when neither source
// answered, and the output then says so and why.
const prUsage = `Usage:
  panewire pr status   <owner/repo> <n> [--fresh] [--json] [connection flags]
  panewire pr findings <owner/repo> <n> [--json] [connection flags]
Connection flags (defaults from the environment):
  --hub-url (PANEWIRE_HUB_URL) --hub-token-env (PANEWIRE_HUB_TOKEN_ENV, a mode-0600 env file path)
  --hub-cf-env (PANEWIRE_HUB_CF_ENV) --machine (PANEWIRE_PR_STATUS_HOST, default oci)`

const (
	prStatusGHTimeout   = 30 * time.Second
	prStatusGHMaxOutput = 1 << 20
	prSourcePanewire    = "panewire"
	prSourceGH          = "gh"
)

// prStatusGHExecutable is the gh binary the fallback runs; tests point it at a
// fake so they never reach GitHub.
var prStatusGHExecutable = "gh"

// prStatusView is what one invocation learned, from either transport. Notes
// are the human sentences for every source that was missing or fell back.
type prStatusView struct {
	Source    string
	Repo      string
	PR        int
	Node      *nodePRStatusResult
	GH        json.RawMessage
	GHThreads []PRStatusFinding
	Fallback  string
	Notes     []string
}

func runPRCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Fprintln(stdout, prUsage)
		return ExitOK
	}
	if len(args) == 0 || (args[0] != "status" && args[0] != "findings") {
		fmt.Fprintln(stderr, prUsage)
		return ExitUsage
	}
	command := args[0]
	flags := flag.NewFlagSet("panewire pr "+command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	connect := nodeReapFlags(flags)
	connect.allowOperator = true
	*connect.hubURL, *connect.tokenPath, *connect.cfPath = os.Getenv("PANEWIRE_HUB_URL"), os.Getenv("PANEWIRE_HUB_TOKEN_ENV"), os.Getenv("PANEWIRE_HUB_CF_ENV")
	*connect.machine = os.Getenv("PANEWIRE_PR_STATUS_HOST")
	if *connect.machine == "" {
		*connect.machine = defaultPRStatusHost
	}
	asJSON := flags.Bool("json", false, "print the merged view as JSON")
	fresh := new(bool)
	if command == "status" {
		fresh = flags.Bool("fresh", false, "add one GitHub read of head, checks and threads (spends one request)")
	}
	// Flags may follow the positionals: parse, take one positional, repeat.
	var positional []string
	rest := args[1:]
	for {
		if flags.Parse(rest) != nil {
			fmt.Fprintln(stderr, prUsage)
			return ExitUsage
		}
		if flags.NArg() == 0 {
			break
		}
		positional = append(positional, flags.Arg(0))
		rest = flags.Args()[1:]
	}
	if len(positional) != 2 {
		fmt.Fprintln(stderr, prUsage)
		return ExitUsage
	}
	number, err := strconv.Atoi(positional[1])
	repo := positional[0]
	if err != nil || strconv.Itoa(number) != positional[1] || !validNodePRStatusArgs(nodePRStatusArgs{Repo: repo, PR: number}) || !machineIDPattern.MatchString(*connect.machine) || *connect.machine == hubOperatorMachineID {
		fmt.Fprintln(stderr, prUsage)
		return ExitUsage
	}
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	view := collectPRStatus(connect, stderr, deps, nodePRStatusArgs{Repo: repo, PR: number, Fresh: *fresh}, command == "findings")
	if view.Source == "" {
		return ExitDaemonUnavailable
	}
	for _, note := range view.Notes {
		fmt.Fprintln(stderr, "note: "+note)
	}
	if command == "findings" {
		return printPRFindings(stdout, stderr, view, *asJSON)
	}
	return printPRStatus(stdout, view, *asJSON, now())
}

// collectPRStatus asks the node op, then decides whether gh is needed. A view
// with no Source means nothing answered at all and the reason is on stderr.
func collectPRStatus(connect nodeReapConnect, stderr io.Writer, deps hubCLIDeps, args nodePRStatusArgs, needFindings bool) prStatusView {
	view := prStatusView{Repo: args.Repo, PR: args.PR}
	result, why := askPRStatusNode(connect, stderr, deps, args)
	if result != nil {
		view.Node = result
		view.Notes = prStatusSourceNotes(*result)
		mergewakeOK := result.Mergewake.Outcome == prStatusOK
		hkOK := result.HK.Outcome == prStatusOK
		if mergewakeOK || (hkOK && !needFindings) {
			view.Source = prSourcePanewire
			return view
		}
		if hkOK {
			view.Notes = append(view.Notes, "findings need the mergewake snapshot, and it is missing; gh was not used because handoffkeep answered")
			view.Source = prSourcePanewire
			return view
		}
		why = "mergewake " + prStatusDescribeMergewake(result.Mergewake) + ", handoffkeep " + prStatusDescribeHK(result.HK)
	}
	view.Notes = nil
	if err := fallbackPRStatusGH(&view, args, needFindings, why); err != nil {
		fmt.Fprintln(stderr, "pr: no source answered ("+why+") and gh failed: "+err.Error())
		return prStatusView{}
	}
	return view
}

// askPRStatusNode returns the node's answer, or nil with the reason it could
// not be asked. No reason ever carries a credential or a response body.
func askPRStatusNode(connect nodeReapConnect, stderr io.Writer, deps hubCLIDeps, args nodePRStatusArgs) (*nodePRStatusResult, string) {
	if *connect.hubURL == "" || *connect.tokenPath == "" {
		return nil, "hub connection is not configured (--hub-url and --hub-token-env)"
	}
	client, _ := connect.open(io.Discard, deps, "pr")
	if client == nil {
		return nil, "hub connection is invalid"
	}
	ctx, cancel := context.WithTimeout(context.Background(), nodeReapClientTimeout)
	defer cancel()
	var result nodePRStatusResult
	err := client.command(ctx, *connect.machine, nodeCommandPRStatus, args, &result)
	var hubErr *NodeReapError
	switch {
	case errors.Is(err, ErrNodeReapRefused):
		return nil, "hub refused the credential for pr.status on " + *connect.machine
	case errors.As(err, &hubErr):
		return nil, "hub answered " + strconv.Itoa(hubErr.Status) + " " + hubErr.Code
	case err != nil:
		return nil, "hub unreachable"
	case !validNodePRStatusResult(result):
		return nil, "hub answered an unusable pr.status result"
	}
	return &result, ""
}

func prStatusDescribeMergewake(source nodePRStatusMergewake) string {
	switch source.Outcome {
	case prStatusNotFound:
		return "has no snapshot for this PR"
	case prStatusOK:
		return "answered"
	}
	return source.Outcome + " (" + source.Reason + ")"
}

func prStatusDescribeHK(source nodePRStatusHK) string {
	switch source.Outcome {
	case prStatusNotFound:
		return "answered 404"
	case prStatusUnconfigured:
		return "is not configured on that node"
	case prStatusTooLarge:
		return "record is too large"
	case prStatusFailed:
		return "failed (" + source.Reason + ")"
	}
	return source.Outcome
}

// prStatusSourceNotes says, for a source that is missing, that it was omitted
// and why; the command still succeeds on the other source.
func prStatusSourceNotes(result nodePRStatusResult) []string {
	var notes []string
	if result.Mergewake.Outcome != prStatusOK {
		notes = append(notes, "mergewake snapshot omitted: mergewake "+prStatusDescribeMergewake(result.Mergewake))
	}
	switch result.HK.Outcome {
	case prStatusOK:
	case prStatusNotFound:
		notes = append(notes, "hk change record omitted: /v1/changes answered 404 (handoffkeep may not have it deployed)")
	default:
		notes = append(notes, "hk change record omitted: handoffkeep "+prStatusDescribeHK(result.HK))
	}
	if snapshot := result.Mergewake.Snapshot; snapshot != nil {
		if snapshot.FindingsOmitted > 0 {
			notes = append(notes, fmt.Sprintf("%d more findings did not fit the node reply; read them with gh", snapshot.FindingsOmitted))
		}
		if snapshot.FreshError != "" {
			notes = append(notes, "fresh read failed: "+oneLine(snapshot.FreshError))
		}
	}
	return notes
}

// runPRStatusGH runs gh with a bounded output and returns stdout only. gh
// authenticates itself; nothing is added to its argv or environment here.
func runPRStatusGH(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), prStatusGHTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, prStatusGHExecutable, args...)
	cmd.WaitDelay = 100 * time.Millisecond
	stdout := &prStatusLimitedBuffer{max: prStatusGHMaxOutput}
	cmd.Stdout = stdout
	if err := cmd.Run(); err != nil {
		return nil, errors.New("gh exited with an error")
	}
	if stdout.overflow {
		return nil, errors.New("gh output too large")
	}
	return stdout.buf.Bytes(), nil
}

const prStatusGHThreadsQuery = `query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){reviewThreads(first:100){nodes{id isResolved path line comments(first:1){totalCount nodes{body url}}}}}}}`

func fallbackPRStatusGH(view *prStatusView, args nodePRStatusArgs, needFindings bool, why string) error {
	view.Source = prSourceGH
	view.Fallback = why
	view.Notes = []string{"fell back to gh: " + why}
	if needFindings {
		owner, name, _ := strings.Cut(args.Repo, "/")
		out, err := runPRStatusGH("api", "graphql", "-f", "query="+prStatusGHThreadsQuery, "-f", "owner="+owner, "-f", "name="+name, "-F", "number="+strconv.Itoa(args.PR))
		if err != nil {
			return err
		}
		var body struct {
			Data struct {
				Repository struct {
					PullRequest struct {
						ReviewThreads struct {
							Nodes []struct {
								ID         string `json:"id"`
								IsResolved bool   `json:"isResolved"`
								Path       string `json:"path"`
								Line       *int   `json:"line"`
								Comments   struct {
									TotalCount int `json:"totalCount"`
									Nodes      []struct {
										Body string `json:"body"`
										URL  string `json:"url"`
									} `json:"nodes"`
								} `json:"comments"`
							} `json:"nodes"`
						} `json:"reviewThreads"`
					} `json:"pullRequest"`
				} `json:"repository"`
			} `json:"data"`
		}
		if json.Unmarshal(out, &body) != nil {
			return errors.New("gh answered unreadable review threads")
		}
		view.GHThreads = []PRStatusFinding{}
		for _, thread := range body.Data.Repository.PullRequest.ReviewThreads.Nodes {
			if thread.IsResolved {
				continue
			}
			location := thread.Path
			if thread.Line != nil {
				location += ":" + strconv.Itoa(*thread.Line)
			}
			finding := PRStatusFinding{ID: thread.ID, Location: location, AuthorReplied: thread.Comments.TotalCount > 1}
			if len(thread.Comments.Nodes) > 0 {
				finding.Summary = firstLine(thread.Comments.Nodes[0].Body)
				finding.URL = thread.Comments.Nodes[0].URL
			}
			view.GHThreads = append(view.GHThreads, finding)
		}
		return nil
	}
	out, err := runPRStatusGH("pr", "view", strconv.Itoa(args.PR), "--repo", args.Repo, "--json", "state,headRefOid,isDraft,mergeable,statusCheckRollup")
	if err != nil {
		return err
	}
	if !json.Valid(out) {
		return errors.New("gh answered unreadable pull request JSON")
	}
	view.GH = json.RawMessage(bytes.TrimSpace(out))
	return nil
}

func firstLine(text string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(text), "\n")
	return utf8PrefixWithin(line, prStatusMaxSummary)
}

// oneLine makes a field safe to print in a line-oriented listing: control
// characters, tabs and newlines become single spaces.
func oneLine(text string) string {
	return strings.Join(strings.FieldsFunc(text, func(r rune) bool { return unicode.IsControl(r) || unicode.IsSpace(r) }), " ")
}

func shortHead(head string) string {
	return utf8PrefixWithin(oneLine(head), 7)
}

func prObservedAge(observed string, now time.Time) string {
	at, err := time.Parse(time.RFC3339, observed)
	if err != nil {
		return "unknown"
	}
	age := now.Sub(at)
	switch {
	case age < time.Minute:
		return "0s ago"
	case age < time.Hour:
		return strconv.Itoa(int(age/time.Minute)) + "m ago"
	case age < 48*time.Hour:
		return strconv.Itoa(int(age/time.Hour)) + "h ago"
	}
	return strconv.Itoa(int(age/(24*time.Hour))) + "d ago"
}

// prChecksSummary is "G/T green" plus a count for every other state, in name
// order, so the line is stable.
func prChecksSummary(checks []PRStatusCheck) string {
	counts := map[string]int{}
	for _, check := range checks {
		counts[oneLine(check.State)]++
	}
	out := fmt.Sprintf("%d/%d green", counts["green"], len(checks))
	var others []string
	for state, count := range counts {
		if state != "green" {
			others = append(others, state+":"+strconv.Itoa(count))
		}
	}
	sort.Strings(others)
	if len(others) > 0 {
		out += " " + strings.Join(others, " ")
	}
	return out
}

// ghCheckState maps a statusCheckRollup entry onto mergewake's vocabulary.
func ghCheckState(conclusion, state string) string {
	switch strings.ToUpper(firstNonEmptyString(conclusion, state)) {
	case "SUCCESS":
		return "green"
	case "FAILURE", "ERROR", "TIMED_OUT", "STARTUP_FAILURE", "ACTION_REQUIRED":
		return "red"
	case "CANCELLED":
		return "cancelled"
	case "SKIPPED", "NEUTRAL":
		return "skipped"
	}
	return "pending"
}

func printPRStatus(stdout io.Writer, view prStatusView, asJSON bool, now time.Time) int {
	if asJSON {
		return writePRJSON(stdout, prMergedView(view))
	}
	head := fmt.Sprintf("%s#%d", view.Repo, view.PR)
	switch {
	case view.Source == prSourceGH:
		var gh struct {
			State  string `json:"state"`
			Head   string `json:"headRefOid"`
			Rollup []struct {
				Conclusion string `json:"conclusion"`
				State      string `json:"state"`
			} `json:"statusCheckRollup"`
		}
		if json.Unmarshal(view.GH, &gh) != nil {
			fmt.Fprintf(stdout, "%s unreadable source=gh (fallback: %s)\n", head, view.Fallback)
			return ExitInternal
		}
		checks := make([]PRStatusCheck, 0, len(gh.Rollup))
		for _, entry := range gh.Rollup {
			checks = append(checks, PRStatusCheck{State: ghCheckState(entry.Conclusion, entry.State)})
		}
		fmt.Fprintf(stdout, "%s %s head=%s blocking=? checks=%s observed=now source=gh (fallback: %s)\n", head, strings.ToLower(oneLine(gh.State)), shortHead(gh.Head), prChecksSummary(checks), oneLine(view.Fallback))
	case view.Node.Mergewake.Snapshot != nil:
		snapshot := view.Node.Mergewake.Snapshot
		line := fmt.Sprintf("%s %s head=%s blocking=%d checks=%s observed=%s", head, oneLine(snapshot.State), shortHead(snapshot.Head), snapshot.Threads.Blocking, prChecksSummary(snapshot.Checks), prObservedAge(snapshot.ObservedAt, now))
		if snapshot.GateHead != "" && snapshot.GateHead != snapshot.Head {
			line += " gate_head=" + shortHead(snapshot.GateHead)
		}
		if len(snapshot.Reasons) > 0 {
			line += " reasons=" + oneLine(strings.Join(snapshot.Reasons, ","))
		}
		if len(snapshot.Fresh) > 0 && string(snapshot.Fresh) != "null" {
			line += " fresh=" + oneLine(string(snapshot.Fresh))
		}
		fmt.Fprintln(stdout, line)
	default:
		fmt.Fprintf(stdout, "%s no mergewake snapshot; handoffkeep change record only (use --json)\n", head)
	}
	return ExitOK
}

func printPRFindings(stdout, stderr io.Writer, view prStatusView, asJSON bool) int {
	findings, omitted, observed := prViewFindings(view)
	if view.Source == prSourcePanewire && (view.Node == nil || view.Node.Mergewake.Snapshot == nil) {
		return ExitDaemonUnavailable
	}
	if asJSON {
		return writePRJSON(stdout, map[string]any{"source": view.Source, "repo": view.Repo, "pr": view.PR, "head": prViewHead(view), "observed_at": observed, "findings": findings, "findings_omitted": omitted, "fallback": view.Fallback, "notes": prNotes(view)})
	}
	if len(findings) == 0 {
		fmt.Fprintln(stderr, "note: no open findings")
		return ExitOK
	}
	for _, finding := range findings {
		blocking := "nonblocking"
		if finding.Blocking {
			blocking = "blocking"
		}
		if view.Source == prSourceGH {
			blocking = "?"
		}
		replied := "unreplied"
		if finding.AuthorReplied {
			replied = "replied"
		}
		severity := oneLine(finding.Severity)
		if severity == "" {
			severity = "-"
		}
		fmt.Fprintf(stdout, "%s\t%s\t%s\t%s\t%s\t%s\n", severity, blocking, oneLine(finding.Location), oneLine(finding.Summary), replied, oneLine(finding.URL))
	}
	return ExitOK
}

func prViewFindings(view prStatusView) (findings []PRStatusFinding, omitted int, observed string) {
	if view.Source == prSourceGH {
		return append([]PRStatusFinding{}, view.GHThreads...), 0, ""
	}
	if view.Node != nil && view.Node.Mergewake.Snapshot != nil {
		snapshot := view.Node.Mergewake.Snapshot
		return append([]PRStatusFinding{}, snapshot.Findings...), snapshot.FindingsOmitted, snapshot.ObservedAt
	}
	return []PRStatusFinding{}, 0, ""
}

func prViewHead(view prStatusView) string {
	if view.Node != nil && view.Node.Mergewake.Snapshot != nil {
		return view.Node.Mergewake.Snapshot.Head
	}
	return ""
}

func prNotes(view prStatusView) []string {
	if view.Notes == nil {
		return []string{}
	}
	return view.Notes
}

// prMergedView is the --json document: mergewake's snapshot and hk's change
// record when present, null when not, plus the notes that explain any gap.
func prMergedView(view prStatusView) map[string]any {
	merged := map[string]any{"source": view.Source, "repo": view.Repo, "pr": view.PR, "mergewake": nil, "hk": nil, "gh": nil, "fallback": view.Fallback, "notes": prNotes(view)}
	if view.Node != nil {
		if view.Node.Mergewake.Snapshot != nil {
			merged["mergewake"] = view.Node.Mergewake.Snapshot
		}
		if view.Node.HK.Outcome == prStatusOK {
			merged["hk"] = view.Node.HK.Change
		}
	}
	if view.GH != nil {
		merged["gh"] = view.GH
	}
	return merged
}

func writePRJSON(stdout io.Writer, value any) int {
	encoded, err := marshalNodeCommandJSON(value)
	if err != nil {
		return ExitInternal
	}
	if _, err := stdout.Write(append(encoded, '\n')); err != nil {
		return ExitInternal
	}
	return ExitOK
}

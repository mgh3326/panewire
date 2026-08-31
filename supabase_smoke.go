package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mgh3326/panewire/stage2/adapters/supabase"
	"github.com/mgh3326/panewire/stage2/core"
)

type smokeDeps struct {
	HTTPClient            *http.Client
	AllowInsecureForTests bool
}

type smokeClientEnv struct {
	URL            string
	MachineID      string
	AccessToken    string
	PublishableKey string
}

type smokeStep struct {
	Name   string
	Result string
	Detail string
}

func runSmokeSupabaseCLI(args []string, stdout, stderr io.Writer, deps smokeDeps) int {
	fs := flag.NewFlagSet("panewire smoke-supabase", flag.ContinueOnError)
	fs.SetOutput(stderr)
	clientEnvAPath := fs.String("client-env-a", "", "explicit mode-0600 client A credential env path")
	clientEnvBPath := fs.String("client-env-b", "", "explicit mode-0600 client B credential env path")
	inboxRoot := fs.String("inbox-root", "", "explicit temporary inbox root")
	confirm := fs.Bool("confirm", false, "perform the live Supabase smoke procedure")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if *clientEnvAPath == "" || *clientEnvBPath == "" || *inboxRoot == "" {
		return ExitUsage
	}
	if !*confirm {
		fmt.Fprintf(stdout, "DRY-RUN smoke plan: use client env A at %s and client env B at %s; create a temporary private staging/inbox tree below %s; verify publish→claim→materialize→ack→completion, RLS claim denial, publishable-key zero-row select, and ack body erasure. No credentials were read and no network call was made.\n", *clientEnvAPath, *clientEnvBPath, *inboxRoot)
		return ExitOK
	}

	steps := make([]smokeStep, 0, 8)
	defer func() { renderSmokeSteps(stdout, steps) }()
	a, err := loadSmokeClientEnv(*clientEnvAPath)
	if err != nil {
		steps = append(steps, smokeFail("load client A", "invalid mode-0600 client credentials"))
		return ExitConditionInvalid
	}
	b, err := loadSmokeClientEnv(*clientEnvBPath)
	if err != nil {
		steps = append(steps, smokeFail("load client B", "invalid mode-0600 client credentials"))
		return ExitConditionInvalid
	}
	if a.URL != b.URL || a.MachineID == b.MachineID {
		steps = append(steps, smokeFail("identity preflight", "A/B must be distinct identities for the same project"))
		return ExitConditionInvalid
	}
	if a.PublishableKey == "" || b.PublishableKey == "" {
		steps = append(steps, smokeFail("identity preflight", "PANEWIRE_SUPABASE_PUBLISHABLE_KEY is required before live smoke"))
		return ExitConditionInvalid
	}
	aAdapter, err := newSmokeAdapter(a, deps)
	if err != nil {
		steps = append(steps, smokeFail("client A adapter", "invalid Supabase endpoint"))
		return ExitConditionInvalid
	}
	bAdapter, err := newSmokeAdapter(b, deps)
	if err != nil {
		steps = append(steps, smokeFail("client B adapter", "invalid Supabase endpoint"))
		return ExitConditionInvalid
	}

	sessionRoot, err := prepareSmokeRoot(*inboxRoot)
	if err != nil {
		steps = append(steps, smokeFail("private staging setup", "temporary inbox root is unavailable"))
		return ExitInternal
	}
	defer os.RemoveAll(sessionRoot)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	aStore, err := core.OpenMetadataStore(filepath.Join(sessionRoot, "a.sqlite3"))
	if err != nil {
		steps = append(steps, smokeFail("A metadata setup", "metadata store unavailable"))
		return ExitInternal
	}
	defer aStore.Close()
	bStore, err := core.OpenMetadataStore(filepath.Join(sessionRoot, "b.sqlite3"))
	if err != nil {
		steps = append(steps, smokeFail("B metadata setup", "metadata store unavailable"))
		return ExitInternal
	}
	defer bStore.Close()

	body := []byte("# Panewire stage 2 smoke\n\nfixture-safe transport round trip\n")
	source := filepath.Join(sessionRoot, "a-source.md")
	if err := os.WriteFile(source, body, 0600); err != nil {
		steps = append(steps, smokeFail("A source setup", "source file unavailable"))
		return ExitInternal
	}
	aSender := &core.Sender{Store: aStore, Transport: aAdapter}
	record, err := aSender.Submit(ctx, core.Submission{
		SourcePath:     source,
		Source:         core.Identity{MachineID: a.MachineID},
		Destination:    core.Destination{MachineID: b.MachineID, InboxNamespace: "smoke", LogicalPath: "roundtrip/request.md"},
		Expect:         core.Expectation{MachineID: b.MachineID},
		Classification: core.ClassificationPublic,
	})
	if err != nil {
		steps = append(steps, smokeFail("A submit", "metadata submission rejected"))
		return ExitInternal
	}
	if err := aSender.PublishOne(ctx, record.DeliveryID); err != nil {
		steps = append(steps, smokeFail("A publish", "transport publish failed"))
		return ExitInternal
	}
	steps = append(steps, smokePass("A publish", "delivery metadata accepted"))

	// The explicit foreign destination turns a missing RLS check into an
	// observable permission error rather than a harmless empty queue.
	foreignClaimErr := aAdapter.Receive(ctx, core.Destination{MachineID: b.MachineID}, func(context.Context, core.Delivery) error { return nil })
	if foreignClaimErr == nil {
		steps = append(steps, smokeFail("RLS: A claim of B destination", "foreign claim was not rejected"))
		return ExitInternal
	}
	steps = append(steps, smokePass("RLS: A claim of B destination", "rejected"))

	rows, err := unauthenticatedQueueRowCount(ctx, b, deps)
	if err != nil || rows != 0 {
		steps = append(steps, smokeFail("RLS: publishable-key select", "unauthenticated select did not return zero rows"))
		return ExitInternal
	}
	steps = append(steps, smokePass("RLS: publishable-key select", "0 rows"))

	bInbox := filepath.Join(sessionRoot, "b-inbox")
	bStage := filepath.Join(sessionRoot, "b-private")
	bReceiver, err := core.NewReceiver(core.ReceiverConfig{
		MachineID:  b.MachineID,
		Namespaces: map[string]string{"smoke": filepath.Join(bInbox, "smoke")},
		InboxRoot:  bInbox, StagingRoot: bStage, Store: bStore, Transport: bAdapter,
	})
	if err != nil {
		steps = append(steps, smokeFail("B receiver setup", "private staging setup failed"))
		return ExitInternal
	}
	if err := bReceiver.PollOnce(ctx); err != nil {
		steps = append(steps, smokeFail("B claim → materialize → ack", "receiver poll failed"))
		return ExitInternal
	}
	materialized := filepath.Join(bInbox, "smoke", "roundtrip", "request.md")
	if !smokeFileMatches(materialized, body) {
		steps = append(steps, smokeFail("B claim → materialize → ack", "logical-path materialization failed"))
		return ExitInternal
	}
	steps = append(steps, smokePass("B claim → private stage → materialize → ack", "logical path materialized"))

	status, err := bAdapter.MessageStatus(ctx, record.DeliveryID)
	if err != nil || !status.BodyErased || (status.State != "acked" && status.State != "rejected") {
		steps = append(steps, smokeFail("ack body erasure", "transport body was not confirmed erased"))
		return ExitInternal
	}
	steps = append(steps, smokePass("ack body erasure", "transport metadata retained; body erased"))

	completion := []byte("# Panewire stage 2 smoke completion\n")
	completionSource := filepath.Join(sessionRoot, "b-completion.md")
	if err := os.WriteFile(completionSource, completion, 0600); err != nil {
		steps = append(steps, smokeFail("B completion setup", "completion source unavailable"))
		return ExitInternal
	}
	bSender := &core.Sender{Store: bStore, Transport: bAdapter}
	completionRecord, err := bSender.Submit(ctx, core.Submission{
		SourcePath:     completionSource,
		Source:         core.Identity{MachineID: b.MachineID},
		Destination:    core.Destination{MachineID: a.MachineID, InboxNamespace: "completion", LogicalPath: "roundtrip/completion.md"},
		Expect:         core.Expectation{MachineID: a.MachineID},
		Classification: core.ClassificationPublic,
		MessageKind:    core.MessageKindCompletion,
		CorrelationID:  record.MessageID,
		CausationID:    "smoke-complete",
	})
	if err != nil || bSender.PublishOne(ctx, completionRecord.DeliveryID) != nil {
		steps = append(steps, smokeFail("B completion publish", "completion transport publish failed"))
		return ExitInternal
	}
	aInbox := filepath.Join(sessionRoot, "a-inbox")
	aReceiver, err := core.NewReceiver(core.ReceiverConfig{
		MachineID:  a.MachineID,
		Namespaces: map[string]string{"completion": filepath.Join(aInbox, "completion")},
		InboxRoot:  aInbox, StagingRoot: filepath.Join(sessionRoot, "a-private"), Store: aStore, Transport: aAdapter,
	})
	if err != nil || aReceiver.PollOnce(ctx) != nil {
		steps = append(steps, smokeFail("A completion receive", "completion receiver failed"))
		return ExitInternal
	}
	stored, found, err := aStore.OutboxByDelivery(ctx, record.DeliveryID)
	if err != nil || !found || stored.State != core.OutboxCompleted || !smokeFileMatches(filepath.Join(aInbox, "completion", "roundtrip", "completion.md"), completion) {
		steps = append(steps, smokeFail("A completion receive", "completion was not terminally recorded"))
		return ExitInternal
	}
	steps = append(steps, smokePass("B completion publish → A completion receive", "completion recorded"))
	return ExitOK
}

func smokePass(name, detail string) smokeStep {
	return smokeStep{Name: name, Result: "PASS", Detail: detail}
}
func smokeFail(name, detail string) smokeStep {
	return smokeStep{Name: name, Result: "FAIL", Detail: detail}
}

func renderSmokeSteps(out io.Writer, steps []smokeStep) {
	if len(steps) == 0 {
		return
	}
	fmt.Fprintln(out, "STEP\tRESULT\tDETAIL")
	for _, step := range steps {
		fmt.Fprintf(out, "%s\t%s\t%s\n", step.Name, step.Result, step.Detail)
	}
}

func loadSmokeClientEnv(path string) (smokeClientEnv, error) {
	values, err := loadMode0600Env(path)
	if err != nil {
		return smokeClientEnv{}, err
	}
	result := smokeClientEnv{
		URL: values["PANEWIRE_SUPABASE_URL"], MachineID: values["PANEWIRE_MACHINE_ID"],
		AccessToken: values["PANEWIRE_SUPABASE_ACCESS_TOKEN"], PublishableKey: values["PANEWIRE_SUPABASE_PUBLISHABLE_KEY"],
	}
	if result.URL == "" || result.AccessToken == "" || !machineIDPattern.MatchString(result.MachineID) {
		return smokeClientEnv{}, errors.New("required client credentials missing")
	}
	return result, nil
}

func newSmokeAdapter(env smokeClientEnv, deps smokeDeps) (*supabase.Adapter, error) {
	return supabase.New(supabase.Config{
		BaseURL: env.URL, AccessToken: env.AccessToken, APIKey: env.PublishableKey,
		HTTPClient: deps.HTTPClient, AllowInsecureForTests: deps.AllowInsecureForTests,
	})
}

func prepareSmokeRoot(root string) (string, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(abs, 0700); err != nil {
		return "", err
	}
	return os.MkdirTemp(abs, "panewire-smoke-")
}

func smokeFileMatches(path string, want []byte) bool {
	got, err := os.ReadFile(path)
	if err != nil || len(got) != len(want) {
		return false
	}
	gotSum, wantSum := sha256.Sum256(got), sha256.Sum256(want)
	return hex.EncodeToString(gotSum[:]) == hex.EncodeToString(wantSum[:])
}

func urlForSmoke(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return nil, errors.New("invalid Supabase URL")
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/")
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed, nil
}

func unauthenticatedQueueRowCount(ctx context.Context, env smokeClientEnv, deps smokeDeps) (int, error) {
	u, err := urlForSmoke(env.URL)
	if err != nil {
		return 0, err
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/rest/v1/message_queue"
	u.RawQuery = "select=delivery_id&limit=1"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Profile", "panewire")
	req.Header.Set("apikey", env.PublishableKey)
	client := deps.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 0, errors.New("unauthenticated queue select failed")
	}
	var rows []json.RawMessage
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&rows); err != nil {
		return 0, err
	}
	return len(rows), nil
}

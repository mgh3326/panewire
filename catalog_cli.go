package panewire

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mgh3326/panewire/internal/fleet"
)

const catalogUsage = `usage:
  panewire catalog list [--json]
  panewire catalog push FILE [--dry-run]
  panewire catalog aa-sync [--out FILE] [--dry-run]`

// runCatalogCLI is the catalog entry point. The catalog, its grades and
// Artificial Analysis annotations are handoffkeep's; no subcommand runs
// pacelever (hk 3249).
func runCatalogCLI(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, catalogUsage)
		return ExitUsage
	}
	switch args[0] {
	case "list":
		return runCatalogList(args[1:], stdout, stderr)
	case "push":
		return runCatalogPush(args[1:], stdout, stderr)
	case "aa-sync":
		return runAASync(args[1:], stdout, stderr)
	}
	fmt.Fprintln(stderr, catalogUsage)
	return ExitUsage
}

func newCatalogClient(stderr io.Writer) (*fleet.CatalogClient, int) {
	client, err := fleet.NewCatalogClient(fleet.CatalogClientConfig{})
	if err != nil {
		fmt.Fprintf(stderr, "catalog: %v\n", err)
		return nil, ExitConditionInvalid
	}
	return client, ExitOK
}

func writeCatalogJSON(out io.Writer, value any) int {
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return ExitInternal
	}
	return ExitOK
}

func runCatalogList(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("panewire catalog list", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	asJSON := flags.Bool("json", false, "JSON output")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		fmt.Fprintln(stderr, catalogUsage)
		return ExitUsage
	}
	client, code := newCatalogClient(stderr)
	if client == nil {
		return code
	}
	data, err := client.Read(context.Background(), false)
	if err != nil {
		fmt.Fprintf(stderr, "catalog list: %v\n", err)
		return ExitConditionInvalid
	}
	for _, warning := range data.Warnings {
		fmt.Fprintf(stderr, "warning: %s\n", warning)
	}
	if *asJSON {
		return writeCatalogJSON(stdout, map[string]any{"catalog": fleet.CatalogWire(data.Rows), "source": data.Info})
	}
	for _, row := range data.Rows {
		fmt.Fprintf(stdout, "%-3s %s@%s model=%s pool=%s gate=%s%s\n", row.Grade, row.Profile, row.Effort, row.ModelID, row.Pool, row.Gate, benchmarkText(row))
	}
	fmt.Fprintf(stdout, "catalog=%s stale=%t\n", data.Info.Source, data.Info.Stale)
	return ExitOK
}

func runCatalogPush(args []string, stdout, stderr io.Writer) int {
	var path string
	dryRun := false
	for _, arg := range args {
		switch {
		case arg == "--dry-run":
			dryRun = true
		case strings.HasPrefix(arg, "-") || path != "":
			fmt.Fprintln(stderr, catalogUsage)
			return ExitUsage
		default:
			path = arg
		}
	}
	if path == "" {
		fmt.Fprintln(stderr, catalogUsage)
		return ExitUsage
	}
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintln(stderr, "cannot read catalog file")
		return ExitConditionInvalid
	}
	var payload struct {
		Catalog []fleet.CatalogEntry `json:"catalog"`
	}
	if json.Unmarshal(data, &payload) != nil {
		fmt.Fprintln(stderr, "invalid catalog JSON")
		return ExitConditionInvalid
	}
	rows := payload.Catalog
	if len(rows) == 0 {
		fmt.Fprintln(stderr, "non-empty catalog required")
		return ExitConditionInvalid
	}
	if err := fleet.ValidateCatalog(rows); err != nil {
		fmt.Fprintln(stderr, err)
		return ExitConditionInvalid
	}
	if dryRun {
		return writeCatalogJSON(stdout, map[string]any{"catalog": fleet.CatalogWire(rows)})
	}
	client, code := newCatalogClient(stderr)
	if client == nil {
		return code
	}
	if err := client.PushCatalog(context.Background(), rows); err != nil {
		fmt.Fprintf(stderr, "catalog push: %v\n", err)
		return ExitConditionInvalid
	}
	return ExitOK
}

type benchmarkAnnotation struct {
	IntelligenceIndex     *float64 `json:"intelligence_index"`
	CodingIndex           *float64 `json:"coding_index"`
	Price1MInputTokens    *float64 `json:"price_1m_input_tokens"`
	Price1MOutputTokens   *float64 `json:"price_1m_output_tokens"`
	OutputTokensPerSecond *float64 `json:"median_output_tokens_per_second"`
	AAName                string   `json:"aa_name"`
	FetchedAt             string   `json:"fetched_at"`
}

func benchmarkText(row fleet.CatalogEntry) string {
	if row.BenchmarkSource == "" && row.BenchmarkAnnotation == "" {
		return ""
	}
	var annotation benchmarkAnnotation
	if json.Unmarshal([]byte(row.BenchmarkAnnotation), &annotation) != nil {
		return fmt.Sprintf(" benchmark_source=%s benchmark_annotation=%q", row.BenchmarkSource, row.BenchmarkAnnotation)
	}
	metric := func(n *float64) string {
		if n == nil {
			return "n/a"
		}
		return fmt.Sprintf("%g", *n)
	}
	return fmt.Sprintf(" benchmark_source=%s aa_name=%q intelligence_index=%s coding_index=%s price_1m_input_tokens=%s price_1m_output_tokens=%s output_tok_s=%s fetched_at=%s", row.BenchmarkSource, annotation.AAName, metric(annotation.IntelligenceIndex), metric(annotation.CodingIndex), metric(annotation.Price1MInputTokens), metric(annotation.Price1MOutputTokens), metric(annotation.OutputTokensPerSecond), annotation.FetchedAt)
}

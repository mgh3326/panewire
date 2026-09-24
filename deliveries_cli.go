package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const deliveriesUsage = `usage: panewire deliveries show <delivery-id or unique prefix> [--db PATH]
       panewire deliveries list [--to TARGET] [--limit N] [--db PATH]
  Reads the prompt delivery audit (one row per panewire prompt) read-only.
  --db defaults to the daemon's database under ~/Library/Application Support/panewire.`

// runDeliveriesCLI reads the deliveries table that panewire prompt writes, so
// a delivery can be traced after the fact (#646 AC4): which pane it resolved
// to, what the submission was judged to be and by which rule. It opens the
// database read-only, so it is safe beside a running daemon.
func runDeliveriesCLI(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || (args[0] != "show" && args[0] != "list") {
		fmt.Fprintln(stderr, deliveriesUsage)
		return ExitUsage
	}
	sub, rest := args[0], args[1:]
	id := ""
	if sub == "show" && len(rest) > 0 && !strings.HasPrefix(rest[0], "-") {
		id, rest = rest[0], rest[1:]
	}
	fs := flag.NewFlagSet("panewire deliveries "+sub, flag.ContinueOnError)
	fs.SetOutput(stderr)
	db := fs.String("db", "", "SQLite path (default: the daemon's)")
	to := fs.String("to", "", "list only deliveries to this target (as given to --to, or its pane id)")
	limit := fs.Int("limit", 20, "list at most this many, newest first")
	if fs.Parse(rest) != nil {
		fmt.Fprintln(stderr, deliveriesUsage)
		return ExitUsage
	}
	if sub == "show" && id == "" && fs.NArg() == 1 {
		id = fs.Arg(0)
	} else if fs.NArg() != 0 {
		fmt.Fprintln(stderr, deliveriesUsage)
		return ExitUsage
	}
	if (sub == "show" && id == "") || *limit <= 0 {
		fmt.Fprintln(stderr, deliveriesUsage)
		return ExitUsage
	}
	path := *db
	if path == "" {
		home, _ := os.UserHomeDir()
		path = filepath.Join(home, "Library", "Application Support", "panewire", "panewire.sqlite3")
	}
	store, err := openStoreReadOnly(path)
	if err != nil {
		fmt.Fprintf(stderr, "deliveries unavailable: %v\n", err)
		return ExitDaemonUnavailable
	}
	defer store.Close()
	ctx := context.Background()
	if sub == "list" {
		return listDeliveries(ctx, store, *to, *limit, stdout, stderr)
	}
	var ids []string
	rows, err := store.db.QueryContext(ctx, `SELECT delivery_id FROM deliveries WHERE substr(delivery_id,1,?)=? LIMIT 2`, len(id), id)
	if err == nil {
		ids, err = scanStrings(rows)
	}
	if err != nil {
		fmt.Fprintf(stderr, "deliveries unavailable: %v\n", err)
		return ExitInternal
	}
	if len(ids) != 1 {
		fmt.Fprintf(stderr, "delivery %q: %d matches (a prefix must name exactly one)\n", id, len(ids))
		return ExitConditionInvalid
	}
	d, _, err := store.GetDelivery(ctx, ids[0])
	if err != nil {
		fmt.Fprintf(stderr, "deliveries unavailable: %v\n", err)
		return ExitInternal
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(deliveryView(d))
	return ExitOK
}

func listDeliveries(ctx context.Context, store *Store, to string, limit int, stdout, stderr io.Writer) int {
	rows, err := store.db.QueryContext(ctx, `SELECT delivery_id FROM deliveries WHERE ?='' OR target_input=? OR resolved_pane_id=? ORDER BY requested_at_ms DESC LIMIT ?`, to, to, to, limit)
	var ids []string
	if err == nil {
		ids, err = scanStrings(rows)
	}
	if err != nil {
		fmt.Fprintf(stderr, "deliveries unavailable: %v\n", err)
		return ExitInternal
	}
	fmt.Fprintln(stdout, "DELIVERY\tREQUESTED_AT\tSENDER\tTARGET\tPANE\tSUBMISSION\tEVIDENCE\tUPTAKE\tERROR")
	for _, id := range ids {
		d, ok, err := store.GetDelivery(ctx, id)
		if err != nil || !ok {
			continue
		}
		uptake := d.UptakeResult
		if d.UptakeMode != "" {
			uptake = d.UptakeMode + ":" + d.UptakeResult
		}
		fmt.Fprintf(stdout, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", shortDeliveryID(d.DeliveryID), formatDeliveryMS(d.RequestedAtMS), d.Sender, d.TargetInput, d.ResolvedPaneID,
			dash(d.SubmissionResult), dash(d.SubmissionEvidence), dash(uptake), dash(d.ErrorCode))
	}
	return ExitOK
}

// deliveryView is the show output: the row with readable times and the
// derived elapsed time, in the column names the table uses.
func deliveryView(d Delivery) map[string]any {
	view := map[string]any{
		"delivery_id": d.DeliveryID, "sender": d.Sender, "target_input": d.TargetInput,
		"resolved_pane_id": d.ResolvedPaneID, "resolved_workspace_id": d.ResolvedWorkspaceID, "source_path": d.SourcePath,
		"prompt_sha256": d.PromptSHA256, "body_stored": d.BodyStored,
		"requested_at": formatDeliveryMS(d.RequestedAtMS), "completed_at": formatDeliveryMS(d.CompletedAtMS),
		"preflight_result": d.PreflightResult, "preflight_revision": d.PreflightRevision, "preflight_read_sha256": d.PreflightReadSHA256,
		"send_revision": d.SendRevision, "herdr_acceptance": d.HerdrAcceptance,
		"submission_result": d.SubmissionResult, "submission_evidence": d.SubmissionEvidence, "evidence_revision": d.EvidenceRevision,
		"uptake_mode": d.UptakeMode, "uptake_result": d.UptakeResult, "error_code": d.ErrorCode, "error_detail": d.ErrorDetail,
	}
	if d.RequestedAtMS > 0 && d.CompletedAtMS >= d.RequestedAtMS {
		view["elapsed_ms"] = d.CompletedAtMS - d.RequestedAtMS
	}
	return view
}

// openStoreReadOnly opens an existing panewire database without creating,
// migrating, or writing it.
func openStoreReadOnly(path string) (*Store, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db, path: path}, nil
}

func scanStrings(rows *sql.Rows) ([]string, error) {
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func shortDeliveryID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

func formatDeliveryMS(ms int64) string {
	if ms <= 0 {
		return "-"
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339Nano)
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

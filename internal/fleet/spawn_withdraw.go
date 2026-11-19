package fleet

// `panewire spawn withdraw REQUEST_ID --reason TEXT` (hk 5429): the requesting
// lane ends its own pending request, cancelled. The hub checks that the lane is
// the submitter of the request and applies the operator abandon's safety
// (docs/hub-spawn-operations.md, "Withdrawing a request"). The CLI asks the hub
// for spawn_withdraw_v1 first, so an older hub is named as the cause.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"
)

// spawnWithdrawFeature is the feature a hub advertises for the withdraw route.
const spawnWithdrawFeature = "spawn_withdraw_v1"

// spawnWithdrawUsage is the usage line, printed by `spawn --help` and
// `spawn withdraw --help`.
const spawnWithdrawUsage = "Usage: panewire spawn withdraw REQUEST_ID --reason TEXT [--requested-by LANE] (the requesting lane ends its own pending request as cancelled; LANE defaults to PANEWIRE_LANE and must be the lane that submitted the request; exit 0 withdrawn, 2 usage or not the requester, 75 the hub kept it or it had already ended; docs/hub-spawn-operations.md)"

// SpawnWithdrawal is the hub's answer to a withdrawal that ended the request.
type SpawnWithdrawal struct {
	Request string
	Job     string
	Status  string
	Reason  string
}

// spawnWithdrawer is a hub transport that can withdraw a pending request.
type spawnWithdrawer interface {
	WithdrawSpawn(ctx context.Context, id, requester, reason string) (SpawnWithdrawal, error)
}

// withdrawReasonOK is one non-empty printable line of at most 256 bytes, the
// same bound the hub applies (validSpawnAbandonReason).
func withdrawReasonOK(reason string) bool {
	if reason == "" || len(reason) > 256 {
		return false
	}
	for _, r := range reason {
		if r < ' ' || r == 0x7f {
			return false
		}
	}
	return true
}

// spawnWithdrawCommand reports whether spawn's arguments name the withdraw
// subcommand.
func spawnWithdrawCommand(args []string) bool {
	return len(args) > 0 && args[0] == "withdraw"
}

// spawnWithdrawCmd is `panewire spawn withdraw REQUEST_ID --reason TEXT`.
func (a *App) spawnWithdrawCmd(ctx context.Context, args []string) error {
	defer a.reportHubRetries()
	if len(args) > 0 && (args[0] == "--help" || args[0] == "-h") {
		fmt.Fprintln(a.Out, spawnWithdrawUsage)
		return nil
	}
	// The id is refused before the hub config is read: a mistyped argument is
	// a usage error whether or not this host has a credential.
	if len(args) == 0 || !validID(args[0]) {
		return fail(exitUsage, "spawn withdraw needs one valid REQUEST_ID (usage: panewire spawn withdraw REQUEST_ID --reason TEXT [--requested-by LANE])")
	}
	id := args[0]
	f := flags("spawn withdraw")
	reason := f.String("reason", "", "one line: why the request is withdrawn")
	requester := f.String("requested-by", a.envOr("PANEWIRE_LANE", "WRK_LANE"), "the lane that submitted the request (default PANEWIRE_LANE)")
	if err := f.Parse(args[1:]); err != nil {
		return fail(exitUsage, "spawn withdraw: %v (usage: panewire spawn withdraw REQUEST_ID --reason TEXT [--requested-by LANE])", err)
	}
	if f.NArg() != 0 {
		return fail(exitUsage, "unexpected spawn withdraw arguments (usage: panewire spawn withdraw REQUEST_ID --reason TEXT [--requested-by LANE])")
	}
	if !withdrawReasonOK(*reason) {
		return fail(exitUsage, "spawn withdraw needs --reason TEXT of at most 256 bytes on one line")
	}
	if *requester == "" {
		return fail(exitUsage, "spawn withdraw needs the requesting lane: --requested-by LANE or PANEWIRE_LANE")
	}
	hub, err := a.hub()
	if err != nil {
		return err
	}
	withdrawer, ok := hub.(spawnWithdrawer)
	if !ok {
		return fail(exitConfig, "hub transport cannot withdraw a spawn; configure a current panewire hub")
	}
	got, err := withdrawer.WithdrawSpawn(ctx, id, *requester, *reason)
	if err != nil {
		return err
	}
	fmt.Fprintf(a.Out, "WITHDRAWN request=%s job=%s status=%s\n", got.Request, got.Job, got.Status)
	return nil
}

// WithdrawSpawn asks the hub to end pending request id as cancelled, for
// requester. It checks the hub advertises spawn_withdraw_v1 first, then sends
// one POST: a withdrawal is never retried, because a retry after a lost answer
// would read the request as already ended. A transport failure after the send
// is reported as unconfirmed, with the status command to read it.
func (c *hubHTTP) WithdrawSpawn(ctx context.Context, id, requester, reason string) (SpawnWithdrawal, error) {
	if !validID(id) {
		return SpawnWithdrawal{}, fail(exitUsage, "invalid spawn request id")
	}
	status, raw, err := c.doIdempotent(ctx, http.MethodGet, "/v1/spawns/features", nil, 5*time.Second)
	if err != nil {
		return SpawnWithdrawal{}, fail(exitConfig, "unable to verify hub support for spawn withdraw: %v", err)
	}
	var features struct {
		Features []string `json:"features"`
	}
	if status != http.StatusOK || json.Unmarshal(raw, &features) != nil {
		return SpawnWithdrawal{}, fail(exitConfig, "unable to verify hub support for spawn withdraw (HTTP %d)", status)
	}
	if !slices.Contains(features.Features, spawnWithdrawFeature) {
		return SpawnWithdrawal{}, fail(exitConfig, "hub does not support %s; upgrade the hub before withdrawing", spawnWithdrawFeature)
	}
	status, raw, err = c.do(ctx, http.MethodPost, "/v1/spawns/"+id+"/withdraw", map[string]string{"reason": reason, "requester": requester}, 10*time.Second)
	if err != nil {
		return SpawnWithdrawal{}, fail(exitStateConflict, "spawn withdrawal of %s unconfirmed (%v); read it with panewire spawn status %s", id, err, id)
	}
	var answer struct {
		Request string `json:"request_id"`
		Job     string `json:"job_id"`
		Status  string `json:"status"`
		Reason  string `json:"reason"`
		Error   string `json:"error"`
		Fix     string `json:"fix"`
	}
	_ = json.Unmarshal(raw, &answer)
	switch {
	case status == http.StatusOK && answer.Status != "":
		return SpawnWithdrawal{Request: firstNonempty(answer.Request, id), Job: answer.Job, Status: answer.Status, Reason: answer.Reason}, nil
	case status == http.StatusForbidden && answer.Error == "not_requester":
		return SpawnWithdrawal{}, fail(exitUsage, "hub refused: %s did not submit request %s; %s", requester, id, answer.Fix)
	case status == http.StatusBadRequest:
		return SpawnWithdrawal{}, fail(exitUsage, "hub refused the withdrawal of %s: %s", id, firstNonempty(answer.Fix, "name a request id, the requesting lane and a one-line --reason"))
	case status == http.StatusNotFound:
		return SpawnWithdrawal{}, fail(exitUsage, "no spawn request %s on the hub", id)
	case status == http.StatusConflict:
		return SpawnWithdrawal{}, fail(exitStateConflict, "hub kept request %s: %s", id, firstNonempty(answer.Error, "withdrawal refused"))
	case status == http.StatusUnauthorized:
		return SpawnWithdrawal{}, fail(exitConfig, "hub refused the operator credential for spawn withdraw")
	}
	return SpawnWithdrawal{}, fail(exitStateConflict, "spawn withdrawal of %s unconfirmed (HTTP %d); read it with panewire spawn status %s", id, status, id)
}

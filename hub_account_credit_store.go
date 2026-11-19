package panewire

import (
	"context"
	"errors"
	"time"
)

// accountCreditMarkSchema is also installed by Postgres migration 0022.
const accountCreditMarkSchema = `CREATE TABLE IF NOT EXISTS account_credit_marks (
 account_key TEXT PRIMARY KEY, pool TEXT NOT NULL, marked_ms BIGINT NOT NULL)`

// accountCreditMark is one account's provider credit rejection: the pool it
// was seen on and when the hub read it. It is held until the first reading of
// the account after Marked, or fleet.AccountCreditMarkTTL, whichever comes
// first (hub_account_credit.go).
type accountCreditMark struct {
	Account string
	Pool    string
	Marked  time.Time
}

// putAccountCreditMark stores mark as the account's current credit mark. A
// later rejection replaces an earlier one, so a mark that lifted cannot come
// back with its old time after a restart.
func (s *laneInboxStore) putAccountCreditMark(parent context.Context, account string, mark accountCreditMark) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(s.dialect.upsert(`INSERT INTO account_credit_marks(account_key,pool,marked_ms) VALUES(?,?,?)`, "account_key", s.dialect.assignIncoming("pool", "marked_ms"))),
		account, mark.Pool, mark.Marked.UnixMilli())
	return err
}

// accountCreditMarks reads every stored mark.
func (s *laneInboxStore) accountCreditMarks(parent context.Context) (map[string]accountCreditMark, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT account_key,pool,marked_ms FROM account_credit_marks`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	marks := map[string]accountCreditMark{}
	for rows.Next() {
		var account string
		var markedMS int64
		var mark accountCreditMark
		if err := rows.Scan(&account, &mark.Pool, &markedMS); err != nil {
			return nil, err
		}
		mark.Account = account
		mark.Marked = time.UnixMilli(markedMS).UTC()
		marks[account] = mark
	}
	return marks, rows.Err()
}

// deleteAccountCreditMark drops a mark that has lifted, so a hub restart does
// not read it as still holding after its reason is gone. The marked time is
// part of the predicate: a new episode written between the lift decision and
// this delete keeps its row.
func (s *laneInboxStore) deleteAccountCreditMark(parent context.Context, account string, marked time.Time) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`DELETE FROM account_credit_marks WHERE account_key=? AND marked_ms=?`), account, marked.UnixMilli())
	return err
}

var errAccountCreditMarkUnstored = errors.New("hub inbox store unavailable")

package panewire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
)

// hubDatabaseURLEnv names the hub's database URL. When it is unset the hub
// keeps the SQLite inbox store; a postgres:// value selects Postgres (see
// docs/hub-postgres.md) and a mysql:// value selects MySQL (see
// docs/hub-mysql.md). The value carries a credential, so it is never logged or
// echoed in an error.
const hubDatabaseURLEnv = "PANEWIRE_HUB_DATABASE_URL"

const hubConnectTimeout = 10 * time.Second

// openLaneInboxStoreURL opens the hub store the database URL names: mysql://
// is MySQL, anything else is read as a Postgres URL. The scheme alone chooses;
// no error names the URL.
func openLaneInboxStoreURL(ctx context.Context, databaseURL string) (*laneInboxStore, error) {
	if strings.HasPrefix(strings.ToLower(databaseURL), "mysql://") {
		return openLaneInboxStoreMySQL(ctx, databaseURL)
	}
	return openLaneInboxStorePostgres(ctx, databaseURL)
}

// hubMigrationContext bounds one migration run. A migration may read whole
// history tables (the outbox migration builds partial indexes and copies owed
// rows, hk 5298), so it gets the start-up budget, not the connect one: a run
// cut off at the connect timeout rolls back and the next start repeats the
// same scan.
func hubMigrationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, laneInboxStartupTimeout)
}

// errHubDatabaseURL deliberately carries no detail: driver parse errors can
// quote parts of the connection string.
var errHubDatabaseURL = errors.New("hub database URL is invalid")

// hubDatabaseErr is a driver error reduced to the failed step and, for a
// server error, its SQLSTATE or MySQL error number. Its text never holds user,
// host, database or password.
type hubDatabaseErr struct{ msg string }

func (e *hubDatabaseErr) Error() string { return e.msg }

// hubDatabaseError reduces err for printing. Connection errors from the
// drivers name the user, host and database, which this repository does not
// print.
func hubDatabaseError(step string, err error) error {
	var reduced *hubDatabaseErr
	if errors.As(err, &reduced) {
		return reduced
	}
	var pgErr *pgconn.PgError
	var myErr *mysql.MySQLError
	switch {
	case errors.As(err, &pgErr):
		return &hubDatabaseErr{fmt.Sprintf("hub database %s failed: SQLSTATE %s", step, pgErr.Code)}
	case errors.As(err, &myErr):
		return &hubDatabaseErr{fmt.Sprintf("hub database %s failed: MySQL error %d", step, myErr.Number)}
	case errors.Is(err, context.DeadlineExceeded):
		return &hubDatabaseErr{fmt.Sprintf("hub database %s failed: timeout", step)}
	case errors.Is(err, context.Canceled):
		return &hubDatabaseErr{fmt.Sprintf("hub database %s failed: cancelled", step)}
	}
	// The error's type, never its text: a net.OpError's text holds the
	// server address.
	return &hubDatabaseErr{fmt.Sprintf("hub database %s failed (%T)", step, err)}
}

// hubConnector reduces every connect error, including the pool's reconnects in
// the middle of a store call, before database/sql returns it.
type hubConnector struct{ driver.Connector }

func (c hubConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, hubDatabaseError("connect", err)
	}
	return conn, nil
}

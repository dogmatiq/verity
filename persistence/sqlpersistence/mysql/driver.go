package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/verity/internal/x/sqlx"
	"github.com/dogmatiq/verity/persistence"
	"go.uber.org/multierr"
)

// timeLayout is the Go date/time "layout" for MySQL date/time strings.
const timeLayout = "2006-01-02 15:04:05.999999"

// Driver is an implementation of sql.Driver for MySQL.
var Driver driver

type driver struct{}

// IsCompatibleWith returns nil if this driver can be used with db.
func (driver) IsCompatibleWith(ctx context.Context, db *sql.DB) error {
	// Verify that ?-style placeholders are supported.
	err := db.QueryRowContext(
		ctx,
		`SELECT 1 WHERE 1 = ?`,
		1,
	).Err()

	if err != nil {
		return err
	}

	// Verify that we're using something compatible with MySQL (because the SHOW
	// VARIABLES syntax is supported) and that InnoDB is available.
	return db.QueryRowContext(
		ctx,
		`SHOW VARIABLES LIKE "innodb_page_size"`,
	).Err()
}

// Begin starts a transaction.
func (driver) Begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	return db.BeginTx(ctx, nil)
}

// LockApplication acquires an exclusive lock on an application's data.
//
// r is a function that releases the lock, if acquired successfully.
func (driver) LockApplication(
	ctx context.Context,
	db *sql.DB,
	ak string,
) (r func() error, err error) {
	defer sqlx.Recover(&err)

	conn := sqlx.Conn(ctx, db)
	defer func() {
		if r == nil {
			conn.Close()
		}
	}()

	row := conn.QueryRowContext(ctx, `SELECT DATABASE()`)

	var database string
	if err := row.Scan(&database); err != nil {
		return nil, err
	}

	// MySQL advisory locked are server-wide, we include the database name in
	// the lock name to scope it to a single database.
	name := fmt.Sprintf("verity(%s, %s)", database, ak)

	if !acquireLock(conn, name) {
		return nil, persistence.ErrDataStoreLocked
	}

	return func() error {
		return multierr.Append(
			releaseLock(conn, name),
			conn.Close(),
		)
	}, nil
}

// acquireLock acquires a mysql advisory lock.
func acquireLock(conn *sql.Conn, name string) bool {
	// Note: the IS_FREE_LOCK() call guards against accidentally acquiring the
	// same lock again on the same connection, which is permitted by GET_LOCK().
	//
	// This helps protect against race-conditions that remain unnoticed when the
	// lock is not released properly.
	n := sqlx.QueryInt64(
		context.Background(),
		conn,
		`SELECT IS_FREE_LOCK(?) AND GET_LOCK(?, 0)`,
		name,
		name,
	)

	return n == 1
}

func releaseLock(conn *sql.Conn, name string) error {
	_, err := conn.ExecContext(
		context.Background(),
		`DO RELEASE_LOCK(?)`,
		name,
	)
	return err
}

// CreateSchema creates any SQL schema elements required by the driver.
func (driver) CreateSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	createAggregateSchema(ctx, db)
	createEventSchema(ctx, db)
	createOffsetSchema(ctx, db)
	createProcessSchema(ctx, db)
	createQueueSchema(ctx, db)

	return nil
}

// DropSchema removes any SQL schema elements created by CreateSchema().
func (driver) DropSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	dropAggregateSchema(ctx, db)
	dropEventSchema(ctx, db)
	dropOffsetSchema(ctx, db)
	dropProcessSchema(ctx, db)
	dropQueueSchema(ctx, db)

	return nil
}

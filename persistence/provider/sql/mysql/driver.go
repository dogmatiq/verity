package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence"
	"go.uber.org/multierr"
)

// timeLayout is the Go date/time "layout" for MySQL date/time strings.
const timeLayout = "2006-01-02 15:04:05.999999"

// Driver is an implementation of sql.Driver for MySQL.
var Driver driver

type driver struct{}

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

	name := fmt.Sprintf("infix(%s)", ak)

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
	createQueueSchema(ctx, db)

	return nil
}

// DropSchema removes any SQL schema elements created by CreateSchema().
func (driver) DropSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	dropAggregateSchema(ctx, db)
	dropEventSchema(ctx, db)
	dropOffsetSchema(ctx, db)
	dropQueueSchema(ctx, db)

	return nil
}

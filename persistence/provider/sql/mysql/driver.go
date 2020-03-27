package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence"
	"go.uber.org/multierr"
)

// Driver is an implementation of sql.Driver for MySQL.
var Driver driver

type driver struct{}

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

	ok, err := acquireLock(conn, name)
	if err != nil {
		return nil, err
	}
	if !ok {
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
func acquireLock(conn *sql.Conn, name string) (_ bool, err error) {
	defer sqlx.Recover(&err)

	// Note: the IS_FREE_LOCK() call guards against accidentally acquiring the
	// same lock again on the same connection, which is permitted by GET_LOCK().
	//
	// This helps protect against race-conditions that remain unnoticed when the
	// lock is not released properly.
	n := sqlx.QueryN(
		context.Background(),
		conn,
		`SELECT IS_FREE_LOCK(?) AND GET_LOCK(?, 0)`,
		name,
		name,
	)

	return n == 1, nil
}

func releaseLock(conn *sql.Conn, name string) error {
	_, err := conn.ExecContext(
		context.Background(),
		`DO RELEASE_LOCK(?)`,
		name,
	)
	return err
}

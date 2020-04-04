package sqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Exec executes a statement on the given DB.
func Exec(
	ctx context.Context,
	db DB,
	query string,
	args ...interface{},
) sql.Result {
	res, err := db.ExecContext(ctx, query, args...)
	Must(err)
	return res
}

// Insert executes an insert statement on the given DB and returns the last
// insert ID.
func Insert(
	ctx context.Context,
	db DB,
	query string,
	args ...interface{},
) int64 {
	res, err := db.ExecContext(ctx, query, args...)
	Must(err)

	n, err := res.RowsAffected()
	Must(err)

	if n != 1 {
		Must(errors.New("no row was inserted"))
	}

	id, err := res.LastInsertId()
	Must(err)

	return id
}

// TryInsert executes an insert statement on the given DB and returns the last
// insert ID.
//
// If no rows were affected, it returns false.
func TryInsert(
	ctx context.Context,
	db DB,
	query string,
	args ...interface{},
) (int64, bool) {
	res, err := db.ExecContext(ctx, query, args...)
	Must(err)

	n, err := res.RowsAffected()
	Must(err)

	if n != 1 {
		return 0, false
	}

	id, err := res.LastInsertId()
	Must(err)

	return id, true
}

// ExecRow executes a statement on the given DB.
//
// It panics if the statement does not affect exactly one row.
//
// Note that for MySQL an column value must actually change to consider the row
// updated. Further, it returns a value of 2 for an INSERT .. ON DUPLICATE KEY
// UPDATE that results in a change to an existing row.
func ExecRow(
	ctx context.Context,
	db DB,
	query string,
	args ...interface{},
) {
	res, err := db.ExecContext(ctx, query, args...)
	Must(err)

	n, err := res.RowsAffected()
	Must(err)

	if n != 1 {
		Must(fmt.Errorf("%d rows updated", n))
	}
}

// TryExecRow executes a statement on the given DB.
//
// It returns false if the update does not affect exactly one row.
//
// Note that for MySQL an column value must actually change to consider the row
// updated. Further, it returns a value of 2 for an INSERT .. ON DUPLICATE KEY
// UPDATE that results in a change to an existing row.
func TryExecRow(
	ctx context.Context,
	db DB,
	query string,
	args ...interface{},
) bool {
	res, err := db.ExecContext(ctx, query, args...)
	Must(err)

	n, err := res.RowsAffected()
	Must(err)

	return n == 1
}

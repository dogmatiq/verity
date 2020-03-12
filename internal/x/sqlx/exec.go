package sqlx

import (
	"context"
	"database/sql"
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
) uint64 {
	res, err := db.ExecContext(ctx, query, args...)
	Must(err)

	id, err := res.LastInsertId()
	Must(err)

	return uint64(id)
}

// UpdateRow executes an update statement on the given DB.
//
// It panics if the update does not affect exactly one row. Note that MySQL
// requires an actual change to occur to consider the row updated.
func UpdateRow(
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

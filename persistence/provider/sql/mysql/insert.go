package mysql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/go-sql-driver/mysql"
)

// mysqlErrDupEntry is the MySQL error code for a duplicate key conflict.
//
// https://dev.mysql.com/doc/refman/8.0/en/server-error-reference.html#error_er_dup_entry
const mysqlErrDupEntry = 1062

// insertIgnore is a helper for running queries that behave *like* MySQL's
// INSERT IGNORE syntax for ignoring duplicate key conflicts.
//
// We don't *actually* use the INSERT IGNORE syntax, because it can ignore far
// more errors than a duplicate key conflict.
//
// It returns a boolean indicating true if the row was inserted, or false if it
// was ignored.
func insertIgnore(
	ctx context.Context,
	tx *sql.Tx,
	query string,
	args ...interface{},
) (bool, error) {
	_, err := tx.ExecContext(ctx, query, args...)
	if err == nil {
		return true, nil
	}

	var merr *mysql.MySQLError

	if errors.As(err, &merr) && merr.Number == mysqlErrDupEntry {
		return false, nil
	}

	return false, err
}

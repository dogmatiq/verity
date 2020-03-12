package sqlx

import (
	"context"
	"database/sql"
)

// Prepare prepares a statement or panics if unable to do so.
func Prepare(ctx context.Context, db DB, query string) *sql.Stmt {
	stmt, err := db.PrepareContext(ctx, query)
	Must(err)
	return stmt
}

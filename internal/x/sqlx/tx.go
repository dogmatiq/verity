package sqlx

import (
	"context"
	"database/sql"
)

// Begin starts a new transaction.
func Begin(ctx context.Context, db *sql.DB) *sql.Tx {
	tx, err := db.BeginTx(ctx, nil)
	Must(err)
	return tx
}

// Commit commits the given transaction.
func Commit(tx *sql.Tx) {
	Must(tx.Commit())
}

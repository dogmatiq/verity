package sqlx

import (
	"context"
	"database/sql"
)

// Conn returns a single connection from the pool or panics if unable to do so.
func Conn(ctx context.Context, db *sql.DB) *sql.Conn {
	conn, err := db.Conn(ctx)
	Must(err)
	return conn
}

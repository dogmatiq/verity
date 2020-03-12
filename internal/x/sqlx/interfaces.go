package sqlx

import (
	"context"
	"database/sql"
)

// DB is an interface satisfied by *sql.DB, *sql.Conn and *sql.Tx.
type DB interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

var (
	_ DB = (*sql.DB)(nil)
	_ DB = (*sql.Tx)(nil)
	_ DB = (*sql.Conn)(nil)
)

// Scanner is an interface satisfied by *sql.Rows and *sql.Row.
type Scanner interface {
	Scan(...interface{}) error
}

var (
	_ Scanner = (*sql.Rows)(nil)
	_ Scanner = (*sql.Row)(nil)
)

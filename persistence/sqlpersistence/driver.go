package sqlpersistence

import (
	"context"
	"database/sql"
)

// Driver is used to interface with the underlying SQL database.
type Driver interface {
	LockDriver
	AggregateDriver
	EventDriver
	OffsetDriver
	ProcessDriver
	QueueDriver

	// IsCompatibleWith returns nil if this driver can be used with db.
	IsCompatibleWith(ctx context.Context, db *sql.DB) error

	// Begin starts a transaction for use in a peristence.Transaction.
	Begin(ctx context.Context, db *sql.DB) (*sql.Tx, error)

	// CreateSchema creates any SQL schema elements required by the driver.
	CreateSchema(ctx context.Context, db *sql.DB) error

	// DropSchema removes any SQL schema elements created by CreateSchema().
	DropSchema(ctx context.Context, db *sql.DB) error
}

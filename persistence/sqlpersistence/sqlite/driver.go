package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/dogmatiq/verity/internal/x/sqlx"
)

// Driver is an implementation of sql.Driver for SQLite.
var Driver = driver{
	lockUpdateInterval: 10 * time.Second,
	lockExpiryOffset:   5 * time.Second,
}

type driver struct {
	// lockUpdateInterval specifies how often an application lock's expiry timestamp
	// should be updated.
	lockUpdateInterval time.Duration

	// lockExpiryOffset specifies how far in the future locks should be set to
	// expire.
	lockExpiryOffset time.Duration
}

// IsCompatibleWith returns nil if this driver can be used with db.
func (driver) IsCompatibleWith(ctx context.Context, db *sql.DB) error {
	// Verify that we're using SQLite and that $1-style placeholders are
	// supported.
	return db.QueryRowContext(
		ctx,
		`SELECT sqlite_version() WHERE 1 = $1`,
		1,
	).Err()
}

// Begin starts a transaction.
func (driver) Begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	return db.BeginTx(ctx, nil)
}

// CreateSchema creates the schema elements required by the SQLite driver.
func (driver) CreateSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	tx := sqlx.Begin(ctx, db)
	defer tx.Rollback()

	createLockSchema(ctx, db)
	createAggregateSchema(ctx, db)
	createEventSchema(ctx, db)
	createOffsetSchema(ctx, db)
	createProcessSchema(ctx, db)
	createQueueSchema(ctx, db)

	return tx.Commit()
}

// DropSchema drops the schema elements required by the SQLite driver.
func (driver) DropSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	dropLockSchema(ctx, db)
	dropAggregateSchema(ctx, db)
	dropEventSchema(ctx, db)
	dropOffsetSchema(ctx, db)
	dropProcessSchema(ctx, db)
	dropQueueSchema(ctx, db)

	return nil
}

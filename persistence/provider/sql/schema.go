package sql

import (
	"context"
	"database/sql"
)

// CreateSchema creates the schema elements necessary to use the given database.
func CreateSchema(ctx context.Context, db *sql.DB) error {
	d, err := selectDriver(ctx, db)
	if err != nil {
		return err
	}

	return d.CreateSchema(ctx, db)
}

// DropSchema drops the schema elements necessary to use the given database.
func DropSchema(ctx context.Context, db *sql.DB) error {
	d, err := selectDriver(ctx, db)
	if err != nil {
		return err
	}

	return d.DropSchema(ctx, db)
}

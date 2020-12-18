package sqlpersistence

import (
	"context"
	"database/sql"
)

// CreateSchema creates the schema elements necessary to use the given database.
//
// It does not return an error if the schema already exists.
func CreateSchema(ctx context.Context, db *sql.DB) error {
	d, err := selectDriver(ctx, db)
	if err != nil {
		return err
	}

	return d.CreateSchema(ctx, db)
}

// DropSchema drops the schema elements necessary to use the given database.
//
// It does not return an error if the schema does not exist.
func DropSchema(ctx context.Context, db *sql.DB) error {
	d, err := selectDriver(ctx, db)
	if err != nil {
		return err
	}

	return d.DropSchema(ctx, db)
}

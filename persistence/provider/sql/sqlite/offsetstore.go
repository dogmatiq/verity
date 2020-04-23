package sqlite

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence/subsystem/offsetstore"
)

// LoadOffset loads the last offset associated with the given application
// key.
func (driver) LoadOffset(
	ctx context.Context,
	db *sql.DB,
	ak string,
) (offsetstore.Offset, error) {
	var o offsetstore.Offset

	row := db.QueryRowContext(
		ctx,
		`SELECT next_offset
		FROM offset_store
		WHERE source_app_key = $1`,
		ak,
	)

	if err := row.Scan(&o); err != nil {
		return 0, err
	}

	return o, nil
}

// InsertOffset inserts a new offset associated with the given application
// key.
//
// It returns false if the row already exists.
func (driver) InsertOffset(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	_, n offsetstore.Offset,
) (bool, error) {
	res, err := tx.ExecContext(
		ctx,
		`INSERT INTO offset_store (
			source_app_key, next_offset
		) VALUES (
			$1, $2
		) ON CONFLICT (source_app_key) DO NOTHING`,
		ak,
		n,
	)
	if err != nil {
		return false, err
	}

	ra, err := res.RowsAffected()
	return ra == 1, err
}

// UpdateOffset updates the offset associated with the given application
// key.
//
// It returns false if the row does not exist or c is not the current
// offset associated with the given application key.
func (driver) UpdateOffset(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	c, n offsetstore.Offset,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`UPDATE offset_store
		SET next_offset = $1
		WHERE source_app_key = $2 AND next_offset = $3`,
		n, ak, c,
	), nil
}

package mysql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/internal/x/sqlx"
)

// LoadOffset loads the last offset associated with the given application
// key.
func (driver) LoadOffset(
	ctx context.Context,
	db *sql.DB,
	ak string,
) (eventstream.Offset, error) {
	var o eventstream.Offset

	row := db.QueryRowContext(
		ctx,
		`SELECT next_offset
		FROM offset_store
		WHERE source_app_key = ?`,
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
	_, n eventstream.Offset,
) (bool, error) {
	return insertIgnore(
		ctx,
		tx,
		`INSERT INTO offset_store VALUES(?, ?)`,
		ak,
		n,
	)
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
	c, n eventstream.Offset,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`UPDATE offset_store
		SET next_offset = ?
		WHERE source_app_key = ? AND next_offset = ?`,
		n, ak, c,
	), nil
}

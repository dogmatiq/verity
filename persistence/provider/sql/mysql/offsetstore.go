package mysql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/x/sqlx"
)

// LoadOffset loads the last offset associated with the given source
// application key sk. ak is the 'owner' application key.
//
// If there is no offset associated with the given source application key,
// the offset is returned as zero and error as nil.
func (driver) LoadOffset(
	ctx context.Context,
	db *sql.DB,
	ak, sk string,
) (uint64, error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT
			next_offset
		FROM offset_store
		WHERE app_key = ?
		AND source_app_key = ?`,
		ak,
		sk,
	)

	var o uint64
	err := row.Scan(&o)
	if err == sql.ErrNoRows {
		err = nil
	}

	return o, err
}

// InsertOffset inserts a new offset associated with the given source
// application key sk. ak is the 'owner' application key.
//
// It returns false if the row already exists.
func (driver) InsertOffset(
	ctx context.Context,
	tx *sql.Tx,
	ak, sk string,
	n uint64,
) (bool, error) {
	return insertIgnore(
		ctx,
		tx,
		`INSERT INTO offset_store SET
			app_key = ?,
			source_app_key = ?,
			next_offset = ?`,
		ak,
		sk,
		n,
	)
}

// UpdateOffset updates the offset associated with the given source
// application key sk. ak is the 'owner' application key.
//
// It returns false if the row does not exist or c is not the current offset
// associated with the given application key.
func (driver) UpdateOffset(
	ctx context.Context,
	tx *sql.Tx,
	ak, sk string,
	c, n uint64,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`UPDATE offset_store SET
			next_offset = ?
		WHERE app_key = ?
		AND source_app_key = ?
		AND next_offset = ?`,
		n,
		ak,
		sk,
		c,
	), nil
}

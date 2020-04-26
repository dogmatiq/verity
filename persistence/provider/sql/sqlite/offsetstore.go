package sqlite

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/eventstream"
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
) (eventstream.Offset, error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT
			next_offset
		FROM offset_store
		WHERE app_key = $1
		AND source_app_key = $2`,
		ak,
		sk,
	)

	var o eventstream.Offset
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
	n eventstream.Offset,
) (bool, error) {
	res, err := tx.ExecContext(
		ctx,
		`INSERT INTO offset_store (
			app_key,
			source_app_key,
			next_offset
		) VALUES (
			$1, $2, $3
		) ON CONFLICT (app_key, source_app_key) DO NOTHING`,
		ak,
		sk,
		n,
	)
	if err != nil {
		return false, err
	}

	ra, err := res.RowsAffected()
	return ra == 1, err
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
	c, n eventstream.Offset,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`UPDATE offset_store SET
			next_offset = $1
		WHERE app_key = $2
		AND source_app_key = $3
		AND next_offset = $4`,
		n,
		ak,
		sk,
		c,
	), nil
}

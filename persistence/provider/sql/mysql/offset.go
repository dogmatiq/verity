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
		FROM stream_offset
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
		`INSERT INTO stream_offset SET
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
		`UPDATE stream_offset SET
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

// createOffsetSchema creates the schema elements for stream offsets.
func createOffsetSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE stream_offset (
			app_key VARBINARY(255) NOT NULL,
			source_app_key VARBINARY(255) NOT NULL,
			next_offset    BIGINT NOT NULL,

			PRIMARY KEY (app_key, source_app_key)
		) ENGINE=InnoDB`,
	)
}

// dropOffsetSchema drops the schema elements for stream offsets.
func dropOffsetSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS stream_offset`)
}

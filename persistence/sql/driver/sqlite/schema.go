package sqlite

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/x/sqlx"
)

// CreateSchema creates the schema elements required by the SQLite driver.
func CreateSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	tx := sqlx.Begin(ctx, db)
	defer tx.Rollback()

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE stream_offset (
			source_app_key TEXT NOT NULL PRIMARY KEY,
			next_offset    BIGINT NOT NULL
		) WITHOUT ROWID`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE stream (
	 		stream_offset       BIGINT NOT NULL,
	 		message_type        TEXT NOT NULL,
	 		description         TEXT NOT NULL,
	 		message_id          TEXT NOT NULL,
	 		causation_id        TEXT NOT NULL,
	 		correlation_id      TEXT NOT NULL,
	 		source_app_name     TEXT NOT NULL,
	 		source_app_key      TEXT NOT NULL,
	 		source_handler_name TEXT NOT NULL,
	 		source_handler_key  TEXT NOT NULL,
	 		source_instance_id  TEXT NOT NULL,
	 		created_at          TEXT NOT NULL, -- RFC3339Nano
	 		media_type          TEXT NOT NULL,
			data                BINARY NOT NULL,

			PRIMARY KEY (source_app_key, stream_offset)
	 	) WITHOUT ROWID`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE INDEX cursor_filter ON stream (
			source_app_key,
			message_type,
			stream_offset
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE stream_filter (
			hash    TEXT NOT NULL,
			used_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE INDEX hash ON stream_filter (
			hash
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE stream_filter_type (
			filter_id    BIGINT NOT NULL,
			message_type TEXT NOT NULL,

			PRIMARY KEY (filter_id, message_type)
		) WITHOUT ROWID`,
	)

	return tx.Commit()
}

// DropSchema drops the schema elements required by the SQLite driver.
func DropSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS stream_offset`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS stream`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS stream_filter`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS stream_filter_type`)

	return nil
}

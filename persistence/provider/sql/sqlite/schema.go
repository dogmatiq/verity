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
		`CREATE TABLE app_lock (
			app_key TEXT NOT NULL PRIMARY KEY,
			expires INTEGER NOT NULL
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE event_offset (
			source_app_key TEXT NOT NULL PRIMARY KEY,
			next_offset    BIGINT NOT NULL
		) WITHOUT ROWID`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE event (
	 		offset              BIGINT NOT NULL,
	 		message_id          TEXT NOT NULL,
	 		causation_id        TEXT NOT NULL,
	 		correlation_id      TEXT NOT NULL,
	 		source_app_name     TEXT NOT NULL,
	 		source_app_key      TEXT NOT NULL,
	 		source_handler_name TEXT NOT NULL,
	 		source_handler_key  TEXT NOT NULL,
	 		source_instance_id  TEXT NOT NULL,
	 		created_at          TEXT NOT NULL, -- RFC3339Nano
	 		portable_name       TEXT NOT NULL,
	 		media_type          TEXT NOT NULL,
			data                BINARY NOT NULL,

			PRIMARY KEY (source_app_key, offset)
	 	) WITHOUT ROWID`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE INDEX eventstore_query ON event (
			source_app_key,
			portable_name,
			offset,
			source_handler_key,
			source_instance_id
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE event_filter (
			hash    TEXT NOT NULL,
			used_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE INDEX hash ON event_filter (
			hash
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE event_filter_type (
			filter_id     BIGINT NOT NULL,
			portable_name TEXT NOT NULL,

			PRIMARY KEY (filter_id, portable_name)
		) WITHOUT ROWID`,
	)

	return tx.Commit()
}

// DropSchema drops the schema elements required by the SQLite driver.
func DropSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS event_offset`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS event`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS event_filter`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS event_filter_type`)

	return nil
}

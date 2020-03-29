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
			app_key TEXT NOT NULL UNIQUE,
			expires INTEGER NOT NULL
		)`,
	)

	createEventStoreSchema(ctx, db)

	return tx.Commit()
}

// DropSchema drops the schema elements required by the SQLite driver.
func DropSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS app_lock`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS event_offset`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS event`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS event_filter`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS event_filter_name`)

	return nil
}

// createEventStoreSchema creates the schema elements required by the event
// store subsystem.
func createEventStoreSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE event_offset (
			source_app_key TEXT NOT NULL PRIMARY KEY,
			next_offset    INTEGER NOT NULL
		) WITHOUT ROWID`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE event (
	 		offset              INTEGER NOT NULL,
	 		message_id          TEXT NOT NULL,
	 		causation_id        TEXT NOT NULL,
	 		correlation_id      TEXT NOT NULL,
	 		source_app_name     TEXT NOT NULL,
	 		source_app_key      TEXT NOT NULL,
	 		source_handler_name TEXT NOT NULL,
	 		source_handler_key  TEXT NOT NULL,
	 		source_instance_id  TEXT NOT NULL,
	 		created_at          TEXT NOT NULL,
	 		portable_name       TEXT NOT NULL,
	 		media_type          TEXT NOT NULL,
			data                BLOB NOT NULL,

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
			app_key VARBINARY(255) NOT NULL UNIQUE
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE event_filter_name (
			filter_id     INTEGER NOT NULL REFERENCES event_filter (rowid) ON DELETE CASCADE,
			portable_name TEXT NOT NULL,

			PRIMARY KEY (filter_id, portable_name)
		) WITHOUT ROWID`,
	)
}

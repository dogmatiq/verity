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

	createAggregateSchema(ctx, db)
	createEventStoreSchema(ctx, db)
	createOffsetSchema(ctx, db)
	createQueueSchema(ctx, db)

	return tx.Commit()
}

// DropSchema drops the schema elements required by the SQLite driver.
func DropSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS aggregate_metadata`)

	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS app_lock`)

	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS event_offset`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS event`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS event_filter`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS event_filter_name`)

	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS stream_offset`)

	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS queue`)

	return nil
}

// createAggregateSchema creates the schema elements required to store
// aggregates.
func createAggregateSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE aggregate_metadata (
			app_key           TEXT NOT NULL,
			handler_key       TEXT NOT NULL,
			instance_id       TEXT NOT NULL,
			revision          INTEGER NOT NULL DEFAULT 1,
			instance_exists   BOOLEAN NOT NULL,
			last_destroyed_by TEXT NOT NULL,

			PRIMARY KEY (app_key, handler_key, instance_id)
		)`,
	)
}

// createEventStoreSchema creates the schema elements required by the event
// store subsystem.
func createEventStoreSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE event_offset (
			source_app_key TEXT NOT NULL PRIMARY KEY,
			next_offset    INTEGER NOT NULL DEFAULT 1
		) WITHOUT ROWID`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE event (
			offset              INTEGER NOT NULL,
			message_id          TEXT NOT NULL UNIQUE,
			causation_id        TEXT NOT NULL,
			correlation_id      TEXT NOT NULL,
			source_app_name     TEXT NOT NULL,
			source_app_key      TEXT NOT NULL,
			source_handler_name TEXT NOT NULL,
			source_handler_key  TEXT NOT NULL,
			source_instance_id  TEXT NOT NULL,
			created_at          TEXT NOT NULL,
			description         TEXT NOT NULL,
			portable_name       TEXT NOT NULL,
			media_type          TEXT NOT NULL,
			data                BLOB NOT NULL,

			PRIMARY KEY (source_app_key, offset)
		) WITHOUT ROWID`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE INDEX by_type ON event (
			source_app_key,
			portable_name,
			offset
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE INDEX by_source ON event (
			source_app_key,
			source_handler_key,
			source_instance_id,
			offset
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE event_filter (
			id      INTEGER PRIMARY KEY,
			app_key TEXT NOT NULL
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE event_filter_name (
			filter_id     INTEGER NOT NULL,
			portable_name TEXT NOT NULL,

			PRIMARY KEY (filter_id, portable_name)
		) WITHOUT ROWID`,
	)
}

// createOffsetSchema creates the schema elements required to store offsets.
func createOffsetSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE stream_offset (
			app_key        TEXT NOT NULL,
			source_app_key TEXT NOT NULL,
			next_offset    INTEGER NOT NULL,

			PRIMARY KEY (app_key, source_app_key)
		)`,
	)
}

// createQueueSchema creates the schema elements required by the message queue
// subsystem.
func createQueueSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE queue (
			app_key             TEXT NOT NULL,
			revision            INTEGER NOT NULL DEFAULT 1,
			failure_count       INTEGER NOT NULL DEFAULT 0,
			next_attempt_at     DATETIME NOT NULL,
			message_id          TEXT NOT NULL,
			causation_id        TEXT NOT NULL,
			correlation_id      TEXT NOT NULL,
			source_app_name     TEXT NOT NULL,
			source_app_key      TEXT NOT NULL,
			source_handler_name TEXT NOT NULL,
			source_handler_key  TEXT NOT NULL,
			source_instance_id  TEXT NOT NULL,
			created_at          TEXT NOT NULL,
			scheduled_for       TEXT NOT NULL,
			description         TEXT NOT NULL,
			portable_name       TEXT NOT NULL,
			media_type          TEXT NOT NULL,
			data                BLOB NOT NULL,

			PRIMARY KEY (app_key, message_id)
		) WITHOUT ROWID`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE INDEX repository_load ON queue (
			app_key,
			next_attempt_at
		)`,
	)
}

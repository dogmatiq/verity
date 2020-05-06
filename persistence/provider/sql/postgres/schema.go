package postgres

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/x/sqlx"
)

// CreateSchema creates the schema elements required by the PostgreSQL driver.
func CreateSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	tx := sqlx.Begin(ctx, db)
	defer tx.Rollback()

	sqlx.Exec(ctx, db, `CREATE SCHEMA infix`)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE infix.app_lock_id (
			app_key TEXT NOT NULL PRIMARY KEY,
			lock_id SERIAL NOT NULL UNIQUE
		)`,
	)

	createAggregateStoreSchema(ctx, db)
	createEventStoreSchema(ctx, db)
	createOffsetStoreSchema(ctx, db)
	createQueueSchema(ctx, db)

	return tx.Commit()
}

// DropSchema drops the schema elements required by the PostgreSQL driver.
func DropSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP SCHEMA IF EXISTS infix CASCADE`)
	return err
}

// createAggregateStoreSchema creates the schema elements required by the
// aggregate store subsystem.
func createAggregateStoreSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE infix.aggregate_metadata (
			app_key           TEXT NOT NULL,
			handler_key       TEXT NOT NULL,
			instance_id       TEXT NOT NULL,
			revision          BIGINT NOT NULL DEFAULT 1,
			instance_exists   BOOLEAN NOT NULL,
			last_destroyed_by TEXT NOT NULL,
			begin_offset      BIGINT NOT NULL,
			end_offset        BIGINT NOT NULL,

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
		`CREATE TABLE infix.event_offset (
			source_app_key TEXT NOT NULL PRIMARY KEY,
			next_offset    BIGINT NOT NULL DEFAULT 1
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE infix.event (
			"offset"            BIGINT NOT NULL,
			message_id          TEXT NOT NULL,
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
			data                BYTEA NOT NULL,

			PRIMARY KEY (source_app_key, "offset")
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE INDEX repository_query ON infix.event (
			source_app_key,
			portable_name,
			messsage_id,
			source_handler_key,
			source_instance_id
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE infix.event_filter (
			id        SERIAL NOT NULL PRIMARY KEY,
			app_key   TEXT NOT NULL
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE infix.event_filter_name (
			filter_id     BIGINT NOT NULL REFERENCES infix.event_filter (id) ON DELETE CASCADE,
			portable_name TEXT NOT NULL,

			PRIMARY KEY (filter_id, portable_name)
		)`,
	)
}

// createOffsetStoreSchema creates the schema elements required by the offset
// store subsystem.
func createOffsetStoreSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE infix.offset_store (
			app_key        TEXT NOT NULL,
			source_app_key TEXT NOT NULL,
			next_offset    BIGINT NOT NULL,

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
		`CREATE TABLE infix.queue (
			app_key             TEXT NOT NULL,
			revision            BIGINT NOT NULL DEFAULT 1,
			failure_count       BIGINT NOT NULL DEFAULT 0,
			next_attempt_at     TIMESTAMP(6) WITH TIME ZONE NOT NULL,
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
			data                BYTEA NOT NULL,

			PRIMARY KEY (app_key, message_id)
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE INDEX repository_load ON infix.queue (
			app_key,
			next_attempt_at
		)`,
	)
}

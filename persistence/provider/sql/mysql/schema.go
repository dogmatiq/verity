package mysql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/x/sqlx"
)

// CreateSchema creates the schema elements required by the MySQL driver.
func CreateSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	createAggregateStoreSchema(ctx, db)
	createEventStoreSchema(ctx, db)
	createOffsetStoreSchema(ctx, db)
	createQueueSchema(ctx, db)

	return nil
}

// DropSchema drops the schema elements required by the MySQL driver.
func DropSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS aggregate_metadata`)

	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS event_offset`)

	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS event`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS event_filter`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS event_filter_name`)

	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS offset_store`)

	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS queue`)

	return nil
}

// createAggrevateStoreSchema creates the schema elements required by the
// aggregate store subsystem.
func createAggregateStoreSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE aggregate_metadata (
			app_key      VARBINARY(255) NOT NULL,
			handler_key  VARBINARY(255) NOT NULL,
			instance_id  VARBINARY(255) NOT NULL,
			revision     BIGINT NOT NULL DEFAULT 1,
			begin_offset BIGINT NOT NULL,
			end_offset   BIGINT NOT NULL,

			PRIMARY KEY (app_key, handler_key, instance_id)
		) ENGINE=InnoDB`,
	)
}

// createEventStoreSchema creates the schema elements required by the event
// store subsystem.
func createEventStoreSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE event_offset (
			source_app_key VARBINARY(255) NOT NULL PRIMARY KEY,
			next_offset    BIGINT NOT NULL DEFAULT 1
		) ENGINE=InnoDB`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE event (
			offset              BIGINT UNSIGNED NOT NULL,
			message_id          VARBINARY(255) NOT NULL,
			causation_id        VARBINARY(255) NOT NULL,
			correlation_id      VARBINARY(255) NOT NULL,
			source_app_name     VARBINARY(255) NOT NULL,
			source_app_key      VARBINARY(255) NOT NULL,
			source_handler_name VARBINARY(255) NOT NULL,
			source_handler_key  VARBINARY(255) NOT NULL,
			source_instance_id  VARBINARY(255) NOT NULL,
			created_at          VARBINARY(255) NOT NULL,
			description         VARBINARY(255) NOT NULL,
			portable_name       VARBINARY(255) NOT NULL,
			media_type          VARBINARY(255) NOT NULL,
			data                LONGBLOB NOT NULL,

			PRIMARY KEY (source_app_key, offset),
			INDEX repository_query (
				source_app_key,
				portable_name,
				offset,
				source_handler_key,
				source_instance_id
			)
		) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE event_filter (
			id      SERIAL PRIMARY KEY,
			app_key VARBINARY(255) NOT NULL
		) ENGINE=InnoDB`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE event_filter_name (
			filter_id     BIGINT UNSIGNED NOT NULL REFERENCES event_filter (id) ON DELETE CASCADE,
			portable_name VARBINARY(255) NOT NULL,

			PRIMARY KEY (filter_id, portable_name)
		) ENGINE=InnoDB`,
	)
}

// createOffsetStoreSchema creates the schema elements required by the
// offset store subsystem.
func createOffsetStoreSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE offset_store (
			app_key VARBINARY(255) NOT NULL,
			source_app_key VARBINARY(255) NOT NULL,
			next_offset    BIGINT NOT NULL DEFAULT 1,

			PRIMARY KEY (app_key, source_app_key)
		) ENGINE=InnoDB`,
	)
}

// createQueueSchema creates the schema elements required by the message queue
// subsystem.
func createQueueSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE queue (
			app_key             VARBINARY(255) NOT NULL,
			revision            BIGINT UNSIGNED NOT NULL DEFAULT 1,
			failure_count       BIGINT UNSIGNED NOT NULL DEFAULT 0,
			next_attempt_at     TIMESTAMP(6) NOT NULL,
			message_id          VARBINARY(255) NOT NULL,
			causation_id        VARBINARY(255) NOT NULL,
			correlation_id      VARBINARY(255) NOT NULL,
			source_app_name     VARBINARY(255) NOT NULL,
			source_app_key      VARBINARY(255) NOT NULL,
			source_handler_name VARBINARY(255) NOT NULL,
			source_handler_key  VARBINARY(255) NOT NULL,
			source_instance_id  VARBINARY(255) NOT NULL,
			created_at          VARBINARY(255) NOT NULL,
			scheduled_for       VARBINARY(255) NOT NULL,
			description         VARBINARY(255) NOT NULL,
			portable_name       VARBINARY(255) NOT NULL,
			media_type          VARBINARY(255) NOT NULL,
			data                LONGBLOB NOT NULL,

			PRIMARY KEY (app_key, message_id),
			INDEX repository_load (app_key, next_attempt_at)
		) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4`,
	)
}

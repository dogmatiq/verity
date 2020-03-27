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

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE infix.event_offset (
			source_app_key TEXT NOT NULL PRIMARY KEY,
			next_offset    BIGINT NOT NULL
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
	 		created_at          BYTEA NOT NULL,
	 		portable_name       TEXT NOT NULL,
	 		media_type          TEXT NOT NULL,
			data                BYTEA NOT NULL,

			PRIMARY KEY (source_app_key, message_id)
	 	)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE INDEX eventstore_query ON infix.event (
			source_app_key,
			portable_name,
			"offset",
			source_handler_key,
			source_instance_id
		)`,
	)

	return tx.Commit()
}

// DropSchema drops the schema elements required by the PostgreSQL driver.
func DropSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP SCHEMA IF EXISTS infix CASCADE`)
	return err
}

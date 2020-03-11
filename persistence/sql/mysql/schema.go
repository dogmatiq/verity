package mysql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/x/sqlx"
)

// CreateSchema creates the schema elements required by the MySQL driver.
func CreateSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE stream_offset (
			singleton   ENUM('') PRIMARY KEY DEFAULT '',
			next_offset BIGINT NOT NULL
		) ENGINE=InnoDB`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE stream (
			offset              BIGINT(20) UNSIGNED NOT NULL,
			message_type        VARBINARY(255) NOT NULL,
			description         VARBINARY(255) NOT NULL,
			message_id          VARBINARY(255) NOT NULL,
			causation_id        VARBINARY(255) NOT NULL,
			correlation_id      VARBINARY(255) NOT NULL,
			source_app_name     VARBINARY(255) NOT NULL,
			source_app_key      VARBINARY(255) NOT NULL,
			source_handler_name VARBINARY(255) NOT NULL,
			source_handler_key  VARBINARY(255) NOT NULL,
			source_instance_id  VARBINARY(255) NOT NULL,
			created_at          VARBINARY(255) NOT NULL, -- RFC3339Nano
			media_type          VARBINARY(255) NOT NULL,
			data                LONGBLOB NOT NULL,

			PRIMARY KEY (source_app_key, offset),
			INDEX (source_app_key, message_type, offset)
		) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4`,
	)

	return nil
}

// DropSchema drops the schema elements required by the MySQL driver.
func DropSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS stream_offset`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS stream`)

	return nil
}

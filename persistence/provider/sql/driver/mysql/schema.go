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
			source_app_key VARBINARY(255) NOT NULL PRIMARY KEY,
			next_offset    BIGINT NOT NULL
		) ENGINE=InnoDB`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE stream (
			stream_offset       BIGINT(20) UNSIGNED NOT NULL,
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

			PRIMARY KEY (source_app_key, stream_offset),
			INDEX (source_app_key, message_type, stream_offset)
		) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE stream_filter (
			id      BIGINT(20) UNSIGNED PRIMARY KEY AUTO_INCREMENT,
			hash    VARBINARY(255) NOT NULL,
			used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

			KEY (hash)
		) ENGINE=InnoDB`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE stream_filter_type (
			filter_id    BIGINT(20) UNSIGNED NOT NULL,
			message_type VARBINARY(255) NOT NULL,

			PRIMARY KEY (filter_id, message_type)
		) ENGINE=InnoDB`,
	)

	return nil
}

// DropSchema drops the schema elements required by the MySQL driver.
func DropSchema(ctx context.Context, db *sql.DB) (err error) {
	defer sqlx.Recover(&err)

	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS stream_offset`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS stream`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS stream_filter`)
	sqlx.Exec(ctx, db, `DROP TABLE IF EXISTS stream_filter_type`)

	return nil
}

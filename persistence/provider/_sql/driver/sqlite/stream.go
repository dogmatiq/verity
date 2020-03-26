package sqlite

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence/provider/sql/internal/streamfilter"
)

// StreamDriver is an implementation of driver.StreamDriver that stores messages
// in a SQLite  database.
type StreamDriver struct{}

// FindFilter finds a filter by its hash and type names.
func (StreamDriver) FindFilter(
	ctx context.Context,
	db *sql.DB,
	hash []byte,
	names []string,
) (_ uint64, _ bool, err error) {
	defer sqlx.Recover(&err)

	tx := sqlx.Begin(ctx, db)
	defer tx.Rollback()

	filterIDs := sqlx.QueryManyN(
		ctx,
		tx,
		`SELECT
			rowid
		FROM stream_filter
		WHERE hash = $1`, // FOR UPDATE
		hash,
	)

	for _, id := range filterIDs {
		fnames := sqlx.QueryManyS(
			ctx,
			tx,
			`SELECT
				message_type
			FROM stream_filter_type
			WHERE filter_id = $1
			ORDER BY message_type`,
			id,
		)

		if streamfilter.CompareNames(names, fnames) {
			sqlx.Exec(
				ctx,
				tx,
				`UPDATE stream_filter SET
					used_at = CURRENT_TIMESTAMP
				WHERE rowid = $1`,
				id,
			)

			return id, true, tx.Commit()
		}
	}

	return 0, false, nil
}

// CreateFilter creates a filter with the specified hash and type names.
func (StreamDriver) CreateFilter(
	ctx context.Context,
	db *sql.DB,
	hash []byte,
	names []string,
) (_ uint64, err error) {
	defer sqlx.Recover(&err)

	tx := sqlx.Begin(ctx, db)
	defer tx.Rollback()

	id := sqlx.Insert(
		ctx,
		tx,
		`INSERT INTO stream_filter (
			hash
		) VALUES (
			$1
		)`,
		hash,
	)

	for _, n := range names {
		sqlx.Exec(
			ctx,
			tx,
			`INSERT INTO stream_filter_type (
				filter_id,
				message_type
			) VALUES (
				$1, $2
			)`,
			id,
			n,
		)
	}

	return id, tx.Commit()
}

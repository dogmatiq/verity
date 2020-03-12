package mysql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence/sql/internal/streamfilter"
)

// findFilter returns a filter ID for an existing filter for the givene mssage
// types.
func findFilter(
	ctx context.Context,
	db *sql.DB,
	hash []byte,
	names []string,
) (uint64, bool) {
	tx := sqlx.Begin(ctx, db)
	defer tx.Rollback()

	filterIDs := sqlx.QueryManyN(
		ctx,
		tx,
		`SELECT
			id
		FROM stream_filter
		WHERE hash = ?
		FOR UPDATE`,
		hash,
	)

	for _, id := range filterIDs {
		fnames := sqlx.QueryManyS(
			ctx,
			tx,
			`SELECT
				message_type
			FROM stream_filter_type
			WHERE filter_id = ?
			ORDER BY message_type`,
			id,
		)

		if streamfilter.CompareNames(names, fnames) {
			sqlx.Exec(
				ctx,
				tx,
				`UPDATE stream_filter SET
					used_at = CURRENT_TIMESTAMP
				WHERE id = ?`,
				id,
			)

			sqlx.Commit(tx)
			return id, true
		}
	}

	return 0, false
}

// createFilter creates a new filter for the given message types.
func createFilter(
	ctx context.Context,
	db *sql.DB,
	hash []byte,
	names []string,
) uint64 {
	tx := sqlx.Begin(ctx, db)
	defer tx.Rollback()

	id := sqlx.Insert(
		ctx,
		tx,
		`INSERT INTO stream_filter SET
			hash = ?`,
		hash,
	)

	for _, n := range names {
		sqlx.Exec(
			ctx,
			tx,
			`INSERT INTO stream_filter_type SET
				filter_id = ?,
				message_type = ?`,
			id,
			n,
		)
	}

	sqlx.Commit(tx)

	return id
}

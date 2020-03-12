package postgres

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence/sql/internal/streamfilter"
)

// findFilter returns the ID of a filter containing the given type names.
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
		FROM infix.stream_filter
		WHERE hash = $1
		FOR UPDATE`,
		hash,
	)

	for _, id := range filterIDs {
		fnames := sqlx.QueryManyS(
			ctx,
			tx,
			`SELECT
				message_type
			FROM infix.stream_filter_type
			WHERE filter_id = $1
			ORDER BY message_type`,
			id,
		)

		if streamfilter.CompareNames(names, fnames) {
			sqlx.Exec(
				ctx,
				tx,
				`UPDATE infix.stream_filter SET
					used_at = NOW()
				WHERE id = $1`,
				id,
			)

			sqlx.Commit(tx)
			return id, true
		}
	}

	return 0, false
}

// createFilter creates a new filter containing the given type names.
func createFilter(
	ctx context.Context,
	db *sql.DB,
	hash []byte,
	names []string,
) uint64 {
	tx := sqlx.Begin(ctx, db)
	defer tx.Rollback()

	id := sqlx.QueryN(
		ctx,
		tx,
		`INSERT INTO infix.stream_filter (
			hash
		) VALUES (
			$1
		)
		RETURNING id`,
		hash,
	)

	for _, n := range names {
		sqlx.Exec(
			ctx,
			tx,
			`INSERT INTO infix.stream_filter_type (
				filter_id,
				message_type
			) VALUES (
				$1, $2
			)`,
			id,
			n,
		)
	}

	sqlx.Commit(tx)

	return id
}

package postgres

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence/provider/sql/internal/streamfilter"
)

// StreamDriver is an implementation of driver.StreamDriver that stores messages
// in a PostgreSQL database.
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

	return id, tx.Commit()
}

// IncrementOffset increments an application stream's next offset by the
// specified amount and returns the new value.
func (StreamDriver) IncrementOffset(
	ctx context.Context,
	tx *sql.Tx,
	appKey string,
	count eventstream.Offset,
) (eventstream.Offset, error) {
	row := tx.QueryRowContext(
		ctx,
		`INSERT INTO infix.stream_offset AS o (
			source_app_key,
			next_offset
		) VALUES (
			$1, $2
		) ON CONFLICT (source_app_key) DO UPDATE SET
			next_offset = o.next_offset + excluded.next_offset
		RETURNING next_offset`,
		appKey,
		count,
	)

	var next eventstream.Offset
	err := row.Scan(&next)
	return next, err
}

// Append appends a single message to an application's stream.
func (StreamDriver) Append(
	ctx context.Context,
	tx *sql.Tx,
	offset eventstream.Offset,
	typename string,
	description string,
	env *envelope.Envelope,
) error {
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO infix.stream (
			stream_offset,
			message_type,
			description,
			message_id,
			causation_id,
			correlation_id,
			source_app_name,
			source_app_key,
			source_handler_name,
			source_handler_key,
			source_instance_id,
			created_at,
			media_type,
			data
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
		)`,
		offset,
		typename,
		description,
		env.MessageID,
		env.CausationID,
		env.CorrelationID,
		env.Source.Application.Name,
		env.Source.Application.Key,
		env.Source.Handler.Name,
		env.Source.Handler.Key,
		env.Source.InstanceID,
		sqlx.MarshalTime(env.CreatedAt),
		env.Packet.MediaType,
		env.Packet.Data,
	)

	return err
}

// Get returns the first event at or after a specific offset that matches a
// specific filter.
func (StreamDriver) Get(
	ctx context.Context,
	db *sql.DB,
	appKey string,
	offset eventstream.Offset,
	filterID uint64,
) (_ *eventstream.Event, _ bool, err error) {
	defer sqlx.Recover(&err)

	row := db.QueryRowContext(
		ctx,
		`SELECT
			stream_offset,
			message_id,
			causation_id,
			correlation_id,
			source_handler_name,
			source_handler_key,
			source_instance_id,
			created_at,
			media_type,
			data
		FROM infix.stream AS s
		INNER JOIN infix.stream_filter_type AS t
		ON t.message_type = s.message_type
		WHERE s.source_app_key = $1
		AND s.stream_offset >= $2
		AND t.filter_id = $3
		ORDER BY s.stream_offset
		LIMIT 1`,
		appKey,
		offset,
		filterID,
	)

	ev := eventstream.Event{
		Envelope: &envelope.Envelope{},
	}

	var createdAt []byte

	if sqlx.TryScan(
		row,
		&ev.Offset,
		&ev.Envelope.MessageID,
		&ev.Envelope.CausationID,
		&ev.Envelope.CorrelationID,
		&ev.Envelope.Source.Handler.Name,
		&ev.Envelope.Source.Handler.Key,
		&ev.Envelope.Source.InstanceID,
		&createdAt,
		&ev.Envelope.Packet.MediaType,
		&ev.Envelope.Packet.Data,
	) {
		ev.Envelope.CreatedAt = sqlx.UnmarshalTime(createdAt)
		return &ev, true, nil
	}

	return nil, false, nil
}

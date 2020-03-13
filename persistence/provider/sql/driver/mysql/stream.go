package mysql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/sql/internal/streamfilter"
)

// StreamDriver is an implementation of driver.StreamDriver that stores messages
// in a MySQL database.
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

	return id, tx.Commit()
}

// IncrementOffset increments an application stream's next offset by the
// specified amount and returns the new value.
func (StreamDriver) IncrementOffset(
	ctx context.Context,
	tx *sql.Tx,
	appKey string,
	count uint64,
) (_ uint64, err error) {
	defer sqlx.Recover(&err)

	sqlx.Exec(
		ctx,
		tx,
		`INSERT INTO stream_offset SET
			source_app_key = ?,
			next_offset = ?
		ON DUPLICATE KEY UPDATE
			next_offset = next_offset + VALUE(next_offset)`,
		appKey,
		count,
	)

	next := sqlx.QueryN(
		ctx,
		tx,
		`SELECT
			next_offset
		FROM stream_offset
		WHERE source_app_key = ?`,
		appKey,
	)

	return next, nil
}

// Append appends a single message to an application's stream.
func (StreamDriver) Append(
	ctx context.Context,
	tx *sql.Tx,
	offset uint64,
	typename string,
	description string,
	env *envelope.Envelope,
) error {
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO stream SET
			stream_offset       = ?,
			message_type        = ?,
			description         = ?,
			message_id          = ?,
			causation_id        = ?,
			correlation_id      = ?,
			source_app_name     = ?,
			source_app_key      = ?,
			source_handler_name = ?,
			source_handler_key  = ?,
			source_instance_id  = ?,
			created_at          = ?,
			media_type          = ?,
			data                = ?`,
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

// Get returns the first message at or after a specific offset that matches
// a specific filter.
func (StreamDriver) Get(
	ctx context.Context,
	db *sql.DB,
	appKey string,
	offset uint64,
	filterID uint64,
) (_ *persistence.StreamMessage, _ bool, err error) {
	defer sqlx.Recover(&err)

	row := db.QueryRowContext(
		ctx,
		`SELECT
			stream_offset,
			message_id,
			causation_id,
			correlation_id,
			source_app_name,
			source_handler_name,
			source_handler_key,
			source_instance_id,
			created_at,
			media_type,
			data
		FROM stream AS s
		INNER JOIN stream_filter_type AS t
		ON t.message_type = s.message_type
		WHERE s.source_app_key = ?
		AND s.stream_offset >= ?
		AND t.filter_id = ?
		ORDER BY s.stream_offset
		LIMIT 1`,
		appKey,
		offset,
		filterID,
	)

	m := persistence.StreamMessage{
		Envelope: &envelope.Envelope{},
	}

	var createdAt []byte

	if sqlx.TryScan(
		row,
		&m.Offset,
		&m.Envelope.MessageID,
		&m.Envelope.CausationID,
		&m.Envelope.CorrelationID,
		&m.Envelope.Source.Application.Name,
		&m.Envelope.Source.Handler.Name,
		&m.Envelope.Source.Handler.Key,
		&m.Envelope.Source.InstanceID,
		&createdAt,
		&m.Envelope.Packet.MediaType,
		&m.Envelope.Packet.Data,
	) {
		m.Envelope.CreatedAt = sqlx.UnmarshalTime(createdAt)
		return &m, true, nil
	}

	return nil, false, nil
}

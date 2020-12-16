package postgres

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/envelopespec"
	"github.com/dogmatiq/verity/internal/x/sqlx"
	"github.com/dogmatiq/verity/persistence"
)

// UpdateNextOffset increments the next offset by one and returns the new value.
func (driver) UpdateNextOffset(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
) (_ uint64, err error) {
	defer sqlx.Recover(&err)

	o := sqlx.QueryInt64(
		ctx,
		tx,
		`INSERT INTO verity.event_offset AS o (
			source_app_key
		) VALUES (
			$1
		) ON CONFLICT (source_app_key) DO UPDATE SET
			next_offset = o.next_offset + 1
		RETURNING next_offset`,
		ak,
	)

	return uint64(o), nil
}

// InsertEvent saves an event at a specific offset.
func (driver) InsertEvent(
	ctx context.Context,
	tx *sql.Tx,
	o uint64,
	env *envelopespec.Envelope,
) error {
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO verity.event (
				"offset",
				message_id,
				causation_id,
				correlation_id,
				source_app_name,
				source_app_key,
				source_handler_name,
				source_handler_key,
				source_instance_id,
				created_at,
				description,
				portable_name,
				media_type,
				data
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
			)`,
		o,
		env.GetMessageId(),
		env.GetCausationId(),
		env.GetCorrelationId(),
		env.GetSourceApplication().GetName(),
		env.GetSourceApplication().GetKey(),
		env.GetSourceHandler().GetName(),
		env.GetSourceHandler().GetKey(),
		env.GetSourceInstanceId(),
		env.GetCreatedAt(),
		env.GetDescription(),
		env.GetPortableName(),
		env.GetMediaType(),
		env.GetData(),
	)

	return err
}

// InsertEventFilter inserts a filter that limits selected events to those with
// a portable name in the given set.
//
// It returns the filter's ID.
func (driver) InsertEventFilter(
	ctx context.Context,
	db *sql.DB,
	ak string,
	f map[string]struct{},
) (_ int64, err error) {
	defer sqlx.Recover(&err)

	tx := sqlx.Begin(ctx, db)
	defer tx.Rollback()

	id := sqlx.QueryInt64(
		ctx,
		tx,
		`INSERT INTO verity.event_filter (
			app_key
		) VALUES (
			$1
		) RETURNING id`,
		ak,
	)

	for n := range f {
		sqlx.Exec(
			ctx,
			tx,
			`INSERT INTO verity.event_filter_name (
				filter_id,
				portable_name
			) VALUES (
				$1, $2
			)`,
			id,
			n,
		)
	}

	return id, tx.Commit()
}

// DeleteEventFilter deletes an event filter.
//
// f is the filter ID, as returned by InsertEventFilter().
func (driver) DeleteEventFilter(
	ctx context.Context,
	db *sql.DB,
	f int64,
) error {
	_, err := db.ExecContext(
		ctx,
		`DELETE FROM verity.event_filter
		WHERE id = $1`,
		f,
	)
	return err
}

// PurgeEventFilters deletes all event filters for the given application.
func (driver) PurgeEventFilters(
	ctx context.Context,
	db *sql.DB,
	ak string,
) error {
	_, err := db.ExecContext(
		ctx,
		`DELETE FROM verity.event_filter
		WHERE app_key = $1`,
		ak,
	)
	return err
}

// SelectNextEventOffset selects the next "unused" offset.
func (driver) SelectNextEventOffset(
	ctx context.Context,
	db *sql.DB,
	ak string,
) (uint64, error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT
			next_offset
		FROM verity.event_offset`,
	)

	var next uint64

	err := row.Scan(&next)
	if err == sql.ErrNoRows {
		err = nil
	}

	return next, err
}

// SelectEventsByType selects events that match the given type filter.
//
// f is a filter ID, as returned by InsertEventFilter(). o is the minimum offset
// to include in the results.
func (driver) SelectEventsByType(
	ctx context.Context,
	db *sql.DB,
	ak string,
	f int64,
	o uint64,
) (*sql.Rows, error) {
	return db.QueryContext(
		ctx,
		`SELECT
			e.offset,
			e.message_id,
			e.causation_id,
			e.correlation_id,
			e.source_app_name,
			e.source_app_key,
			e.source_handler_name,
			e.source_handler_key,
			e.source_instance_id,
			e.created_at,
			e.description,
			e.portable_name,
			e.media_type,
			e.data
		FROM verity.event AS e
		INNER JOIN verity.event_filter_name AS ft
		ON ft.portable_name = e.portable_name
		WHERE e.source_app_key = $1
		AND e.offset >= $2
		AND ft.filter_id = $3
		ORDER BY e.offset`,
		ak,
		o,
		f,
	)
}

// SelectEventsBySource selects events that were produced by a specific handler.
func (driver) SelectEventsBySource(
	ctx context.Context,
	db *sql.DB,
	ak, hk, id string,
	o uint64,
) (*sql.Rows, error) {
	return db.QueryContext(
		ctx,
		`SELECT
			e.offset,
			e.message_id,
			e.causation_id,
			e.correlation_id,
			e.source_app_name,
			e.source_app_key,
			e.source_handler_name,
			e.source_handler_key,
			e.source_instance_id,
			e.created_at,
			e.description,
			e.portable_name,
			e.media_type,
			e.data
		FROM verity.event AS e
		WHERE e.source_app_key = $1
		AND e.source_handler_key = $2
		AND e.source_instance_id = $3
		AND e.offset >= $4
		ORDER BY e.offset`,
		ak,
		hk,
		id,
		o,
	)
}

// SelectOffsetByMessageID selects the offset of the message with the given
// ID. It returns false as a second return value if the message cannot be
// found.
func (driver) SelectOffsetByMessageID(
	ctx context.Context,
	db *sql.DB,
	id string,
) (uint64, bool, error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT
			e.offset
		FROM verity.event AS e
		WHERE e.message_id = $1`,
		id,
	)

	var o uint64
	err := row.Scan(&o)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}

	return o, true, err
}

// ScanEvent scans the next event from a row-set returned by SelectEvents().
func (driver) ScanEvent(
	rows *sql.Rows,
	ev *persistence.Event,
) error {
	return rows.Scan(
		&ev.Offset,
		&ev.Envelope.MessageId,
		&ev.Envelope.CausationId,
		&ev.Envelope.CorrelationId,
		&ev.Envelope.SourceApplication.Name,
		&ev.Envelope.SourceApplication.Key,
		&ev.Envelope.SourceHandler.Name,
		&ev.Envelope.SourceHandler.Key,
		&ev.Envelope.SourceInstanceId,
		&ev.Envelope.CreatedAt,
		&ev.Envelope.Description,
		&ev.Envelope.PortableName,
		&ev.Envelope.MediaType,
		&ev.Envelope.Data,
	)
}

// createEventSchema creates the schema elements for events.
func createEventSchema(ctx context.Context, db *sql.DB) {
	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE verity.event_offset (
			source_app_key TEXT NOT NULL PRIMARY KEY,
			next_offset    BIGINT NOT NULL DEFAULT 1
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE verity.event (
			"offset"            BIGINT NOT NULL,
			message_id          TEXT NOT NULL UNIQUE,
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
		`CREATE INDEX event_by_type ON verity.event (
			source_app_key,
			portable_name,
			"offset"
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE INDEX event_by_source ON verity.event (
			source_app_key,
			source_handler_key,
			source_instance_id,
			"offset"
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE verity.event_filter (
			id        SERIAL NOT NULL PRIMARY KEY,
			app_key   TEXT NOT NULL
		)`,
	)

	sqlx.Exec(
		ctx,
		db,
		`CREATE TABLE verity.event_filter_name (
			filter_id     BIGINT NOT NULL REFERENCES verity.event_filter (id) ON DELETE CASCADE,
			portable_name TEXT NOT NULL,

			PRIMARY KEY (filter_id, portable_name)
		)`,
	)
}

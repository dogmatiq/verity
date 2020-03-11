package mysql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence"
	infixsql "github.com/dogmatiq/infix/persistence/sql"
)

// StreamDriver is an implementation of sql.StreamDriver for MySQL and
// compatible databases such as MariaDB.
type StreamDriver struct{}

// Append appends messages to a stream.
//
// It returns the next free offset.
func (StreamDriver) Append(
	ctx context.Context,
	tx *sql.Tx,
	appKey string,
	messages []infixsql.AppendMessage,
) (_ uint64, err error) {
	defer sqlx.Recover(&err)

	count := uint64(len(messages))

	sqlx.Exec(
		ctx,
		tx,
		`INSERT INTO stream_offset SET
			next_offset = ?
		ON DUPLICATE KEY UPDATE
			next_offset = next_offset + VALUE(next_offset)`,
		count,
	)

	next := sqlx.QueryN(
		ctx,
		tx,
		`SELECT
			next_offset
		FROM stream_offset`,
	) - count

	for _, m := range messages {
		sqlx.Exec(
			ctx,
			tx,
			`INSERT INTO stream SET
				offset              = ?,
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
			next,
			m.MessageType,
			m.Description,
			m.Envelope.MessageID,
			m.Envelope.CausationID,
			m.Envelope.CorrelationID,
			m.Envelope.Source.Application.Name,
			m.Envelope.Source.Application.Key,
			m.Envelope.Source.Handler.Name,
			m.Envelope.Source.Handler.Key,
			m.Envelope.Source.InstanceID,
			sqlx.MarshalTime(m.Envelope.CreatedAt),
			m.Envelope.Packet.MediaType,
			m.Envelope.Packet.Data,
		)

		next++
	}

	return next, nil
}

// Open prepares a statement used to query for messages.
func (StreamDriver) Open(
	ctx context.Context,
	db *sql.DB,
	appKey string,
	types []string,
) (_ *sql.Stmt, err error) {
	defer sqlx.Recover(&err)

	conn := sqlx.Conn(ctx, db)
	defer func() {
		if err != nil {
			conn.Close()
		}
	}()

	sqlx.Exec(
		ctx,
		conn,
		`CREATE TEMPORARY TABLE IF NOT EXISTS __cursor_filter (
			message_type VARBINARY(255) NOT NULL PRIMARY KEY
		)`,
	)

	sqlx.Exec(ctx, conn, `TRUNCATE __cursor_filter`)

	for _, t := range types {
		sqlx.Exec(
			ctx,
			conn,
			`INSERT INTO __cursor_filter SET message_type = ?`,
			t,
		)
	}

	return sqlx.Prepare(
		ctx,
		conn,
		`SELECT
			offset,
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
		INNER JOIN __cursor_filter AS f
		ON f.message_type = s.message_type
		WHERE source_app_key = ?
		AND offset >= ?
		LIMIT 1`,
	), nil
}

// Get reads the next message at or above the given offset, using a prepared
// statement returned by Open().
//
// It returns false if the offset is beyond the end of the stream.
func (StreamDriver) Get(
	ctx context.Context,
	stmt *sql.Stmt,
	appKey string,
	offset uint64,
) (_ *persistence.StreamMessage, _ bool, err error) {
	defer sqlx.Recover(&err)

	row := stmt.QueryRowContext(
		ctx,
		appKey,
		offset,
	)

	m := persistence.StreamMessage{
		Envelope: &envelope.Envelope{},
	}

	var createdAt []byte

	ok := sqlx.TryScan(
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
	)

	m.Envelope.Source.Application.Key = appKey
	m.Envelope.CreatedAt = sqlx.UnmarshalTime(createdAt)

	if ok {
		return &m, true, nil
	}

	return nil, false, nil
}

package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/eventstore"
	"github.com/dogmatiq/infix/persistence/provider/sql/sqlite"
)

// Driver is used to interface with the underlying SQL database.
type Driver interface {
	// UpdateNextOffset increments the eventstore offset by n and returns the
	// new value.
	UpdateNextOffset(
		ctx context.Context,
		tx *sql.Tx,
		appKey string,
		n eventstore.Offset,
	) (eventstore.Offset, error)

	// InsertEvents saves events to the eventstore, starting at a specific
	// offset.
	InsertEvents(
		ctx context.Context,
		tx *sql.Tx,
		o eventstore.Offset,
		envelopes []*envelopespec.Envelope,
	) error

	// SelectEvents selects events from the eventstore that match the given
	// query.
	SelectEvents(
		ctx context.Context,
		db *sql.DB,
		appKey string,
		q eventstore.Query,
	) (*sql.Rows, error)

	// ScanEvent scans the next event from a row-set returned by SelectEvents().
	ScanEvent(rows *sql.Rows) (*eventstore.Event, error)
}

// NewDriver returns the appropriate driver to use with the given database.
func NewDriver(db *sql.DB) (Driver, error) {
	// if mysql.IsCompatibleWith(db) {
	// 	return &Driver{
	// 		mysql.StreamDriver{},
	// 	}, nil
	// }

	// if postgres.IsCompatibleWith(db) {
	// 	return &Driver{
	// 		postgres.StreamDriver{},
	// 	}, nil
	// }

	if sqlite.IsCompatibleWith(db) {
		return sqlite.Driver, nil
	}

	return nil, fmt.Errorf(
		"can not deduce the appropriate SQL driver for %T",
		db.Driver(),
	)
}

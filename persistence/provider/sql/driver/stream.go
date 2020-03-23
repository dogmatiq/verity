package driver

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/eventstream"
)

// StreamDriver is an interface used by sql.Stream to interface with the
// underlying database.
type StreamDriver interface {
	// FindFilter finds a filter by its hash and type names.
	FindFilter(
		ctx context.Context,
		db *sql.DB,
		hash []byte,
		names []string,
	) (uint64, bool, error)

	// CreateFilter creates a filter with the specified hash and type names.
	CreateFilter(
		ctx context.Context,
		db *sql.DB,
		hash []byte,
		names []string,
	) (uint64, error)

	// IncrementOffset increments an application stream's next offset by the
	// specified amount and returns the new value.
	IncrementOffset(
		ctx context.Context,
		tx *sql.Tx,
		appKey string,
		count eventstream.Offset,
	) (eventstream.Offset, error)

	// Append appends a single message to an application's stream.
	Append(
		ctx context.Context,
		tx *sql.Tx,
		offset eventstream.Offset,
		typename string,
		description string,
		env *envelope.Envelope,
	) error

	// Get returns the first event at or after a specific offset that matches a
	// specific filter.
	//
	// The driver may leave the source application and the dogma.Message fields
	// of the envelope empty, as they are populated by the sql.cursor
	// implementation.
	Get(
		ctx context.Context,
		db *sql.DB,
		appKey string,
		offset eventstream.Offset,
		filterID uint64,
	) (*eventstream.Event, bool, error)
}

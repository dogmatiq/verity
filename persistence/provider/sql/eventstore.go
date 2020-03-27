package sql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/eventstore"
)

// eventStoreDriver is the subset of the Driver interfaces that is concerned
// with the eventstore.
type eventStoreDriver interface {
	// UpdateNextOffset increments the eventstore offset by n and returns the
	// new value.
	UpdateNextOffset(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
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
		ak string,
		q eventstore.Query,
	) (*sql.Rows, error)

	// ScanEvent scans the next event from a row-set returned by SelectEvents().
	ScanEvent(
		rows *sql.Rows,
		ev *eventstore.Event,
	) error
}

// SaveEvents persists events in the application's event store.
//
// It returns the next free offset in the store.
func (t *transaction) SaveEvents(
	ctx context.Context,
	envelopes []*envelopespec.Envelope,
) (_ eventstore.Offset, err error) {
	if err := t.begin(ctx); err != nil {
		return 0, err
	}

	n := eventstore.Offset(
		len(envelopes),
	)

	next, err := t.ds.driver.UpdateNextOffset(
		ctx,
		t.actual,
		t.ds.appKey,
		n,
	)
	if err != nil {
		return 0, err
	}

	if err := t.ds.driver.InsertEvents(
		ctx,
		t.actual,
		next-n,
		envelopes,
	); err != nil {
		return 0, err
	}

	return next, nil
}

// eventStoreRepository is an implementation of eventstore.Repository that
// stores events in an SQL database.
type eventStoreRepository struct {
	db     *sql.DB
	driver Driver
	appKey string
}

// QueryEvents queries events in the repository.
func (r *eventStoreRepository) QueryEvents(
	ctx context.Context,
	q eventstore.Query,
) (eventstore.Result, error) {
	rows, err := r.driver.SelectEvents(
		ctx,
		r.db,
		r.appKey,
		q,
	)
	if err != nil {
		return nil, err
	}

	return &eventStoreResult{
		rows:   rows,
		driver: r.driver,
	}, nil
}

// eventStoreResult is an implementation of eventstore.Result for the SQL event
// store.
type eventStoreResult struct {
	rows   *sql.Rows
	driver Driver
}

// Next returns the next event in the result.
//
// It returns false if the are no more events in the result.
func (r *eventStoreResult) Next(
	ctx context.Context,
) (*eventstore.Event, bool, error) {
	if ctx.Err() != nil {
		return nil, false, ctx.Err()
	}

	if r.rows.Next() {
		ev := &eventstore.Event{
			Envelope: &envelopespec.Envelope{
				MetaData: &envelopespec.MetaData{
					Source: &envelopespec.Source{
						Application: &envelopespec.Identity{},
						Handler:     &envelopespec.Identity{},
					},
				},
			},
		}

		err := r.driver.ScanEvent(r.rows, ev)

		return ev, true, err
	}

	return nil, false, nil
}

// Close closes the cursor.
func (r *eventStoreResult) Close() error {
	return r.rows.Close()
}

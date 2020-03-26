package sql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence/eventstore"
)

// SaveEvents persists events in the application's event store.
//
// It returns the next free offset in the store.
func (t *transaction) SaveEvents(
	ctx context.Context,
	envelopes []*envelopespec.Envelope,
) (_ eventstore.Offset, err error) {
	defer sqlx.Recover(&err)

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
	rows, err := r.driver.SelectEvents(ctx, r.db, r.appKey, q)
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
	if r.rows.Next() {
		ev, err := r.driver.ScanEvent(r.rows)
		return ev, true, err
	}

	return nil, false, nil
}

// Close closes the cursor.
func (r *eventStoreResult) Close() error {
	return r.rows.Close()
}

package sql

import (
	"context"
	"database/sql"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"go.uber.org/multierr"
)

// eventStoreDriver is the subset of the Driver interface that is concerned
// with the eventstore subsystem.
type eventStoreDriver interface {
	// UpdateNextOffset increments the eventstore offset by 1 and returns the
	// new value.
	UpdateNextOffset(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
	) (eventstore.Offset, error)

	// InsertEvent saves an event to the eventstore at a specific offset.
	InsertEvent(
		ctx context.Context,
		tx *sql.Tx,
		o eventstore.Offset,
		env *envelopespec.Envelope,
	) error

	// InsertEventFilter inserts a filter that limits selected events to those
	// with a portable name in the given set.
	//
	// It returns the filter's ID.
	InsertEventFilter(
		ctx context.Context,
		db *sql.DB,
		ak string,
		f eventstore.Filter,
	) (int64, error)

	// DeleteEventFilter deletes an event filter.
	//
	// f is the filter ID, as returned by InsertEventFilter().
	DeleteEventFilter(
		ctx context.Context,
		db *sql.DB,
		f int64,
	) error

	// PurgeEventFilters deletes all event filters for the given application.
	PurgeEventFilters(
		ctx context.Context,
		db *sql.DB,
		ak string,
	) error

	// SelectEvents selects events from the eventstore that match the given
	// query.
	//
	// f is a filter ID, as returned by InsertEventFilter(). If the query does
	// not use a filter, f is zero.
	SelectEvents(
		ctx context.Context,
		db *sql.DB,
		ak string,
		q eventstore.Query,
		f int64,
	) (*sql.Rows, error)

	// ScanEvent scans the next event from a row-set returned by SelectEvents().
	ScanEvent(
		rows *sql.Rows,
		i *eventstore.Item,
	) error
}

// SaveEvent persists an event in the application's event store.
//
// It returns the event's offset.
func (t *transaction) SaveEvent(
	ctx context.Context,
	env *envelopespec.Envelope,
) (_ eventstore.Offset, err error) {
	if err := t.begin(ctx); err != nil {
		return 0, err
	}

	next, err := t.ds.driver.UpdateNextOffset(
		ctx,
		t.actual,
		t.ds.appKey,
	)
	if err != nil {
		return 0, err
	}

	next--

	return next, t.ds.driver.InsertEvent(
		ctx,
		t.actual,
		next,
		env,
	)
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
	var filterID int64

	if len(q.Filter) > 0 {
		var err error
		filterID, err = r.driver.InsertEventFilter(
			ctx,
			r.db,
			r.appKey,
			q.Filter,
		)
		if err != nil {
			return nil, err
		}
	}

	rows, err := r.driver.SelectEvents(
		ctx,
		r.db,
		r.appKey,
		q,
		filterID,
	)
	if err != nil {
		return nil, err
	}

	return &eventStoreResult{
		db:       r.db,
		rows:     rows,
		driver:   r.driver,
		filterID: filterID,
	}, nil
}

// closeTimeout is maximum duration that a call to eventStoreResult.Close() can
// take to delete the event filter.
const closeTimeout = 1 * time.Second

// eventStoreResult is an implementation of eventstore.Result for the SQL event
// store.
type eventStoreResult struct {
	db       *sql.DB
	rows     *sql.Rows
	driver   Driver
	filterID int64
}

// Next returns the next event in the result.
//
// It returns false if the are no more events in the result.
func (r *eventStoreResult) Next(
	ctx context.Context,
) (*eventstore.Item, bool, error) {
	if ctx.Err() != nil {
		return nil, false, ctx.Err()
	}

	if r.rows.Next() {
		i := &eventstore.Item{
			Envelope: &envelopespec.Envelope{
				MetaData: &envelopespec.MetaData{
					Source: &envelopespec.Source{
						Application: &envelopespec.Identity{},
						Handler:     &envelopespec.Identity{},
					},
				},
			},
		}

		err := r.driver.ScanEvent(r.rows, i)

		return i, true, err
	}

	return nil, false, nil
}

// Close closes the cursor.
func (r *eventStoreResult) Close() error {
	err := r.rows.Close()

	if r.filterID != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		defer cancel()

		err = multierr.Append(
			err,
			r.driver.DeleteEventFilter(
				ctx,
				r.db,
				r.filterID,
			),
		)
	}

	return err
}

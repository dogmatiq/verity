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
	) (uint64, error)

	// InsertEvent saves an event to the eventstore at a specific offset.
	InsertEvent(
		ctx context.Context,
		tx *sql.Tx,
		o uint64,
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

	// SelectNextEventOffset selects the next "unused" offset from the
	// eventstore.
	SelectNextEventOffset(
		ctx context.Context,
		db *sql.DB,
		ak string,
	) (uint64, error)

	// SelectEventsByType selects events from the eventstore that match the
	// given event types query.
	//
	// f is a filter ID, as returned by InsertEventFilter(). If the query does
	// not use a filter, f is zero.
	SelectEventsByType(
		ctx context.Context,
		db *sql.DB,
		ak string,
		q eventstore.Query,
		f int64,
	) (*sql.Rows, error)

	// SelectEventsBySource selects events from the eventstore that match the
	// given source, namely the source's key and id. o is an optional offset
	// parameter from which the messages should be loaded.
	SelectEventsBySource(
		ctx context.Context,
		db *sql.DB,
		ak, hk, id string,
		o uint64,
	) (*sql.Rows, error)

	// SelectOffsetByMessageID selects the offset of the message with the given
	// ID.
	SelectOffsetByMessageID(
		ctx context.Context,
		db *sql.DB,
		id string,
	) (uint64, error)

	// ScanEvent scans the next event from a row-set returned by
	// SelectEventsByType(), SelectEventsBySource(), and
	// SelectEventsBySourceAfterMessage().
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
) (_ uint64, err error) {
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

	if err := t.ds.driver.InsertEvent(
		ctx,
		t.actual,
		next,
		env,
	); err != nil {
		return 0, err
	}

	t.result.EventStoreItems = append(
		t.result.EventStoreItems,
		&eventstore.Item{
			Offset:   next,
			Envelope: env,
		},
	)

	return next, nil
}

// eventStoreRepository is an implementation of eventstore.Repository that
// stores events in an SQL database.
type eventStoreRepository struct {
	db     *sql.DB
	driver Driver
	appKey string
}

// NextEventOffset returns the next "unused" offset within the store.
func (r *eventStoreRepository) NextEventOffset(
	ctx context.Context,
) (uint64, error) {
	return r.driver.SelectNextEventOffset(
		ctx,
		r.db,
		r.appKey,
	)
}

// LoadEventsBySource loads the events produced by the specified source with
// the given handler key and id.
//
// d is the optional parameter, it represents ID of the message that was
// recorded when the instance was last destroyed.
func (r *eventStoreRepository) LoadEventsBySource(
	ctx context.Context,
	hk, id, d string,
) (eventstore.Result, error) {
	var o uint64

	if d != "" {
		var err error

		o, err = r.driver.SelectOffsetByMessageID(
			ctx,
			r.db,
			d,
		)
		if err != nil {
			return nil, err
		}

		// increment the offset to select all messages after the barier 'd'
		// message exclusively.
		o++
	}

	rows, err := r.driver.SelectEventsBySource(
		ctx,
		r.db,
		r.appKey, hk, id, o,
	)

	return &eventStoreResult{
		db:     r.db,
		rows:   rows,
		driver: r.driver,
	}, err
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

	rows, err := r.driver.SelectEventsByType(
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

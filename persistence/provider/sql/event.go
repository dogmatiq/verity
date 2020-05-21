package sql

import (
	"context"
	"database/sql"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence"
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
		f map[string]struct{},
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
	// given query.
	//
	// f is a filter ID, as returned by InsertEventFilter(). o is the minimum
	// offset to include in the results.
	SelectEventsByType(
		ctx context.Context,
		db *sql.DB,
		ak string,
		f int64,
		o uint64,
	) (*sql.Rows, error)

	// SelectEventsBySource selects events from the eventstore that were
	// produced by a specific handler.
	SelectEventsBySource(
		ctx context.Context,
		db *sql.DB,
		ak, hk, id string,
		o uint64,
	) (*sql.Rows, error)

	// SelectOffsetByMessageID selects the offset of the message with the given
	// ID. It returns false as a second return value if the message cannot be
	// found.
	SelectOffsetByMessageID(
		ctx context.Context,
		db *sql.DB,
		id string,
	) (uint64, bool, error)

	// ScanEvent scans the next event from a row-set returned by
	// SelectEventsByType() and SelectEventsBySource().
	ScanEvent(
		rows *sql.Rows,
		i *eventstore.Item,
	) error
}

// NextEventOffset returns the next "unused" offset within the store.
func (ds *dataStore) NextEventOffset(
	ctx context.Context,
) (uint64, error) {
	return ds.driver.SelectNextEventOffset(ctx, ds.db, ds.appKey)
}

// LoadEventsByType loads events that match a specific set of message types.
//
// f is the set of message types to include in the result. The keys of f are
// the "portable type name" produced when the events are marshaled.
//
// o specifies the (inclusive) lower-bound of the offset range to include in
// the results.
func (ds *dataStore) LoadEventsByType(
	ctx context.Context,
	f map[string]struct{},
	o uint64,
) (eventstore.Result, error) {
	filterID, err := ds.driver.InsertEventFilter(
		ctx,
		ds.db,
		ds.appKey,
		f,
	)
	if err != nil {
		return nil, err
	}

	rows, err := ds.driver.SelectEventsByType(
		ctx,
		ds.db,
		ds.appKey,
		filterID,
		o,
	)
	if err != nil {
		return nil, err
	}

	return &eventResult{
		db:       ds.db,
		rows:     rows,
		driver:   ds.driver,
		filterID: filterID,
	}, nil
}

// LoadEventsBySource loads the events produced by a specific handler.
//
// hk is the handler's identity key.
//
// id is the instance ID, which must be empty if the handler type does not
// use instances.
//
// m is ID of a "barrier" message. If supplied, the results are limited to
// events with higher offsets than the barrier message. If the message
// cannot be found, UnknownMessageError is returned.
func (ds *dataStore) LoadEventsBySource(
	ctx context.Context,
	hk, id, m string,
) (eventstore.Result, error) {
	var offset uint64

	if m != "" {
		o, ok, err := ds.driver.SelectOffsetByMessageID(
			ctx,
			ds.db,
			m,
		)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, eventstore.UnknownMessageError{
				MessageID: m,
			}
		}

		offset = o + 1 // start with the message AFTER the barrier message.
	}

	rows, err := ds.driver.SelectEventsBySource(
		ctx,
		ds.db,
		ds.appKey,
		hk,
		id,
		offset,
	)

	return &eventResult{
		db:     ds.db,
		rows:   rows,
		driver: ds.driver,
	}, err
}

// closeTimeout is maximum duration that a call to eventResult.Close() can
// take to delete the event filter.
const closeTimeout = 1 * time.Second

// eventResult is an implementation of eventstore.Result for the SQL event
// store.
type eventResult struct {
	db       *sql.DB
	rows     *sql.Rows
	driver   Driver
	filterID int64
}

// Next returns the next event in the result.
//
// It returns false if the are no more events in the result.
func (r *eventResult) Next(
	ctx context.Context,
) (*eventstore.Item, bool, error) {
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
func (r *eventResult) Close() error {
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

// VisitSaveEvent applies the changes in a "SaveEvent" operation to the
// database.
func (c *committer) VisitSaveEvent(
	ctx context.Context,
	op persistence.SaveEvent,
) error {
	offset, err := c.driver.UpdateNextOffset(
		ctx,
		c.tx,
		c.appKey,
	)
	if err != nil {
		return err
	}

	offset--

	if err := c.driver.InsertEvent(
		ctx,
		c.tx,
		offset,
		op.Envelope,
	); err != nil {
		return err
	}

	if c.result.EventOffsets == nil {
		c.result.EventOffsets = map[string]uint64{}
	}

	c.result.EventOffsets[op.Envelope.MetaData.MessageId] = offset

	return nil
}

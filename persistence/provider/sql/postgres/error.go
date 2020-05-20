package postgres

import (
	"context"
	"database/sql"
	"errors"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/lib/pq"
)

// convertContextErrors converts PostgreSQL "query_canceled" errors into a
// context.Canceled or DeadlineExceeeded error.
//
// See https://github.com/dogmatiq/infix/issues/35.
func convertContextErrors(ctx context.Context, err error) error {
	if e, ok := unwrapError(err); ok {
		if e.Code.Name() == "query_canceled" {
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}

	return err
}

// unwrapError returns a *pq.Error if err is either a pq.Error or *pq.Error.
//
// It appears as through *pq.Error is returned from the methods of the native
// SQL driver, however the Error() method has a non-pointer receiver, so a
// pq.Error (non-pointer) also satisfies the Error interface.
func unwrapError(err error) (*pq.Error, bool) {
	e := &pq.Error{}

	if errors.As(err, e) ||
		errors.As(err, &e) {
		return e, true
	}

	return nil, false
}

// errorConverter is an implementation of persistence.Driver that decorates the
// PostgreSQL driver in order to convert native "query_canceled" errors into
// regular context.Canceled / DeadlineExceeded errors.
//
// The error conversion is implemented this way so that conversions don't get
// missed when new methods are added to the persistence.Driver interface.
type errorConverter struct {
	d driver
}

func (d errorConverter) Begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	tx, err := d.d.Begin(ctx, db)
	return tx, convertContextErrors(ctx, err)
}

func (d errorConverter) LockApplication(
	ctx context.Context,
	db *sql.DB,
	ak string,
) (func() error, error) {
	r, err := d.d.LockApplication(ctx, db, ak)
	return r, convertContextErrors(ctx, err)
}

//
// aggregate
//

func (d errorConverter) InsertAggregateMetaData(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	md *persistence.AggregateMetaData,
) (bool, error) {
	ok, err := d.d.InsertAggregateMetaData(ctx, tx, ak, md)
	return ok, convertContextErrors(ctx, err)
}

func (d errorConverter) UpdateAggregateMetaData(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	md *persistence.AggregateMetaData,
) (bool, error) {
	ok, err := d.d.UpdateAggregateMetaData(ctx, tx, ak, md)
	return ok, convertContextErrors(ctx, err)
}

func (d errorConverter) SelectAggregateMetaData(
	ctx context.Context,
	db *sql.DB,
	ak, hk, id string,
) (*persistence.AggregateMetaData, error) {
	md, err := d.d.SelectAggregateMetaData(ctx, db, ak, hk, id)
	return md, convertContextErrors(ctx, err)
}

//
// eventstore
//

func (d errorConverter) UpdateNextOffset(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
) (uint64, error) {
	o, err := d.d.UpdateNextOffset(ctx, tx, ak)
	return o, convertContextErrors(ctx, err)
}

func (d errorConverter) InsertEvent(
	ctx context.Context,
	tx *sql.Tx,
	o uint64,
	env *envelopespec.Envelope,
) error {
	err := d.d.InsertEvent(ctx, tx, o, env)
	return convertContextErrors(ctx, err)
}

func (d errorConverter) InsertEventFilter(
	ctx context.Context,
	db *sql.DB,
	ak string,
	f eventstore.Filter,
) (int64, error) {
	id, err := d.d.InsertEventFilter(ctx, db, ak, f)
	return id, convertContextErrors(ctx, err)
}

func (d errorConverter) DeleteEventFilter(
	ctx context.Context,
	db *sql.DB,
	f int64,
) error {
	err := d.d.DeleteEventFilter(ctx, db, f)
	return convertContextErrors(ctx, err)
}

func (d errorConverter) PurgeEventFilters(
	ctx context.Context,
	db *sql.DB,
	ak string,
) error {
	err := d.d.PurgeEventFilters(ctx, db, ak)
	return convertContextErrors(ctx, err)
}

func (d errorConverter) SelectNextEventOffset(
	ctx context.Context,
	db *sql.DB,
	ak string,
) (uint64, error) {
	next, err := d.d.SelectNextEventOffset(ctx, db, ak)
	return next, convertContextErrors(ctx, err)
}

func (d errorConverter) SelectEventsByType(
	ctx context.Context,
	db *sql.DB,
	ak string,
	q eventstore.Query,
	f int64,
) (*sql.Rows, error) {
	rows, err := d.d.SelectEventsByType(ctx, db, ak, q, f)
	return rows, convertContextErrors(ctx, err)
}

func (d errorConverter) SelectEventsBySource(
	ctx context.Context,
	db *sql.DB,
	ak, hk, id string,
	o uint64,
) (*sql.Rows, error) {
	rows, err := d.d.SelectEventsBySource(ctx, db, ak, hk, id, o)
	return rows, convertContextErrors(ctx, err)
}

func (d errorConverter) SelectOffsetByMessageID(
	ctx context.Context,
	db *sql.DB,
	id string,
) (uint64, bool, error) {
	o, ok, err := d.d.SelectOffsetByMessageID(ctx, db, id)
	return o, ok, convertContextErrors(ctx, err)
}

func (d errorConverter) ScanEvent(
	rows *sql.Rows,
	i *eventstore.Item,
) error {
	return d.d.ScanEvent(rows, i)
}

//
// offsetstore
//

func (d errorConverter) LoadOffset(
	ctx context.Context,
	db *sql.DB,
	ak, sk string,
) (uint64, error) {
	o, err := d.d.LoadOffset(ctx, db, ak, sk)
	return o, convertContextErrors(ctx, err)
}

func (d errorConverter) InsertOffset(
	ctx context.Context,
	tx *sql.Tx,
	ak, sk string,
	n uint64,
) (bool, error) {
	ok, err := d.d.InsertOffset(ctx, tx, ak, sk, n)
	return ok, convertContextErrors(ctx, err)
}

func (d errorConverter) UpdateOffset(
	ctx context.Context,
	tx *sql.Tx,
	ak, sk string,
	c, n uint64,
) (bool, error) {
	ok, err := d.d.UpdateOffset(ctx, tx, ak, sk, c, n)
	return ok, convertContextErrors(ctx, err)
}

//
// queue
//

func (d errorConverter) InsertQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	i *queuestore.Item,
) (bool, error) {
	ok, err := d.d.InsertQueueMessage(ctx, tx, ak, i)
	return ok, convertContextErrors(ctx, err)
}

func (d errorConverter) UpdateQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	i *queuestore.Item,
) (bool, error) {
	ok, err := d.d.UpdateQueueMessage(ctx, tx, ak, i)
	return ok, convertContextErrors(ctx, err)
}

func (d errorConverter) DeleteQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	i *queuestore.Item,
) (bool, error) {
	ok, err := d.d.DeleteQueueMessage(ctx, tx, ak, i)
	return ok, convertContextErrors(ctx, err)
}

func (d errorConverter) SelectQueueMessages(
	ctx context.Context,
	db *sql.DB,
	ak string,
	n int,
) (*sql.Rows, error) {
	rows, err := d.d.SelectQueueMessages(ctx, db, ak, n)
	return rows, convertContextErrors(ctx, err)
}

func (d errorConverter) ScanQueueMessage(
	rows *sql.Rows,
	i *queuestore.Item,
) error {
	return d.d.ScanQueueMessage(rows, i)
}

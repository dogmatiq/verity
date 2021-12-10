package postgres

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/dogmatiq/interopspec/envelopespec"
	"github.com/dogmatiq/verity/persistence"
)

// convertContextErrors converts PostgreSQL "query_canceled" errors into a
// context.Canceled or DeadlineExceeeded error.
//
// The "pq" postgres driver appears to prefer returning its own error if the
// context is canceled after a query is already started.
//
// See https://github.com/lib/pq/blob/master/go18_test.go#L90
// See https://github.com/dogmatiq/verity/issues/35.
func convertContextErrors(ctx context.Context, err error) error {
	if err != nil && ctx.Err() != nil {
		if strings.Contains(err.Error(), "canceling statement due to user request") {
			return ctx.Err()
		}
	}

	return err
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

func (d errorConverter) IsCompatibleWith(ctx context.Context, db *sql.DB) error {
	err := d.d.IsCompatibleWith(ctx, db)
	return convertContextErrors(ctx, err)
}

func (d errorConverter) Begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	tx, err := d.d.Begin(ctx, db)
	return tx, convertContextErrors(ctx, err)
}

func (d errorConverter) CreateSchema(ctx context.Context, db *sql.DB) error {
	err := d.d.CreateSchema(ctx, db)
	return convertContextErrors(ctx, err)
}

func (d errorConverter) DropSchema(ctx context.Context, db *sql.DB) error {
	err := d.d.DropSchema(ctx, db)
	return convertContextErrors(ctx, err)
}

//
// lock
//

func (d errorConverter) AcquireLock(
	ctx context.Context,
	db *sql.DB,
	ak string,
	ttl time.Duration,
) (int64, bool, error) {
	id, ok, err := d.d.AcquireLock(ctx, db, ak, ttl)
	return id, ok, convertContextErrors(ctx, err)
}

func (d errorConverter) RenewLock(
	ctx context.Context,
	db *sql.DB,
	id int64,
	ttl time.Duration,
) (bool, error) {
	ok, err := d.d.RenewLock(ctx, db, id, ttl)
	return ok, convertContextErrors(ctx, err)
}

func (d errorConverter) ReleaseLock(
	ctx context.Context,
	db *sql.DB,
	id int64,
) error {
	err := d.d.ReleaseLock(ctx, db, id)
	return convertContextErrors(ctx, err)
}

//
// aggregate
//

func (d errorConverter) InsertAggregateMetaData(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	md persistence.AggregateMetaData,
) (bool, error) {
	ok, err := d.d.InsertAggregateMetaData(ctx, tx, ak, md)
	return ok, convertContextErrors(ctx, err)
}

func (d errorConverter) UpdateAggregateMetaData(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	md persistence.AggregateMetaData,
) (bool, error) {
	ok, err := d.d.UpdateAggregateMetaData(ctx, tx, ak, md)
	return ok, convertContextErrors(ctx, err)
}

func (d errorConverter) SelectAggregateMetaData(
	ctx context.Context,
	db *sql.DB,
	ak, hk, id string,
) (persistence.AggregateMetaData, error) {
	md, err := d.d.SelectAggregateMetaData(ctx, db, ak, hk, id)
	return md, convertContextErrors(ctx, err)
}

func (d errorConverter) InsertAggregateSnapshot(
	ctx context.Context,
	tx *sql.Tx,
	ak string, md persistence.AggregateSnapshot,
) (bool, error) {
	ok, err := d.d.InsertAggregateSnapshot(ctx, tx, ak, md)
	return ok, convertContextErrors(ctx, err)
}

func (d errorConverter) UpdateAggregateSnapshot(
	ctx context.Context,
	tx *sql.Tx,
	ak string, md persistence.AggregateSnapshot,
) (bool, error) {
	ok, err := d.d.UpdateAggregateSnapshot(ctx, tx, ak, md)
	return ok, convertContextErrors(ctx, err)
}

func (d errorConverter) SelectAggregateSnapshot(
	ctx context.Context,
	db *sql.DB,
	ak, hk, id string,
) (persistence.AggregateSnapshot, bool, error) {
	ss, ok, err := d.d.SelectAggregateSnapshot(ctx, db, ak, hk, id)
	return ss, ok, convertContextErrors(ctx, err)
}

func (d errorConverter) DeleteAggregateSnapshot(ctx context.Context, tx *sql.Tx, ak string, inst persistence.AggregateSnapshot) (bool, error) {
	ok, err := d.d.DeleteAggregateSnapshot(ctx, tx, ak, inst)
	return ok, convertContextErrors(ctx, err)
}

//
// event
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
	f map[string]struct{},
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
	f int64,
	o uint64,
) (*sql.Rows, error) {
	rows, err := d.d.SelectEventsByType(ctx, db, ak, f, o)
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
	ev *persistence.Event,
) error {
	return d.d.ScanEvent(rows, ev)
}

//
// offset
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
// process
//

func (d errorConverter) InsertProcessInstance(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	inst persistence.ProcessInstance,
) (bool, error) {
	ok, err := d.d.InsertProcessInstance(ctx, tx, ak, inst)
	return ok, convertContextErrors(ctx, err)
}

func (d errorConverter) UpdateProcessInstance(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	inst persistence.ProcessInstance,
) (bool, error) {
	ok, err := d.d.UpdateProcessInstance(ctx, tx, ak, inst)
	return ok, convertContextErrors(ctx, err)
}

func (d errorConverter) DeleteProcessInstance(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	inst persistence.ProcessInstance,
) (bool, error) {
	ok, err := d.d.DeleteProcessInstance(ctx, tx, ak, inst)
	return ok, convertContextErrors(ctx, err)
}

func (d errorConverter) SelectProcessInstance(
	ctx context.Context,
	db *sql.DB,
	ak, hk, id string,
) (persistence.ProcessInstance, error) {
	inst, err := d.d.SelectProcessInstance(ctx, db, ak, hk, id)
	return inst, convertContextErrors(ctx, err)
}

//
// queue
//

func (d errorConverter) InsertQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	m persistence.QueueMessage,
) (bool, error) {
	ok, err := d.d.InsertQueueMessage(ctx, tx, ak, m)
	return ok, convertContextErrors(ctx, err)
}

func (d errorConverter) UpdateQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	m persistence.QueueMessage,
) (bool, error) {
	ok, err := d.d.UpdateQueueMessage(ctx, tx, ak, m)
	return ok, convertContextErrors(ctx, err)
}

func (d errorConverter) DeleteQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	m persistence.QueueMessage,
) (bool, error) {
	ok, err := d.d.DeleteQueueMessage(ctx, tx, ak, m)
	return ok, convertContextErrors(ctx, err)
}

func (d errorConverter) DeleteQueueTimeoutMessagesByProcessInstance(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	inst persistence.ProcessInstance,
) error {
	err := d.d.DeleteQueueTimeoutMessagesByProcessInstance(ctx, tx, ak, inst)
	return convertContextErrors(ctx, err)
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
	m *persistence.QueueMessage,
) error {
	return d.d.ScanQueueMessage(rows, m)
}

package boltdb

import (
	"context"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/eventstore"
	"go.etcd.io/bbolt"
)

// transaction is an implementation of persistence.Transaction for BoltDB
// data stores.
type transaction struct {
	db      *database
	appKey  []byte
	actual  *bbolt.Tx
	release func()
}

// SaveEvents persists events in the application's event store.
//
// It returns the next free offset in the store.
func (t *transaction) SaveEvents(
	ctx context.Context,
	envelopes []*envelopespec.Envelope,
) (_ eventstore.Offset, err error) {
	defer bboltx.Recover(&err)

	t.begin(ctx)

	store := bboltx.CreateBucketIfNotExists(
		t.actual,
		t.appKey,
		eventStoreBucketKey,
	)

	events := bboltx.CreateBucketIfNotExists(
		store,
		eventsBucketKey,
	)

	o := loadNextOffset(store)
	o = saveEvents(events, o, envelopes)
	storeNextOffset(store, o)

	return o, nil
}

// Commit applies the changes from the transaction.
func (t *transaction) Commit(ctx context.Context) (err error) {
	defer bboltx.Recover(&err)

	if t.actual != nil {
		err = t.actual.Commit()
	}

	t.end()

	return err
}

// Rollback aborts the transaction.
func (t *transaction) Rollback() (err error) {
	defer bboltx.Recover(&err)

	if t.actual != nil {
		err = t.actual.Rollback()
	}

	t.end()

	return err
}

// begin starts the actual BoltDB transaction.
func (t *transaction) begin(ctx context.Context) {
	if t.release == nil {
		bboltx.Must(persistence.ErrTransactionClosed)
	}

	if t.actual == nil {
		t.actual = t.db.Begin(ctx)
	}
}

// end releases the database lock, if held, and notifies the data-store that the
// transaction has ended.
func (t *transaction) end() {
	if t.release == nil {
		bboltx.Must(persistence.ErrTransactionClosed)
	}

	if t.actual != nil {
		t.db.End()
		t.actual = nil
	}

	fn := t.release
	t.release = nil

	fn()
}

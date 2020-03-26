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
	ds     *dataStore
	appKey []byte
	actual *bbolt.Tx
}

// SaveEvents persists events in the application's event store.
//
// It returns the next free offset in the store.
func (t *transaction) SaveEvents(
	ctx context.Context,
	envelopes []*envelopespec.Envelope,
) (_ eventstore.Offset, err error) {
	defer bboltx.Recover(&err)

	if err := t.lock(ctx); err != nil {
		return 0, err
	}

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
	defer t.unlock()

	if t.ds == nil {
		return persistence.ErrTransactionClosed
	}

	if err := t.ds.checkOpen(); err != nil {
		return err
	}

	if t.actual != nil {
		bboltx.Commit(t.actual)
	}

	return nil
}

// Rollback aborts the transaction.
func (t *transaction) Rollback() (err error) {
	defer bboltx.Recover(&err)
	defer t.unlock()

	if t.ds == nil {
		return persistence.ErrTransactionClosed
	}

	if err := t.ds.checkOpen(); err != nil {
		return err
	}

	if t.actual != nil {
		return t.actual.Rollback()
	}

	return nil
}

// lock acquires a write-lock on the database and begins an actual BoltDB
// transaction.
func (t *transaction) lock(ctx context.Context) error {
	if t.ds.db == nil {
		return persistence.ErrTransactionClosed
	}

	if err := t.ds.checkOpen(); err != nil {
		return err
	}

	if t.actual == nil {
		t.actual = t.ds.db.Begin(ctx)
	}

	return nil
}

// unlock releases the database lock if it has been acquired, and marks the
// transaction as ended.
func (t *transaction) unlock() {
	if t.actual != nil {
		t.actual.Rollback()
		t.ds.db.End()
		t.actual = nil
	}

	t.ds = nil
}

package boltdb

import (
	"context"
	"sync"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence"
	"go.etcd.io/bbolt"
)

// transaction is an implementation of persistence.Transaction for BoltDB
// data stores.
type transaction struct {
	m      sync.Mutex
	tx     *bbolt.Tx
	bucket *bbolt.Bucket
	remove func()
}

// SaveEvents persists events in the application's event store.
//
// It returns the next unused on the stream.
func (t *transaction) SaveEvents(
	ctx context.Context,
	envelopes ...*envelope.Envelope,
) (err error) {
	defer bboltx.Recover(&err)

	b := bboltx.CreateBucketIfNotExists(t.bucket, eventStoreBucketKey)

	o := loadNextOffset(b)
	o = appendEvents(b, o, envelopes)
	storeNextOffset(b, o)

	return nil
}

// Commit applies the changes from the transaction.
func (t *transaction) Commit(ctx context.Context) error {
	t.m.Lock()
	defer t.m.Unlock()

	if t.tx == nil {
		return persistence.ErrDataStoreClosed
	}

	err := t.tx.Commit()

	t.remove()

	if err == bbolt.ErrTxClosed {
		err = persistence.ErrTransactionClosed
	}

	return err
}

// Rollback aborts the transaction.
func (t *transaction) Rollback() error {
	t.m.Lock()
	defer t.m.Unlock()

	if t.tx == nil {
		return persistence.ErrDataStoreClosed
	}

	err := t.tx.Rollback()

	t.remove()

	if err == bbolt.ErrTxClosed {
		err = persistence.ErrTransactionClosed
	}

	return err
}

// forceClose is called when the transaction's data-store is closed.
func (t *transaction) forceClose() error {
	t.m.Lock()
	defer t.m.Unlock()

	if t.tx == nil {
		return nil
	}

	tx := t.tx
	t.tx = nil

	return tx.Rollback()
}

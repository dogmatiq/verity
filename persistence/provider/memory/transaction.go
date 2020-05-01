package memory

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
)

// transaction is an implementation of persistence.Transaction for in-memory
// data stores.
type transaction struct {
	ds        *dataStore
	hasLock   bool
	aggregate aggregateStoreChangeSet
	event     eventStoreChangeSet
	offset    offsetStoreChangeSet
	queue     queueStoreChangeSet
}

// Commit applies the changes from the transaction.
func (t *transaction) Commit(
	ctx context.Context,
) (*persistence.TransactionResult, error) {
	defer t.end()

	if t.ds == nil {
		return nil, persistence.ErrTransactionClosed
	}

	if err := t.ds.checkOpen(); err != nil {
		return nil, err
	}

	if !t.hasLock {
		return nil, nil
	}

	t.ds.db.aggregate.apply(&t.aggregate)
	t.ds.db.event.apply(&t.event)
	t.ds.db.offset.apply(&t.offset)
	t.ds.db.queue.apply(&t.queue)

	return &persistence.TransactionResult{
		EventItems: t.event.items,
	}, nil
}

// Rollback aborts the transaction.
func (t *transaction) Rollback() error {
	defer t.end()

	if t.ds == nil {
		return persistence.ErrTransactionClosed
	}

	return t.ds.checkOpen()
}

// begin acquires a write-lock on the database.
func (t *transaction) begin(ctx context.Context) error {
	if t.ds == nil {
		return persistence.ErrTransactionClosed
	}

	if err := t.ds.checkOpen(); err != nil {
		return err
	}

	if t.hasLock {
		return nil
	}

	if err := t.ds.db.Lock(ctx); err != nil {
		return err
	}

	t.hasLock = true

	return nil
}

// end releases the database lock and marks the transaction as ended.
func (t *transaction) end() {
	if t.hasLock {
		t.ds.db.Unlock()
		t.hasLock = false
	}

	t.ds = nil
}

package memory

import (
	"context"
	"sync"

	"github.com/dogmatiq/infix/persistence"
)

// transaction is an implementation of persistence.Transaction for in-memory
// data stores.
type transaction struct {
	m  sync.Mutex
	ds *dataStore
}

// Commit applies the changes from the transaction.
func (t *transaction) Commit(ctx context.Context) error {
	t.m.Lock()
	defer t.m.Unlock()

	if t.ds == nil {
		return persistence.ErrTransactionClosed
	}

	_, err := t.ds.get()
	if err != nil {
		return err
	}

	t.ds = nil

	return nil
}

// Rollback aborts the transaction.
func (t *transaction) Rollback() error {
	t.m.Lock()
	defer t.m.Unlock()

	if t.ds == nil {
		return persistence.ErrTransactionClosed
	}

	t.ds = nil

	return nil
}

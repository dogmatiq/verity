package memory

import (
	"context"
	"errors"
	"sync"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/eventstore"
)

// transaction is an implementation of persistence.Transaction for in-memory
// data stores.
type transaction struct {
	m  sync.Mutex
	ds *dataStore
}

// SaveEvents persists events in the application's event store.
//
// It returns the next unused on the stream.
func (t *transaction) SaveEvents(
	ctx context.Context,
	envelopes ...*envelope.Envelope,
) (eventstore.Offset, error) {
	t.m.Lock()
	defer t.m.Unlock()

	if t.ds == nil {
		return 0, persistence.ErrTransactionClosed
	}

	return 0, errors.New("not implemented")
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

package memory

import (
	"context"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/eventstore"
)

// transaction is an implementation of persistence.Transaction for in-memory
// data stores.
type transaction struct {
	ds      *dataStore
	hasLock bool

	uncommitted struct {
		events []*envelopespec.Envelope
	}
}

// Commit applies the changes from the transaction.
func (t *transaction) Commit(ctx context.Context) error {
	defer t.end()

	if t.ds == nil {
		return persistence.ErrTransactionClosed
	}

	if err := t.ds.checkOpen(); err != nil {
		return err
	}

	if !t.hasLock {
		return nil
	}

	next := eventstore.Offset(
		len(t.ds.db.events),
	)

	for _, env := range t.uncommitted.events {
		t.ds.db.events = append(
			t.ds.db.events,
			eventstore.Event{
				Offset:   next,
				Envelope: env,
			},
		)

		next++
	}

	return nil
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

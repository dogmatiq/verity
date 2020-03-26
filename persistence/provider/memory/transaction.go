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

// SaveEvents persists events in the application's event store.
//
// It returns the next free offset in the store.
func (t *transaction) SaveEvents(
	ctx context.Context,
	envelopes []*envelopespec.Envelope,
) (eventstore.Offset, error) {
	if err := t.lock(ctx); err != nil {
		return 0, err
	}

	t.uncommitted.events = append(
		t.uncommitted.events,
		envelopes...,
	)

	return eventstore.Offset(
		len(t.ds.db.events) + len(t.uncommitted.events),
	), nil
}

// Commit applies the changes from the transaction.
func (t *transaction) Commit(ctx context.Context) error {
	defer t.unlock()

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
	defer t.unlock()

	if t.ds == nil {
		return persistence.ErrTransactionClosed
	}

	return t.ds.checkOpen()
}

// lock acquires a write-lock on the database.
func (t *transaction) lock(ctx context.Context) error {
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

// unlock releases the database lock if it has been acquired, and marks the
// transaction as ended.
func (t *transaction) unlock() {
	if t.hasLock {
		t.ds.db.Unlock()
		t.hasLock = false
	}

	t.ds = nil
}

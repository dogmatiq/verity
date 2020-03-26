package memory

import (
	"context"

	"github.com/dogmatiq/infix/persistence"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/eventstore"
)

// transaction is an implementation of persistence.Transaction for in-memory
// data stores.
type transaction struct {
	db      *database
	begun   bool
	release func()

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
	if err := t.begin(ctx); err != nil {
		return 0, err
	}

	t.uncommitted.events = append(
		t.uncommitted.events,
		envelopes...,
	)

	return eventstore.Offset(
		len(t.db.events) + len(t.uncommitted.events),
	), nil
}

// Commit applies the changes from the transaction.
func (t *transaction) Commit(ctx context.Context) error {
	if t.begun {
		next := eventstore.Offset(
			len(t.db.events),
		)

		for _, env := range t.uncommitted.events {
			t.db.events = append(
				t.db.events,
				eventstore.Event{
					Offset:   next,
					Envelope: env,
				},
			)

			next++
		}
	}

	return t.end()
}

// Rollback aborts the transaction.
func (t *transaction) Rollback() error {
	return t.end()
}

// begin starts the transaction by acquiring a write-lock on the database.
func (t *transaction) begin(ctx context.Context) error {
	if t.release == nil {
		return persistence.ErrTransactionClosed
	}

	if t.begun {
		return nil
	}

	if err := t.db.Lock(ctx); err != nil {
		return err
	}

	t.begun = true

	return nil
}

// end releases the database lock, if held, and notifies the data-store that the
// transaction has ended.
func (t *transaction) end() error {
	if t.release == nil {
		return persistence.ErrTransactionClosed
	}

	if t.begun {
		t.db.Unlock()
		t.begun = false
	}

	fn := t.release
	t.release = nil

	fn()

	return nil
}

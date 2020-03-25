package memory

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/eventstore"
)

// transaction is an implementation of persistence.Transaction for in-memory
// data stores.
type transaction struct {
	m      sync.Mutex
	ds     *dataStore
	events []*envelope.Envelope
}

// SaveEvents persists events in the application's event store.
//
// It returns the next unused on the stream.
func (t *transaction) SaveEvents(
	ctx context.Context,
	envelopes ...*envelope.Envelope,
) error {
	if err := t.lock(); err != nil {
		return err
	}
	defer t.unlock()

	t.events = append(t.events, envelopes...)

	return nil
}

// Commit applies the changes from the transaction.
func (t *transaction) Commit(ctx context.Context) error {
	t.m.Lock()
	defer t.m.Unlock()

	if t.ds == nil {
		return persistence.ErrTransactionClosed
	}

	db, err := t.ds.database()
	if err != nil {
		return err
	}

	db.m.Lock()
	defer db.m.Unlock()

	if len(t.events) > 0 {
		next := eventstore.Offset(len(db.events))

		for _, env := range t.events {
			db.events = append(
				db.events,
				&event{
					Type: message.TypeOf(env.Message),
					Event: eventstore.Event{
						Offset:   next,
						MetaData: env.MetaData,
						Packet:   env.Packet,
					},
				},
			)

			next++
		}
	}

	t.close()

	return nil
}

// Rollback aborts the transaction.
func (t *transaction) Rollback() error {
	t.m.Lock()
	defer t.m.Unlock()

	if t.ds == nil {
		return persistence.ErrTransactionClosed
	}

	t.close()

	return nil
}

func (t *transaction) lock() error {
	t.m.Lock()

	if t.ds == nil {
		t.m.Unlock()
		return persistence.ErrTransactionClosed
	}

	return nil
}

func (t *transaction) unlock() {
	t.m.Unlock()
}

func (t *transaction) close() {
	t.events = nil
	t.ds = nil
}

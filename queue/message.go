package queue

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
)

// Message represents a message that has been popped from a Queue.
type Message struct {
	queue *Queue
	elem  *elem
	tx    persistence.Transaction
	done  bool
}

// ID returns the ID of the message.
//
// The ID is available even if the complete message envelope can not be
// unmarshaled.
func (m *Message) ID() string {
	return m.elem.persisted.ID()
}

// FailureCount returns the number of times this message has already been
// attempted, not including this attempt.
func (m *Message) FailureCount() uint {
	return m.elem.persisted.FailureCount
}

// Envelope returns the envelope containing the message to be handled.
func (m *Message) Envelope() (*envelope.Envelope, error) {
	var err error

	if m.elem.memory == nil {
		m.elem.memory, err = envelope.Unmarshal(
			m.queue.Marshaler,
			m.elem.persisted.Envelope,
		)
	}

	return m.elem.memory, err
}

// Tx returns the transaction under which the message must be handled, starting
// it if necessary.
func (m *Message) Tx(ctx context.Context) (persistence.ManagedTransaction, error) {
	if m.tx == nil {
		tx, err := m.queue.DataStore.Begin(ctx)
		if err != nil {
			return nil, err
		}

		m.tx = tx
	}

	return m.tx, nil
}

// Ack acknowledges successful handling of the message.
//
// It commits the changes performed in the message's transaction.
func (m *Message) Ack(ctx context.Context) error {
	_, err := m.Tx(ctx)
	if err != nil {
		return err
	}

	if err := m.tx.RemoveMessageFromQueue(ctx, m.elem.persisted); err != nil {
		return err
	}

	if err := m.tx.Commit(ctx); err != nil {
		return err
	}

	m.done = true
	m.elem.persisted.Revision = 0

	return nil
}

// Nack indicates an error while handling the message.
//
// It discards the changes performed in the message's transaction and defers
// handling of the message until n.
func (m *Message) Nack(ctx context.Context, n time.Time) error {
	if m.tx != nil {
		if err := m.tx.Rollback(); err != nil {
			return err
		}
	}

	m.done = true
	m.elem.persisted.FailureCount++
	m.elem.persisted.NextAttemptAt = n

	if err := persistence.WithTransaction(
		ctx,
		m.queue.DataStore,
		func(tx persistence.ManagedTransaction) error {
			return tx.SaveMessageToQueue(ctx, m.elem.persisted)
		},
	); err != nil {
		return err
	}

	m.elem.persisted.Revision++

	return nil
}

// Close releases the message.
//
// It must be called regardless of whether Ack() or Nack() are called.
func (m *Message) Close() error {
	if m.elem == nil {
		return nil
	}

	m.queue.notify(m.elem)
	m.elem = nil

	if m.tx != nil && !m.done {
		return m.tx.Rollback()
	}

	return nil
}

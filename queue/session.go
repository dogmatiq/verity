package queue

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
)

// A Session encapsulates an atomic application state change brought about by a
// single queued message.
type Session struct {
	tx   persistence.Transaction
	elem *elem
}

// Tx returns the transaction under which the message must be handled.
func (m *Session) Tx() persistence.ManagedTransaction {
	return m.tx
}

// Envelope returns the envelope containing the message to be handled.
//
// TODO: defer unmarshaling to the session so that a failure is visible to the
// logic that has access to the backoff strategy.
func (m *Session) Envelope() *envelope.Envelope {
	return m.elem.env
}

// Commit commits the changes performed in the session.
func (m *Session) Commit(ctx context.Context) error {
	return errors.New("not implemented")
}

// Rollback finalizes the session after a failure handling the message.
// The message is re-queued for be attempted at n.
func (m *Session) Rollback(ctx context.Context, n time.Time) error {
	return errors.New("not implemented")
}

// Close releases the message.
//
// It must be called regardless of whether Ack() or Nack() are called.
func (m *Session) Close() error {
	return m.tx.Rollback()
}

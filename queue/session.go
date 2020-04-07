package queue

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
)

// A Session encapsulates an atomic application state change brought about by a
// single queued message.
type Session struct {
	queue *Queue
	elem  *elem
	tx    persistence.Transaction
	done  bool
}

// MessageID returns the ID of the message that is handled within the session.
func (s *Session) MessageID() string {
	return s.elem.message.ID()
}

// Tx returns the transaction under which the message must be handled, starting
// it if necessary.
func (s *Session) Tx(ctx context.Context) (persistence.ManagedTransaction, error) {
	if s.tx == nil {
		tx, err := s.queue.DataStore.Begin(ctx)
		if err != nil {
			return nil, err
		}

		s.tx = tx
	}

	return s.tx, nil
}

// Envelope returns the envelope containing the message to be handled.
func (s *Session) Envelope() (*envelope.Envelope, error) {
	var err error

	if s.elem.env == nil {
		s.elem.env, err = envelope.Unmarshal(s.queue.Marshaler, s.elem.message.Envelope)
	}

	return s.elem.env, err
}

// Commit commits the changes performed in the session.
func (s *Session) Commit(ctx context.Context) error {
	_, err := s.Tx(ctx)
	if err != nil {
		return err
	}

	if err := s.tx.RemoveMessageFromQueue(ctx, s.elem.message); err != nil {
		return err
	}

	if err := s.tx.Commit(ctx); err != nil {
		return err
	}

	s.done = true
	s.elem.message.Revision = 0

	return nil
}

// Rollback finalizes the session after a failure handling the message.
// The message is re-queued for be attempted at n.
func (s *Session) Rollback(ctx context.Context, n time.Time) error {
	if s.tx != nil {
		if err := s.tx.Rollback(); err != nil {
			return err
		}
	}

	s.done = true
	s.elem.message.NextAttemptAt = n

	if err := persistence.WithTransaction(
		ctx,
		s.queue.DataStore,
		func(tx persistence.ManagedTransaction) error {
			return tx.SaveMessageToQueue(ctx, s.elem.message)
		},
	); err != nil {
		return err
	}

	s.elem.message.Revision++

	return nil
}

// Close releases the message.
//
// It must be called regardless of whether Ack() or Nack() are called.
func (s *Session) Close() error {
	if s.elem == nil {
		return nil
	}

	s.queue.notify(s.elem)
	s.elem = nil

	if s.tx != nil && !s.done {
		return s.tx.Rollback()
	}

	return nil
}

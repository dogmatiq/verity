package queue

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
)

// session is an implementation of pipeline.Session for messages obtained from
// the queue.
type session struct {
	queue *Queue
	elem  *elem
	tx    persistence.Transaction
	done  bool
}

// MessageID returns the ID of the message that is handled within the session.
func (s *session) MessageID() string {
	return s.elem.message.ID()
}

// FailureCount returns the number of times this message has already been
// attempted, not including this attempt.
func (s *session) FailureCount() uint {
	// TODO: https://github.com/dogmatiq/infix/issues/110
	return 0
}

// Envelope returns the envelope containing the message to be handled.
func (s *session) Envelope() (*envelope.Envelope, error) {
	var err error

	if s.elem.env == nil {
		s.elem.env, err = envelope.Unmarshal(s.queue.Marshaler, s.elem.message.Envelope)
	}

	return s.elem.env, err
}

// Tx returns the transaction under which the message must be handled, starting
// it if necessary.
func (s *session) Tx(ctx context.Context) (persistence.ManagedTransaction, error) {
	if s.tx == nil {
		tx, err := s.queue.DataStore.Begin(ctx)
		if err != nil {
			return nil, err
		}

		s.tx = tx
	}

	return s.tx, nil
}

// Ack acknowledges successful handling of the message.
//
// It commits the changes performed in the session's transaction.
func (s *session) Ack(ctx context.Context) error {
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

// Nack indicates an error while handling the message.
//
// It discards the changes performed in the session's transaction and defers
// handling of the message until n.
func (s *session) Nack(ctx context.Context, n time.Time) error {
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

// Close releases the session.
//
// It must be called regardless of whether Ack() or Nack() are called.
func (s *session) Close() error {
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

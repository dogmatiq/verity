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
	queue  *Queue
	tx     persistence.Transaction
	elem   *elem
	commit bool
}

// Tx returns the transaction under which the message must be handled.
func (s *Session) Tx() persistence.ManagedTransaction {
	return s.tx
}

// Envelope returns the envelope containing the message to be handled.
//
// TODO: defer unmarshaling to the session so that a failure is visible to the
// logic that has access to the backoff strategy.
func (s *Session) Envelope() *envelope.Envelope {
	return s.elem.env
}

// Commit commits the changes performed in the session.
func (s *Session) Commit(ctx context.Context) error {
	// TODO:
	// if err := s.tx.RemoveMessageFromQueue(ctx, s.elem.message); err != nil {
	// 	return err
	// }

	if err := s.tx.Commit(ctx); err != nil {
		return err
	}

	s.queue.discard(s.elem)

	return nil
}

// Rollback finalizes the session after a failure handling the message.
// The message is re-queued for be attempted at n.
func (s *Session) Rollback(ctx context.Context, n time.Time) error {
	if err := s.tx.Rollback(); err != nil {
		return err
	}

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
	s.queue.pushPending(false, s.elem)

	return nil
}

// Close releases the message.
//
// It must be called regardless of whether Ack() or Nack() are called.
func (s *Session) Close() error {
	if err := s.tx.Rollback(); err != nil {
		return err
	}

	s.queue.pushPending(false, s.elem)

	return nil
}

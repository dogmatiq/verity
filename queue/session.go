package queue

import (
	"context"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
)

// Session represents a message popped from the queue.
//
// It implements the pipeline.Session interface.
type Session struct {
	queue *Queue
	elem  *elem
	tx    persistence.Transaction
	done  bool
}

// MessageID returns the ID of the message that is handled within the session.
//
// The ID is available even if the complete message envelope can not be
// unmarshaled.
func (s *Session) MessageID() string {
	return s.elem.Parcel.ID()
}

// FailureCount returns the number of times this message has already been
// attempted, not including this attempt.
func (s *Session) FailureCount() uint {
	return s.elem.Parcel.FailureCount
}

// Envelope returns the envelope containing the message to be handled.
func (s *Session) Envelope() *envelopespec.Envelope {
	return s.elem.Parcel.Envelope
}

// Message returns the Dogma message that is to be handled.
//
// It returns an error if the message can not be unpacked.
func (s *Session) Message() (dogma.Message, error) {
	var err error

	if s.elem.Message == nil {
		s.elem.Message, err = envelope.UnmarshalMessage(
			s.queue.Marshaler,
			s.elem.Parcel.Envelope,
		)
	}

	return s.elem.Message, err
}

// Tx returns the transaction under which the message must be handled, starting
// it if necessary.
//
// It starts the transaction if it has not already been started.
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

// Ack acknowledges successful handling of the message.
//
// It commits the changes performed in the session's transaction.
func (s *Session) Ack(ctx context.Context) error {
	_, err := s.Tx(ctx)
	if err != nil {
		return err
	}

	if err := s.tx.RemoveMessageFromQueue(ctx, s.elem.Parcel); err != nil {
		return err
	}

	if err := s.tx.Commit(ctx); err != nil {
		return err
	}

	s.done = true
	s.elem.Parcel.Revision = 0

	return nil
}

// Nack indicates an error while handling the message.
//
// It discards the changes performed in the session's transaction and defers
// handling of the message until n.
func (s *Session) Nack(ctx context.Context, n time.Time) error {
	if s.tx != nil {
		if err := s.tx.Rollback(); err != nil {
			return err
		}
	}

	s.done = true
	s.elem.Parcel.FailureCount++
	s.elem.Parcel.NextAttemptAt = n

	if err := persistence.WithTransaction(
		ctx,
		s.queue.DataStore,
		func(tx persistence.ManagedTransaction) error {
			return tx.SaveMessageToQueue(ctx, s.elem.Parcel)
		},
	); err != nil {
		return err
	}

	s.elem.Parcel.Revision++

	return nil
}

// Close releases the session.
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

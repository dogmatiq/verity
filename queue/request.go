package queue

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/refactor251"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
)

// Request is a request to handle an enqueued message.
//
// It implements the pipeline.Request interface.
type Request struct {
	queue *Queue
	elem  *elem
	tx    persistence.Transaction
	done  bool
}

// FailureCount returns the number of times this message has already been
// attempted without success, not including this request.
func (r *Request) FailureCount() uint {
	return r.elem.item.FailureCount
}

// Envelope returns the message envelope.
func (r *Request) Envelope() *envelopespec.Envelope {
	return r.elem.item.Envelope
}

// Parcel returns a parcel containing the original Dogma message.
func (r *Request) Parcel() (*parcel.Parcel, error) {
	var err error

	if r.elem.parcel == nil {
		r.elem.parcel, err = parcel.FromEnvelope(
			r.queue.Marshaler,
			r.elem.item.Envelope,
		)

		if err != nil {
			return nil, err
		}
	}

	return r.elem.parcel, nil
}

// Tx returns the transaction used to persist data within this request.
//
// It starts the transaction if it has not already been started.
func (r *Request) Tx(ctx context.Context) (persistence.ManagedTransaction, error) {
	if r.tx == nil {
		tx, err := r.queue.DataStore.Begin(ctx)
		if err != nil {
			return nil, err
		}

		r.tx = tx
	}

	return r.tx, nil
}

// Ack acknowledges successful handling of the request.
//
// It commits the changes performed in the request's transaction.
func (r *Request) Ack(ctx context.Context, batch persistence.Batch) error {
	_, err := r.Tx(ctx)
	if err != nil {
		return err
	}

	if err := refactor251.PersistTx(ctx, r.tx, batch); err != nil {
		return err
	}

	if err := r.tx.RemoveMessageFromQueue(ctx, r.elem.item); err != nil {
		return err
	}

	if _, err := r.tx.Commit(ctx); err != nil {
		return err
	}

	r.done = true
	r.elem.item.Revision = 0

	return nil
}

// Nack indicates an error while handling the message.
//
// It discards the changes performed in the request's transaction and defers
// handling of the message until n.
func (r *Request) Nack(ctx context.Context, n time.Time) error {
	if r.tx != nil {
		if err := r.tx.Rollback(); err != nil {
			return err
		}
	}

	r.done = true
	r.elem.item.FailureCount++
	r.elem.item.NextAttemptAt = n

	if _, err := persistence.WithTransaction(
		ctx,
		r.queue.DataStore,
		func(tx persistence.ManagedTransaction) error {
			return tx.SaveMessageToQueue(ctx, r.elem.item)
		},
	); err != nil {
		return err
	}

	r.elem.item.Revision++

	return nil
}

// Close releases the request.
//
// It must be called regardless of whether Ack() or Nack() are called.
func (r *Request) Close() error {
	if r.elem == nil {
		return nil
	}

	r.queue.notify(r.elem)
	r.elem = nil

	if r.tx != nil && !r.done {
		return r.tx.Rollback()
	}

	return nil
}

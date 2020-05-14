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
	return r.elem.Item.FailureCount
}

// Envelope returns the message envelope.
func (r *Request) Envelope() *envelopespec.Envelope {
	return r.elem.Item.Envelope
}

// Parcel returns a parcel containing the original Dogma message.
func (r *Request) Parcel() *parcel.Parcel {
	return r.elem.Parcel
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
func (r *Request) Ack(ctx context.Context, batch persistence.Batch) (persistence.Result, error) {
	_, err := r.Tx(ctx)
	if err != nil {
		return persistence.Result{}, err
	}

	if err := refactor251.PersistTx(ctx, r.tx, batch); err != nil {
		return persistence.Result{}, err
	}

	if err := r.tx.RemoveMessageFromQueue(ctx, r.elem.Item); err != nil {
		return persistence.Result{}, err
	}

	br, err := r.tx.Commit(ctx)
	if err != nil {
		return br, err
	}

	r.done = true
	r.elem.Item.Revision = 0

	return br, nil
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
	r.elem.Item.FailureCount++
	r.elem.Item.NextAttemptAt = n

	if _, err := persistence.WithTransaction(
		ctx,
		r.queue.DataStore,
		func(tx persistence.ManagedTransaction) error {
			return tx.SaveMessageToQueue(ctx, r.elem.Item)
		},
	); err != nil {
		return err
	}

	r.elem.Item.Revision++

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

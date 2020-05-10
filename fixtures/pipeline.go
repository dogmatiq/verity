package fixtures

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/refactor251"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/pipeline"
)

// PipelineRequestStub is a test implementation of the pipeline.Request
// interface.
type PipelineRequestStub struct {
	pipeline.Request

	FailureCountFunc func() uint
	EnvelopeFunc     func() *envelopespec.Envelope
	ParcelFunc       func() (*parcel.Parcel, error)
	TxFunc           func(context.Context) (persistence.ManagedTransaction, error)
	AckFunc          func(context.Context, persistence.Batch) (persistence.Result, error)
	NackFunc         func(context.Context, time.Time) error
	CloseFunc        func() error
}

// NewPipelineRequestStub returns a new request stub with pre-configured
// Envelope() and Tx() methods.
func NewPipelineRequestStub(
	p *parcel.Parcel,
	ds *DataStoreStub,
) (*PipelineRequestStub, *TransactionStub) {
	var tx *TransactionStub

	if ds == nil {
		tx = &TransactionStub{}
	} else {
		t, err := ds.Begin(context.Background())
		if err != nil {
			panic(err)
		}

		tx = t.(*TransactionStub)
	}

	req := &PipelineRequestStub{
		EnvelopeFunc: func() *envelopespec.Envelope {
			return p.Envelope
		},
		ParcelFunc: func() (*parcel.Parcel, error) {
			return p, nil
		},
		TxFunc: func(context.Context) (persistence.ManagedTransaction, error) {
			return tx, nil
		},
		AckFunc: func(ctx context.Context, batch persistence.Batch) (persistence.Result, error) {
			if err := refactor251.PersistTx(ctx, tx, batch); err != nil {
				return persistence.Result{}, err
			}

			return tx.Commit(ctx)
		},
		CloseFunc: func() error {
			return tx.Rollback()
		},
	}

	return req, tx
}

// FailureCount returns the number of times this message has already been
// attempted without success, not including this request.
func (r *PipelineRequestStub) FailureCount() uint {
	if r.FailureCountFunc != nil {
		return r.FailureCountFunc()
	}

	if r.Request != nil {
		return r.Request.FailureCount()
	}

	return 0
}

// Envelope returns the message envelope.
func (r *PipelineRequestStub) Envelope() *envelopespec.Envelope {
	if r.EnvelopeFunc != nil {
		return r.EnvelopeFunc()
	}

	if r.Request != nil {
		return r.Request.Envelope()
	}

	return nil
}

// Parcel returns a parcel containing the original Dogma message.
func (r *PipelineRequestStub) Parcel() (*parcel.Parcel, error) {
	if r.EnvelopeFunc != nil {
		return r.ParcelFunc()
	}

	if r.Request != nil {
		return r.Request.Parcel()
	}

	return nil, nil
}

// Tx returns the transaction used to persist data within this request.
//
// It starts the transaction if it has not already been started.
func (r *PipelineRequestStub) Tx(ctx context.Context) (persistence.ManagedTransaction, error) {
	if r.TxFunc != nil {
		return r.TxFunc(ctx)
	}

	if r.Request != nil {
		return r.Request.Tx(ctx)
	}

	return nil, nil
}

// Ack acknowledges successful handling of the request.
//
// It commits the changes performed in the request's transaction.
func (r *PipelineRequestStub) Ack(ctx context.Context, batch persistence.Batch) (persistence.Result, error) {
	if r.AckFunc != nil {
		return r.AckFunc(ctx, batch)
	}

	if r.Request != nil {
		return r.Request.Ack(ctx, batch)
	}

	return persistence.Result{}, nil
}

// Nack indicates an error while handling the message.
//
// It discards the changes performed in the request's transaction and defers
// handling of the message until n.
func (r *PipelineRequestStub) Nack(ctx context.Context, n time.Time) error {
	if r.NackFunc != nil {
		return r.NackFunc(ctx, n)
	}

	if r.Request != nil {
		return r.Request.Nack(ctx, n)
	}

	return nil
}

// Close releases the request.
//
// It must be called regardless of whether Ack() or Nack() are called.
func (r *PipelineRequestStub) Close() error {
	if r.CloseFunc != nil {
		return r.CloseFunc()
	}

	if r.Request != nil {
		return r.Request.Close()
	}

	return nil
}

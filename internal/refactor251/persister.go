package refactor251

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
)

// Persist commits a batch of operations atomically.
//
// It is designed to be used by persistence.DataStore implementations to help
// implement a persistence.Persister early in the refactoring process.
func Persist(
	ctx context.Context,
	ds persistence.DataStore,
	batch persistence.Batch,
) (persistence.Result, error) {
	batch.MustValidate()

	tx, err := ds.Begin(ctx)
	if err != nil {
		return persistence.Result{}, err
	}
	defer tx.Rollback()

	if err := PersistTx(ctx, tx, batch); err != nil {
		return persistence.Result{}, err
	}

	return tx.Commit(ctx)
}

// PersistTx performs the operations in batch on tx.
func PersistTx(
	ctx context.Context,
	tx persistence.Transaction,
	batch persistence.Batch,
) error {
	v := transactionAdaptorVisitor{tx}

	for _, op := range batch {
		if err := op.AcceptVisitor(ctx, v); err != nil {
			return err
		}
	}

	return nil
}

type transactionAdaptorVisitor struct {
	tx persistence.Transaction
}

func (v transactionAdaptorVisitor) VisitSaveAggregateMetaData(
	ctx context.Context,
	op persistence.SaveAggregateMetaData,
) error {
	err := v.tx.SaveAggregateMetaData(ctx, &op.MetaData)

	if err == persistence.ErrConflict {
		return persistence.ConflictError{Cause: op}
	}

	return err
}

func (v transactionAdaptorVisitor) VisitSaveEvent(
	ctx context.Context,
	op persistence.SaveEvent,
) error {
	_, err := v.tx.SaveEvent(ctx, op.Envelope)
	return err
}

func (v transactionAdaptorVisitor) VisitSaveQueueItem(
	ctx context.Context,
	op persistence.SaveQueueItem,
) error {
	err := v.tx.SaveMessageToQueue(ctx, &op.Item)

	if err == persistence.ErrConflict {
		return persistence.ConflictError{Cause: op}
	}

	return err
}

func (v transactionAdaptorVisitor) VisitRemoveQueueItem(
	ctx context.Context,
	op persistence.RemoveQueueItem,
) error {
	err := v.tx.RemoveMessageFromQueue(ctx, &op.Item)

	if err == persistence.ErrConflict {
		return persistence.ConflictError{Cause: op}
	}

	return err
}

func (v transactionAdaptorVisitor) VisitSaveOffset(
	ctx context.Context,
	op persistence.SaveOffset,
) error {
	err := v.tx.SaveOffset(
		ctx,
		op.ApplicationKey,
		op.CurrentOffset,
		op.NextOffset,
	)

	if err == persistence.ErrConflict {
		return persistence.ConflictError{Cause: op}
	}

	return err
}

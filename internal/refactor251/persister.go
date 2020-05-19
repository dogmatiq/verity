package refactor251

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
)

// PersistTx performs the operations in batch on tx.
func PersistTx(
	ctx context.Context,
	tx Transaction,
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
	tx Transaction
}

func (v transactionAdaptorVisitor) VisitSaveAggregateMetaData(
	ctx context.Context,
	op persistence.SaveAggregateMetaData,
) error {
	err := v.tx.SaveAggregateMetaData(ctx, &op.MetaData)

	if err == ErrConflict {
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

	if err == ErrConflict {
		return persistence.ConflictError{Cause: op}
	}

	return err
}

func (v transactionAdaptorVisitor) VisitRemoveQueueItem(
	ctx context.Context,
	op persistence.RemoveQueueItem,
) error {
	err := v.tx.RemoveMessageFromQueue(ctx, &op.Item)

	if err == ErrConflict {
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

	if err == ErrConflict {
		return persistence.ConflictError{Cause: op}
	}

	return err
}

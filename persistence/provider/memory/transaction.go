package memory

import (
	"context"
	"errors"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/envelope"
)

// transaction is an implementation of persistence.QueueTransaction for
// in-memory persistence.
type transaction struct {
}

// LoadAggregate loads an aggregate instance.
func (t *transaction) LoadAggregate(
	ctx context.Context,
	hk, id string,
) (dogma.AggregateRoot, uint64, error) {
	return nil, 0, errors.New("not implemented")
}

// PersistAggregate updates (or creates) an aggregate instance.
func (t *transaction) PersistAggregate(
	ctx context.Context,
	hk, id string,
	rev uint64,
	r dogma.AggregateRoot,
) error {
	return errors.New("not implemented")
}

// DeleteAggregate deletes an aggregate instance.
func (t *transaction) DeleteAggregate(
	ctx context.Context,
	hk, id string,
	rev uint64,
) error {
	return errors.New("not implemented")
}

// LoadProcess loads a process instance.
func (t *transaction) LoadProcess(
	ctx context.Context,
	hk, id string,
) (dogma.ProcessRoot, uint64, error) {
	return nil, 0, errors.New("not implemented")
}

// SaveProcess updates (or creates) a process instance.
func (t *transaction) SaveProcess(
	ctx context.Context,
	hk, id string,
	rev uint64,
	r dogma.ProcessRoot,
) error {
	return errors.New("not implemented")
}

// DeleteProcess deletes a process instance.
func (t *transaction) DeleteProcess(
	ctx context.Context,
	hk, id string,
	rev uint64,
) error {
	return errors.New("not implemented")
}

// Enqueue adds a message to the application's message queue.
func (t *transaction) Enqueue(ctx context.Context, env *envelope.Envelope) error {
	return errors.New("not implemented")
}

// Append adds a message to the application's event stream.
func (t *transaction) Append(ctx context.Context, env *envelope.Envelope) error {
	return errors.New("not implemented")
}

// Commit applies the changes from the transaction.
func (t *transaction) Commit(ctx context.Context) error {
	return errors.New("not implemented")
}

// Rollback cancels the transaction due to an error.
func (t *transaction) Rollback(ctx context.Context, err error) error {
	return errors.New("not implemented")
}

// Close closes the transaction. It must be called regardless of whether the
// transactions is committed or rolled-back.
func (t *transaction) Close() error {
	return errors.New("not implemented")
}

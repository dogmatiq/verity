package boltdb

import (
	"context"
	"errors"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/persistence"
)

// OffsetRepository is an implementation of persistence.OffsetRepository that
// stores offsets in a BoltDB database.
type OffsetRepository struct {
}

// NextOffset returns the offset of the next event to be consumed for a
// specific source application and handler.
//
// a is the identity of the source application, and h is the identity of the
// handler.
func (r *OffsetRepository) NextOffset(
	ctx context.Context,
	a, h configkit.Identity,
) (uint64, error) {
	return 0, errors.New("not implemented")
}

// Begin starts a transaction for a message obtained from an application's
// event stream.
//
// h is the identity of the handler that will handle the message. o must be
// the "next offset" that is currently persisted in the repository.
//
// When transaction is committed successfully, the "next offset" is updated
// to ev.Offset + 1.
func (r *OffsetRepository) Begin(
	ctx context.Context,
	h configkit.Identity,
	o uint64,
	ev *eventstream.Event,
) (persistence.Transaction, error) {
	return nil, errors.New("not implemented")
}

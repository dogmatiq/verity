package pipeline

import (
	"context"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/dogmatiq/marshalkit"
)

// Scope exposes operations that a pipeline stage can perform within the context
// of a specific message.
//
// The operations are performed atomically, only taking effect if the handler
// succeeds.
//
// TODO: consider embedding ManagedTransaction into the scope. The
// SaveMessageToQueue() and SaveEvent() methods would be shadowed by those
// defined here, eliminating the need to warn against using their Tx equivalents
// directly, and keeping all persistence operations on one interface.
type Scope struct {
	// Session is the session under which the message is handled.
	Session Session

	// Marshaler is the application's marshaler, used to marshal new messages
	// that are produced as a result of handling this message.
	Marshaler marshalkit.Marshaler

	// Logger is the logger to use for informational messages within the context
	// of the message that is being handled.
	Logger logging.Logger

	// Enqueued is a slice of all messages enqueued via the scope.
	Enqueued []queuestore.Pair

	// Recorded is a slice of all events recorded via the scope.
	Recorded []eventstore.Pair
}

// EnqueueMessage adds a message to the queue.
func (s *Scope) EnqueueMessage(
	ctx context.Context,
	p *parcel.Parcel,
) error {
	tx, err := s.Session.Tx(ctx)
	if err != nil {
		return err
	}

	n := p.ScheduledFor
	if n.IsZero() {
		n = time.Now()
	}

	sp := &queuestore.Parcel{
		NextAttemptAt: n,
		Envelope:      p.Envelope,
	}

	if err := tx.SaveMessageToQueue(ctx, sp); err != nil {
		return err
	}

	sp.Revision++

	s.Enqueued = append(
		s.Enqueued,
		queuestore.Pair{
			Parcel:  sp,
			Message: p.Message,
		},
	)

	return nil
}

// RecordEvent appends an event to the event stream.
func (s *Scope) RecordEvent(
	ctx context.Context,
	p *parcel.Parcel,
) (eventstore.Offset, error) {
	tx, err := s.Session.Tx(ctx)
	if err != nil {
		return 0, err
	}

	o, err := tx.SaveEvent(ctx, p.Envelope)
	if err != nil {
		return 0, err
	}

	s.Recorded = append(
		s.Recorded,
		eventstore.Pair{
			Parcel: &eventstore.Parcel{
				Offset:   o,
				Envelope: p.Envelope,
			},
			Message: p.Message,
		},
	)

	return o, nil
}

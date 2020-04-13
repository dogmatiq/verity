package pipeline

import (
	"context"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/dogmatiq/marshalkit"
)

// Scope exposes operations that a pipeline stage can perform within the context
// of a specific message.
//
// The operations are performed atomically, only taking effect if the handler
// succeeds.
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
	env *envelope.Envelope,
) error {
	tx, err := s.Session.Tx(ctx)
	if err != nil {
		return err
	}

	n := env.ScheduledFor

	if n.IsZero() {
		n = env.CreatedAt
	}

	p := &queuestore.Parcel{
		NextAttemptAt: n,
		Envelope:      envelope.MustMarshal(s.Marshaler, env),
	}

	if err := tx.SaveMessageToQueue(ctx, p); err != nil {
		return err
	}

	p.Revision++

	s.Enqueued = append(
		s.Enqueued,
		queuestore.Pair{
			Parcel:   p,
			Original: env,
		},
	)

	return nil
}

// RecordEvent appends an event to the event stream.
func (s *Scope) RecordEvent(
	ctx context.Context,
	env *envelope.Envelope,
) (eventstore.Offset, error) {
	tx, err := s.Session.Tx(ctx)
	if err != nil {
		return 0, err
	}

	penv := envelope.MustMarshal(s.Marshaler, env)

	o, err := tx.SaveEvent(ctx, penv)
	if err != nil {
		return 0, err
	}

	s.Recorded = append(
		s.Recorded,
		eventstore.Pair{
			Parcel: &eventstore.Parcel{
				Offset:   o,
				Envelope: penv,
			},
			Original: env,
		},
	)

	return o, nil
}

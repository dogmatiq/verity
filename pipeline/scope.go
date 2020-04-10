package pipeline

import (
	"context"
	"time"

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
	// Session is the session being processed.
	Session Session

	// Marshaler is the application's marshaler.
	Marshaler marshalkit.Marshaler

	// DefaultHandlerTimeout is the default timeout for the Dogma handler if the
	// handler does not provide a timeout hint.
	DefaultHandlerTimeout time.Duration

	// Logger is the logger to use for informational messages within the context
	// of the message that is being handled.
	Logger logging.Logger

	// enqueued is a slice of all messages enqueued via the scope.
	enqueued []EnqueuedMessage

	// recorded is a slice of all events recorded via the scope.
	recorded []RecordedEvent
}

// EnqueueMessage adds a message to the queue.
func (s *Scope) EnqueueMessage(
	ctx context.Context,
	env *envelope.Envelope,
) error {
	n := env.ScheduledFor

	if n.IsZero() {
		n = env.CreatedAt
	}

	m := &queuestore.Message{
		NextAttemptAt: n,
		Envelope:      envelope.MustMarshal(s.Marshaler, env),
	}

	tx, err := s.Session.Tx(ctx)
	if err != nil {
		return err
	}

	if err := tx.SaveMessageToQueue(ctx, m); err != nil {
		return err
	}

	m.Revision++

	s.enqueued = append(
		s.enqueued,
		EnqueuedMessage{
			Memory:    env,
			Persisted: m,
		},
	)

	return nil
}

// RecordEvent appends an event to the event stream.
func (s *Scope) RecordEvent(
	ctx context.Context,
	env *envelope.Envelope,
) (eventstore.Offset, error) {
	penv := envelope.MustMarshal(s.Marshaler, env)

	tx, err := s.Session.Tx(ctx)
	if err != nil {
		return 0, err
	}

	o, err := tx.SaveEvent(ctx, penv)
	if err != nil {
		return 0, err
	}

	s.recorded = append(
		s.recorded,
		RecordedEvent{
			Memory: env,
			Persisted: &eventstore.Event{
				Offset:   o,
				Envelope: penv,
			},
		},
	)

	return o, nil
}

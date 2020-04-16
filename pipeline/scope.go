package pipeline

import (
	"context"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// Scope exposes operations that a pipeline stage can perform within the context
// of a specific message.
//
// The operations are performed atomically, only taking effect if the handler
// succeeds.
type Scope struct {
	// Session is the session under which the message is handled.
	Session Session

	// Logger is the logger to use for informational messages within the context
	// of the message that is being handled.
	Logger logging.Logger

	// Enqueued is a slice of all messages enqueued via the scope.
	Enqueued []EnqueuedMessage

	// Recorded is a slice of all events recorded via the scope.
	Recorded []RecordedEvent
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

	i := &queuestore.Item{
		NextAttemptAt: n,
		Envelope:      p.Envelope,
	}

	if err := tx.SaveMessageToQueue(ctx, i); err != nil {
		return err
	}

	i.Revision++

	s.Enqueued = append(
		s.Enqueued,
		EnqueuedMessage{
			Parcel:    p,
			Persisted: i,
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
		RecordedEvent{
			Parcel: p,
			Persisted: &eventstore.Item{
				Offset:   o,
				Envelope: p.Envelope,
			},
		},
	)

	return o, nil
}

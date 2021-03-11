package queue

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/verity/eventstream"
	"github.com/dogmatiq/verity/persistence"
)

// StreamAdaptor is an eventstream.Handler that adds the consumed events to a
// queue.
type StreamAdaptor struct {
	Queue            *Queue
	OffsetRepository persistence.OffsetRepository
	Persister        persistence.Persister
}

// NextOffset returns the offset of the next event to be consumed from a
// specific application's event stream.
//
// id is the identity of the source application.
func (a *StreamAdaptor) NextOffset(ctx context.Context, id configkit.Identity) (uint64, error) {
	return a.OffsetRepository.LoadOffset(ctx, id.Key)
}

// HandleEvent handles an event obtained from the event stream.
//
// o must be the offset that would be returned by NextOffset(). On success,
// the next call to NextOffset() will return ev.Offset + 1.
func (a *StreamAdaptor) HandleEvent(ctx context.Context, o uint64, ev eventstream.Event) error {
	qm := persistence.QueueMessage{
		NextAttemptAt: ev.Parcel.CreatedAt,
		Envelope:      ev.Parcel.Envelope,
	}

	if _, err := a.Persister.Persist(
		ctx,
		persistence.Batch{
			persistence.SaveQueueMessage{
				Message: qm,
			},
			persistence.SaveOffset{
				ApplicationKey: ev.Parcel.Envelope.GetSourceApplication().GetKey(),
				CurrentOffset:  o,
				NextOffset:     o + 1,
			},
		},
	); err != nil {
		return err
	}

	qm.Revision++

	a.Queue.Add(
		[]Message{
			{qm, ev.Parcel},
		},
	)

	return nil
}

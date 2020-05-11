package pipeline

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/dogmatiq/infix/queue"
)

// Response is the result from a pipeline stage.
//
// It encapsulates the messages that were produced, so they may be observed by
// other components of the engine.
type Response struct {
	result Result
	batch  persistence.Batch
	events map[string]*parcel.Parcel
}

// ExecuteCommand enqueues a command for execution.
func (r *Response) ExecuteCommand(p *parcel.Parcel) {
	item := queuestore.Item{
		NextAttemptAt: p.CreatedAt,
		Envelope:      p.Envelope,
	}

	r.batch = append(
		r.batch,
		persistence.SaveQueueItem{
			Item: item,
		},
	)

	item.Revision++

	r.result.QueueMessages = append(
		r.result.QueueMessages,
		queue.Message{
			Parcel: p,
			Item:   &item,
		},
	)
}

// EnqueueMessage is a helper method that adds a message to the queue and
// adds it to the response.
func (r *Response) EnqueueMessage(
	ctx context.Context,
	tx persistence.ManagedTransaction,
	p *parcel.Parcel,
) error {
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

	r.result.QueueMessages = append(
		r.result.QueueMessages,
		queue.Message{
			Parcel: p,
			Item:   i,
		},
	)

	return nil
}

// RecordEvent is a helper method that appends an event to the event stream and
// adds it to the response.
func (r *Response) RecordEvent(
	ctx context.Context,
	tx persistence.ManagedTransaction,
	p *parcel.Parcel,
) (uint64, error) {
	if r.events == nil {
		r.events = map[string]*parcel.Parcel{}
	}

	r.events[p.Envelope.MetaData.MessageId] = p

	return tx.SaveEvent(ctx, p.Envelope)
}

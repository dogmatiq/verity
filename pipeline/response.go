package pipeline

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/eventstream"
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
	r.saveQueueItem(p, p.CreatedAt)
}

// ScheduleTimeout schedules a timeout message for execution.
func (r *Response) ScheduleTimeout(p *parcel.Parcel) {
	r.saveQueueItem(p, p.ScheduledFor)
}

// RecordEvent records an event message.
func (r *Response) RecordEvent(p *parcel.Parcel) {
	r.saveEvent(p)
	// TODO: also enqueue, if necessary
}

// RecordEventX is a helper method that appends an event to the event stream and
// adds it to the response.
//
// TODO: remove
func (r *Response) RecordEventX(
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

// saveEvent adds a SaveEvent operation to the batch for the message in p.
func (r *Response) saveEvent(p *parcel.Parcel) {
	r.batch = append(
		r.batch,
		persistence.SaveEvent{
			Envelope: p.Envelope,
		},
	)

	if r.events == nil {
		r.events = map[string]*parcel.Parcel{}
	}

	r.events[p.Envelope.MetaData.MessageId] = p
}

// saveEvent adds a SaveQueueItem operation to the batch for the message in p.
func (r *Response) saveQueueItem(p *parcel.Parcel, next time.Time) {
	item := queuestore.Item{
		NextAttemptAt: next,
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

// correlateEvents correlates the event store items from a persistence result
// with the parcels that produced them in order to populate r.result with
// eventstore.Event values.
func (r *Response) correlateEvents(pr persistence.Result) {
	for _, item := range pr.EventStoreItems {
		r.result.Events = append(
			r.result.Events,
			&eventstream.Event{
				Offset: item.Offset,
				Parcel: r.events[item.ID()],
			},
		)
	}
}

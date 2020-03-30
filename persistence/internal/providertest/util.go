package providertest

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queue"
)

// saveEvent persists an events to the store.
func saveEvent(
	ctx context.Context,
	ds persistence.DataStore,
	env *envelopespec.Envelope,
) (eventstore.Offset, error) {
	tx, err := ds.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	o, err := tx.SaveEvent(ctx, env)
	if err != nil {
		return 0, err
	}

	return o, tx.Commit(ctx)
}

// saveEvents persists the given events to the store.
func saveEvents(
	ctx context.Context,
	ds persistence.DataStore,
	envelopes ...*envelopespec.Envelope,
) error {
	tx, err := ds.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, env := range envelopes {
		_, err := tx.SaveEvent(ctx, env)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// queryEvents queries an event store and returns a slice of the results.
func queryEvents(
	ctx context.Context,
	r eventstore.Repository,
	q eventstore.Query,
) ([]*eventstore.Event, error) {
	res, err := r.QueryEvents(ctx, q)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var events []*eventstore.Event

	for {
		ev, ok, err := res.Next(ctx)
		if !ok || err != nil {
			return events, err
		}

		events = append(events, ev)
	}
}

// addMessageToQueue persists the given message to the queue.
func addMessageToQueue(
	ctx context.Context,
	ds persistence.DataStore,
	env *envelopespec.Envelope,
	t time.Time,
) error {
	tx, err := ds.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := tx.AddMessageToQueue(ctx, env, t); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// loadQueueMessage loads the next message from the queue.
func loadQueueMessage(
	ctx context.Context,
	r queue.Repository,
) (*queue.Message, error) {
	messages, err := r.LoadQueueMessages(ctx, 1)
	if err != nil {
		return nil, err
	}

	if len(messages) == 0 {
		return nil, errors.New("no messages returned")
	}

	return messages[0], nil
}

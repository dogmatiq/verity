package providertest

import (
	"context"
	"errors"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/golang/protobuf/proto"
	"github.com/onsi/gomega"
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
	return persistence.WithTransaction(
		ctx,
		ds,
		func(tx persistence.ManagedTransaction) error {
			for _, env := range envelopes {
				_, err := tx.SaveEvent(ctx, env)
				if err != nil {
					return err
				}
			}

			return nil
		},
	)
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

// saveMessagesToQueue persists the given message to the queue.
func saveMessagesToQueue(
	ctx context.Context,
	ds persistence.DataStore,
	messages ...*queuestore.Message,
) error {
	return persistence.WithTransaction(
		ctx,
		ds,
		func(tx persistence.ManagedTransaction) error {
			for _, m := range messages {
				if err := tx.SaveMessageToQueue(ctx, m); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

// removeMessagesFromQueue removes the given message from the queue.
func removeMessagesFromQueue(
	ctx context.Context,
	ds persistence.DataStore,
	messages ...*queuestore.Message,
) error {
	return persistence.WithTransaction(
		ctx,
		ds,
		func(tx persistence.ManagedTransaction) error {
			for _, m := range messages {
				if err := tx.RemoveMessageFromQueue(ctx, m); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

// loadQueueMessage loads the next message from the queue.
func loadQueueMessage(
	ctx context.Context,
	r queuestore.Repository,
) (*queuestore.Message, error) {
	messages, err := r.LoadQueueMessages(ctx, 1)
	if err != nil {
		return nil, err
	}

	if len(messages) == 0 {
		return nil, errors.New("no messages returned")
	}

	return messages[0], nil
}

// expectQueueMessageToEqual asserts that a queue message equals an expected
// value. It does not compare the revisions.
func expectQueueMessageToEqual(check, expect *queuestore.Message, desc ...interface{}) {
	gomega.Expect(check.NextAttemptAt).To(
		gomega.BeTemporally("~", expect.NextAttemptAt),
	)

	expectProtoToEqual(check.Envelope, expect.Envelope, desc...)
}

// expectProtoToEqual asserts that a protobuf message equals an expected value.
//
// TODO: https://github.com/dogmatiq/infix/issues/100
// Use helpers like this and expectQueueMessageToEqual() in eventstore tests.
func expectProtoToEqual(check, expect proto.Message, desc ...interface{}) {
	if !proto.Equal(check, expect) {
		gomega.Expect(check).To(
			gomega.Equal(expect),
			desc...,
		)
	}
}

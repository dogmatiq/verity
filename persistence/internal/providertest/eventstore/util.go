package eventstore

import (
	"context"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/onsi/gomega"
)

// saveEvent persists an event to the store and returns its offset.
func saveEvent(
	ctx context.Context,
	ds persistence.DataStore,
	env *envelopespec.Envelope,
) uint64 {
	var offset uint64

	_, err := persistence.WithTransaction(
		ctx,
		ds,
		func(tx persistence.ManagedTransaction) error {
			var err error
			offset, err = tx.SaveEvent(ctx, env)
			return err
		},
	)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	return offset
}

// saveEvents persists the given events to the store.
func saveEvents(
	ctx context.Context,
	ds persistence.DataStore,
	envelopes ...*envelopespec.Envelope,
) {
	_, err := persistence.WithTransaction(
		ctx,
		ds,
		func(tx persistence.ManagedTransaction) error {
			for _, env := range envelopes {
				if _, err := tx.SaveEvent(ctx, env); err != nil {
					return err
				}
			}

			return nil
		},
	)

	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
}

// queryEvents queries the event store and returns a slice of the results.
func queryEvents(
	ctx context.Context,
	r eventstore.Repository,
	q eventstore.Query,
) []*eventstore.Item {
	res, err := r.QueryEvents(ctx, q)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer res.Close()

	var items []*eventstore.Item

	for {
		i, ok, err := res.Next(ctx)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

		if !ok {
			return items
		}

		items = append(items, i)
	}
}

// loadEventsBySource loads the events produced by a specific handler.
//
// hk is the handler's identity key.
//
// id is the instance ID, which must be empty if the handler type does not
// use instances.
//
// m is ID of a "barrier" message. If supplied, the results are limited to
// events with higher offsets than the barrier message.
func loadEventsBySource(
	ctx context.Context,
	r eventstore.Repository,
	hk, id string,
	d string,
) []*eventstore.Item {
	res, err := r.LoadEventsBySource(ctx, hk, id, d)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer res.Close()

	var items []*eventstore.Item

	for {
		i, ok, err := res.Next(ctx)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

		if !ok {
			return items
		}

		items = append(items, i)
	}
}

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
) eventstore.Offset {
	tx, err := ds.Begin(ctx)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer tx.Rollback()

	o, err := tx.SaveEvent(ctx, env)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	err = tx.Commit(ctx)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	return o
}

// saveEvents persists the given events to the store.
func saveEvents(
	ctx context.Context,
	ds persistence.DataStore,
	envelopes ...*envelopespec.Envelope,
) {
	err := persistence.WithTransaction(
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

	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
}

// queryEvents queries the event store and returns a slice of the results.
func queryEvents(
	ctx context.Context,
	r eventstore.Repository,
	q eventstore.Query,
) []*eventstore.Event {
	res, err := r.QueryEvents(ctx, q)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer res.Close()

	var events []*eventstore.Event

	for {
		ev, ok, err := res.Next(ctx)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

		if !ok {
			return events
		}

		events = append(events, ev)
	}
}

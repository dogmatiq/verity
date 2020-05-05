package providertest

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/onsi/gomega"
)

// loadAggregateMetaData loads aggregate meta-data for a specific instance.
func loadAggregateMetaData(
	ctx context.Context,
	r aggregatestore.Repository,
	hk, id string,
) *aggregatestore.MetaData {
	md, err := r.LoadMetaData(ctx, hk, id)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	return md
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

// persist persists a batch of operations and asserts that there was no failure.
func persist(
	ctx context.Context,
	p persistence.Persister,
	batch ...persistence.Operation,
) persistence.BatchResult {
	res, err := p.Persist(ctx, batch)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	return res
}

package providertest

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// loadAggregateMetaData loads aggregate meta-data for a specific instance.
func loadAggregateMetaData(
	ctx context.Context,
	r persistence.AggregateRepository,
	hk, id string,
) persistence.AggregateMetaData {
	md, err := r.LoadAggregateMetaData(ctx, hk, id)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	return *md
}

// loadEventsByType loads events of a specifc type.
func loadEventsByType(
	ctx context.Context,
	r persistence.EventRepository,
	f map[string]struct{},
	o uint64,
) []eventstore.Item {
	res, err := r.LoadEventsByType(ctx, f, o)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer res.Close()

	var items []eventstore.Item

	for {
		i, ok, err := res.Next(ctx)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

		if !ok {
			return items
		}

		items = append(items, *i)
	}
}

// loadEventsBySource loads events produced by a specific handler.
func loadEventsBySource(
	ctx context.Context,
	r persistence.EventRepository,
	hk, id string,
	m string,
) []eventstore.Item {
	res, err := r.LoadEventsBySource(ctx, hk, id, m)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer res.Close()

	var items []eventstore.Item

	for {
		i, ok, err := res.Next(ctx)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

		if !ok {
			return items
		}

		items = append(items, *i)
	}
}

// loadOffset loads the offset from the repository with the given application
// key.
func loadOffset(
	ctx context.Context,
	repository persistence.OffsetRepository,
	ak string,
) uint64 {
	o, err := repository.LoadOffset(ctx, ak)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	return o
}

// loadQueueItem loads the item at the head of the queue.
func loadQueueItem(
	ctx context.Context,
	r queuestore.Repository,
) queuestore.Item {
	items := loadQueueItems(ctx, r, 1)

	if len(items) == 0 {
		ginkgo.Fail("no messages returned")
	}

	return items[0]
}

// loadQueueItems loads n items at the head of the queue.
func loadQueueItems(
	ctx context.Context,
	r queuestore.Repository,
	n int,
) []queuestore.Item {
	pointers, err := r.LoadQueueMessages(ctx, n)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	items := make([]queuestore.Item, len(pointers))
	for i, p := range pointers {
		items[i] = *p
	}

	return items
}

// persist persists a batch of operations and asserts that there was no failure.
func persist(
	ctx context.Context,
	p persistence.Persister,
	batch ...persistence.Operation,
) persistence.Result {
	res, err := p.Persist(ctx, batch)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	return res
}

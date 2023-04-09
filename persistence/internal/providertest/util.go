package providertest

import (
	"context"

	"github.com/dogmatiq/verity/persistence"
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
	gomega.ExpectWithOffset(1, err).ShouldNot(gomega.HaveOccurred())

	return md
}

// loadProcessInstance loads a process instance.
func loadProcessInstance(
	ctx context.Context,
	r persistence.ProcessRepository,
	hk, id string,
) persistence.ProcessInstance {
	inst, err := r.LoadProcessInstance(ctx, hk, id)
	gomega.ExpectWithOffset(1, err).ShouldNot(gomega.HaveOccurred())

	return inst
}

// loadEventsByType loads events of a specifc type.
func loadEventsByType(
	ctx context.Context,
	r persistence.EventRepository,
	f map[string]struct{},
	o uint64,
) []persistence.Event {
	res, err := r.LoadEventsByType(ctx, f, o)
	gomega.ExpectWithOffset(1, err).ShouldNot(gomega.HaveOccurred())
	defer res.Close()

	var events []persistence.Event

	for {
		ev, ok, err := res.Next(ctx)
		gomega.ExpectWithOffset(1, err).ShouldNot(gomega.HaveOccurred())

		if !ok {
			return events
		}

		events = append(events, ev)
	}
}

// loadEventsBySource loads events produced by a specific handler.
func loadEventsBySource(
	ctx context.Context,
	r persistence.EventRepository,
	hk, id string,
	m string,
) []persistence.Event {
	res, err := r.LoadEventsBySource(ctx, hk, id, m)
	gomega.ExpectWithOffset(1, err).ShouldNot(gomega.HaveOccurred())
	defer res.Close()

	var events []persistence.Event

	for {
		ev, ok, err := res.Next(ctx)
		gomega.ExpectWithOffset(1, err).ShouldNot(gomega.HaveOccurred())

		if !ok {
			return events
		}

		events = append(events, ev)
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
	gomega.ExpectWithOffset(1, err).ShouldNot(gomega.HaveOccurred())

	return o
}

// loadQueueMessage loads the message at the head of the queue.
func loadQueueMessage(
	ctx context.Context,
	r persistence.QueueRepository,
) persistence.QueueMessage {
	messages := loadQueueMessages(ctx, r, 1)

	if len(messages) == 0 {
		ginkgo.Fail("no messages returned")
	}

	return messages[0]
}

// loadQueueMessages loads n messages at the head of the queue.
func loadQueueMessages(
	ctx context.Context,
	r persistence.QueueRepository,
	n int,
) []persistence.QueueMessage {
	messages, err := r.LoadQueueMessages(ctx, n)
	gomega.ExpectWithOffset(1, err).ShouldNot(gomega.HaveOccurred())

	return messages
}

// persist persists a batch of operations and asserts that there was no failure.
func persist(
	ctx context.Context,
	p persistence.Persister,
	batch ...persistence.Operation,
) persistence.Result {
	res, err := p.Persist(ctx, batch)
	gomega.ExpectWithOffset(1, err).ShouldNot(gomega.HaveOccurred())

	return res
}

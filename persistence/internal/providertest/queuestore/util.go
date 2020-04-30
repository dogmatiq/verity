package queuestore

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// saveMessages persists the given messages to the queue.
func saveMessages(
	ctx context.Context,
	ds persistence.DataStore,
	items ...*queuestore.Item,
) {
	_, err := persistence.WithTransaction(
		ctx,
		ds,
		func(tx persistence.ManagedTransaction) error {
			for _, i := range items {
				if err := tx.SaveMessageToQueue(ctx, i); err != nil {
					return err
				}
			}
			return nil
		},
	)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	for _, i := range items {
		i.Revision++
	}
}

// removeMessages removes the given messages from the queue.
func removeMessages(
	ctx context.Context,
	ds persistence.DataStore,
	items ...*queuestore.Item,
) {
	_, err := persistence.WithTransaction(
		ctx,
		ds,
		func(tx persistence.ManagedTransaction) error {
			for _, i := range items {
				if err := tx.RemoveMessageFromQueue(ctx, i); err != nil {
					return err
				}
			}
			return nil
		},
	)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	for _, i := range items {
		i.Revision = 0
	}
}

// loadMessage loads the message at the head of the queue.
func loadMessage(
	ctx context.Context,
	r queuestore.Repository,
) *queuestore.Item {
	items := loadMessages(ctx, r, 1)

	if len(items) == 0 {
		ginkgo.Fail("no messages returned")
	}

	return items[0]
}

// loadMessages loads the messages at the head of the queue.
func loadMessages(
	ctx context.Context,
	r queuestore.Repository,
	n int,
) []*queuestore.Item {
	items, err := r.LoadQueueMessages(ctx, n)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	return items
}

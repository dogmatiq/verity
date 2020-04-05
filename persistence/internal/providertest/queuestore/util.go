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
	messages ...*queuestore.Message,
) {
	err := persistence.WithTransaction(
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

	if err != nil {
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	}

	for _, m := range messages {
		m.Revision++
	}
}

// removeMessages removes the given messages from the queue.
func removeMessages(
	ctx context.Context,
	ds persistence.DataStore,
	messages ...*queuestore.Message,
) {
	err := persistence.WithTransaction(
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

	if err != nil {
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	}

	for _, m := range messages {
		m.Revision = 0
	}
}

// loadMessage loads the message at the head of the queue.
func loadMessage(
	ctx context.Context,
	r queuestore.Repository,
) *queuestore.Message {
	messages := loadMessages(ctx, r, 1)

	if len(messages) == 0 {
		ginkgo.Fail("no messages returned")
	}

	return messages[0]
}

// loadMessages loads the messages at the head of the queue.
func loadMessages(
	ctx context.Context,
	r queuestore.Repository,
	n int,
) []*queuestore.Message {
	messages, err := r.LoadQueueMessages(ctx, n)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	return messages
}

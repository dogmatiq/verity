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
	parcels ...*queuestore.Parcel,
) {
	err := persistence.WithTransaction(
		ctx,
		ds,
		func(tx persistence.ManagedTransaction) error {
			for _, p := range parcels {
				if err := tx.SaveMessageToQueue(ctx, p); err != nil {
					return err
				}
			}
			return nil
		},
	)

	if err != nil {
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	}

	for _, p := range parcels {
		p.Revision++
	}
}

// removeMessages removes the given messages from the queue.
func removeMessages(
	ctx context.Context,
	ds persistence.DataStore,
	parcels ...*queuestore.Parcel,
) {
	err := persistence.WithTransaction(
		ctx,
		ds,
		func(tx persistence.ManagedTransaction) error {
			for _, p := range parcels {
				if err := tx.RemoveMessageFromQueue(ctx, p); err != nil {
					return err
				}
			}
			return nil
		},
	)

	if err != nil {
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	}

	for _, p := range parcels {
		p.Revision = 0
	}
}

// loadMessage loads the message at the head of the queue.
func loadMessage(
	ctx context.Context,
	r queuestore.Repository,
) *queuestore.Parcel {
	parcels := loadMessages(ctx, r, 1)

	if len(parcels) == 0 {
		ginkgo.Fail("no messages returned")
	}

	return parcels[0]
}

// loadMessages loads the messages at the head of the queue.
func loadMessages(
	ctx context.Context,
	r queuestore.Repository,
	n int,
) []*queuestore.Parcel {
	parcels, err := r.LoadQueueMessages(ctx, n)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	return parcels
}

package aggregatestore

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/onsi/gomega"
)

// loadRevision loads an aggregate instance revision.
func loadRevision(
	ctx context.Context,
	r aggregatestore.Repository,
	hk, id string,
) aggregatestore.Revision {
	rev, err := r.LoadRevision(ctx, hk, id)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	return rev
}

// incrementRevision increments the revision of an aggregate instance.
func incrementRevision(
	ctx context.Context,
	ds persistence.DataStore,
	hk, id string,
	c aggregatestore.Revision,
) {
	err := persistence.WithTransaction(
		ctx,
		ds,
		func(tx persistence.ManagedTransaction) error {
			return tx.IncrementAggregateRevision(ctx, hk, id, c)
		},
	)

	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
}

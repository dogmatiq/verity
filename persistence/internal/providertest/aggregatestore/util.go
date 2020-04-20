package aggregatestore

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/onsi/gomega"
)

// loadMetaData loads aggregate meta-data for a specific instance.
func loadMetaData(
	ctx context.Context,
	r aggregatestore.Repository,
	hk, id string,
) *aggregatestore.MetaData {
	md, err := r.LoadMetaData(ctx, hk, id)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	return md
}

// saveMetaData persistes meta-data for an aggregate instance.
func saveMetaData(
	ctx context.Context,
	ds persistence.DataStore,
	md *aggregatestore.MetaData,
) {
	err := persistence.WithTransaction(
		ctx,
		ds,
		func(tx persistence.ManagedTransaction) error {
			return tx.SaveAggregateMetaData(ctx, md)
		},
	)

	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
}

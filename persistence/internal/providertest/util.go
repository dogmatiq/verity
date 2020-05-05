package providertest

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
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

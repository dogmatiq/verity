package offsetstore

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/offsetstore"
	"github.com/onsi/gomega"
)

// saveOffset persists an event store offset associated with the given
// application key.
func saveOffset(
	ctx context.Context,
	ds persistence.DataStore,
	ak string,
	c, n uint64,
) {
	_, err := persistence.WithTransaction(
		ctx,
		ds,
		func(tx persistence.ManagedTransaction) error {
			return tx.SaveOffset(ctx, ak, c, n)
		},
	)

	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
}

// loadOffset loads the offset from the repository with the given application
// key.
func loadOffset(
	ctx context.Context,
	repository offsetstore.Repository,
	ak string,
) uint64 {
	o, err := repository.LoadOffset(ctx, ak)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	return o
}

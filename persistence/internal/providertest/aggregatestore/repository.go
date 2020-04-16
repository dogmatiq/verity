package aggregatestore

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// DeclareRepositoryTests declares a functional test-suite for a specific
// aggregatestore.Repository implementation.
func DeclareRepositoryTests(tc *common.TestContext) {
	ginkgo.Describe("type aggregatestore.Repository", func() {
		var (
			dataStore  persistence.DataStore
			repository aggregatestore.Repository
			tearDown   func()
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()
			repository = dataStore.AggregateStoreRepository()
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("func LoadRevision()", func() {
			ginkgo.It("returns zero if the instance does not exist", func() {
				rev := loadRevision(tc.Context, repository, "<handler-key>", "<instance>")
				gomega.Expect(rev).To(gomega.BeEquivalentTo(0))
			})

			ginkgo.XIt("returns the current revision", func() {
				incrementRevision(
					tc.Context,
					dataStore,
					"<handler-key>",
					"<instance>",
					0,
				)

				rev := loadRevision(tc.Context, repository, "<handler-key>", "<instance>")
				gomega.Expect(rev).To(gomega.BeEquivalentTo(1))
			})

			ginkgo.XIt("returns an error if the context is canceled", func() {
				ctx, cancel := context.WithCancel(tc.Context)
				cancel()

				_, err := repository.LoadRevision(ctx, "<handler-key>", "<instance>")
				gomega.Expect(err).To(gomega.Equal(context.Canceled))
			})
		})
	})
}

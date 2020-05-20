package providertest

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// declareAggregateRepositoryTests declares a functional test-suite for a
// specific aggregatestore.Repository implementation.
func declareAggregateRepositoryTests(tc *TestContext) {
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

		ginkgo.Describe("func LoadAggregateMetaData()", func() {
			ginkgo.It("returns meta-data with default values if the instance does not exist", func() {
				md := loadAggregateMetaData(tc.Context, repository, "<handler-key>", "<instance>")
				gomega.Expect(md).To(gomega.Equal(
					aggregatestore.MetaData{
						HandlerKey: "<handler-key>",
						InstanceID: "<instance>",
					},
				))
			})

			ginkgo.It("returns the current persisted meta-data", func() {
				expect := aggregatestore.MetaData{
					HandlerKey:      "<handler-key>",
					InstanceID:      "<instance>",
					InstanceExists:  true,
					LastDestroyedBy: "<message-id>",
				}
				persist(
					tc.Context,
					dataStore,
					persistence.SaveAggregateMetaData{
						MetaData: expect,
					},
				)
				expect.Revision++

				md := loadAggregateMetaData(tc.Context, repository, "<handler-key>", "<instance>")
				gomega.Expect(md).To(gomega.Equal(expect))
			})

			ginkgo.It("does not block if the context is canceled", func() {
				// This test ensures that the implementation returns
				// immediately, either with a context.Canceled error, or with
				// the correct result.

				expect := aggregatestore.MetaData{
					HandlerKey:      "<handler-key>",
					InstanceID:      "<instance>",
					InstanceExists:  true,
					LastDestroyedBy: "<message-id>",
				}
				persist(
					tc.Context,
					dataStore,
					persistence.SaveAggregateMetaData{
						MetaData: expect,
					},
				)
				expect.Revision++

				ctx, cancel := context.WithCancel(tc.Context)
				cancel()

				md, err := repository.LoadAggregateMetaData(ctx, "<handler-key>", "<instance>")
				if err != nil {
					gomega.Expect(err).To(gomega.Equal(context.Canceled))
				} else {
					gomega.Expect(md).To(gomega.Equal(&expect))
				}
			})
		})
	})
}

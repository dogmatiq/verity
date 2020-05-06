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

		ginkgo.Describe("func LoadMetaData()", func() {
			ginkgo.It("returns meta-data with default values if the instance does not exist", func() {
				md := loadMetaData(tc.Context, repository, "<handler-key>", "<instance>")
				gomega.Expect(md).To(gomega.Equal(
					&aggregatestore.MetaData{
						HandlerKey: "<handler-key>",
						InstanceID: "<instance>",
					},
				))
			})

			ginkgo.It("returns the current persisted meta-data", func() {
				expect := &aggregatestore.MetaData{
					HandlerKey:      "<handler-key>",
					InstanceID:      "<instance>",
					InstanceExists:  true,
					LastDestroyedBy: "<message-id>",
					BeginOffset:     1,
					EndOffset:       2,
				}
				saveMetaData(tc.Context, dataStore, expect)
				expect.Revision++

				md := loadMetaData(tc.Context, repository, "<handler-key>", "<instance>")
				gomega.Expect(md).To(gomega.Equal(expect))
			})

			ginkgo.It("returns an error if the context is canceled", func() {
				ctx, cancel := context.WithCancel(tc.Context)
				cancel()

				_, err := repository.LoadMetaData(ctx, "<handler-key>", "<instance>")
				gomega.Expect(err).To(gomega.Equal(context.Canceled))
			})
		})
	})
}

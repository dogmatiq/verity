package providertest

import (
	"context"

	"github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/persistence"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// declareAggregateRepositoryTests declares a functional test-suite for a
// specific persistence.AggregateRepository implementation.
func declareAggregateRepositoryTests(tc *TestContext) {
	ginkgo.Describe("type persistence.AggregateRepository", func() {
		var (
			dataStore persistence.DataStore
			tearDown  func()
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("func LoadAggregateMetaData()", func() {
			ginkgo.It("returns meta-data with default values if the instance does not exist", func() {
				md := loadAggregateMetaData(tc.Context, dataStore, fixtures.DefaultHandlerKey, "<instance>")
				gomega.Expect(md).To(gomega.Equal(
					persistence.AggregateMetaData{
						HandlerKey: fixtures.DefaultHandlerKey,
						InstanceID: "<instance>",
					},
				))
			})

			ginkgo.It("returns the current persisted meta-data", func() {
				expect := persistence.AggregateMetaData{
					HandlerKey:     fixtures.DefaultHandlerKey,
					InstanceID:     "<instance>",
					InstanceExists: true,
					LastEventID:    "<last-event-id>",
					BarrierEventID: "<barrier-event-id>",
				}
				persist(
					tc.Context,
					dataStore,
					persistence.SaveAggregateMetaData{
						MetaData: expect,
					},
				)
				expect.Revision++

				md := loadAggregateMetaData(tc.Context, dataStore, fixtures.DefaultHandlerKey, "<instance>")
				gomega.Expect(md).To(gomega.Equal(expect))
			})

			ginkgo.It("does not block if the context is canceled", func() {
				// This test ensures that the implementation returns
				// immediately, either with a context.Canceled error, or with
				// the correct result.

				expect := persistence.AggregateMetaData{
					HandlerKey:     fixtures.DefaultHandlerKey,
					InstanceID:     "<instance>",
					InstanceExists: true,
					LastEventID:    "<last-event-id>",
					BarrierEventID: "<barrier-event-id>",
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

				md, err := dataStore.LoadAggregateMetaData(ctx, fixtures.DefaultHandlerKey, "<instance>")
				if err != nil {
					gomega.Expect(err).To(gomega.Equal(context.Canceled))
				} else {
					gomega.Expect(md).To(gomega.Equal(expect))
				}
			})
		})
	})
}

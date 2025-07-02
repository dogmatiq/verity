package providertest

import (
	"sync"

	"github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/persistence"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// declareAggregateOperationTests declares a functional test-suite for
// persistence operations related to aggregates.
func declareAggregateOperationTests(tc *TestContext) {
	ginkgo.Context("aggregate operations", func() {
		var dataStore persistence.DataStore

		ginkgo.BeforeEach(func() {
			var tearDown func()
			dataStore, tearDown = tc.SetupDataStore()
			ginkgo.DeferCleanup(tearDown)
		})

		ginkgo.Describe("type persistence.SaveAggregateMetaData", func() {
			ginkgo.When("the instance does not exist", func() {
				ginkgo.It("saves the meta-data with a revision of 1", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveAggregateMetaData{
							MetaData: persistence.AggregateMetaData{
								HandlerKey: fixtures.DefaultHandlerKey,
								InstanceID: "<instance>",
							},
						},
					)

					md := loadAggregateMetaData(tc.Context, dataStore, fixtures.DefaultHandlerKey, "<instance>")
					gomega.Expect(md.Revision).To(gomega.BeEquivalentTo(1))
				})

				ginkgo.It("does not save the meta-data when an OCC conflict occurs", func() {
					op := persistence.SaveAggregateMetaData{
						MetaData: persistence.AggregateMetaData{
							HandlerKey: fixtures.DefaultHandlerKey,
							InstanceID: "<instance>",
							Revision:   123,
						},
					}

					_, err := dataStore.Persist(
						tc.Context,
						persistence.Batch{op},
					)
					gomega.Expect(err).To(gomega.Equal(
						persistence.ConflictError{
							Cause: op,
						},
					))

					md := loadAggregateMetaData(tc.Context, dataStore, fixtures.DefaultHandlerKey, "<instance>")
					gomega.Expect(md).To(gomega.Equal(
						persistence.AggregateMetaData{
							HandlerKey: fixtures.DefaultHandlerKey,
							InstanceID: "<instance>",
							Revision:   0,
						},
					))
				})
			})

			ginkgo.When("the instance exists", func() {
				ginkgo.BeforeEach(func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveAggregateMetaData{
							MetaData: persistence.AggregateMetaData{
								HandlerKey: fixtures.DefaultHandlerKey,
								InstanceID: "<instance>",
							},
						},
					)
				})

				ginkgo.It("increments the revision even if no meta-data has changed", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveAggregateMetaData{
							MetaData: persistence.AggregateMetaData{
								HandlerKey: fixtures.DefaultHandlerKey,
								InstanceID: "<instance>",
								Revision:   1,
							},
						},
					)

					md := loadAggregateMetaData(tc.Context, dataStore, fixtures.DefaultHandlerKey, "<instance>")
					gomega.Expect(md.Revision).To(gomega.BeEquivalentTo(2))
				})

				ginkgo.DescribeTable(
					"it does not save the meta-data when an OCC conflict occurs",
					func(conflictingRevision int) {
						// Increment the meta-data once more so that it's up to
						// revision 2. Otherwise we can't test for 1 as a
						// too-low value.
						persist(
							tc.Context,
							dataStore,
							persistence.SaveAggregateMetaData{
								MetaData: persistence.AggregateMetaData{
									HandlerKey: fixtures.DefaultHandlerKey,
									InstanceID: "<instance>",
									Revision:   1,
								},
							},
						)

						op := persistence.SaveAggregateMetaData{
							MetaData: persistence.AggregateMetaData{
								HandlerKey: fixtures.DefaultHandlerKey,
								InstanceID: "<instance>",
								Revision:   uint64(conflictingRevision),
							},
						}

						_, err := dataStore.Persist(
							tc.Context,
							persistence.Batch{op},
						)
						gomega.Expect(err).To(gomega.Equal(
							persistence.ConflictError{
								Cause: op,
							},
						))

						md := loadAggregateMetaData(tc.Context, dataStore, fixtures.DefaultHandlerKey, "<instance>")
						gomega.Expect(md).To(gomega.Equal(
							persistence.AggregateMetaData{
								HandlerKey: fixtures.DefaultHandlerKey,
								InstanceID: "<instance>",
								Revision:   2,
							},
						))
					},
					ginkgo.Entry("zero", 0),
					ginkgo.Entry("too low", 1),
					ginkgo.Entry("too high", 100),
				)
			})

			ginkgo.It("serializes operations from concurrent persist calls", func() {
				var g sync.WaitGroup

				fn := func(hk, id string, count uint64) {
					defer ginkgo.GinkgoRecover()
					defer g.Done()

					for i := uint64(0); i < count; i++ {
						persist(
							tc.Context,
							dataStore,
							persistence.SaveAggregateMetaData{
								MetaData: persistence.AggregateMetaData{
									HandlerKey: hk,
									InstanceID: id,
									Revision:   i,
								},
							},
						)
					}
				}

				// Note the overlap of handler keys and instance IDs.
				g.Add(3)
				go fn("62f12b60-52e4-4223-9b44-68ec63139f8a", "<instance-a>", 1)
				go fn("62f12b60-52e4-4223-9b44-68ec63139f8a", "<instance-b>", 2)
				go fn("dd581546-2cc2-486b-888d-b4a8e035fd9c", "<instance-a>", 3)
				g.Wait()

				md := loadAggregateMetaData(tc.Context, dataStore, "62f12b60-52e4-4223-9b44-68ec63139f8a", "<instance-a>")
				gomega.Expect(md).To(gomega.Equal(
					persistence.AggregateMetaData{
						HandlerKey: "62f12b60-52e4-4223-9b44-68ec63139f8a",
						InstanceID: "<instance-a>",
						Revision:   1,
					},
				))

				md = loadAggregateMetaData(tc.Context, dataStore, "62f12b60-52e4-4223-9b44-68ec63139f8a", "<instance-b>")
				gomega.Expect(md).To(gomega.Equal(
					persistence.AggregateMetaData{
						HandlerKey: "62f12b60-52e4-4223-9b44-68ec63139f8a",
						InstanceID: "<instance-b>",
						Revision:   2,
					},
				))

				md = loadAggregateMetaData(tc.Context, dataStore, "dd581546-2cc2-486b-888d-b4a8e035fd9c", "<instance-a>")
				gomega.Expect(md).To(gomega.Equal(
					persistence.AggregateMetaData{
						HandlerKey: "dd581546-2cc2-486b-888d-b4a8e035fd9c",
						InstanceID: "<instance-a>",
						Revision:   3,
					},
				))
			})
		})
	})
}

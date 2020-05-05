package providertest

import (
	"sync"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
)

// declareAggregateOperationTests declares a functional test-suite for
// persistence operations related to aggregates.
func declareAggregateOperationTests(tc *common.TestContext) {
	ginkgo.Context("aggregate operations", func() {
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

		ginkgo.Describe("type persistence.SaveAggregateMetaData", func() {
			ginkgo.When("the instance does not exist", func() {
				ginkgo.It("saves the meta-data with a revision of 1", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveAggregateMetaData{
							MetaData: aggregatestore.MetaData{
								HandlerKey: "<handler-key>",
								InstanceID: "<instance>",
							},
						},
					)

					md := loadAggregateMetaData(tc.Context, repository, "<handler-key>", "<instance>")
					gomega.Expect(md.Revision).To(gomega.BeEquivalentTo(1))
				})

				ginkgo.It("does not save the meta-data when an OCC conflict occurs", func() {
					op := persistence.SaveAggregateMetaData{
						MetaData: aggregatestore.MetaData{
							HandlerKey: "<handler-key>",
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

					md := loadAggregateMetaData(tc.Context, repository, "<handler-key>", "<instance>")
					gomega.Expect(md.Revision).To(gomega.BeEquivalentTo(0))
				})
			})

			ginkgo.When("the instance exists", func() {
				ginkgo.BeforeEach(func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveAggregateMetaData{
							MetaData: aggregatestore.MetaData{
								HandlerKey: "<handler-key>",
								InstanceID: "<instance>",
							},
						},
					)
				})

				ginkgo.It("increments the revision", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveAggregateMetaData{
							MetaData: aggregatestore.MetaData{
								HandlerKey:  "<handler-key>",
								InstanceID:  "<instance>",
								Revision:    1,
								BeginOffset: 1,
								EndOffset:   2,
							},
						},
					)

					md := loadAggregateMetaData(tc.Context, repository, "<handler-key>", "<instance>")
					gomega.Expect(md.Revision).To(gomega.BeEquivalentTo(2))
				})

				ginkgo.It("increments the revision even if no meta-data has changed", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveAggregateMetaData{
							MetaData: aggregatestore.MetaData{
								HandlerKey: "<handler-key>",
								InstanceID: "<instance>",
								Revision:   1,
							},
						},
					)

					md := loadAggregateMetaData(tc.Context, repository, "<handler-key>", "<instance>")
					gomega.Expect(md.Revision).To(gomega.BeEquivalentTo(2))
				})

				table.DescribeTable(
					"it does not increment the revision when an OCC conflict occurs",
					func(conflictingRevision int) {
						// Increment the revision once more so that it's up to
						// revision 2. Otherwise we can't test for 1 as a
						// too-low value.
						persist(
							tc.Context,
							dataStore,
							persistence.SaveAggregateMetaData{
								MetaData: aggregatestore.MetaData{
									HandlerKey: "<handler-key>",
									InstanceID: "<instance>",
									Revision:   1,
								},
							},
						)

						op := persistence.SaveAggregateMetaData{
							MetaData: aggregatestore.MetaData{
								HandlerKey: "<handler-key>",
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

						md := loadAggregateMetaData(tc.Context, repository, "<handler-key>", "<instance>")
						gomega.Expect(md.Revision).To(gomega.BeEquivalentTo(2))
					},
					table.Entry("zero", 0),
					table.Entry("too low", 1),
					table.Entry("too high", 100),
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
								MetaData: aggregatestore.MetaData{
									HandlerKey:  hk,
									InstanceID:  id,
									Revision:    i,
									BeginOffset: 100 + i,
									EndOffset:   200 + i,
								},
							},
						)
					}
				}

				// Note the overlap of handler keys and instance IDs.
				g.Add(3)
				go fn("<handler-key-1>", "<instance-a>", 1)
				go fn("<handler-key-1>", "<instance-b>", 2)
				go fn("<handler-key-2>", "<instance-a>", 3)
				g.Wait()

				md := loadAggregateMetaData(tc.Context, repository, "<handler-key-1>", "<instance-a>")
				gomega.Expect(md).To(gomega.Equal(&aggregatestore.MetaData{
					HandlerKey:  "<handler-key-1>",
					InstanceID:  "<instance-a>",
					Revision:    1,
					BeginOffset: 100,
					EndOffset:   200,
				}))

				md = loadAggregateMetaData(tc.Context, repository, "<handler-key-1>", "<instance-b>")
				gomega.Expect(md).To(gomega.Equal(&aggregatestore.MetaData{
					HandlerKey:  "<handler-key-1>",
					InstanceID:  "<instance-b>",
					Revision:    2,
					BeginOffset: 101,
					EndOffset:   201,
				}))

				md = loadAggregateMetaData(tc.Context, repository, "<handler-key-2>", "<instance-a>")
				gomega.Expect(md).To(gomega.Equal(&aggregatestore.MetaData{
					HandlerKey:  "<handler-key-2>",
					InstanceID:  "<instance-a>",
					Revision:    3,
					BeginOffset: 102,
					EndOffset:   202,
				}))
			})
		})
	})
}

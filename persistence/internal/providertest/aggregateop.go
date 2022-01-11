package providertest

import (
	"github.com/dogmatiq/marshalkit/fixtures"
	"strconv"
	"sync"

	"github.com/dogmatiq/verity/persistence"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
)

// declareAggregateOperationTests declares a functional test-suite for
// persistence operations related to aggregates.
func declareAggregateOperationTests(tc *TestContext) {
	ginkgo.Context("aggregate operations", func() {
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

		ginkgo.Describe("type persistence.SaveAggregateMetaData", func() {
			ginkgo.When("the instance does not exist", func() {
				ginkgo.It("saves the meta-data with a revision of 1", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveAggregateMetaData{
							MetaData: persistence.AggregateMetaData{
								HandlerKey: "<handler-key>",
								InstanceID: "<instance>",
							},
						},
					)

					md := loadAggregateMetaData(tc.Context, dataStore, "<handler-key>", "<instance>")
					gomega.Expect(md.Revision).To(gomega.BeEquivalentTo(1))
				})

				ginkgo.It("does not save the meta-data when an OCC conflict occurs", func() {
					op := persistence.SaveAggregateMetaData{
						MetaData: persistence.AggregateMetaData{
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

					md := loadAggregateMetaData(tc.Context, dataStore, "<handler-key>", "<instance>")
					gomega.Expect(md).To(gomega.Equal(
						persistence.AggregateMetaData{
							HandlerKey: "<handler-key>",
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
								HandlerKey: "<handler-key>",
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
								HandlerKey: "<handler-key>",
								InstanceID: "<instance>",
								Revision:   1,
							},
						},
					)

					md := loadAggregateMetaData(tc.Context, dataStore, "<handler-key>", "<instance>")
					gomega.Expect(md.Revision).To(gomega.BeEquivalentTo(2))
				})

				table.DescribeTable(
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
									HandlerKey: "<handler-key>",
									InstanceID: "<instance>",
									Revision:   1,
								},
							},
						)

						op := persistence.SaveAggregateMetaData{
							MetaData: persistence.AggregateMetaData{
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

						md := loadAggregateMetaData(tc.Context, dataStore, "<handler-key>", "<instance>")
						gomega.Expect(md).To(gomega.Equal(
							persistence.AggregateMetaData{
								HandlerKey: "<handler-key>",
								InstanceID: "<instance>",
								Revision:   2,
							},
						))
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
				go fn("<handler-key-1>", "<instance-a>", 1)
				go fn("<handler-key-1>", "<instance-b>", 2)
				go fn("<handler-key-2>", "<instance-a>", 3)
				g.Wait()

				md := loadAggregateMetaData(tc.Context, dataStore, "<handler-key-1>", "<instance-a>")
				gomega.Expect(md).To(gomega.Equal(
					persistence.AggregateMetaData{
						HandlerKey: "<handler-key-1>",
						InstanceID: "<instance-a>",
						Revision:   1,
					},
				))

				md = loadAggregateMetaData(tc.Context, dataStore, "<handler-key-1>", "<instance-b>")
				gomega.Expect(md).To(gomega.Equal(
					persistence.AggregateMetaData{
						HandlerKey: "<handler-key-1>",
						InstanceID: "<instance-b>",
						Revision:   2,
					},
				))

				md = loadAggregateMetaData(tc.Context, dataStore, "<handler-key-2>", "<instance-a>")
				gomega.Expect(md).To(gomega.Equal(
					persistence.AggregateMetaData{
						HandlerKey: "<handler-key-2>",
						InstanceID: "<instance-a>",
						Revision:   3,
					},
				))
			})
		})

		ginkgo.Describe("type persistence.SaveAggregateSnapshot", func() {
			ginkgo.When("there is no snapshot of the instance", func() {
				ginkgo.It("creates the instance", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveAggregateSnapshot{
							Snapshot: persistence.AggregateSnapshot{
								HandlerKey:  "<handler-key>",
								InstanceID:  "<instance>",
								LastEventID: "<last-event-id>",
								Packet:      fixtures.MessageA1Packet,
							},
						},
					)

					ss, ok := loadAggregateSnapshot(tc.Context, dataStore, "<handler-key>", "<instance>")

					gomega.Expect(ok).To(gomega.BeTrue())
					gomega.Expect(ss).To(gomega.BeEquivalentTo(persistence.AggregateSnapshot{
						HandlerKey:  "<handler-key>",
						InstanceID:  "<instance>",
						LastEventID: "<last-event-id>",
						Packet:      fixtures.MessageA1Packet,
					}))
				})
			})

			ginkgo.When("the instance exists", func() {
				ginkgo.BeforeEach(func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveAggregateSnapshot{
							Snapshot: persistence.AggregateSnapshot{
								HandlerKey:  "<handler-key>",
								InstanceID:  "<instance>",
								LastEventID: "<last-event-id>",
								Packet:      fixtures.MessageA1Packet,
							},
						},
					)
				})

				ginkgo.It("updates the instance", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveAggregateSnapshot{
							Snapshot: persistence.AggregateSnapshot{
								HandlerKey:  "<handler-key>",
								InstanceID:  "<instance>",
								LastEventID: "<last-event-id-2>",
								Packet:      fixtures.MessageA1Packet,
							},
						},
					)

					ss, ok := loadAggregateSnapshot(tc.Context, dataStore, "<handler-key>", "<instance>")

					gomega.Expect(ok).To(gomega.BeTrue())
					gomega.Expect(ss).To(gomega.BeEquivalentTo(persistence.AggregateSnapshot{
						HandlerKey:  "<handler-key>",
						InstanceID:  "<instance>",
						LastEventID: "<last-event-id-2>",
						Packet:      fixtures.MessageA1Packet,
					}))
				})
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
							persistence.SaveAggregateSnapshot{
								Snapshot: persistence.AggregateSnapshot{
									HandlerKey:  hk,
									InstanceID:  id,
									LastEventID: strconv.FormatUint(i, 10),
									Packet:      fixtures.MessageA1Packet,
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

				ss, ok := loadAggregateSnapshot(tc.Context, dataStore, "<handler-key-1>", "<instance-a>")
				gomega.Expect(ok).To(gomega.BeTrue())
				gomega.Expect(ss).To(gomega.Equal(
					persistence.AggregateSnapshot{
						HandlerKey:  "<handler-key-1>",
						InstanceID:  "<instance-a>",
						LastEventID: "0",
						Packet:      fixtures.MessageA1Packet,
					},
				))

				ss, ok = loadAggregateSnapshot(tc.Context, dataStore, "<handler-key-1>", "<instance-b>")
				gomega.Expect(ok).To(gomega.BeTrue())
				gomega.Expect(ss).To(gomega.Equal(
					persistence.AggregateSnapshot{
						HandlerKey:  "<handler-key-1>",
						InstanceID:  "<instance-b>",
						LastEventID: "1",
						Packet:      fixtures.MessageA1Packet,
					},
				))

				ss, ok = loadAggregateSnapshot(tc.Context, dataStore, "<handler-key-2>", "<instance-a>")
				gomega.Expect(ok).To(gomega.BeTrue())
				gomega.Expect(ss).To(gomega.Equal(
					persistence.AggregateSnapshot{
						HandlerKey:  "<handler-key-2>",
						InstanceID:  "<instance-a>",
						LastEventID: "2",
						Packet:      fixtures.MessageA1Packet,
					},
				))
			})
		})
	})
}

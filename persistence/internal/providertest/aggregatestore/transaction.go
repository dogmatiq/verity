package aggregatestore

import (
	"sync"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
)

// DeclareTransactionTests declares a functional test-suite for a specific
// aggregatestore.Transaction implementation.
func DeclareTransactionTests(tc *common.TestContext) {
	ginkgo.Describe("type aggregatestore.Transaction", func() {
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

		ginkgo.Describe("func SaveAggregateMetaData()", func() {
			ginkgo.When("the instance does not exist", func() {
				ginkgo.It("saves the meta-data with a revision of 1", func() {
					saveMetaData(
						tc.Context,
						dataStore,
						&aggregatestore.MetaData{
							HandlerKey: "<handler-key>",
							InstanceID: "<instance>",
						},
					)

					md := loadMetaData(tc.Context, repository, "<handler-key>", "<instance>")
					gomega.Expect(md.Revision).To(gomega.BeEquivalentTo(1))
				})

				ginkgo.It("does not save the meta-data if the transaction is rolled back", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							return tx.SaveAggregateMetaData(
								tc.Context,
								&aggregatestore.MetaData{
									HandlerKey: "<handler-key>",
									InstanceID: "<instance>",
								},
							)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					md := loadMetaData(tc.Context, repository, "<handler-key>", "<instance>")
					gomega.Expect(md.Revision).To(gomega.BeEquivalentTo(0))
				})

				ginkgo.It("does not save the message when an OCC conflict occurs", func() {
					_, err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							err := tx.SaveAggregateMetaData(
								tc.Context,
								&aggregatestore.MetaData{
									HandlerKey: "<handler-key>",
									InstanceID: "<instance>",
									Revision:   123,
								},
							)
							gomega.Expect(err).To(gomega.Equal(aggregatestore.ErrConflict))
							return nil // let the transaction commit despite error
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					md := loadMetaData(tc.Context, repository, "<handler-key>", "<instance>")
					gomega.Expect(md.Revision).To(gomega.BeEquivalentTo(0))
				})
			})

			ginkgo.When("the instance exists", func() {
				ginkgo.BeforeEach(func() {
					saveMetaData(
						tc.Context,
						dataStore,
						&aggregatestore.MetaData{
							HandlerKey: "<handler-key>",
							InstanceID: "<instance>",
						},
					)
				})

				ginkgo.It("increments the revision", func() {
					saveMetaData(
						tc.Context,
						dataStore,
						&aggregatestore.MetaData{
							HandlerKey:      "<handler-key>",
							InstanceID:      "<instance>",
							InstanceExists:  true,
							LastDestroyedBy: "<message-id>",
							Revision:        1,
						},
					)

					md := loadMetaData(tc.Context, repository, "<handler-key>", "<instance>")
					gomega.Expect(md.Revision).To(gomega.BeEquivalentTo(2))
				})

				ginkgo.It("increments the revision even if no meta-data has changed", func() {
					saveMetaData(
						tc.Context,
						dataStore,
						&aggregatestore.MetaData{
							HandlerKey: "<handler-key>",
							InstanceID: "<instance>",
							Revision:   1,
						},
					)

					md := loadMetaData(tc.Context, repository, "<handler-key>", "<instance>")
					gomega.Expect(md.Revision).To(gomega.BeEquivalentTo(2))
				})

				ginkgo.It("does not update the meta-data if the transaction is rolled-back", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							return tx.SaveAggregateMetaData(
								tc.Context,
								&aggregatestore.MetaData{
									HandlerKey:      "<handler-key>",
									InstanceID:      "<instance>",
									InstanceExists:  true,
									LastDestroyedBy: "<message-id>",
									Revision:        1,
								},
							)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					md := loadMetaData(tc.Context, repository, "<handler-key>", "<instance>")
					gomega.Expect(md.Revision).To(gomega.BeEquivalentTo(1))
				})

				table.DescribeTable(
					"it does not increment the revision when an OCC conflict occurs",
					func(conflictingRevision int) {
						// Increment the revision once more so that it's up to
						// revision 2. Otherwise we can't test for 1 as a
						// too-low value.
						saveMetaData(
							tc.Context,
							dataStore,
							&aggregatestore.MetaData{
								HandlerKey: "<handler-key>",
								InstanceID: "<instance>",
								Revision:   1,
							},
						)

						_, err := persistence.WithTransaction(
							tc.Context,
							dataStore,
							func(tx persistence.ManagedTransaction) error {
								err := tx.SaveAggregateMetaData(
									tc.Context,
									&aggregatestore.MetaData{
										HandlerKey: "<handler-key>",
										InstanceID: "<instance>",
										Revision:   uint64(conflictingRevision),
									},
								)
								gomega.Expect(err).To(gomega.Equal(aggregatestore.ErrConflict))

								return nil // let the transaction commit despite error
							},
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						md := loadMetaData(tc.Context, repository, "<handler-key>", "<instance>")
						gomega.Expect(md.Revision).To(gomega.BeEquivalentTo(2))
					},
					table.Entry("zero", 0),
					table.Entry("too low", 1),
					table.Entry("too high", 100),
				)
			})

			ginkgo.When("an instance's meta-data is saved more than once in the same transaction", func() {
				ginkgo.It("saves the meta-data from the most recent call", func() {
					_, err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if err := tx.SaveAggregateMetaData(
								tc.Context,
								&aggregatestore.MetaData{
									HandlerKey: "<handler-key>",
									InstanceID: "<instance>",
								},
							); err != nil {
								return err
							}

							return tx.SaveAggregateMetaData(
								tc.Context,
								&aggregatestore.MetaData{
									HandlerKey: "<handler-key>",
									InstanceID: "<instance>",
									Revision:   1,
								},
							)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					md := loadMetaData(tc.Context, repository, "<handler-key>", "<instance>")
					gomega.Expect(md).To(gomega.Equal(
						&aggregatestore.MetaData{
							HandlerKey: "<handler-key>",
							InstanceID: "<instance>",
							Revision:   2,
						},
					))
				})

				ginkgo.It("uses the uncommitted revision for OCC checks", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if err := tx.SaveAggregateMetaData(
								tc.Context,
								&aggregatestore.MetaData{
									HandlerKey: "<handler-key>",
									InstanceID: "<instance>",
								},
							); err != nil {
								return err
							}

							// Note that we are passing the same revision again.
							return tx.SaveAggregateMetaData(
								tc.Context,
								&aggregatestore.MetaData{
									HandlerKey: "<handler-key>",
									InstanceID: "<instance>",
								},
							)
						},
					)
					gomega.Expect(err).To(gomega.Equal(aggregatestore.ErrConflict))
				})
			})

			ginkgo.It("serializes operations from competing transactions", func() {
				ginkgo.By("running several transactions in parallel")

				var g sync.WaitGroup

				fn := func(hk, id string, count int) {
					defer ginkgo.GinkgoRecover()
					defer g.Done()

					_, err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							for i := 0; i < count; i++ {
								if err := tx.SaveAggregateMetaData(
									tc.Context,
									&aggregatestore.MetaData{
										HandlerKey: hk,
										InstanceID: id,
										Revision:   uint64(i),
									},
								); err != nil {
									return err
								}
							}

							return nil
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				}

				// Note the overlap of handler keys and instance IDs.
				g.Add(3)
				go fn("<handler-key-1>", "<instance-a>", 1)
				go fn("<handler-key-1>", "<instance-b>", 2)
				go fn("<handler-key-2>", "<instance-a>", 3)
				g.Wait()

				md := loadMetaData(tc.Context, repository, "<handler-key-1>", "<instance-a>")
				gomega.Expect(md).To(gomega.Equal(&aggregatestore.MetaData{
					HandlerKey: "<handler-key-1>",
					InstanceID: "<instance-a>",
					Revision:   1,
				}))

				md = loadMetaData(tc.Context, repository, "<handler-key-1>", "<instance-b>")
				gomega.Expect(md).To(gomega.Equal(&aggregatestore.MetaData{
					HandlerKey: "<handler-key-1>",
					InstanceID: "<instance-b>",
					Revision:   2,
				}))

				md = loadMetaData(tc.Context, repository, "<handler-key-2>", "<instance-a>")
				gomega.Expect(md).To(gomega.Equal(&aggregatestore.MetaData{
					HandlerKey: "<handler-key-2>",
					InstanceID: "<instance-a>",
					Revision:   3,
				}))
			})
		})
	})
}

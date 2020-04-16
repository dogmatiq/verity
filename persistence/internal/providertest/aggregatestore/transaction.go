package aggregatestore

import (
	"sync"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
)

// DeclareTransactionTests declares a functional test-suite for a specific
// aggregatestore.Transaction implementation.
func DeclareTransactionTests(tc *common.TestContext) {
	ginkgo.XDescribe("type aggregatestore.Transaction", func() {
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

		ginkgo.Describe("func IncrementAggregateRevision()", func() {
			ginkgo.When("the instance does not exist", func() {
				ginkgo.It("sets the revision to 1", func() {
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

				ginkgo.It("does not increment the revision if the transaction is rolled back", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							return tx.IncrementAggregateRevision(
								tc.Context,
								"<handler-key>",
								"<instance>",
								0,
							)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					rev := loadRevision(tc.Context, repository, "<handler-key>", "<instance>")
					gomega.Expect(rev).To(gomega.BeEquivalentTo(0))
				})

				ginkgo.It("does not save the message when an OCC conflict occurs", func() {
					err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							err := tx.IncrementAggregateRevision(
								tc.Context,
								"<handler-key>",
								"<instance>",
								123,
							)
							gomega.Expect(err).To(gomega.Equal(aggregatestore.ErrConflict))
							return nil // let the transaction commit despite error
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					rev := loadRevision(tc.Context, repository, "<handler-key>", "<instance>")
					gomega.Expect(rev).To(gomega.BeEquivalentTo(0))
				})
			})

			ginkgo.When("the instance exists", func() {
				ginkgo.BeforeEach(func() {
					incrementRevision(
						tc.Context,
						dataStore,
						"<handler-key>",
						"<instance>",
						0,
					)
				})

				ginkgo.It("increments the revision", func() {
					incrementRevision(
						tc.Context,
						dataStore,
						"<handler-key>",
						"<instance>",
						1,
					)

					rev := loadRevision(tc.Context, repository, "<handler-key>", "<instance>")
					gomega.Expect(rev).To(gomega.BeEquivalentTo(2))
				})

				ginkgo.It("does not update the message if the transaction is rolled-back", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							return tx.IncrementAggregateRevision(
								tc.Context,
								"<handler-key>",
								"<instance>",
								1,
							)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					rev := loadRevision(tc.Context, repository, "<handler-key>", "<instance>")
					gomega.Expect(rev).To(gomega.BeEquivalentTo(1))
				})

				table.XDescribeTable(
					"it does not increment the revision when an OCC conflict occurs",
					func(conflictingRevision aggregatestore.Revision) {
						// Increment the revision once more so that it's up to
						// revision 2. Otherwise we can't test for 1 as a
						// too-low value.
						incrementRevision(
							tc.Context,
							dataStore,
							"<handler-key>",
							"<instance>",
							1,
						)

						err := persistence.WithTransaction(
							tc.Context,
							dataStore,
							func(tx persistence.ManagedTransaction) error {
								err := tx.IncrementAggregateRevision(
									tc.Context,
									"<handler-key>",
									"<instance>",
									conflictingRevision,
								)
								gomega.Expect(err).To(gomega.Equal(aggregatestore.ErrConflict))

								return nil // let the transaction commit despite error
							},
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						rev := loadRevision(tc.Context, repository, "<handler-key>", "<instance>")
						gomega.Expect(rev).To(gomega.BeEquivalentTo(1))
					},
					table.Entry("zero", 0),
					table.Entry("too low", 1),
					table.Entry("too high", 100),
				)
			})

			ginkgo.When("an instance's revision is incremented more than once in the same transaction", func() {
				ginkgo.It("saves the highest revision", func() {
					err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if err := tx.IncrementAggregateRevision(
								tc.Context,
								"<handler-key>",
								"<instance>",
								0,
							); err != nil {
								return err
							}

							return tx.IncrementAggregateRevision(
								tc.Context,
								"<handler-key>",
								"<instance>",
								1,
							)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					rev := loadRevision(tc.Context, repository, "<handler-key>", "<instance>")
					gomega.Expect(rev).To(gomega.BeEquivalentTo(2))
				})

				ginkgo.It("uses the uncommitted revision for OCC checks", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if err := tx.IncrementAggregateRevision(
								tc.Context,
								"<handler-key>",
								"<instance>",
								0,
							); err != nil {
								return err
							}

							// Note that we are passing the same revision again.
							return tx.IncrementAggregateRevision(
								tc.Context,
								"<handler-key>",
								"<instance>",
								0,
							)
						},
					)
					gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))
				})
			})

			ginkgo.It("serializes operations from competing transactions", func() {
				ginkgo.By("running several transactions in parallel")

				var g sync.WaitGroup

				fn := func(hk, id string, count int) {
					defer ginkgo.GinkgoRecover()
					defer g.Done()

					err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							for i := 0; i < count; i++ {
								if err := tx.IncrementAggregateRevision(
									tc.Context,
									"<handler-key>",
									"<instance>",
									aggregatestore.Revision(count),
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

				rev := loadRevision(tc.Context, repository, "<handler-key-1>", "<instance-a>")
				gomega.Expect(rev).To(gomega.BeEquivalentTo(1))

				rev = loadRevision(tc.Context, repository, "<handler-key-1>", "<instance-b>")
				gomega.Expect(rev).To(gomega.BeEquivalentTo(2))

				rev = loadRevision(tc.Context, repository, "<handler-key-2>", "<instance-a>")
				gomega.Expect(rev).To(gomega.BeEquivalentTo(3))
			})
		})
	})
}

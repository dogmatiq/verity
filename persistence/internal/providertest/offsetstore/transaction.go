package offsetstore

import (
	"sync"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	"github.com/dogmatiq/infix/persistence/subsystem/offsetstore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
)

// DeclareTransactionTests declares a functional test-suite for a specific
// offsetstore.Transaction implementation.
func DeclareTransactionTests(tc *common.TestContext) {
	ginkgo.Describe("type offsetstore.Transaction", func() {
		var (
			dataStore  persistence.DataStore
			repository offsetstore.Repository
			tearDown   func()
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()
			repository = dataStore.OffsetStoreRepository()
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("func SaveOffset()", func() {
			ginkgo.When("application has no previous offsets associated", func() {
				ginkgo.It("saves an offset", func() {
					saveOffset(tc.Context, dataStore, "<source-app-key>", 0, 1)

					actual := loadOffset(tc.Context, repository, "<source-app-key>")
					gomega.Expect(actual).To(gomega.BeEquivalentTo(1))
				})

				ginkgo.It("it does not update the offset when an OCC conflict occurs", func() {
					err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							return tx.SaveOffset(
								tc.Context,
								"<source-app-key>",
								1,
								2,
							)
						},
					)
					gomega.Expect(err).To(gomega.Equal(offsetstore.ErrConflict))

					actual := loadOffset(tc.Context, repository, "<source-app-key>")
					gomega.Expect(actual).To(gomega.BeEquivalentTo(0))
				})

				ginkgo.It("does not save the offset when the transaction is rolled-back", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							return tx.SaveOffset(
								tc.Context,
								"<source-app-key>",
								0,
								1,
							)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					actual := loadOffset(tc.Context, repository, "<source-app-key>")
					gomega.Expect(actual).To(gomega.BeEquivalentTo(0))
				})
			})

			ginkgo.When("application has previous offsets associated", func() {
				var current uint64

				ginkgo.BeforeEach(func() {
					current = 0
					saveOffset(tc.Context, dataStore, "<source-app-key>", current, 5)
					current = 5
				})

				ginkgo.It("updates the offset", func() {
					saveOffset(tc.Context, dataStore, "<source-app-key>", current, 123)

					actual := loadOffset(tc.Context, repository, "<source-app-key>")
					gomega.Expect(actual).To(gomega.BeEquivalentTo(123))
				})

				table.DescribeTable(
					"it does not update the offset when an OCC conflict occurs",
					func(conflictingOffset uint64) {
						err := persistence.WithTransaction(
							tc.Context,
							dataStore,
							func(tx persistence.ManagedTransaction) error {
								err := tx.SaveOffset(
									tc.Context,
									"<source-app-key>",
									conflictingOffset,
									123,
								)
								gomega.Expect(err).To(gomega.Equal(offsetstore.ErrConflict))

								return nil // let the transaction commit despite error
							},
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						actual := loadOffset(tc.Context, repository, "<source-app-key>")
						gomega.Expect(actual).To(gomega.BeEquivalentTo(current))
					},
					table.Entry("zero", uint64(0)),
					table.Entry("too low", uint64(1)),
					table.Entry("too high", uint64(100)),
				)

				ginkgo.It("does not save the offset when the transaction is rolled-back", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							return tx.SaveOffset(
								tc.Context,
								"<source-app-key>",
								current,
								123,
							)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					actual := loadOffset(tc.Context, repository, "<source-app-key>")
					gomega.Expect(actual).To(gomega.BeEquivalentTo(current))
				})
			})

			ginkgo.When("called multiple times within the same transaction", func() {
				ginkgo.It("saves the last offset", func() {
					err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							err := tx.SaveOffset(tc.Context, "<source-app-key>", 0, 123)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

							err = tx.SaveOffset(tc.Context, "<source-app-key>", 123, 456)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

							return nil
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					actual := loadOffset(tc.Context, repository, "<source-app-key>")
					gomega.Expect(actual).To(gomega.BeEquivalentTo(456))
				})

				ginkgo.It("uses the uncommitted revision for OCC checks", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if err := tx.SaveOffset(
								tc.Context,
								"<source-app-key>",
								0, 1,
							); err != nil {
								return err
							}

							// Note that we did not increment the current offset
							// after the the first save.
							return tx.SaveOffset(
								tc.Context,
								"<source-app-key>",
								0, 123,
							)
						},
					)
					gomega.Expect(err).To(gomega.Equal(offsetstore.ErrConflict))
				})
			})

			ginkgo.When("used in concurrent transactions", func() {
				ginkgo.It("saves offsets concurrently", func() {
					var g sync.WaitGroup

					fn := func(ak string, n uint64) {
						defer ginkgo.GinkgoRecover()
						defer g.Done()

						err := persistence.WithTransaction(
							tc.Context,
							dataStore,
							func(tx persistence.ManagedTransaction) error {
								return tx.SaveOffset(tc.Context, ak, 0, n)
							},
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					}

					g.Add(3)
					go fn("<source-app1-key>", 1)
					go fn("<source-app2-key>", 2)
					go fn("<source-app3-key>", 3)
					g.Wait()

					actual := loadOffset(tc.Context, repository, "<source-app1-key>")
					gomega.Expect(actual).To(gomega.BeEquivalentTo(1))

					actual = loadOffset(tc.Context, repository, "<source-app2-key>")
					gomega.Expect(actual).To(gomega.BeEquivalentTo(2))

					actual = loadOffset(tc.Context, repository, "<source-app3-key>")
					gomega.Expect(actual).To(gomega.BeEquivalentTo(3))
				})
			})
		})
	})
}

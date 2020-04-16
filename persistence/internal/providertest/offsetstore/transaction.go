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
					c := offsetstore.Offset(0)
					n := offsetstore.Offset(1)
					saveOffset(tc.Context, dataStore, "<source-app-key>", c, n)

					actual, err := repository.LoadOffset(tc.Context, "<source-app-key>")
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(actual).To(gomega.BeEquivalentTo(n))
				})

				table.DescribeTable(
					"it does not update the offset when an OCC conflict occurs",
					func(conflictingCurrentOffset, nextOffset offsetstore.Offset) {
						err := persistence.WithTransaction(
							tc.Context,
							dataStore,
							func(tx persistence.ManagedTransaction) error {
								err := tx.SaveOffset(
									tc.Context,
									"<source-app-key>",
									conflictingCurrentOffset,
									nextOffset,
								)
								gomega.Expect(err).To(gomega.Equal(offsetstore.ErrConflict))

								return nil // let the transaction commit despite error
							},
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						actual, err := repository.LoadOffset(tc.Context, "<source-app-key>")
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(actual).To(gomega.BeEquivalentTo(0))
					},
					table.Entry("high pair low range", 1, 2),
					table.Entry("high pair high range", 99, 100),
				)
			})

			ginkgo.When("application has previous offsets associated", func() {
				ginkgo.It("updates the offset", func() {
					c := offsetstore.Offset(0)
					n := offsetstore.Offset(1)
					saveOffset(tc.Context, dataStore, "<source-app-key>", c, n)

					c = n
					n += 123

					saveOffset(tc.Context, dataStore, "<source-app-key>", c, n)

					actual, err := repository.LoadOffset(tc.Context, "<source-app-key>")
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(actual).To(gomega.BeEquivalentTo(n))
				})

				table.DescribeTable(
					"it does not update the offset when an OCC conflict occurs",
					func(conflictingCurrentOffset, nextOffset offsetstore.Offset) {
						// Update the current offset twice so that it's up to 1.
						c := offsetstore.Offset(0)
						n := offsetstore.Offset(1)

						saveOffset(tc.Context, dataStore, "<source-app-key>", c, n)

						err := persistence.WithTransaction(
							tc.Context,
							dataStore,
							func(tx persistence.ManagedTransaction) error {
								err := tx.SaveOffset(
									tc.Context,
									"<source-app-key>",
									conflictingCurrentOffset,
									nextOffset,
								)
								gomega.Expect(err).To(gomega.Equal(offsetstore.ErrConflict))

								return nil // let the transaction commit despite error
							},
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						actual, err := repository.LoadOffset(tc.Context, "<source-app-key>")
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(actual).To(gomega.BeEquivalentTo(n))
					},
					table.Entry("too low pair", 0, 1),
					table.Entry("too high pair", 99, 100),
				)
			})

			ginkgo.When("called multiple times within the same transaction", func() {
				ginkgo.It("saves the last offset", func() {
					c := offsetstore.Offset(0)
					n := offsetstore.Offset(1)

					err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							err := tx.SaveOffset(tc.Context, "<source-app-key>", c, n)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

							c = n
							n += 123

							err = tx.SaveOffset(tc.Context, "<source-app-key>", c, n)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

							return nil
						},
					)

					actual, err := repository.LoadOffset(tc.Context, "<source-app-key>")
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(actual).To(gomega.BeEquivalentTo(n))
				})
			})

			ginkgo.When("the transaction is rolled-back", func() {
				ginkgo.It("does not save the offset", func() {
					c := offsetstore.Offset(0)
					n := offsetstore.Offset(1)

					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							return tx.SaveOffset(
								tc.Context,
								"<source-app-key>",
								c,
								n,
							)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					actual, err := repository.LoadOffset(tc.Context, "<source-app-key>")
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(actual).To(gomega.BeEquivalentTo(c))
				})
			})

			ginkgo.When("used in concurrent transactions", func() {
				ginkgo.It("saves offsets concurrently", func() {
					var g sync.WaitGroup

					fn := func(ak string, c, n offsetstore.Offset) {
						defer ginkgo.GinkgoRecover()
						defer g.Done()

						err := persistence.WithTransaction(
							tc.Context,
							dataStore,
							func(tx persistence.ManagedTransaction) error {
								return tx.SaveOffset(tc.Context, ak, c, n)
							},
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					}

					g.Add(3)
					go fn("<source-app1-key>", 0, 1)
					go fn("<source-app2-key>", 0, 2)
					go fn("<source-app3-key>", 0, 3)
					g.Wait()
				})
			})
		})
	})
}

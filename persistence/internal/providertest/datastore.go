package providertest

import (
	"context"
	"time"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
)

func declareDataStoreTests(
	ctx *context.Context,
	in *In,
	out *Out,
) {
	ginkgo.Describe("type DataStore (interface)", func() {
		var (
			provider      persistence.Provider
			closeProvider func()
			dataStore     persistence.DataStore
		)

		ginkgo.BeforeEach(func() {
			provider, closeProvider = out.NewProvider()

			var err error
			dataStore, err = provider.Open(*ctx, "<app-key>")
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.AfterEach(func() {
			if dataStore != nil {
				dataStore.Close()
			}

			if closeProvider != nil {
				closeProvider()
			}
		})

		ginkgo.Describe("func EventStoreRepository()", func() {
			ginkgo.It("returns a non-nil repository", func() {
				r := dataStore.EventStoreRepository()
				gomega.Expect(r).NotTo(gomega.BeNil())
			})
		})

		ginkgo.Describe("func QueueRepository()", func() {
			ginkgo.It("returns a non-nil repository", func() {
				r := dataStore.QueueRepository()
				gomega.Expect(r).NotTo(gomega.BeNil())
			})
		})

		ginkgo.Describe("func Begin()", func() {
			ginkgo.It("returns a non-nil transaction", func() {
				tx, err := dataStore.Begin(*ctx)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(tx).NotTo(gomega.BeNil())

				tx.Rollback()
			})
		})

		ginkgo.Describe("func Close()", func() {
			ginkgo.It("returns an error if the data-store is already closed", func() {
				err := dataStore.Close()
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				err = dataStore.Close()
				gomega.Expect(err).To(gomega.Equal(persistence.ErrDataStoreClosed))
			})

			ginkgo.It("prevents transactions from being started", func() {
				err := dataStore.Close()
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				tx, err := dataStore.Begin(*ctx)
				if tx != nil {
					tx.Rollback()
				}
				gomega.Expect(err).To(gomega.Equal(persistence.ErrDataStoreClosed))
			})

			table.DescribeTable(
				"it blocks until transactions end or otherwise causes them to fail",
				func(commit, write bool) {
					ginkgo.By("starting a transaction")

					// The transaction is started outside the goroutine to
					// ensure it happens before the data-store is closed.
					tx, err := dataStore.Begin(*ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer tx.Rollback()

					result := make(chan error, 1)
					go func() {
						// Delay long enough to tell the difference between a
						// blocking call to Close() and a non-blocking one.
						time.Sleep(50 * time.Millisecond)

						if write {
							ginkgo.By("performing a write operation")

							env := infixfixtures.NewEnvelopeProto("<id>", dogmafixtures.MessageA1)

							if _, err := tx.SaveEvent(*ctx, env); err != nil {
								result <- err
								return
							}
						}

						if commit {
							ginkgo.By("committing the transaction")
							result <- tx.Commit(*ctx)
						} else {
							ginkgo.By("rolling the transaction back")
							result <- tx.Rollback()
						}
					}()

					ginkgo.By("closing the data-store")

					err = dataStore.Close()
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					select {
					case err := <-result:
						// It appears that Close() blocks until the transactions
						// are committed or rolled-back, otherwise we'd not
						// expect to see the transaction result already.
						//
						// If the implementation blocks, we assume the intent is
						// to allow thetransactions to finish successfully.

						ginkgo.By("assuming Close() blocks")

						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					default:
						// If there is no result yet, it's appers that Close()
						// does not block, and hence we would expect an error to
						// occur when the transactions is used.

						ginkgo.By("assuming Close() does not block")

						select {
						case err := <-result:
							// We don't know what error will happen, but it
							// should definitely fail.
							gomega.Expect(err).Should(gomega.HaveOccurred())
						case <-(*ctx).Done():
							err := (*ctx).Err()
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						}
					}
				},
				table.Entry("committed, without a write operation", true, false),
				table.Entry("committed, with a write operation", true, true),
				table.Entry("rolled-back, without a write operation", false, false),
				table.Entry("rolled-back, with a write operation", false, true),
			)
		})
	})
}

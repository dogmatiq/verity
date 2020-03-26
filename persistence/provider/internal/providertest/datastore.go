package providertest

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/persistence"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

func declareDataStoreTests(
	ctx *context.Context,
	in *In,
	out *Out,
) {
	ginkgo.Describe("type DataStore (interface)", func() {
		var (
			provider  persistence.Provider
			close     func()
			dataStore persistence.DataStore
		)

		ginkgo.BeforeEach(func() {
			provider, close = out.NewProvider()

			var err error
			dataStore, err = provider.Open(*ctx, "<app-key>")
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.AfterEach(func() {
			if dataStore != nil {
				dataStore.Close()
			}

			if close != nil {
				close()
			}
		})

		ginkgo.Describe("func EventStoreRepository()", func() {
			ginkgo.It("returns a non-nil repository", func() {
				r := dataStore.EventStoreRepository()
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

			ginkgo.It("blocks until transactions end or causes them to fail", func() {
				tx1, err := dataStore.Begin(*ctx)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer tx1.Rollback()

				tx2, err := dataStore.Begin(*ctx)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer tx2.Rollback()

				errors := make(chan error, 2)

				go func() {
					time.Sleep(50 * time.Millisecond)
					errors <- tx1.Commit(*ctx)
					errors <- tx2.Rollback()
				}()

				err = dataStore.Close()
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				select {
				case err := <-errors:
					// It appears that Close() blocks until the transactions are
					// committed or rolled back.
					//
					// If the implementation blocks, we assume the intent is to
					// allow thetransactions to finish successfully.
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					err = <-errors
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				default:
					// If there are no errors yet, it's appers that Close() does
					// not block, and hence we would expect errors to occur when
					// the transactions are ended.
					gomega.Expect(<-errors).To(gomega.Equal(persistence.ErrDataStoreClosed))
					gomega.Expect(<-errors).To(gomega.Equal(persistence.ErrDataStoreClosed))
				}
			})
		})
	})
}

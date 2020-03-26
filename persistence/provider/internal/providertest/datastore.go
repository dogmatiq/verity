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
		var dataStore persistence.DataStore

		ginkgo.BeforeEach(func() {
			var err error
			dataStore, err = out.Provider.Open(*ctx, "<app-key>")
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.AfterEach(func() {
			if dataStore != nil {
				dataStore.Close()
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

			ginkgo.It("blocks until all in-flight transactions are closed", func() {
				tx1, err := dataStore.Begin(*ctx)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer tx1.Rollback()

				tx2, err := dataStore.Begin(*ctx)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer tx2.Rollback()

				done := make(chan struct{})
				go func() {
					defer close(done)
					time.Sleep(50 * time.Millisecond)
					tx1.Commit(*ctx)
					tx2.Rollback()
				}()

				err = dataStore.Close()
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				select {
				case <-done:
				default:
					ginkgo.Fail("Close() returned before the in-flight transactions were closed")
				}
			})
		})
	})
}

package providertest

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

func declareTransactionTests(
	ctx *context.Context,
	in *In,
	out *Out,
) {
	ginkgo.Describe("type Transaction (interface)", func() {
		var (
			dataStore   persistence.DataStore
			transaction persistence.Transaction
		)

		ginkgo.BeforeEach(func() {
			var err error
			dataStore, err = out.Provider.Open(*ctx, "<app-key>")
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

			transaction, err = dataStore.Begin(*ctx)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.AfterEach(func() {
			if transaction != nil {
				transaction.Rollback()
			}

			if dataStore != nil {
				dataStore.Close()
			}
		})

		ginkgo.Describe("func Commit()", func() {
			ginkgo.It("returns an error if the transaction has already been committed", func() {
				err := transaction.Commit(*ctx)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				err = transaction.Commit(*ctx)
				gomega.Expect(err).Should(gomega.HaveOccurred())
			})

			ginkgo.It("returns an error if the transaction has been rolled-back", func() {
				err := transaction.Rollback()
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				err = transaction.Commit(*ctx)
				gomega.Expect(err).Should(gomega.HaveOccurred())
			})
		})

		ginkgo.Describe("func Rollback()", func() {
			ginkgo.It("returns an error if the transaction has already been rolled-back", func() {
				err := transaction.Rollback()
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				err = transaction.Rollback()
				gomega.Expect(err).Should(gomega.HaveOccurred())
			})

			ginkgo.It("returns an error if the transaction has been committed", func() {
				err := transaction.Commit(*ctx)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				err = transaction.Rollback()
				gomega.Expect(err).Should(gomega.HaveOccurred())
			})
		})
	})
}

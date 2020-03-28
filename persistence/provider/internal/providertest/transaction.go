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
			provider    persistence.Provider
			close       func()
			dataStore   persistence.DataStore
			transaction persistence.Transaction
		)

		ginkgo.BeforeEach(func() {
			provider, close = out.NewProvider()

			var err error
			dataStore, err = provider.Open(*ctx, "<app-key>")
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

			if close != nil {
				close()
			}
		})

		ginkgo.When("the transaction has been committed", func() {
			ginkgo.BeforeEach(func() {
				err := transaction.Commit(*ctx)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			})

			ginkgo.Describe("func SaveEvents()", func() {
				ginkgo.It("returns an error", func() {
					_, err := transaction.SaveEvents(*ctx, nil)
					gomega.Expect(err).To(gomega.Equal(persistence.ErrTransactionClosed))
				})
			})

			ginkgo.Describe("func Commit()", func() {
				ginkgo.It("returns an error", func() {
					err := transaction.Commit(*ctx)
					gomega.Expect(err).To(gomega.Equal(persistence.ErrTransactionClosed))
				})
			})

			ginkgo.Describe("func Rollback()", func() {
				ginkgo.It("returns an error", func() {
					err := transaction.Commit(*ctx)
					gomega.Expect(err).To(gomega.Equal(persistence.ErrTransactionClosed))
				})
			})
		})

		ginkgo.When("the transaction has been rolled-back", func() {
			ginkgo.BeforeEach(func() {
				err := transaction.Rollback()
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			})

			ginkgo.Describe("func SaveEvents()", func() {
				ginkgo.It("returns an error", func() {
					_, err := transaction.SaveEvents(*ctx, nil)
					gomega.Expect(err).To(gomega.Equal(persistence.ErrTransactionClosed))
				})
			})

			ginkgo.Describe("func Commit()", func() {
				ginkgo.It("returns an error", func() {
					err := transaction.Commit(*ctx)
					gomega.Expect(err).To(gomega.Equal(persistence.ErrTransactionClosed))
				})
			})

			ginkgo.Describe("func Rollback()", func() {
				ginkgo.It("returns an error", func() {
					err := transaction.Commit(*ctx)
					gomega.Expect(err).To(gomega.Equal(persistence.ErrTransactionClosed))
				})
			})
		})
	})
}

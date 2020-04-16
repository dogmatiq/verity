package providertest

import (
	"context"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
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
			provider      persistence.Provider
			closeProvider func()
			dataStore     persistence.DataStore
			transaction   persistence.Transaction
		)

		ginkgo.BeforeEach(func() {
			provider, closeProvider = out.NewProvider()

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

			if closeProvider != nil {
				closeProvider()
			}
		})

		ginkgo.When("the transaction has been committed", func() {
			ginkgo.BeforeEach(func() {
				err := transaction.Commit(*ctx)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			})

			ginkgo.Describe("func IncrementAggregateRevision()", func() {
				ginkgo.It("returns an error", func() {
					err := transaction.IncrementAggregateRevision(*ctx, "<handler-key>", "<instance>", 0)
					gomega.Expect(err).To(gomega.Equal(persistence.ErrTransactionClosed))
				})
			})

			ginkgo.Describe("func SaveEvent()", func() {
				ginkgo.It("returns an error", func() {
					_, err := transaction.SaveEvent(*ctx, &envelopespec.Envelope{})
					gomega.Expect(err).To(gomega.Equal(persistence.ErrTransactionClosed))
				})
			})

			ginkgo.Describe("func SaveMessageToQueue()", func() {
				ginkgo.It("returns an error", func() {
					err := transaction.SaveMessageToQueue(*ctx, &queuestore.Item{})
					gomega.Expect(err).To(gomega.Equal(persistence.ErrTransactionClosed))
				})
			})

			ginkgo.Describe("func RemoveMessageFromQueue()", func() {
				ginkgo.It("returns an error", func() {
					err := transaction.RemoveMessageFromQueue(*ctx, &queuestore.Item{})
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

			ginkgo.Describe("func IncrementAggregateRevision()", func() {
				ginkgo.It("returns an error", func() {
					err := transaction.IncrementAggregateRevision(*ctx, "<handler-key>", "<instance>", 0)
					gomega.Expect(err).To(gomega.Equal(persistence.ErrTransactionClosed))
				})
			})

			ginkgo.Describe("func SaveEvent()", func() {
				ginkgo.It("returns an error", func() {
					_, err := transaction.SaveEvent(*ctx, &envelopespec.Envelope{})
					gomega.Expect(err).To(gomega.Equal(persistence.ErrTransactionClosed))
				})
			})

			ginkgo.Describe("func SaveMessageToQueue()", func() {
				ginkgo.It("returns an error", func() {
					err := transaction.SaveMessageToQueue(*ctx, &queuestore.Item{})
					gomega.Expect(err).To(gomega.Equal(persistence.ErrTransactionClosed))
				})
			})

			ginkgo.Describe("func RemoveMessageFromQueue()", func() {
				ginkgo.It("returns an error", func() {
					err := transaction.RemoveMessageFromQueue(*ctx, &queuestore.Item{})
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

package providertest

import (
	"context"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queue"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

func declareQueueTests(
	ctx *context.Context,
	in *In,
	out *Out,
) {
	ginkgo.Context("package queue", func() {
		var (
			provider      persistence.Provider
			closeProvider func()
			dataStore     persistence.DataStore
			repository    queue.Repository

			command1 = infixfixtures.NewEnvelopeProto("<command-1>", dogmafixtures.MessageC1)
			command2 = infixfixtures.NewEnvelopeProto("<command-2>", dogmafixtures.MessageC2)
			command3 = infixfixtures.NewEnvelopeProto("<command-3>", dogmafixtures.MessageC3)
		)

		ginkgo.BeforeEach(func() {
			provider, closeProvider = out.NewProvider()

			var err error
			dataStore, err = provider.Open(*ctx, "<app-key>")
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

			repository = dataStore.QueueRepository()
		})

		ginkgo.AfterEach(func() {
			if dataStore != nil {
				dataStore.Close()
			}

			if closeProvider != nil {
				closeProvider()
			}
		})

		ginkgo.Describe("type Transaction (interface)", func() {
			ginkgo.Describe("func EnqueueMessages()", func() {
				ginkgo.It("sets the initial revision to 1", func() {
					err := enqueueMessages(
						*ctx,
						dataStore,
						command1,
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					messages, err := repository.LoadQueuedMessages(*ctx, 1)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(messages).To(gomega.HaveLen(1))
					gomega.Expect(messages[0].Revision).To(
						gomega.Equal(queue.Revision(1)),
					)
				})

				ginkgo.XIt("ignores messages that are already on the queue", func() {
				})

				ginkgo.When("the transaction is rolled-back", func() {
					ginkgo.BeforeEach(func() {
						tx, err := dataStore.Begin(*ctx)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						defer tx.Rollback()

						err = tx.EnqueueMessages(
							*ctx,
							[]*envelopespec.Envelope{
								command1,
								command2,
								command3,
							},
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						err = tx.Rollback()
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					})

					ginkgo.It("does not enqueue any messages", func() {
						messages, err := repository.LoadQueuedMessages(*ctx, 10)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(messages).To(gomega.BeEmpty())
					})
				})
			})
		})

		ginkgo.Describe("type Repository (interface)", func() {
			ginkgo.Describe("func LoadQueuedMessages()", func() {
				ginkgo.It("returns an empty result if the queue is empty", func() {
					messages, err := repository.LoadQueuedMessages(*ctx, 10)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(messages).To(gomega.BeEmpty())
				})
			})
		})
	})
}

// enqueueMessages persists the given messages to the queue.
func enqueueMessages(
	ctx context.Context,
	ds persistence.DataStore,
	envelopes ...*envelopespec.Envelope,
) error {
	tx, err := ds.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := tx.EnqueueMessages(ctx, envelopes); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

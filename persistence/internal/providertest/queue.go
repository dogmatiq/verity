package providertest

import (
	"context"
	"errors"
	"time"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queue"
	"github.com/golang/protobuf/proto"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
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

			now = time.Now()

			command1CreatedAt = now.Add(1 * time.Second)
			command2CreatedAt = now.Add(2 * time.Second)
			command3CreatedAt = now.Add(3 * time.Second)

			command1 = infixfixtures.NewEnvelopeProto("<command-1>", dogmafixtures.MessageC1, command1CreatedAt)
			command2 = infixfixtures.NewEnvelopeProto("<command-2>", dogmafixtures.MessageC2, command2CreatedAt)
			command3 = infixfixtures.NewEnvelopeProto("<command-3>", dogmafixtures.MessageC3, command3CreatedAt)

			timeout1ScheduledFor = now.Add(1 * time.Hour)
			timeout1             = infixfixtures.NewEnvelopeProto(
				"<timeout-1>",
				dogmafixtures.MessageT1,
				now,
				timeout1ScheduledFor,
			)
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

					m, err := loadQueuedMessage(*ctx, repository)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(m.Revision).To(
						gomega.Equal(queue.Revision(1)),
					)
				})

				ginkgo.It("schedules commands to be attempted at their created-at time", func() {
					err := enqueueMessages(
						*ctx,
						dataStore,
						command1,
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					m, err := loadQueuedMessage(*ctx, repository)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(m.NextAttemptAt).To(
						gomega.BeTemporally("~", command1CreatedAt),
					)
				})

				ginkgo.It("schedules timeouts to be attempted at their scheduled-for time", func() {
					err := enqueueMessages(
						*ctx,
						dataStore,
						timeout1,
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					m, err := loadQueuedMessage(*ctx, repository)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(m.NextAttemptAt).To(
						gomega.BeTemporally("~", timeout1ScheduledFor),
					)
				})

				ginkgo.It("ignores messages that are already on the queue", func() {
					err := enqueueMessages(
						*ctx,
						dataStore,
						command1,
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					// conflict has the same message ID as command1, but
					// a different message type.
					conflict := infixfixtures.NewEnvelopeProto(
						"<command-1>",
						dogmafixtures.MessageX1,
					)

					err = enqueueMessages(
						*ctx,
						dataStore,
						conflict,
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					messages, err := repository.LoadQueuedMessages(*ctx, 2)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(messages).To(gomega.HaveLen(1))

					gomega.Expect(messages[0].Envelope.PortableName).To(
						gomega.Equal(command1.PortableName),
					)
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

				table.DescribeTable(
					"it returns messages from the queue, ordered by their next attempt time",
					func(n int, expected ...*envelopespec.Envelope) {
						ginkgo.By("enqueuing some messages")

						err := enqueueMessages(
							*ctx,
							dataStore,
							command1,
							command2,
							command3,
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						ginkgo.By("loading the messages")

						messages, err := repository.LoadQueuedMessages(*ctx, n)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(messages).To(gomega.HaveLen(len(expected)))

						ginkgo.By("iterating through the result")

						for i, m := range messages {
							x := expected[i]

							if !proto.Equal(m.Envelope, x) {
								gomega.Expect(m.Envelope).To(gomega.Equal(x))
							}
						}
					},
					table.Entry(
						"it returns all the messages if the limit is larger than the actual number of messages",
						10,
						command1,
						command2,
						command3,
					),
					table.Entry(
						"it limits the result size to the given number of messages",
						2,
						command1,
						command2,
					),
				)
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

// loadQueuedMessage loads the next message from the queue.
func loadQueuedMessage(
	ctx context.Context,
	r queue.Repository,
) (*queue.Message, error) {
	messages, err := r.LoadQueuedMessages(ctx, 1)
	if err != nil {
		return nil, err
	}

	if len(messages) == 0 {
		return nil, errors.New("no messages returned")
	}

	return messages[0], nil
}

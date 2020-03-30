package providertest

import (
	"context"
	"errors"
	"math/rand"
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

			command1CreatedAt = now.Add(-1 * time.Minute)
			command2CreatedAt = now
			command3CreatedAt = now.Add(+1 * time.Minute)

			timeout1ScheduledFor = now.Add(-1 * time.Hour)
			timeout2ScheduledFor = now.Add(+1 * time.Hour)
			timeout3ScheduledFor = now.Add(+2 * time.Hour)

			// Note, we use generated UUIDs for the message IDs to avoid them
			// having any predictable effect on the queue order.
			command1 = infixfixtures.NewEnvelopeProto("", dogmafixtures.MessageC1, command1CreatedAt)
			command2 = infixfixtures.NewEnvelopeProto("", dogmafixtures.MessageC2, command2CreatedAt)
			command3 = infixfixtures.NewEnvelopeProto("", dogmafixtures.MessageC3, command3CreatedAt)

			timeout1 = infixfixtures.NewEnvelopeProto("", dogmafixtures.MessageT1, now, timeout1ScheduledFor)
			timeout2 = infixfixtures.NewEnvelopeProto("", dogmafixtures.MessageT2, now, timeout2ScheduledFor)
			timeout3 = infixfixtures.NewEnvelopeProto("", dogmafixtures.MessageT3, now, timeout3ScheduledFor)
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
			ginkgo.Describe("func AddMessagesToQueue()", func() {
				ginkgo.It("sets the initial revision to 1", func() {
					err := addMessagesToQueue(
						*ctx,
						dataStore,
						command1,
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					m, err := loadQueueMessage(*ctx, repository)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(m.Revision).To(
						gomega.Equal(queue.Revision(1)),
					)
				})

				ginkgo.It("schedules commands to be attempted at their created-at time", func() {
					err := addMessagesToQueue(
						*ctx,
						dataStore,
						command1,
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					m, err := loadQueueMessage(*ctx, repository)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(m.NextAttemptAt).To(
						gomega.BeTemporally("~", command1CreatedAt),
					)
				})

				ginkgo.It("schedules timeouts to be attempted at their scheduled-for time", func() {
					err := addMessagesToQueue(
						*ctx,
						dataStore,
						timeout1,
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					m, err := loadQueueMessage(*ctx, repository)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(m.NextAttemptAt).To(
						gomega.BeTemporally("~", timeout1ScheduledFor),
					)
				})

				ginkgo.It("ignores messages that are already on the queue", func() {
					err := addMessagesToQueue(
						*ctx,
						dataStore,
						command1,
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					// conflict has the same message ID as command1, but
					// a different message type.
					conflict := infixfixtures.NewEnvelopeProto(
						command1.MetaData.MessageId,
						dogmafixtures.MessageX1,
					)

					err = addMessagesToQueue(
						*ctx,
						dataStore,
						conflict,
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					messages, err := repository.LoadQueueMessages(*ctx, 2)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(messages).To(gomega.HaveLen(1))

					m := messages[0]

					gomega.Expect(m.Envelope.PortableName).To(
						gomega.Equal(command1.PortableName),
					)

					gomega.Expect(m.Revision).To(
						gomega.Equal(queue.Revision(1)),
					)

					gomega.Expect(m.NextAttemptAt).To(
						gomega.BeTemporally("~", command1CreatedAt),
					)
				})

				ginkgo.When("the transaction is rolled-back", func() {
					ginkgo.BeforeEach(func() {
						tx, err := dataStore.Begin(*ctx)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						defer tx.Rollback()

						err = tx.AddMessagesToQueue(
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
						messages, err := repository.LoadQueueMessages(*ctx, 10)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(messages).To(gomega.BeEmpty())
					})
				})
			})
		})

		ginkgo.Describe("type Repository (interface)", func() {
			ginkgo.Describe("func LoadQueueMessages()", func() {
				ginkgo.It("returns an empty result if the queue is empty", func() {
					messages, err := repository.LoadQueueMessages(*ctx, 10)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(messages).To(gomega.BeEmpty())
				})

				table.DescribeTable(
					"it returns messages from the queue, ordered by their next attempt time",
					func(n int, expected ...*envelopespec.Envelope) {
						ginkgo.By("enqueuing some messages in random order")

						envelopes := []*envelopespec.Envelope{
							command1,
							command2,
							command3,
							timeout1,
							timeout2,
							timeout3,
						}

						// Shuffle the order that the envelopes are enqueue to
						// ensure that they are sorted properly on the way out.
						rand.Shuffle(
							len(envelopes),
							func(i, j int) {
								envelopes[i], envelopes[j] = envelopes[j], envelopes[i]
							},
						)

						err := addMessagesToQueue(
							*ctx,
							dataStore,
							envelopes...,
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						ginkgo.By("loading the messages")

						messages, err := repository.LoadQueueMessages(*ctx, n)
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
						"it returns all the messages if the limit is equal the length of the queue",
						6,
						timeout1,
						command1,
						command2,
						command3,
						timeout2,
						timeout3,
					),
					table.Entry(
						"it returns all the messages if the limit is larger than the length of the queue",
						10,
						timeout1,
						command1,
						command2,
						command3,
						timeout2,
						timeout3,
					),
					table.Entry(
						"it returns the messages with the earliest next-attempt times if the limit is less than the length of the queue",
						3,
						timeout1,
						command1,
						command2,
					),
				)
			})
		})
	})
}

// addMessagesToQueue persists the given messages to the queue.
func addMessagesToQueue(
	ctx context.Context,
	ds persistence.DataStore,
	envelopes ...*envelopespec.Envelope,
) error {
	tx, err := ds.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := tx.AddMessagesToQueue(ctx, envelopes); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// loadQueueMessage loads the next message from the queue.
func loadQueueMessage(
	ctx context.Context,
	r queue.Repository,
) (*queue.Message, error) {
	messages, err := r.LoadQueueMessages(ctx, 1)
	if err != nil {
		return nil, err
	}

	if len(messages) == 0 {
		return nil, errors.New("no messages returned")
	}

	return messages[0], nil
}

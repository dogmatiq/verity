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

			// Note, we use generated UUIDs for the message IDs to avoid them
			// having any predictable effect on the queue order.
			env0 = infixfixtures.NewEnvelopeProto("", dogmafixtures.MessageA3)
			env1 = infixfixtures.NewEnvelopeProto("", dogmafixtures.MessageA1)
			env2 = infixfixtures.NewEnvelopeProto("", dogmafixtures.MessageA2)
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
			ginkgo.Describe("func AddMessageToQueue()", func() {
				ginkgo.It("sets the initial revision to 1", func() {
					err := addMessageToQueue(
						*ctx,
						dataStore,
						env0,
						now,
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					m, err := loadQueueMessage(*ctx, repository)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(m.Revision).To(
						gomega.Equal(queue.Revision(1)),
					)
				})

				ginkgo.It("stores the correct next-attempt time", func() {
					t := now.Add(1 * time.Hour)

					err := addMessageToQueue(
						*ctx,
						dataStore,
						env0,
						t,
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					m, err := loadQueueMessage(*ctx, repository)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(m.NextAttemptAt).To(
						gomega.BeTemporally("~", t),
					)
				})

				ginkgo.It("ignores messages that are already on the queue", func() {
					t := now.Add(1 * time.Hour)

					err := addMessageToQueue(
						*ctx,
						dataStore,
						env0,
						t,
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					// conflict has the same message ID as env0, but a different
					// message type.
					conflict := infixfixtures.NewEnvelopeProto(
						env0.MetaData.MessageId,
						dogmafixtures.MessageX1,
					)

					err = addMessageToQueue(
						*ctx,
						dataStore,
						conflict,
						time.Now(), // note: not t
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					messages, err := repository.LoadQueueMessages(*ctx, 2)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(messages).To(gomega.HaveLen(1))

					m := messages[0]

					gomega.Expect(m.Envelope.PortableName).To(
						gomega.Equal(env0.PortableName),
					)

					gomega.Expect(m.Revision).To(
						gomega.Equal(queue.Revision(1)),
					)

					gomega.Expect(m.NextAttemptAt).To(
						gomega.BeTemporally("~", t),
					)
				})

				ginkgo.When("the transaction is rolled-back", func() {
					ginkgo.BeforeEach(func() {
						tx, err := dataStore.Begin(*ctx)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						defer tx.Rollback()

						err = tx.AddMessageToQueue(
							*ctx,
							env0,
							time.Now(),
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
						ginkgo.By("enqueuing some messages out of order")

						// The expected order is env1, env2, env0.
						err := addMessageToQueue(
							*ctx,
							dataStore,
							env0,
							time.Now().Add(3*time.Hour),
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						err = addMessageToQueue(
							*ctx,
							dataStore,
							env1,
							time.Now().Add(-10*time.Hour),
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						err = addMessageToQueue(
							*ctx,
							dataStore,
							env2,
							time.Now().Add(2*time.Hour),
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
						3,
						env1,
						env2,
						env0,
					),
					table.Entry(
						"it returns all the messages if the limit is larger than the length of the queue",
						10,
						env1,
						env2,
						env0,
					),
					table.Entry(
						"it returns the messages with the earliest next-attempt times if the limit is less than the length of the queue",
						2,
						env1,
						env2,
					),
				)
			})
		})
	})
}

// addMessageToQueue persists the given message to the queue.
func addMessageToQueue(
	ctx context.Context,
	ds persistence.DataStore,
	env *envelopespec.Envelope,
	t time.Time,
) error {
	tx, err := ds.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := tx.AddMessageToQueue(ctx, env, t); err != nil {
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

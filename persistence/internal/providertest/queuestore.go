package providertest

import (
	"context"
	"fmt"
	"time"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
	"golang.org/x/sync/errgroup"
)

func declareQueueTests(
	ctx *context.Context,
	in *In,
	out *Out,
) {
	ginkgo.Context("package queuestore", func() {
		var (
			provider      persistence.Provider
			closeProvider func()
			dataStore     persistence.DataStore
			repository    queuestore.Repository

			env0, env1, env2             *envelopespec.Envelope
			message0, message1, message2 *queuestore.Message
		)

		ginkgo.BeforeEach(func() {
			// Note, we use generated UUIDs for the message IDs to avoid them
			// having any predictable effect on the queue order. Likewise, we
			// don't use the fixture messages in order.
			env0 = infixfixtures.NewEnvelopeProto("", dogmafixtures.MessageA3)
			env1 = infixfixtures.NewEnvelopeProto("", dogmafixtures.MessageA1)
			env2 = infixfixtures.NewEnvelopeProto("", dogmafixtures.MessageA2)

			message0 = &queuestore.Message{
				NextAttemptAt: time.Now(),
				Envelope:      env0,
			}

			message1 = &queuestore.Message{
				NextAttemptAt: time.Now(),
				Envelope:      env1,
			}

			message2 = &queuestore.Message{
				NextAttemptAt: time.Now(),
				Envelope:      env2,
			}

			provider, closeProvider = out.NewProvider()

			var err error
			dataStore, err = provider.Open(*ctx, "<app-key>")
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

			repository = dataStore.QueueStoreRepository()
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
			ginkgo.Describe("func SaveMessageToQueue()", func() {
				ginkgo.When("the message is already on the queue", func() {
					ginkgo.BeforeEach(func() {
						err := saveMessagesToQueue(*ctx, dataStore, message0)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						message0.Revision++
					})

					ginkgo.It("increments the revision even if no meta-data has changed", func() {
						err := saveMessagesToQueue(*ctx, dataStore, message0)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						m, err := loadQueueMessage(*ctx, repository)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(m.Revision).To(
							gomega.Equal(queuestore.Revision(2)),
						)
					})

					ginkgo.It("increments the revision when the meta-data has changed", func() {
						message0.NextAttemptAt = time.Now().Add(1 * time.Hour)

						err := saveMessagesToQueue(*ctx, dataStore, message0)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						m, err := loadQueueMessage(*ctx, repository)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(m.Revision).To(
							gomega.Equal(queuestore.Revision(2)),
						)
					})

					ginkgo.It("updates the message meta-data", func() {
						message0.NextAttemptAt = time.Now().Add(1 * time.Hour)

						err := saveMessagesToQueue(*ctx, dataStore, message0)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						m, err := loadQueueMessage(*ctx, repository)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						expectQueueMessageToEqual(m, message0)
					})

					ginkgo.XIt("sorts by the updated next-attempt time", func() {

					})

					ginkgo.It("does not update the envelope", func() {
						// Use the env0 message ID on env1 so that they collide.
						env1.MetaData.MessageId = env0.MetaData.MessageId
						message1.Revision++

						err := saveMessagesToQueue(*ctx, dataStore, message1)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						m, err := loadQueueMessage(*ctx, repository)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						// Then we ensure we still have the envelope data from
						// env0, not env1.
						expectProtoToEqual(
							m.Envelope,
							env0,
							"message envelope was updated, not just the meta-data",
						)
					})

					table.DescribeTable(
						"it does not update the message when an OCC conflict occurs",
						func(conflictingRevision int) {
							// Update the message once more so that it's up to
							// revision 2. This lets us test for both 0 (the
							// special case) and 1 as incorrect revisions.
							err := saveMessagesToQueue(*ctx, dataStore, message0)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

							// Now try the save with a revision that we expect
							// to fail. Note that we change the meta-data so we
							// can detect whether any change was persisted.
							err = saveMessagesToQueue(
								*ctx,
								dataStore,
								&queuestore.Message{
									Revision:      queuestore.Revision(conflictingRevision),
									NextAttemptAt: time.Now().Add(5 * time.Minute),
									Envelope:      env0, // same env as message already on queue
								},
							)
							gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))

							m, err := loadQueueMessage(*ctx, repository)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
							gomega.Expect(m.Revision).To(
								gomega.Equal(queuestore.Revision(2)),
							)
							expectQueueMessageToEqual(m, message0)
						},
						table.Entry("zero", 0),
						table.Entry("too low", 1),
						table.Entry("too high", 100),
					)

					ginkgo.When("the message has already been modified in the same transaction", func() {
						var tx persistence.Transaction

						ginkgo.BeforeEach(func() {
							var err error
							tx, err = dataStore.Begin(*ctx)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

							message0.NextAttemptAt = time.Now().Add(1 * time.Hour)
							err = tx.SaveMessageToQueue(*ctx, message0)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
							message0.Revision++
						})

						ginkgo.AfterEach(func() {
							if tx != nil {
								tx.Rollback()
							}
						})

						ginkgo.It("saves the data from the most recent call", func() {
							message0.NextAttemptAt = time.Now().Add(2 * time.Hour)
							err := tx.SaveMessageToQueue(*ctx, message0)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

							err = tx.Commit(*ctx)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

							m, err := loadQueueMessage(*ctx, repository)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

							gomega.Expect(m.Revision).To(
								gomega.Equal(queuestore.Revision(3)),
							)
							expectQueueMessageToEqual(m, message0)
						})

						ginkgo.It("returns an error when an OCC conflict occurs (based on the uncommitted revision)", func() {
							message0.Revision = 1 // as before tx was begun
							err := tx.SaveMessageToQueue(*ctx, message0)
							gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))
						})
					})

					ginkgo.When("the transaction is rolled-back", func() {
						ginkgo.BeforeEach(func() {
							tx, err := dataStore.Begin(*ctx)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
							defer tx.Rollback()

							// Note that we change the meta-data so we
							// can detect whether any change was persisted.
							err = tx.SaveMessageToQueue(
								*ctx,
								&queuestore.Message{
									Revision:      1,
									NextAttemptAt: time.Now().Add(5 * time.Minute),
									Envelope:      env0, // same env as message already on queue
								},
							)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

							err = tx.Rollback()
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						})

						ginkgo.It("does not persist the changes", func() {
							m, err := loadQueueMessage(*ctx, repository)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

							gomega.Expect(m.Revision).To(
								gomega.Equal(queuestore.Revision(1)),
							)
							expectQueueMessageToEqual(m, message0)
						})
					})
				})

				ginkgo.When("the message is not yet on the queue", func() {
					ginkgo.It("sets the initial revision to 1", func() {
						err := saveMessagesToQueue(*ctx, dataStore, message0)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						m, err := loadQueueMessage(*ctx, repository)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(m.Revision).To(
							gomega.Equal(queuestore.Revision(1)),
						)
					})

					ginkgo.It("stores the message meta-data", func() {
						err := saveMessagesToQueue(*ctx, dataStore, message0)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						m, err := loadQueueMessage(*ctx, repository)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						expectQueueMessageToEqual(m, message0)
					})

					ginkgo.It("does not persist the message when an OCC conflict occurs", func() {
						message0.Revision = 123

						err := saveMessagesToQueue(*ctx, dataStore, message0)
						gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))

						messages, err := repository.LoadQueueMessages(*ctx, 1)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(messages).To(gomega.BeEmpty())
					})

					ginkgo.When("the message has already been modified in the same transaction", func() {
						var tx persistence.Transaction

						ginkgo.BeforeEach(func() {
							var err error
							tx, err = dataStore.Begin(*ctx)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

							err = tx.SaveMessageToQueue(*ctx, message0)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
							message0.Revision++
						})

						ginkgo.AfterEach(func() {
							if tx != nil {
								tx.Rollback()
							}
						})

						ginkgo.It("saves the data from the most recent call", func() {
							message0.NextAttemptAt = time.Now().Add(1 * time.Hour)
							err := tx.SaveMessageToQueue(*ctx, message0)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

							err = tx.Commit(*ctx)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

							m, err := loadQueueMessage(*ctx, repository)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

							gomega.Expect(m.Revision).To(
								gomega.Equal(queuestore.Revision(2)),
							)
							expectQueueMessageToEqual(m, message0)
						})

						ginkgo.It("returns an error when an OCC conflict occurs (based on the uncommitted revision)", func() {
							message0.Revision = 0 // as before tx was begun
							err := tx.SaveMessageToQueue(*ctx, message0)
							gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))
						})
					})

					ginkgo.When("the transaction is rolled-back", func() {
						ginkgo.BeforeEach(func() {
							tx, err := dataStore.Begin(*ctx)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
							defer tx.Rollback()

							err = tx.SaveMessageToQueue(*ctx, message0)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

							err = tx.Rollback()
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						})

						ginkgo.It("does not persist any messages", func() {
							messages, err := repository.LoadQueueMessages(*ctx, 10)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
							gomega.Expect(messages).To(gomega.BeEmpty())
						})
					})
				})

				ginkgo.It("does not update the revision field of the passed value", func() {
					err := saveMessagesToQueue(*ctx, dataStore, message0)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(message0.Revision).To(
						gomega.Equal(queuestore.Revision(0)),
					)
				})

				ginkgo.It("saves messages that were not created by a handler", func() {
					message0.Envelope.MetaData.Source.Handler = nil
					message0.Envelope.MetaData.Source.InstanceId = ""

					err := saveMessagesToQueue(*ctx, dataStore, message0)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					messages, err := repository.LoadQueueMessages(*ctx, 2)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(messages).To(gomega.HaveLen(1))
				})

				ginkgo.It("serializes operations from competing transactions", func() {
					ginkgo.By("running several transactions in parallel")

					g, gctx := errgroup.WithContext(*ctx)

					g.Go(func() error {
						return saveMessagesToQueue(gctx, dataStore, message0)
					})

					g.Go(func() error {
						message1.NextAttemptAt = time.Now().Add(-1 * time.Hour)
						message2.NextAttemptAt = time.Now().Add(+1 * time.Hour)
						return saveMessagesToQueue(gctx, dataStore, message1, message2)
					})

					err := g.Wait()
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					ginkgo.By("loading the messages")

					// Expected is the messages we expect to have been queued,
					// in the order we expect them.
					expected := []*queuestore.Message{message1, message0, message2}

					// Now we query the queue and verify that each specific
					// message ended up in the order specified by the next
					// attempt times.
					messages, err := repository.LoadQueueMessages(*ctx, len(expected)+1)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(messages).To(gomega.HaveLen(len(expected)))

					for i, m := range messages {
						expectQueueMessageToEqual(
							m,
							expected[i],
							fmt.Sprintf("message at index #%d does not match the expected value", i),
						)
					}
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
					func(n int, expected ...**queuestore.Message) {
						ginkgo.By("enqueuing some messages out of order")

						// The expected order is env1, env2, env0.
						message0.NextAttemptAt = time.Now().Add(3 * time.Hour)
						message1.NextAttemptAt = time.Now().Add(-10 * time.Hour)
						message2.NextAttemptAt = time.Now().Add(2 * time.Hour)

						err := saveMessagesToQueue(
							*ctx,
							dataStore,
							message0,
							message1,
							message2,
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						ginkgo.By("loading the messages")

						messages, err := repository.LoadQueueMessages(*ctx, n)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(messages).To(gomega.HaveLen(len(expected)))

						ginkgo.By("iterating through the result")

						for i, m := range messages {
							expectQueueMessageToEqual(
								m,
								*expected[i],
								fmt.Sprintf("message at index #%d in does not match the expected value", i),
							)
						}
					},
					table.Entry(
						"it returns all the messages if the limit is equal the length of the queue",
						3,
						&message1, &message2, &message0,
					),
					table.Entry(
						"it returns all the messages if the limit is larger than the length of the queue",
						10,
						&message1, &message2, &message0,
					),
					table.Entry(
						"it returns the messages with the earliest next-attempt times if the limit is less than the length of the queue",
						2,
						&message1, &message2,
					),
				)
			})

			ginkgo.It("returns an error if the context is canceled", func() {
				ctx, cancel := context.WithCancel(*ctx)
				cancel()

				_, err := repository.LoadQueueMessages(ctx, 1)
				gomega.Expect(err).To(gomega.Equal(context.Canceled))
			})
		})
	})
}

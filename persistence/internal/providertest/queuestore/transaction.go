package queuestore

import (
	"sync"
	"time"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
)

// DeclareTransactionTests declares a functional test-suite for a specific
// queuestore.Transaction implementation.
func DeclareTransactionTests(tc *common.TestContext) {
	ginkgo.Describe("type queuestore.Transaction", func() {
		var (
			dataStore  persistence.DataStore
			repository queuestore.Repository
			tearDown   func()

			env0, env1, env2             *envelopespec.Envelope
			message0, message1, message2 *queuestore.Message
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()
			repository = dataStore.QueueStoreRepository()

			env0 = infixfixtures.NewEnvelopeProto("<message-0>", dogmafixtures.MessageA1)
			env1 = infixfixtures.NewEnvelopeProto("<message-1>", dogmafixtures.MessageA2)
			env2 = infixfixtures.NewEnvelopeProto("<message-2>", dogmafixtures.MessageA3)

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
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("func SaveMessageToQueue()", func() {
			ginkgo.When("the message is already on the queue", func() {
				ginkgo.BeforeEach(func() {
					saveMessages(tc.Context, dataStore, message0)
				})

				ginkgo.It("updates the message", func() {
					message0.NextAttemptAt = time.Now().Add(1 * time.Hour)
					saveMessages(tc.Context, dataStore, message0)

					m := loadMessage(tc.Context, repository)
					expectMessageToEqual(m, message0)
				})

				ginkgo.It("does not update the message if the transaction is rolled-back", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							clone := *message0
							clone.NextAttemptAt = clone.NextAttemptAt.Add(1 * time.Hour)
							return tx.SaveMessageToQueue(tc.Context, &clone)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					m := loadMessage(tc.Context, repository)
					expectMessageToEqual(m, message0)
				})

				ginkgo.It("increments the revision even if no meta-data has changed", func() {
					saveMessages(tc.Context, dataStore, message0)

					m := loadMessage(tc.Context, repository)
					gomega.Expect(m.Revision).To(gomega.BeEquivalentTo(2))
				})

				ginkgo.It("maintains the correct queue order", func() {
					// Place message1 before existing message0.
					message1.NextAttemptAt = time.Now().Add(-1 * time.Hour)
					saveMessages(tc.Context, dataStore, message1)

					// Then move message0 to the front of the queue.
					message0.NextAttemptAt = time.Now().Add(-10 * time.Hour)
					saveMessages(tc.Context, dataStore, message0)

					m := loadMessage(tc.Context, repository)
					expectMessageToEqual(m, message0)
				})

				ginkgo.It("does not update the envelope", func() {
					message0.Envelope = env1
					saveMessages(tc.Context, dataStore, message1)

					m := loadMessage(tc.Context, repository)
					common.ExpectProtoToEqual(
						m.Envelope,
						env0,
						"message envelope was updated, not just the meta-data",
					)
				})

				table.DescribeTable(
					"it does not update the message when an OCC conflict occurs",
					func(conflictingRevision int) {
						// Update the message once more so that it's up to
						// revision 2. Otherwise we can't test for 1 as a
						// too-low value.
						saveMessages(tc.Context, dataStore, message0)

						err := persistence.WithTransaction(
							tc.Context,
							dataStore,
							func(tx persistence.ManagedTransaction) error {
								clone := *message0
								clone.Revision = queuestore.Revision(conflictingRevision)
								clone.NextAttemptAt = clone.NextAttemptAt.Add(1 * time.Hour)

								err := tx.SaveMessageToQueue(tc.Context, &clone)
								gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))

								return nil // let the transaction commit despite error
							},
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						m := loadMessage(tc.Context, repository)
						expectMessageToEqual(m, message0)
					},
					table.Entry("zero", 0),
					table.Entry("too low", 1),
					table.Entry("too high", 100),
				)
			})

			ginkgo.When("the message is not yet on the queue", func() {
				ginkgo.It("saves the message with an initial revision of 1", func() {
					saveMessages(tc.Context, dataStore, message0)

					m := loadMessage(tc.Context, repository)
					expectMessageToEqual(m, message0)
				})

				ginkgo.It("saves messages that were not created by a handler", func() {
					message0.Envelope.MetaData.Source.Handler = nil
					message0.Envelope.MetaData.Source.InstanceId = ""

					saveMessages(tc.Context, dataStore, message0)

					m := loadMessage(tc.Context, repository)

					// We can't use the regular protobuf comparison methods,
					// because even proto.Equal() treats a nil proto.Message
					// differently to a zero-value.
					src := m.Envelope.GetMetaData().GetSource()
					gomega.Expect(src.GetHandler().GetName()).To(gomega.Equal(""))
					gomega.Expect(src.GetHandler().GetKey()).To(gomega.Equal(""))
					gomega.Expect(src.GetInstanceId()).To(gomega.Equal(""))
				})

				ginkgo.It("does not save the message if the transaction is rolled back", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							return tx.SaveMessageToQueue(tc.Context, message0)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					messages := loadMessages(tc.Context, repository, 1)
					gomega.Expect(messages).To(gomega.BeEmpty())
				})

				ginkgo.It("does not save the message when an OCC conflict occurs", func() {
					message0.Revision = 123

					err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							err := tx.SaveMessageToQueue(tc.Context, message0)
							gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))
							return nil // let the transaction commit despite error
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					messages := loadMessages(tc.Context, repository, 1)
					gomega.Expect(messages).To(gomega.BeEmpty())
				})

			})

			ginkgo.XWhen("the a message is saved more than once in the same transaction", func() {
			})

			ginkgo.It("does not update the revision field of the argument", func() {
				err := persistence.WithTransaction(
					tc.Context,
					dataStore,
					func(tx persistence.ManagedTransaction) error {
						before := message0.Revision
						err := tx.SaveMessageToQueue(tc.Context, message0)
						gomega.Expect(message0.Revision).To(gomega.Equal(before))

						return err
					},
				)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			})
		})

		ginkgo.Describe("func RemoveMessageFromQueue()", func() {
			ginkgo.When("the message is on the queue", func() {
				ginkgo.BeforeEach(func() {
					saveMessages(tc.Context, dataStore, message0)
				})

				ginkgo.It("removes the message from the queue", func() {
					removeMessages(tc.Context, dataStore, message0)

					messages := loadMessages(tc.Context, repository, 1)
					gomega.Expect(messages).To(gomega.BeEmpty())
				})

				ginkgo.It("does not remove the message if the transaction is rolled-back", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							return tx.RemoveMessageFromQueue(tc.Context, message0)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					m := loadMessage(tc.Context, repository)
					expectMessageToEqual(m, message0)
				})

				ginkgo.It("maintains the correct queue order", func() {
					// add message1 after message0, then remove message0
					message1.NextAttemptAt = time.Now().Add(1 * time.Hour)
					saveMessages(tc.Context, dataStore, message1)
					removeMessages(tc.Context, dataStore, message0)

					m := loadMessage(tc.Context, repository)
					expectMessageToEqual(m, message1)
				})

				table.DescribeTable(
					"it does not remove the message when an OCC conflict occurs",
					func(conflictingRevision int) {
						// Update the message once more so that it's up to
						// revision 2. Otherwise we can't test for 1 as a
						// too-low value.
						saveMessages(tc.Context, dataStore, message0)

						err := persistence.WithTransaction(
							tc.Context,
							dataStore,
							func(tx persistence.ManagedTransaction) error {
								clone := *message0
								clone.Revision = queuestore.Revision(conflictingRevision)

								err := tx.RemoveMessageFromQueue(tc.Context, &clone)
								gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))

								return nil // let the transaction commit despite error
							},
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						m := loadMessage(tc.Context, repository)
						expectMessageToEqual(m, message0)
					},
					table.Entry("zero", 0),
					table.Entry("too low", 1),
					table.Entry("too high", 100),
				)
			})

			ginkgo.When("the message is not on the queue", func() {
				table.DescribeTable(
					"returns an OCC conflict error",
					func(conflictingRevision int) {
						err := persistence.WithTransaction(
							tc.Context,
							dataStore,
							func(tx persistence.ManagedTransaction) error {
								clone := *message0
								clone.Revision = queuestore.Revision(conflictingRevision)

								err := tx.RemoveMessageFromQueue(tc.Context, &clone)
								gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))

								return nil // let the transaction commit despite error
							},
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						messages := loadMessages(tc.Context, repository, 1)
						gomega.Expect(messages).To(
							gomega.BeEmpty(),
							"removal of non-existent message caused it to exist",
						)
					},
					table.Entry("zero", 0),
					table.Entry("non-zero", 100),
				)
			})

			ginkgo.XWhen("the a message is saved then deleted in the same transaction", func() {
			})

			ginkgo.XWhen("the a message is deleted then saved in the same transaction", func() {
			})
		})

		ginkgo.It("serializes operations from competing transactions", func() {
			ginkgo.By("running several transactions in parallel")

			saveMessages(tc.Context, dataStore, message0, message1)

			var g sync.WaitGroup
			g.Add(3)

			// save
			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()
				saveMessages(tc.Context, dataStore, message2)
			}()

			// update
			message1.NextAttemptAt = time.Now().Add(+1 * time.Hour)
			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()
				saveMessages(tc.Context, dataStore, message1)
			}()

			// remove
			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()
				removeMessages(tc.Context, dataStore, message0)
			}()

			g.Wait()

			ginkgo.By("loading the messages")

			expected := []*queuestore.Message{message2, message1}
			messages := loadMessages(tc.Context, repository, len(expected)+1)
			expectMessagesToEqual(messages, expected)
		})
	})
}

// 	ginkgo.When("the message has already been modified in the same transaction", func() {
// 		var tx persistence.Transaction

// 		ginkgo.BeforeEach(func() {
// 			var err error
// 			tx, err = dataStore.Begin(tc.Context)
// 			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

// 			err = tx.SaveMessageToQueue(tc.Context, message0)
// 			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
// 			message0.Revision++
// 		})

// 		ginkgo.AfterEach(func() {
// 			if tx != nil {
// 				tx.Rollback()
// 			}
// 		})

// 		ginkgo.It("saves the data from the most recent call", func() {
// 			message0.NextAttemptAt = time.Now().Add(1 * time.Hour)
// 			err := tx.SaveMessageToQueue(tc.Context, message0)
// 			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

// 			err = tx.Commit(tc.Context)
// 			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

// 			m, err := loadMessage(tc.Context, repository)
// 			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

// 			gomega.Expect(m.Revision).To(
// 				gomega.Equal(queuestore.Revision(2)),
// 			)
// 			expectMessageToEqual(m, message0)
// 		})

// 		ginkgo.It("returns an error when an OCC conflict occurs (based on the uncommitted revision)", func() {
// 			message0.Revision = 0 // as before tx was begun
// 			err := tx.SaveMessageToQueue(tc.Context, message0)
// 			gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))
// 		})
// 	})

// ginkgo.When("the message has already been modified in the same transaction", func() {
// 	var tx persistence.Transaction

// 	ginkgo.BeforeEach(func() {
// 		var err error
// 		tx, err = dataStore.Begin(tc.Context)
// 		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

// 		message0.NextAttemptAt = time.Now().Add(1 * time.Hour)
// 		err = tx.SaveMessageToQueue(tc.Context, message0)
// 		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
// 		message0.Revision++
// 	})

// 	ginkgo.AfterEach(func() {
// 		if tx != nil {
// 			tx.Rollback()
// 		}
// 	})

// 	ginkgo.It("saves the data from the most recent call", func() {
// 		message0.NextAttemptAt = time.Now().Add(2 * time.Hour)
// 		err := tx.SaveMessageToQueue(tc.Context, message0)
// 		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

// 		err = tx.Commit(tc.Context)
// 		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

// 		m, err := loadMessage(tc.Context, repository)
// 		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

// 		gomega.Expect(m.Revision).To(
// 			gomega.Equal(queuestore.Revision(3)),
// 		)
// 		expectMessageToEqual(m, message0)
// 	})

// 	ginkgo.It("returns an error when an OCC conflict occurs (based on the uncommitted revision)", func() {
// 		message0.Revision = 1 // as before tx was begun
// 		err := tx.SaveMessageToQueue(tc.Context, message0)
// 		gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))
// 	})
// })

// ginkgo.It("serializes operations from competing transactions", func() {
// 	ginkgo.By("running several transactions in parallel")

// 	g, gctx := errgroup.WithContext(tc.Context)

// 	g.Go(func() error {
// 		return saveMessages(gctx, dataStore, message0)
// 	})

// 	g.Go(func() error {
// 		message1.NextAttemptAt = time.Now().Add(-1 * time.Hour)
// 		message2.NextAttemptAt = time.Now().Add(+1 * time.Hour)
// 		return saveMessages(gctx, dataStore, message1, message2)
// 	})

// 	err := g.Wait()
// 	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

// 	ginkgo.By("loading the messages")

// 	// Expected is the messages we expect to have been queued,
// 	// in the order we expect them.
// 	expected := []*queuestore.Message{message1, message0, message2}

// 	// Now we query the queue and verify that each specific
// 	// message ended up in the order specified by the next
// 	// attempt times.
// 	messages, err := repository.LoadQueueMessages(tc.Context, len(expected)+1)
// 	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
// 	gomega.Expect(messages).To(gomega.HaveLen(len(expected)))

// 	for i, m := range messages {
// 		expectMessageToEqual(
// 			m,
// 			expected[i],
// 			fmt.Sprintf("message at index #%d does not match the expected value", i),
// 		)
// 	}
// })

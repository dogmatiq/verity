package queuestore

import (
	"sync"
	"time"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
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

			item0, item1, item2 *queuestore.Item
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()
			repository = dataStore.QueueStoreRepository()

			item0 = &queuestore.Item{
				NextAttemptAt: time.Now(),
				Envelope:      infixfixtures.NewEnvelope("<message-0>", dogmafixtures.MessageA1),
			}

			item1 = &queuestore.Item{
				NextAttemptAt: time.Now(),
				Envelope:      infixfixtures.NewEnvelope("<message-1>", dogmafixtures.MessageA2),
			}

			item2 = &queuestore.Item{
				NextAttemptAt: time.Now(),
				Envelope:      infixfixtures.NewEnvelope("<message-2>", dogmafixtures.MessageA3),
			}
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("func SaveMessageToQueue()", func() {
			ginkgo.When("the message is already on the queue", func() {
				ginkgo.BeforeEach(func() {
					saveMessages(tc.Context, dataStore, item0)
				})

				ginkgo.It("updates the message", func() {
					item0.NextAttemptAt = time.Now().Add(1 * time.Hour)
					item0.FailureCount = 123

					saveMessages(tc.Context, dataStore, item0)

					i := loadMessage(tc.Context, repository)
					expectItemToEqual(i, item0)
				})

				ginkgo.It("does not update the message if the transaction is rolled-back", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							clone := *item0
							clone.NextAttemptAt = clone.NextAttemptAt.Add(1 * time.Hour)
							return tx.SaveMessageToQueue(tc.Context, &clone)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					i := loadMessage(tc.Context, repository)
					expectItemToEqual(i, item0)
				})

				ginkgo.It("increments the revision even if no meta-data has changed", func() {
					saveMessages(tc.Context, dataStore, item0)

					i := loadMessage(tc.Context, repository)
					gomega.Expect(i.Revision).To(gomega.BeEquivalentTo(2))
				})

				ginkgo.It("maintains the correct queue order", func() {
					// Place item1 before existing item0.
					item1.NextAttemptAt = time.Now().Add(-1 * time.Hour)
					saveMessages(tc.Context, dataStore, item1)

					// Then move item0 to the front of the queue.
					item0.NextAttemptAt = time.Now().Add(-10 * time.Hour)
					saveMessages(tc.Context, dataStore, item0)

					i := loadMessage(tc.Context, repository)
					expectItemToEqual(i, item0)
				})

				ginkgo.It("does not update the envelope", func() {
					original := item0.Envelope
					item0.Envelope = item1.Envelope
					saveMessages(tc.Context, dataStore, item1)

					i := loadMessage(tc.Context, repository)
					common.ExpectProtoToEqual(
						i.Envelope,
						original,
						"envelope was updated, not just the meta-data",
					)
				})

				table.DescribeTable(
					"it does not update the message when an OCC conflict occurs",
					func(conflictingRevision int) {
						// Update the message once more so that it's up to
						// revision 2. Otherwise we can't test for 1 as a
						// too-low value.
						saveMessages(tc.Context, dataStore, item0)

						_, err := persistence.WithTransaction(
							tc.Context,
							dataStore,
							func(tx persistence.ManagedTransaction) error {
								clone := *item0
								clone.Revision = uint64(conflictingRevision)
								clone.NextAttemptAt = clone.NextAttemptAt.Add(1 * time.Hour)

								err := tx.SaveMessageToQueue(tc.Context, &clone)
								gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))

								return nil // let the transaction commit despite error
							},
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						i := loadMessage(tc.Context, repository)
						expectItemToEqual(i, item0)
					},
					table.Entry("zero", 0),
					table.Entry("too low", 1),
					table.Entry("too high", 100),
				)
			})

			ginkgo.When("the message is not yet on the queue", func() {
				ginkgo.It("saves the message with an initial revision of 1", func() {
					saveMessages(tc.Context, dataStore, item0)

					i := loadMessage(tc.Context, repository)
					expectItemToEqual(i, item0)
				})

				ginkgo.It("saves messages that were not created by a handler", func() {
					item0.Envelope.MetaData.Source.Handler = nil
					item0.Envelope.MetaData.Source.InstanceId = ""

					saveMessages(tc.Context, dataStore, item0)

					i := loadMessage(tc.Context, repository)

					// We can't use the regular protobuf comparison methods,
					// because even proto.Equal() treats a nil proto.Message
					// differently to a zero-value.
					//
					// TODO: https://github.com/dogmatiq/infix/issues/151
					src := i.Envelope.GetMetaData().GetSource()
					gomega.Expect(src.GetHandler().GetName()).To(gomega.Equal(""))
					gomega.Expect(src.GetHandler().GetKey()).To(gomega.Equal(""))
					gomega.Expect(src.GetInstanceId()).To(gomega.Equal(""))
				})

				ginkgo.It("does not save the message if the transaction is rolled back", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							return tx.SaveMessageToQueue(tc.Context, item0)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					items := loadMessages(tc.Context, repository, 1)
					gomega.Expect(items).To(gomega.BeEmpty())
				})

				ginkgo.It("does not save the message when an OCC conflict occurs", func() {
					item0.Revision = 123

					_, err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							err := tx.SaveMessageToQueue(tc.Context, item0)
							gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))
							return nil // let the transaction commit despite error
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					items := loadMessages(tc.Context, repository, 1)
					gomega.Expect(items).To(gomega.BeEmpty())
				})
			})

			ginkgo.When("a message is saved more than once in the same transaction", func() {
				ginkgo.It("saves the meta-data from the most recent call", func() {
					_, err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if err := tx.SaveMessageToQueue(tc.Context, item0); err != nil {
								return err
							}

							item0.Revision++
							item0.NextAttemptAt = time.Now().Add(1 * time.Hour)

							return tx.SaveMessageToQueue(tc.Context, item0)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					i := loadMessage(tc.Context, repository)

					item0.Revision++
					expectItemToEqual(i, item0)
				})

				ginkgo.It("uses the uncommitted revision for OCC checks", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if err := tx.SaveMessageToQueue(tc.Context, item0); err != nil {
								return err
							}

							// Note that we did not increment item0.Revision
							// after the first save.
							return tx.SaveMessageToQueue(tc.Context, item0)
						},
					)
					gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))
				})
			})

			ginkgo.It("does not update the revision field of the argument", func() {
				_, err := persistence.WithTransaction(
					tc.Context,
					dataStore,
					func(tx persistence.ManagedTransaction) error {
						before := item0.Revision
						err := tx.SaveMessageToQueue(tc.Context, item0)
						gomega.Expect(item0.Revision).To(gomega.Equal(before))

						return err
					},
				)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			})
		})

		ginkgo.Describe("func RemoveMessageFromQueue()", func() {
			ginkgo.When("the message is on the queue", func() {
				ginkgo.BeforeEach(func() {
					saveMessages(tc.Context, dataStore, item0)
				})

				ginkgo.It("removes the message from the queue", func() {
					removeMessages(tc.Context, dataStore, item0)

					items := loadMessages(tc.Context, repository, 1)
					gomega.Expect(items).To(gomega.BeEmpty())
				})

				ginkgo.It("does not remove the message if the transaction is rolled-back", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							return tx.RemoveMessageFromQueue(tc.Context, item0)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					i := loadMessage(tc.Context, repository)
					expectItemToEqual(i, item0)
				})

				ginkgo.It("maintains the correct queue order", func() {
					// add item1 after item0, then remove item0
					item1.NextAttemptAt = time.Now().Add(1 * time.Hour)
					saveMessages(tc.Context, dataStore, item1)
					removeMessages(tc.Context, dataStore, item0)

					i := loadMessage(tc.Context, repository)
					expectItemToEqual(i, item1)
				})

				table.DescribeTable(
					"it does not remove the message when an OCC conflict occurs",
					func(conflictingRevision int) {
						// Update the message once more so that it's up to
						// revision 2. Otherwise we can't test for 1 as a
						// too-low value.
						saveMessages(tc.Context, dataStore, item0)

						_, err := persistence.WithTransaction(
							tc.Context,
							dataStore,
							func(tx persistence.ManagedTransaction) error {
								clone := *item0
								clone.Revision = uint64(conflictingRevision)

								err := tx.RemoveMessageFromQueue(tc.Context, &clone)
								gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))

								return nil // let the transaction commit despite error
							},
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						i := loadMessage(tc.Context, repository)
						expectItemToEqual(i, item0)
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
						_, err := persistence.WithTransaction(
							tc.Context,
							dataStore,
							func(tx persistence.ManagedTransaction) error {
								clone := *item0
								clone.Revision = uint64(conflictingRevision)

								err := tx.RemoveMessageFromQueue(tc.Context, &clone)
								gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))

								return nil // let the transaction commit despite error
							},
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						items := loadMessages(tc.Context, repository, 1)
						gomega.Expect(items).To(
							gomega.BeEmpty(),
							"removal of non-existent message caused it to exist",
						)
					},
					table.Entry("zero", 0),
					table.Entry("non-zero", 100),
				)
			})

			ginkgo.When("a message is saved then deleted in the same transaction", func() {
				ginkgo.It("does not save the new message", func() {
					_, err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if err := tx.SaveMessageToQueue(tc.Context, item0); err != nil {
								return err
							}

							item0.Revision++

							return tx.RemoveMessageFromQueue(tc.Context, item0)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					items := loadMessages(tc.Context, repository, 1)
					gomega.Expect(items).To(gomega.BeEmpty())
				})

				ginkgo.It("uses the uncommitted revision for OCC checks", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if err := tx.SaveMessageToQueue(tc.Context, item0); err != nil {
								return err
							}

							// Note that we did not increment item0.Revision
							// after the save.
							return tx.RemoveMessageFromQueue(tc.Context, item0)
						},
					)
					gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))
				})
			})

			ginkgo.When("a message is deleted then saved in the same transaction", func() {
				ginkgo.BeforeEach(func() {
					saveMessages(tc.Context, dataStore, item0)
				})

				ginkgo.It("saves the new message", func() {
					_, err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if err := tx.RemoveMessageFromQueue(tc.Context, item0); err != nil {
								return err
							}

							item0.Revision = 0
							item0.NextAttemptAt = time.Now().Add(1 * time.Hour)

							return tx.SaveMessageToQueue(tc.Context, item0)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					i := loadMessage(tc.Context, repository)

					item0.Revision++
					expectItemToEqual(i, item0)
				})

				ginkgo.It("uses the uncommitted revision for OCC checks", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if err := tx.RemoveMessageFromQueue(tc.Context, item0); err != nil {
								return err
							}

							// Note that we did set item0.Revision back to
							// zero after the remove.
							return tx.SaveMessageToQueue(tc.Context, item0)
						},
					)
					gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))
				})
			})
		})

		ginkgo.It("serializes operations from competing transactions", func() {
			ginkgo.By("running several transactions in parallel")

			saveMessages(tc.Context, dataStore, item0, item1)

			var g sync.WaitGroup
			g.Add(3)

			// save
			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()
				saveMessages(tc.Context, dataStore, item2)
			}()

			// update
			item1.NextAttemptAt = time.Now().Add(+1 * time.Hour)
			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()
				saveMessages(tc.Context, dataStore, item1)
			}()

			// remove
			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()
				removeMessages(tc.Context, dataStore, item0)
			}()

			g.Wait()

			ginkgo.By("loading the messages")

			expected := []*queuestore.Item{item2, item1}
			items := loadMessages(tc.Context, repository, len(expected)+1)
			expectItemsToEqual(items, expected)
		})
	})
}

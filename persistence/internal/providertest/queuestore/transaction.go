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

			parcel0, parcel1, parcel2 *queuestore.Parcel
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()
			repository = dataStore.QueueStoreRepository()

			parcel0 = &queuestore.Parcel{
				NextAttemptAt: time.Now(),
				Envelope:      infixfixtures.NewEnvelopeProto("<message-0>", dogmafixtures.MessageA1),
			}

			parcel1 = &queuestore.Parcel{
				NextAttemptAt: time.Now(),
				Envelope:      infixfixtures.NewEnvelopeProto("<message-1>", dogmafixtures.MessageA2),
			}

			parcel2 = &queuestore.Parcel{
				NextAttemptAt: time.Now(),
				Envelope:      infixfixtures.NewEnvelopeProto("<message-2>", dogmafixtures.MessageA3),
			}
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("func SaveMessageToQueue()", func() {
			ginkgo.When("the message is already on the queue", func() {
				ginkgo.BeforeEach(func() {
					saveMessages(tc.Context, dataStore, parcel0)
				})

				ginkgo.It("updates the message", func() {
					parcel0.NextAttemptAt = time.Now().Add(1 * time.Hour)
					parcel0.FailureCount = 123

					saveMessages(tc.Context, dataStore, parcel0)

					p := loadMessage(tc.Context, repository)
					expectParcelToEqual(p, parcel0)
				})

				ginkgo.It("does not update the message if the transaction is rolled-back", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							clone := *parcel0
							clone.NextAttemptAt = clone.NextAttemptAt.Add(1 * time.Hour)
							return tx.SaveMessageToQueue(tc.Context, &clone)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					p := loadMessage(tc.Context, repository)
					expectParcelToEqual(p, parcel0)
				})

				ginkgo.It("increments the revision even if no meta-data has changed", func() {
					saveMessages(tc.Context, dataStore, parcel0)

					p := loadMessage(tc.Context, repository)
					gomega.Expect(p.Revision).To(gomega.BeEquivalentTo(2))
				})

				ginkgo.It("maintains the correct queue order", func() {
					// Place parcel1 before existing parcel0.
					parcel1.NextAttemptAt = time.Now().Add(-1 * time.Hour)
					saveMessages(tc.Context, dataStore, parcel1)

					// Then move parcel0 to the front of the queue.
					parcel0.NextAttemptAt = time.Now().Add(-10 * time.Hour)
					saveMessages(tc.Context, dataStore, parcel0)

					p := loadMessage(tc.Context, repository)
					expectParcelToEqual(p, parcel0)
				})

				ginkgo.It("does not update the envelope", func() {
					original := parcel0.Envelope
					parcel0.Envelope = parcel1.Envelope
					saveMessages(tc.Context, dataStore, parcel1)

					p := loadMessage(tc.Context, repository)
					common.ExpectProtoToEqual(
						p.Envelope,
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
						saveMessages(tc.Context, dataStore, parcel0)

						err := persistence.WithTransaction(
							tc.Context,
							dataStore,
							func(tx persistence.ManagedTransaction) error {
								clone := *parcel0
								clone.Revision = queuestore.Revision(conflictingRevision)
								clone.NextAttemptAt = clone.NextAttemptAt.Add(1 * time.Hour)

								err := tx.SaveMessageToQueue(tc.Context, &clone)
								gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))

								return nil // let the transaction commit despite error
							},
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						p := loadMessage(tc.Context, repository)
						expectParcelToEqual(p, parcel0)
					},
					table.Entry("zero", 0),
					table.Entry("too low", 1),
					table.Entry("too high", 100),
				)
			})

			ginkgo.When("the message is not yet on the queue", func() {
				ginkgo.It("saves the message with an initial revision of 1", func() {
					saveMessages(tc.Context, dataStore, parcel0)

					p := loadMessage(tc.Context, repository)
					expectParcelToEqual(p, parcel0)
				})

				ginkgo.It("saves messages that were not created by a handler", func() {
					parcel0.Envelope.MetaData.Source.Handler = nil
					parcel0.Envelope.MetaData.Source.InstanceId = ""

					saveMessages(tc.Context, dataStore, parcel0)

					p := loadMessage(tc.Context, repository)

					// We can't use the regular protobuf comparison methods,
					// because even proto.Equal() treats a nil proto.Message
					// differently to a zero-value.
					src := p.Envelope.GetMetaData().GetSource()
					gomega.Expect(src.GetHandler().GetName()).To(gomega.Equal(""))
					gomega.Expect(src.GetHandler().GetKey()).To(gomega.Equal(""))
					gomega.Expect(src.GetInstanceId()).To(gomega.Equal(""))
				})

				ginkgo.It("does not save the message if the transaction is rolled back", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							return tx.SaveMessageToQueue(tc.Context, parcel0)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					parcels := loadMessages(tc.Context, repository, 1)
					gomega.Expect(parcels).To(gomega.BeEmpty())
				})

				ginkgo.It("does not save the message when an OCC conflict occurs", func() {
					parcel0.Revision = 123

					err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							err := tx.SaveMessageToQueue(tc.Context, parcel0)
							gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))
							return nil // let the transaction commit despite error
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					parcels := loadMessages(tc.Context, repository, 1)
					gomega.Expect(parcels).To(gomega.BeEmpty())
				})
			})

			ginkgo.When("a message is saved more than once in the same transaction", func() {
				ginkgo.It("saves the meta-data from the most recent call", func() {
					err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if err := tx.SaveMessageToQueue(tc.Context, parcel0); err != nil {
								return err
							}

							parcel0.Revision++
							parcel0.NextAttemptAt = time.Now().Add(1 * time.Hour)

							return tx.SaveMessageToQueue(tc.Context, parcel0)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					p := loadMessage(tc.Context, repository)

					parcel0.Revision++
					expectParcelToEqual(p, parcel0)
				})

				ginkgo.It("uses the uncommitted revision for OCC checks", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if err := tx.SaveMessageToQueue(tc.Context, parcel0); err != nil {
								return err
							}

							// Note that we did not increment parcel0.Revision
							// after the first save.
							return tx.SaveMessageToQueue(tc.Context, parcel0)
						},
					)
					gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))
				})
			})

			ginkgo.It("does not update the revision field of the argument", func() {
				err := persistence.WithTransaction(
					tc.Context,
					dataStore,
					func(tx persistence.ManagedTransaction) error {
						before := parcel0.Revision
						err := tx.SaveMessageToQueue(tc.Context, parcel0)
						gomega.Expect(parcel0.Revision).To(gomega.Equal(before))

						return err
					},
				)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			})
		})

		ginkgo.Describe("func RemoveMessageFromQueue()", func() {
			ginkgo.When("the message is on the queue", func() {
				ginkgo.BeforeEach(func() {
					saveMessages(tc.Context, dataStore, parcel0)
				})

				ginkgo.It("removes the message from the queue", func() {
					removeMessages(tc.Context, dataStore, parcel0)

					parcels := loadMessages(tc.Context, repository, 1)
					gomega.Expect(parcels).To(gomega.BeEmpty())
				})

				ginkgo.It("does not remove the message if the transaction is rolled-back", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							return tx.RemoveMessageFromQueue(tc.Context, parcel0)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					p := loadMessage(tc.Context, repository)
					expectParcelToEqual(p, parcel0)
				})

				ginkgo.It("maintains the correct queue order", func() {
					// add parcel1 after parcel0, then remove parcel0
					parcel1.NextAttemptAt = time.Now().Add(1 * time.Hour)
					saveMessages(tc.Context, dataStore, parcel1)
					removeMessages(tc.Context, dataStore, parcel0)

					p := loadMessage(tc.Context, repository)
					expectParcelToEqual(p, parcel1)
				})

				table.DescribeTable(
					"it does not remove the message when an OCC conflict occurs",
					func(conflictingRevision int) {
						// Update the message once more so that it's up to
						// revision 2. Otherwise we can't test for 1 as a
						// too-low value.
						saveMessages(tc.Context, dataStore, parcel0)

						err := persistence.WithTransaction(
							tc.Context,
							dataStore,
							func(tx persistence.ManagedTransaction) error {
								clone := *parcel0
								clone.Revision = queuestore.Revision(conflictingRevision)

								err := tx.RemoveMessageFromQueue(tc.Context, &clone)
								gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))

								return nil // let the transaction commit despite error
							},
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						p := loadMessage(tc.Context, repository)
						expectParcelToEqual(p, parcel0)
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
								clone := *parcel0
								clone.Revision = queuestore.Revision(conflictingRevision)

								err := tx.RemoveMessageFromQueue(tc.Context, &clone)
								gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))

								return nil // let the transaction commit despite error
							},
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						parcels := loadMessages(tc.Context, repository, 1)
						gomega.Expect(parcels).To(
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
					err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if err := tx.SaveMessageToQueue(tc.Context, parcel0); err != nil {
								return err
							}

							parcel0.Revision++

							return tx.RemoveMessageFromQueue(tc.Context, parcel0)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					parcels := loadMessages(tc.Context, repository, 1)
					gomega.Expect(parcels).To(gomega.BeEmpty())
				})

				ginkgo.It("uses the uncommitted revision for OCC checks", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if err := tx.SaveMessageToQueue(tc.Context, parcel0); err != nil {
								return err
							}

							// Note that we did not increment parcel0.Revision
							// after the save.
							return tx.RemoveMessageFromQueue(tc.Context, parcel0)
						},
					)
					gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))
				})
			})

			ginkgo.When("a message is deleted then saved in the same transaction", func() {
				ginkgo.BeforeEach(func() {
					saveMessages(tc.Context, dataStore, parcel0)
				})

				ginkgo.It("saves the new message", func() {
					err := persistence.WithTransaction(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if err := tx.RemoveMessageFromQueue(tc.Context, parcel0); err != nil {
								return err
							}

							parcel0.Revision = 0
							parcel0.NextAttemptAt = time.Now().Add(1 * time.Hour)

							return tx.SaveMessageToQueue(tc.Context, parcel0)
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					p := loadMessage(tc.Context, repository)

					parcel0.Revision++
					expectParcelToEqual(p, parcel0)
				})

				ginkgo.It("uses the uncommitted revision for OCC checks", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if err := tx.RemoveMessageFromQueue(tc.Context, parcel0); err != nil {
								return err
							}

							// Note that we did set parcel0.Revision back to
							// zero after the remove.
							return tx.SaveMessageToQueue(tc.Context, parcel0)
						},
					)
					gomega.Expect(err).To(gomega.Equal(queuestore.ErrConflict))
				})
			})
		})

		ginkgo.It("serializes operations from competing transactions", func() {
			ginkgo.By("running several transactions in parallel")

			saveMessages(tc.Context, dataStore, parcel0, parcel1)

			var g sync.WaitGroup
			g.Add(3)

			// save
			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()
				saveMessages(tc.Context, dataStore, parcel2)
			}()

			// update
			parcel1.NextAttemptAt = time.Now().Add(+1 * time.Hour)
			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()
				saveMessages(tc.Context, dataStore, parcel1)
			}()

			// remove
			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()
				removeMessages(tc.Context, dataStore, parcel0)
			}()

			g.Wait()

			ginkgo.By("loading the messages")

			expected := []*queuestore.Parcel{parcel2, parcel1}
			parcels := loadMessages(tc.Context, repository, len(expected)+1)
			expectParcelsToEqual(parcels, expected)
		})
	})
}

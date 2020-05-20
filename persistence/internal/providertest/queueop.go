package providertest

import (
	"sync"
	"time"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/jmalloc/gomegax"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

// declareQueueOperationTests declares a functional test-suite for
// persistence operations related to the message queue.
func declareQueueOperationTests(tc *TestContext) {
	ginkgo.Context("queue operations", func() {
		var (
			dataStore  persistence.DataStore
			repository queuestore.Repository
			tearDown   func()
			now        time.Time

			env0, env1, env2 *envelopespec.Envelope
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()
			repository = dataStore.QueueStoreRepository()

			env0 = infixfixtures.NewEnvelope("<message-0>", dogmafixtures.MessageA1)
			env1 = infixfixtures.NewEnvelope("<message-1>", dogmafixtures.MessageA2)
			env2 = infixfixtures.NewEnvelope("<message-2>", dogmafixtures.MessageA3)

			now = time.Now().Truncate(time.Millisecond) // we only expect NextAttemptAt to have millisecond precision
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("type persistence.SaveQueueItem", func() {
			ginkgo.When("the message is already on the queue", func() {
				ginkgo.BeforeEach(func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueItem{
							Item: queuestore.Item{
								NextAttemptAt: now,
								Envelope:      env0,
							},
						},
					)
				})

				ginkgo.It("updates the item", func() {
					next := now.Add(1 * time.Hour)

					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueItem{
							Item: queuestore.Item{
								Revision:      1,
								NextAttemptAt: next,
								FailureCount:  123,
								Envelope:      env0,
							},
						},
					)

					item := loadQueueItem(tc.Context, repository)
					gomega.Expect(item).To(gomegax.EqualX(
						queuestore.Item{
							Revision:      2,
							NextAttemptAt: next,
							FailureCount:  123,
							Envelope:      env0,
						},
					))
				})

				ginkgo.It("increments the revision even if no meta-data has changed", func() {
					item := loadQueueItem(tc.Context, repository)

					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueItem{
							Item: item,
						},
					)

					item = loadQueueItem(tc.Context, repository)
					gomega.Expect(item.Revision).To(
						gomega.BeEquivalentTo(2),
						"revision was not incremented correctly",
					)
				})

				ginkgo.It("maintains the correct queue order", func() {
					ginkgo.By("placing env1 before env0 in the queue")
					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueItem{
							Item: queuestore.Item{
								NextAttemptAt: now.Add(-1 * time.Hour),
								Envelope:      env1,
							},
						},
					)

					ginkgo.By("updating env0 to be before env1")
					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueItem{
							Item: queuestore.Item{
								Revision:      1,
								NextAttemptAt: now.Add(-10 * time.Hour),
								Envelope:      env0,
							},
						},
					)

					item := loadQueueItem(tc.Context, repository)
					gomega.Expect(item.Envelope).To(
						gomegax.EqualX(env0),
						"env0 was expected to be at the head of the queue",
					)
				})

				ginkgo.It("does not update the envelope", func() {
					env := proto.Clone(env0).(*envelopespec.Envelope)
					env.MetaData.CausationId = "<different>"

					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueItem{
							Item: queuestore.Item{
								Revision:      1,
								NextAttemptAt: now,
								Envelope:      env,
							},
						},
					)

					item := loadQueueItem(tc.Context, repository)
					gomega.Expect(item.Envelope).To(
						gomegax.EqualX(env0),
						"envelope was updated, not just the meta-data",
					)
				})

				table.DescribeTable(
					"it does not update the item when an OCC conflict occurs",
					func(conflictingRevision int) {
						// Update the item once more so that it's up to
						// revision 2. Otherwise we can't test for 1 as a
						// too-low value.
						persist(
							tc.Context,
							dataStore,
							persistence.SaveQueueItem{
								Item: queuestore.Item{
									Revision:      1,
									NextAttemptAt: now,
									Envelope:      env0,
								},
							},
						)

						op := persistence.SaveQueueItem{
							Item: queuestore.Item{
								Revision:      uint64(conflictingRevision),
								NextAttemptAt: now,
								Envelope:      env0,
							},
						}

						_, err := dataStore.Persist(
							tc.Context,
							persistence.Batch{op},
						)
						gomega.Expect(err).To(gomega.Equal(
							persistence.ConflictError{
								Cause: op,
							},
						))

						item := loadQueueItem(tc.Context, repository)
						gomega.Expect(item).To(gomegax.EqualX(
							queuestore.Item{
								Revision:      2,
								NextAttemptAt: now,
								Envelope:      env0,
							},
						))
					},
					table.Entry("zero", 0),
					table.Entry("too low", 1),
					table.Entry("too high", 100),
				)
			})

			ginkgo.When("the message is not yet on the queue", func() {
				ginkgo.It("saves the item with an initial revision of 1", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueItem{
							Item: queuestore.Item{
								NextAttemptAt: now,
								Envelope:      env0,
							},
						},
					)

					item := loadQueueItem(tc.Context, repository)
					gomega.Expect(item.Revision).To(gomega.BeEquivalentTo(1))
				})

				ginkgo.It("saves messages that were not created by a handler", func() {
					env0.MetaData.Source.Handler = &envelopespec.Identity{}
					env0.MetaData.Source.InstanceId = ""

					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueItem{
							Item: queuestore.Item{
								NextAttemptAt: now,
								Envelope:      env0,
							},
						},
					)

					item := loadQueueItem(tc.Context, repository)
					gomega.Expect(
						item.Envelope.MetaData.Source,
					).To(gomegax.EqualX(
						env0.MetaData.Source,
					))
				})

				ginkgo.It("does not save the item when an OCC conflict occurs", func() {
					op := persistence.SaveQueueItem{
						Item: queuestore.Item{
							Revision:      123,
							NextAttemptAt: now,
							Envelope:      env0,
						},
					}

					_, err := dataStore.Persist(
						tc.Context,
						persistence.Batch{op},
					)
					gomega.Expect(err).To(gomega.Equal(
						persistence.ConflictError{
							Cause: op,
						},
					))

					items := loadQueueItems(tc.Context, repository, 1)
					gomega.Expect(items).To(gomega.BeEmpty())
				})
			})
		})

		ginkgo.Describe("type RemoveQueueItem", func() {
			ginkgo.When("the message is on the queue", func() {
				ginkgo.BeforeEach(func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueItem{
							Item: queuestore.Item{
								NextAttemptAt: now,
								Envelope:      env0,
							},
						},
					)
				})

				ginkgo.It("removes the message from the queue", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.RemoveQueueItem{
							Item: queuestore.Item{
								Revision: 1,
								Envelope: env0,
							},
						},
					)

					items := loadQueueItems(tc.Context, repository, 1)
					gomega.Expect(items).To(gomega.BeEmpty())
				})

				ginkgo.It("maintains the correct queue order", func() {
					ginkgo.By("placing env1 after env0 in the queue")

					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueItem{
							Item: queuestore.Item{
								NextAttemptAt: now.Add(1 * time.Hour),
								Envelope:      env1,
							},
						},
					)

					ginkgo.By("removing env0 from the queue")

					persist(
						tc.Context,
						dataStore,
						persistence.RemoveQueueItem{
							Item: queuestore.Item{
								Revision: 1,
								Envelope: env0,
							},
						},
					)

					item := loadQueueItem(tc.Context, repository)
					gomega.Expect(item.Envelope).To(
						gomegax.EqualX(env1),
						"env1 was expected to be at the head of the queue",
					)
				})

				table.DescribeTable(
					"it does not remove the message when an OCC conflict occurs",
					func(conflictingRevision int) {
						// Update the message once more so that it's up to
						// revision 2. Otherwise we can't test for 1 as a
						// too-low value.
						persist(
							tc.Context,
							dataStore,
							persistence.SaveQueueItem{
								Item: queuestore.Item{
									Revision:      1,
									NextAttemptAt: now,
									Envelope:      env0,
								},
							},
						)

						op := persistence.RemoveQueueItem{
							Item: queuestore.Item{
								Revision:      uint64(conflictingRevision),
								NextAttemptAt: now,
								Envelope:      env0,
							},
						}

						_, err := dataStore.Persist(
							tc.Context,
							persistence.Batch{op},
						)
						gomega.Expect(err).To(gomega.Equal(
							persistence.ConflictError{
								Cause: op,
							},
						))

						item := loadQueueItem(tc.Context, repository)
						gomega.Expect(item).To(gomegax.EqualX(
							queuestore.Item{
								Revision:      2,
								NextAttemptAt: now,
								Envelope:      env0,
							},
						))
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
						op := persistence.RemoveQueueItem{
							Item: queuestore.Item{
								Revision:      uint64(conflictingRevision),
								NextAttemptAt: now,
								Envelope:      env0,
							},
						}

						_, err := dataStore.Persist(
							tc.Context,
							persistence.Batch{op},
						)
						gomega.Expect(err).To(gomega.Equal(
							persistence.ConflictError{
								Cause: op,
							},
						))

						items := loadQueueItems(tc.Context, repository, 1)
						gomega.Expect(items).To(
							gomega.BeEmpty(),
							"removal of non-existent message caused it to exist",
						)
					},
					table.Entry("zero", 0),
					table.Entry("non-zero", 100),
				)
			})
		})

		ginkgo.It("serializes operations from competing transactions", func() {
			item0 := queuestore.Item{
				NextAttemptAt: now,
				Envelope:      env0,
			}

			item1 := queuestore.Item{
				NextAttemptAt: now,
				Envelope:      env1,
			}

			item2 := queuestore.Item{
				NextAttemptAt: now,
				Envelope:      env2,
			}

			persist(
				tc.Context,
				dataStore,
				persistence.SaveQueueItem{
					Item: item0,
				},
				persistence.SaveQueueItem{
					Item: item1,
				},
			)

			item0.Revision++
			item1.Revision++

			var g sync.WaitGroup
			g.Add(3)

			// create
			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()

				persist(
					tc.Context,
					dataStore,
					persistence.SaveQueueItem{
						Item: item2,
					},
				)

				item2.Revision++
			}()

			// update
			item1.NextAttemptAt = now.Add(+1 * time.Hour)
			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()

				persist(
					tc.Context,
					dataStore,
					persistence.SaveQueueItem{
						Item: item1,
					},
				)

				item1.Revision++
			}()

			// remove
			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()

				persist(
					tc.Context,
					dataStore,
					persistence.RemoveQueueItem{
						Item: item0,
					},
				)
			}()

			g.Wait()

			expected := []queuestore.Item{item2, item1}
			items := loadQueueItems(tc.Context, repository, len(expected)+1)
			gomega.Expect(items).To(gomegax.EqualX(expected))
		})
	})
}

package providertest

import (
	"sync"
	"time"

	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/interopspec/envelopespec"
	verityfixtures "github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/persistence"
	"github.com/jmalloc/gomegax"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

// declareQueueOperationTests declares a functional test-suite for
// persistence operations related to the message queue.
func declareQueueOperationTests(tc *TestContext) {
	ginkgo.Context("queue operations", func() {
		var (
			dataStore persistence.DataStore
			now       time.Time

			env0, env1, env2 *envelopespec.Envelope
		)

		ginkgo.BeforeEach(func() {
			var tearDown func()
			dataStore, tearDown = tc.SetupDataStore()
			ginkgo.DeferCleanup(tearDown)

			env0 = verityfixtures.NewEnvelope("<message-0>", stubs.CommandA1, now, now)
			env1 = verityfixtures.NewEnvelope("<message-1>", stubs.CommandA2, now)
			env2 = verityfixtures.NewEnvelope("<message-2>", stubs.CommandA3, now)

			now = time.Now().Truncate(time.Millisecond) // we only expect NextAttemptAt to have millisecond precision
		})

		ginkgo.Describe("type persistence.SaveQueueMessage", func() {
			ginkgo.When("the message is already on the queue", func() {
				ginkgo.BeforeEach(func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueMessage{
							Message: persistence.QueueMessage{
								NextAttemptAt: now,
								Envelope:      env0,
							},
						},
					)
				})

				ginkgo.It("updates the message", func() {
					next := now.Add(1 * time.Hour)

					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueMessage{
							Message: persistence.QueueMessage{
								Revision:      1,
								NextAttemptAt: next,
								FailureCount:  123,
								Envelope:      env0,
							},
						},
					)

					m := loadQueueMessage(tc.Context, dataStore)
					gomega.Expect(m).To(gomegax.EqualX(
						persistence.QueueMessage{
							Revision:      2,
							NextAttemptAt: next,
							FailureCount:  123,
							Envelope:      env0,
						},
					))
				})

				ginkgo.It("increments the revision even if no meta-data has changed", func() {
					m := loadQueueMessage(tc.Context, dataStore)

					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueMessage{
							Message: m,
						},
					)

					m = loadQueueMessage(tc.Context, dataStore)
					gomega.Expect(m.Revision).To(
						gomega.BeEquivalentTo(2),
						"revision was not incremented correctly",
					)
				})

				ginkgo.It("maintains the correct queue order", func() {
					ginkgo.By("placing env1 before env0 in the queue")
					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueMessage{
							Message: persistence.QueueMessage{
								NextAttemptAt: now.Add(-1 * time.Hour),
								Envelope:      env1,
							},
						},
					)

					ginkgo.By("updating env0 to be before env1")
					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueMessage{
							Message: persistence.QueueMessage{
								Revision:      1,
								NextAttemptAt: now.Add(-10 * time.Hour),
								Envelope:      env0,
							},
						},
					)

					m := loadQueueMessage(tc.Context, dataStore)
					gomega.Expect(m.Envelope).To(
						gomegax.EqualX(env0),
						"env0 was expected to be at the head of the queue",
					)
				})

				ginkgo.It("does not update the envelope", func() {
					env := proto.Clone(env0).(*envelopespec.Envelope)
					env.CausationId = "<different>"

					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueMessage{
							Message: persistence.QueueMessage{
								Revision:      1,
								NextAttemptAt: now,
								Envelope:      env,
							},
						},
					)

					m := loadQueueMessage(tc.Context, dataStore)
					gomega.Expect(m.Envelope).To(
						gomegax.EqualX(env0),
						"envelope was updated, not just the meta-data",
					)
				})

				ginkgo.DescribeTable(
					"it does not update the message when an OCC conflict occurs",
					func(conflictingRevision int) {
						// Update the message once more so that it's up to
						// revision 2. Otherwise we can't test for 1 as a
						// too-low value.
						persist(
							tc.Context,
							dataStore,
							persistence.SaveQueueMessage{
								Message: persistence.QueueMessage{
									Revision:      1,
									NextAttemptAt: now,
									Envelope:      env0,
								},
							},
						)

						op := persistence.SaveQueueMessage{
							Message: persistence.QueueMessage{
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

						m := loadQueueMessage(tc.Context, dataStore)
						gomega.Expect(m).To(gomegax.EqualX(
							persistence.QueueMessage{
								Revision:      2,
								NextAttemptAt: now,
								Envelope:      env0,
							},
						))
					},
					ginkgo.Entry("zero", 0),
					ginkgo.Entry("too low", 1),
					ginkgo.Entry("too high", 100),
				)
			})

			ginkgo.When("the message is not yet on the queue", func() {
				ginkgo.It("saves the message with an initial revision of 1", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueMessage{
							Message: persistence.QueueMessage{
								NextAttemptAt: now,
								Envelope:      env0,
							},
						},
					)

					m := loadQueueMessage(tc.Context, dataStore)
					gomega.Expect(m.Revision).To(gomega.BeEquivalentTo(1))
				})

				ginkgo.It("saves messages that were not created by a handler", func() {
					env0.SourceHandler = &envelopespec.Identity{}
					env0.SourceInstanceId = ""
					env0.ScheduledFor = ""

					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueMessage{
							Message: persistence.QueueMessage{
								NextAttemptAt: now,
								Envelope:      env0,
							},
						},
					)

					m := loadQueueMessage(tc.Context, dataStore)
					gomega.Expect(m.Envelope).To(gomegax.EqualX(env0))
				})

				ginkgo.It("does not save the message when an OCC conflict occurs", func() {
					op := persistence.SaveQueueMessage{
						Message: persistence.QueueMessage{
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

					messages := loadQueueMessages(tc.Context, dataStore, 1)
					gomega.Expect(messages).To(gomega.BeEmpty())
				})
			})
		})

		ginkgo.Describe("type RemoveQueueMessage", func() {
			ginkgo.When("the message is on the queue", func() {
				ginkgo.BeforeEach(func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueMessage{
							Message: persistence.QueueMessage{
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
						persistence.RemoveQueueMessage{
							Message: persistence.QueueMessage{
								Revision: 1,
								Envelope: env0,
							},
						},
					)

					messages := loadQueueMessages(tc.Context, dataStore, 1)
					gomega.Expect(messages).To(gomega.BeEmpty())
				})

				ginkgo.It("maintains the correct queue order", func() {
					ginkgo.By("placing env1 after env0 in the queue")

					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueMessage{
							Message: persistence.QueueMessage{
								NextAttemptAt: now.Add(1 * time.Hour),
								Envelope:      env1,
							},
						},
					)

					ginkgo.By("removing env0 from the queue")

					persist(
						tc.Context,
						dataStore,
						persistence.RemoveQueueMessage{
							Message: persistence.QueueMessage{
								Revision: 1,
								Envelope: env0,
							},
						},
					)

					m := loadQueueMessage(tc.Context, dataStore)
					gomega.Expect(m.Envelope).To(
						gomegax.EqualX(env1),
						"env1 was expected to be at the head of the queue",
					)
				})

				ginkgo.It("maintains the correct queue order when removing a message that is not at the head of the queue", func() {
					ginkgo.By("placing env1 and env2 after env0 in the queue")

					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueMessage{
							Message: persistence.QueueMessage{
								NextAttemptAt: now.Add(1 * time.Hour),
								Envelope:      env1,
							},
						},
						persistence.SaveQueueMessage{
							Message: persistence.QueueMessage{
								NextAttemptAt: now.Add(2 * time.Hour),
								Envelope:      env2,
							},
						},
					)

					ginkgo.By("removing env1 from the queue")

					persist(
						tc.Context,
						dataStore,
						persistence.RemoveQueueMessage{
							Message: persistence.QueueMessage{
								Revision: 1,
								Envelope: env1,
							},
						},
					)

					messages := loadQueueMessages(tc.Context, dataStore, 3)
					gomega.Expect(messages).To(gomega.HaveLen(2))
					gomega.Expect(messages[0].Envelope).To(gomegax.EqualX(env0))
					gomega.Expect(messages[1].Envelope).To(gomegax.EqualX(env2))
				})

				ginkgo.DescribeTable(
					"it does not remove the message when an OCC conflict occurs",
					func(conflictingRevision int) {
						// Update the message once more so that it's up to
						// revision 2. Otherwise we can't test for 1 as a
						// too-low value.
						persist(
							tc.Context,
							dataStore,
							persistence.SaveQueueMessage{
								Message: persistence.QueueMessage{
									Revision:      1,
									NextAttemptAt: now,
									Envelope:      env0,
								},
							},
						)

						op := persistence.RemoveQueueMessage{
							Message: persistence.QueueMessage{
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

						m := loadQueueMessage(tc.Context, dataStore)
						gomega.Expect(m).To(gomegax.EqualX(
							persistence.QueueMessage{
								Revision:      2,
								NextAttemptAt: now,
								Envelope:      env0,
							},
						))
					},
					ginkgo.Entry("zero", 0),
					ginkgo.Entry("too low", 1),
					ginkgo.Entry("too high", 100),
				)
			})

			ginkgo.When("the message is not on the queue", func() {
				ginkgo.DescribeTable(
					"returns an OCC conflict error",
					func(conflictingRevision int) {
						op := persistence.RemoveQueueMessage{
							Message: persistence.QueueMessage{
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

						messages := loadQueueMessages(tc.Context, dataStore, 1)
						gomega.Expect(messages).To(
							gomega.BeEmpty(),
							"removal of non-existent message caused it to exist",
						)
					},
					ginkgo.Entry("zero", 0),
					ginkgo.Entry("non-zero", 100),
				)
			})
		})

		ginkgo.It("serializes operations from concurrent persist calls", func() {
			m0 := persistence.QueueMessage{
				NextAttemptAt: now,
				Envelope:      env0,
			}

			m1 := persistence.QueueMessage{
				NextAttemptAt: now,
				Envelope:      env1,
			}

			m2 := persistence.QueueMessage{
				NextAttemptAt: now,
				Envelope:      env2,
			}

			persist(
				tc.Context,
				dataStore,
				persistence.SaveQueueMessage{
					Message: m0,
				},
				persistence.SaveQueueMessage{
					Message: m1,
				},
			)

			m0.Revision++
			m1.Revision++

			var g sync.WaitGroup
			g.Add(3)

			// create
			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()

				persist(
					tc.Context,
					dataStore,
					persistence.SaveQueueMessage{
						Message: m2,
					},
				)

				m2.Revision++
			}()

			// update
			m1.NextAttemptAt = now.Add(+1 * time.Hour)
			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()

				persist(
					tc.Context,
					dataStore,
					persistence.SaveQueueMessage{
						Message: m1,
					},
				)

				m1.Revision++
			}()

			// remove
			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()

				persist(
					tc.Context,
					dataStore,
					persistence.RemoveQueueMessage{
						Message: m0,
					},
				)
			}()

			g.Wait()

			messages := loadQueueMessages(tc.Context, dataStore, 3)
			gomega.Expect(messages).To(gomegax.EqualX(
				[]persistence.QueueMessage{
					m2,
					m1,
				},
			))
		})
	})
}

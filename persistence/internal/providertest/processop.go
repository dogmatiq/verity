package providertest

import (
	"sync"
	"time"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/marshalkit"
	verityfixtures "github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/persistence"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// declareProcessOperationTests declares a functional test-suite for
// persistence operations related to processes.
func declareProcessOperationTests(tc *TestContext) {
	ginkgo.Context("process operations", func() {
		var dataStore persistence.DataStore

		ginkgo.BeforeEach(func() {
			var tearDown func()
			dataStore, tearDown = tc.SetupDataStore()
			ginkgo.DeferCleanup(tearDown)
		})

		ginkgo.Describe("type persistence.SaveProcessInstance", func() {
			ginkgo.When("the instance does not exist", func() {
				ginkgo.It("saves the instance with a revision of 1", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveProcessInstance{
							Instance: persistence.ProcessInstance{
								HandlerKey: verityfixtures.DefaultHandlerKey,
								InstanceID: "<instance>",
							},
						},
					)

					inst := loadProcessInstance(tc.Context, dataStore, verityfixtures.DefaultHandlerKey, "<instance>")
					gomega.Expect(inst.Revision).To(gomega.BeEquivalentTo(1))
				})

				ginkgo.It("does not save the instance when an OCC conflict occurs", func() {
					op := persistence.SaveProcessInstance{
						Instance: persistence.ProcessInstance{
							HandlerKey: verityfixtures.DefaultHandlerKey,
							InstanceID: "<instance>",
							Revision:   123,
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

					inst := loadProcessInstance(tc.Context, dataStore, verityfixtures.DefaultHandlerKey, "<instance>")
					gomega.Expect(inst).To(gomega.Equal(
						persistence.ProcessInstance{
							HandlerKey: verityfixtures.DefaultHandlerKey,
							InstanceID: "<instance>",
							Revision:   0,
						},
					))
				})
			})

			ginkgo.When("the instance exists", func() {
				ginkgo.BeforeEach(func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveProcessInstance{
							Instance: persistence.ProcessInstance{
								HandlerKey: verityfixtures.DefaultHandlerKey,
								InstanceID: "<instance>",
							},
						},
					)
				})

				ginkgo.It("increments the revision even if no meta-data has changed", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveProcessInstance{
							Instance: persistence.ProcessInstance{
								HandlerKey: verityfixtures.DefaultHandlerKey,
								InstanceID: "<instance>",
								Revision:   1,
							},
						},
					)

					inst := loadProcessInstance(tc.Context, dataStore, verityfixtures.DefaultHandlerKey, "<instance>")
					gomega.Expect(inst.Revision).To(gomega.BeEquivalentTo(2))
				})

				table.DescribeTable(
					"it does not save the instance when an OCC conflict occurs",
					func(conflictingRevision int) {
						// Increment the instance once more so that it's up to
						// revision 2. Otherwise we can't test for 1 as a
						// too-low value.
						persist(
							tc.Context,
							dataStore,
							persistence.SaveProcessInstance{
								Instance: persistence.ProcessInstance{
									HandlerKey: verityfixtures.DefaultHandlerKey,
									InstanceID: "<instance>",
									Revision:   1,
								},
							},
						)

						op := persistence.SaveProcessInstance{
							Instance: persistence.ProcessInstance{
								HandlerKey: verityfixtures.DefaultHandlerKey,
								InstanceID: "<instance>",
								Revision:   uint64(conflictingRevision),
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

						inst := loadProcessInstance(tc.Context, dataStore, verityfixtures.DefaultHandlerKey, "<instance>")
						gomega.Expect(inst).To(gomega.Equal(
							persistence.ProcessInstance{
								HandlerKey: verityfixtures.DefaultHandlerKey,
								InstanceID: "<instance>",
								Revision:   2,
							},
						))
					},
					table.Entry("zero", 0),
					table.Entry("too low", 1),
					table.Entry("too high", 100),
				)
			})
		})

		ginkgo.Describe("type persistence.RemoveProcessInstance", func() {
			ginkgo.When("the instance exists", func() {
				ginkgo.BeforeEach(func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveProcessInstance{
							Instance: persistence.ProcessInstance{
								HandlerKey: verityfixtures.DefaultHandlerKey,
								InstanceID: "<instance>",
								Packet: marshalkit.Packet{
									MediaType: "<media-type>",
									Data:      []byte("<data>"),
								},
							},
						},
					)
				})

				ginkgo.It("removes the instance", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.RemoveProcessInstance{
							Instance: persistence.ProcessInstance{
								HandlerKey: verityfixtures.DefaultHandlerKey,
								InstanceID: "<instance>",
								Revision:   1,
							},
						},
					)

					inst := loadProcessInstance(tc.Context, dataStore, verityfixtures.DefaultHandlerKey, "<instance>")
					gomega.Expect(inst).To(gomega.Equal(
						persistence.ProcessInstance{
							HandlerKey: verityfixtures.DefaultHandlerKey,
							InstanceID: "<instance>",
						},
					))
				})

				ginkgo.It("removes the timeout messages for the removed instance only", func() {
					now := time.Now()

					m0 := persistence.QueueMessage{
						NextAttemptAt: now,
						Envelope: verityfixtures.NewEnvelope(
							"<message-0>",
							dogmafixtures.MessageT1,
							now,
							now,
						),
					}

					m1 := persistence.QueueMessage{
						NextAttemptAt: now.Add(1 * time.Hour),
						Envelope: verityfixtures.NewEnvelope(
							"<message-1>",
							dogmafixtures.MessageT1,
							now,
							now,
						),
					}
					m1.Envelope.SourceInstanceId = "<other-instance>"

					m2 := persistence.QueueMessage{
						NextAttemptAt: now.Add(2 * time.Hour),
						Envelope: verityfixtures.NewEnvelope(
							"<message-2>",
							dogmafixtures.MessageT1,
							now,
							now,
						),
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
						persistence.SaveQueueMessage{
							Message: m2,
						},
					)
					m0.Revision++
					m1.Revision++
					m2.Revision++

					persist(
						tc.Context,
						dataStore,
						persistence.RemoveProcessInstance{
							Instance: persistence.ProcessInstance{
								HandlerKey: verityfixtures.DefaultHandlerKey,
								InstanceID: "<instance>",
								Revision:   1,
							},
						},
					)

					messages := loadQueueMessages(tc.Context, dataStore, 3)
					gomega.Expect(messages).To(gomega.HaveLen(1))
					gomega.Expect(messages[0].ID()).To(gomega.Equal("<message-1>"))
				})

				table.DescribeTable(
					"it does not remove the instance when an OCC conflict occurs",
					func(conflictingRevision int) {
						// Update the instance once more so that it's up to
						// revision 2. Otherwise we can't test for 1 as a
						// too-low value.
						persist(
							tc.Context,
							dataStore,
							persistence.SaveProcessInstance{
								Instance: persistence.ProcessInstance{
									HandlerKey: verityfixtures.DefaultHandlerKey,
									InstanceID: "<instance>",
									Revision:   1,
								},
							},
						)

						op := persistence.RemoveProcessInstance{
							Instance: persistence.ProcessInstance{
								HandlerKey: verityfixtures.DefaultHandlerKey,
								InstanceID: "<instance>",
								Revision:   uint64(conflictingRevision),
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

						inst := loadProcessInstance(tc.Context, dataStore, verityfixtures.DefaultHandlerKey, "<instance>")
						gomega.Expect(inst).To(gomega.Equal(
							persistence.ProcessInstance{
								HandlerKey: verityfixtures.DefaultHandlerKey,
								InstanceID: "<instance>",
								Revision:   2,
							},
						))
					},
					table.Entry("zero", 0),
					table.Entry("too low", 1),
					table.Entry("too high", 100),
				)
			})

			ginkgo.When("the instance does not exist", func() {
				table.DescribeTable(
					"returns an OCC conflict error",
					func(conflictingRevision int) {
						op := persistence.RemoveProcessInstance{
							Instance: persistence.ProcessInstance{
								HandlerKey: verityfixtures.DefaultHandlerKey,
								InstanceID: "<instance>",
								Revision:   uint64(conflictingRevision),
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

						inst := loadProcessInstance(tc.Context, dataStore, "<handler-key-2>", "<instance-a>")
						gomega.Expect(inst.Revision).To(
							gomega.BeEquivalentTo(0),
							"removal of non-existent process instance caused it to exist",
						)
					},
					table.Entry("zero", 0),
					table.Entry("non-zero", 100),
				)
			})
		})

		ginkgo.It("serializes operations from concurrent persist calls", func() {
			// Note the overlap of handler keys and instance IDs.

			i0 := persistence.ProcessInstance{
				HandlerKey: "<handler-key-1>",
				InstanceID: "<instance-a>",
			}

			i1 := persistence.ProcessInstance{
				HandlerKey: "<handler-key-1>",
				InstanceID: "<instance-b>",
			}

			i2 := persistence.ProcessInstance{
				HandlerKey: "<handler-key-2>",
				InstanceID: "<instance-a>",
			}

			persist(
				tc.Context,
				dataStore,
				persistence.SaveProcessInstance{
					Instance: i0,
				},
				persistence.SaveProcessInstance{
					Instance: i1,
				},
			)

			i0.Revision++
			i1.Revision++

			var g sync.WaitGroup
			g.Add(3)

			// create
			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()

				persist(
					tc.Context,
					dataStore,
					persistence.SaveProcessInstance{
						Instance: i2,
					},
				)

				i2.Revision++
			}()

			// update
			i1.Packet = marshalkit.Packet{
				MediaType: "<media-type>",
				Data:      []byte("<data>"),
			}

			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()

				persist(
					tc.Context,
					dataStore,
					persistence.SaveProcessInstance{
						Instance: i1,
					},
				)

				i1.Revision++
			}()

			// remove
			go func() {
				defer ginkgo.GinkgoRecover()
				defer g.Done()

				persist(
					tc.Context,
					dataStore,
					persistence.RemoveProcessInstance{
						Instance: i0,
					},
				)
			}()

			g.Wait()

			inst := loadProcessInstance(tc.Context, dataStore, "<handler-key-1>", "<instance-a>")
			gomega.Expect(inst).To(gomega.Equal(
				persistence.ProcessInstance{
					HandlerKey: "<handler-key-1>",
					InstanceID: "<instance-a>",
					Revision:   0,
				},
			))

			inst = loadProcessInstance(tc.Context, dataStore, "<handler-key-1>", "<instance-b>")
			gomega.Expect(inst).To(gomega.Equal(
				persistence.ProcessInstance{
					HandlerKey: "<handler-key-1>",
					InstanceID: "<instance-b>",
					Revision:   2,
					Packet: marshalkit.Packet{
						MediaType: "<media-type>",
						Data:      []byte("<data>"),
					},
				},
			))

			inst = loadProcessInstance(tc.Context, dataStore, "<handler-key-2>", "<instance-a>")
			gomega.Expect(inst).To(gomega.Equal(
				persistence.ProcessInstance{
					HandlerKey: "<handler-key-2>",
					InstanceID: "<instance-a>",
					Revision:   1,
				},
			))
		})
	})
}

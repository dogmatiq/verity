package providertest

import (
	"sync"

	"github.com/dogmatiq/infix/persistence"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
)

// declareProcessOperationTests declares a functional test-suite for
// persistence operations related to processes.
func declareProcessOperationTests(tc *TestContext) {
	ginkgo.Context("process operations", func() {
		var (
			dataStore persistence.DataStore
			tearDown  func()
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("type persistence.SaveProcessInstance", func() {
			ginkgo.When("the instance does not exist", func() {
				ginkgo.It("saves the instance with a revision of 1", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveProcessInstance{
							Instance: persistence.ProcessInstance{
								HandlerKey: "<handler-key>",
								InstanceID: "<instance>",
							},
						},
					)

					inst := loadProcessInstance(tc.Context, dataStore, "<handler-key>", "<instance>")
					gomega.Expect(inst.Revision).To(gomega.BeEquivalentTo(1))
				})

				ginkgo.It("does not save the instance when an OCC conflict occurs", func() {
					op := persistence.SaveProcessInstance{
						Instance: persistence.ProcessInstance{
							HandlerKey: "<handler-key>",
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

					inst := loadProcessInstance(tc.Context, dataStore, "<handler-key>", "<instance>")
					gomega.Expect(inst).To(gomega.Equal(
						persistence.ProcessInstance{
							HandlerKey: "<handler-key>",
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
								HandlerKey: "<handler-key>",
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
								HandlerKey: "<handler-key>",
								InstanceID: "<instance>",
								Revision:   1,
							},
						},
					)

					inst := loadProcessInstance(tc.Context, dataStore, "<handler-key>", "<instance>")
					gomega.Expect(inst.Revision).To(gomega.BeEquivalentTo(2))
				})

				table.DescribeTable(
					"it does not save the instance when an OCC conflict occurs",
					func(conflictingRevision int) {
						// Increment the revision once more so that it's up to
						// revision 2. Otherwise we can't test for 1 as a
						// too-low value.
						persist(
							tc.Context,
							dataStore,
							persistence.SaveProcessInstance{
								Instance: persistence.ProcessInstance{
									HandlerKey: "<handler-key>",
									InstanceID: "<instance>",
									Revision:   1,
								},
							},
						)

						op := persistence.SaveProcessInstance{
							Instance: persistence.ProcessInstance{
								HandlerKey: "<handler-key>",
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

						inst := loadProcessInstance(tc.Context, dataStore, "<handler-key>", "<instance>")
						gomega.Expect(inst).To(gomega.Equal(
							persistence.ProcessInstance{
								HandlerKey: "<handler-key>",
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

			ginkgo.It("serializes operations from concurrent persist calls", func() {
				var g sync.WaitGroup

				fn := func(hk, id string, count uint64) {
					defer ginkgo.GinkgoRecover()
					defer g.Done()

					for i := uint64(0); i < count; i++ {
						persist(
							tc.Context,
							dataStore,
							persistence.SaveProcessInstance{
								Instance: persistence.ProcessInstance{
									HandlerKey: hk,
									InstanceID: id,
									Revision:   i,
								},
							},
						)
					}
				}

				// Note the overlap of handler keys and instance IDs.
				g.Add(3)
				go fn("<handler-key-1>", "<instance-a>", 1)
				go fn("<handler-key-1>", "<instance-b>", 2)
				go fn("<handler-key-2>", "<instance-a>", 3)
				g.Wait()

				inst := loadProcessInstance(tc.Context, dataStore, "<handler-key-1>", "<instance-a>")
				gomega.Expect(inst).To(gomega.Equal(
					persistence.ProcessInstance{
						HandlerKey: "<handler-key-1>",
						InstanceID: "<instance-a>",
						Revision:   1,
					},
				))

				inst = loadProcessInstance(tc.Context, dataStore, "<handler-key-1>", "<instance-b>")
				gomega.Expect(inst).To(gomega.Equal(
					persistence.ProcessInstance{
						HandlerKey: "<handler-key-1>",
						InstanceID: "<instance-b>",
						Revision:   2,
					},
				))

				inst = loadProcessInstance(tc.Context, dataStore, "<handler-key-2>", "<instance-a>")
				gomega.Expect(inst).To(gomega.Equal(
					persistence.ProcessInstance{
						HandlerKey: "<handler-key-2>",
						InstanceID: "<instance-a>",
						Revision:   3,
					},
				))
			})
		})
	})
}

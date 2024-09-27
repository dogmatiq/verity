package providertest

import (
	"context"

	"github.com/dogmatiq/enginekit/marshaler"
	"github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/persistence"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// declareProcessRepositoryTests declares a functional test-suite for a
// specific persistence.ProcessRepository implementation.
func declareProcessRepositoryTests(tc *TestContext) {
	ginkgo.Describe("type persistence.ProcessRepository", func() {
		var dataStore persistence.DataStore

		ginkgo.BeforeEach(func() {
			var tearDown func()
			dataStore, tearDown = tc.SetupDataStore()
			ginkgo.DeferCleanup(tearDown)
		})

		ginkgo.Describe("func LoadProcessInstance()", func() {
			ginkgo.It("returns an instance with default values if the instance does not exist", func() {
				inst := loadProcessInstance(tc.Context, dataStore, fixtures.DefaultHandlerKey, "<instance>")
				gomega.Expect(inst).To(gomega.Equal(
					persistence.ProcessInstance{
						HandlerKey: fixtures.DefaultHandlerKey,
						InstanceID: "<instance>",
					},
				))
			})

			ginkgo.It("returns the current persisted instance", func() {
				expect := persistence.ProcessInstance{
					HandlerKey: fixtures.DefaultHandlerKey,
					InstanceID: "<instance>",
					Packet: marshaler.Packet{
						MediaType: "<media-type>",
						Data:      []byte("<data>"),
					},
				}
				persist(
					tc.Context,
					dataStore,
					persistence.SaveProcessInstance{
						Instance: expect,
					},
				)
				expect.Revision++

				inst := loadProcessInstance(tc.Context, dataStore, fixtures.DefaultHandlerKey, "<instance>")
				gomega.Expect(inst).To(gomega.Equal(expect))
			})

			ginkgo.It("does not block if the context is canceled", func() {
				// This test ensures that the implementation returns
				// immediately, either with a context.Canceled error, or with
				// the correct result.

				expect := persistence.ProcessInstance{
					HandlerKey: fixtures.DefaultHandlerKey,
					InstanceID: "<instance>",
					Packet: marshaler.Packet{
						MediaType: "<media-type>",
						Data:      []byte("<data>"),
					},
				}
				persist(
					tc.Context,
					dataStore,
					persistence.SaveProcessInstance{
						Instance: expect,
					},
				)
				expect.Revision++

				ctx, cancel := context.WithCancel(tc.Context)
				cancel()

				md, err := dataStore.LoadProcessInstance(ctx, fixtures.DefaultHandlerKey, "<instance>")
				if err != nil {
					gomega.Expect(err).To(gomega.Equal(context.Canceled))
				} else {
					gomega.Expect(md).To(gomega.Equal(expect))
				}
			})
		})
	})
}

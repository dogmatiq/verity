package providertest

import (
	"context"

	"github.com/dogmatiq/verity/persistence"
	"github.com/dogmatiq/marshalkit"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// declareProcessRepositoryTests declares a functional test-suite for a
// specific persistence.ProcessRepository implementation.
func declareProcessRepositoryTests(tc *TestContext) {
	ginkgo.Describe("type persistence.ProcessRepository", func() {
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

		ginkgo.Describe("func LoadProcessInstance()", func() {
			ginkgo.It("returns an instance with default values if the instance does not exist", func() {
				inst := loadProcessInstance(tc.Context, dataStore, "<handler-key>", "<instance>")
				gomega.Expect(inst).To(gomega.Equal(
					persistence.ProcessInstance{
						HandlerKey: "<handler-key>",
						InstanceID: "<instance>",
					},
				))
			})

			ginkgo.It("returns the current persisted instance", func() {
				expect := persistence.ProcessInstance{
					HandlerKey: "<handler-key>",
					InstanceID: "<instance>",
					Packet: marshalkit.Packet{
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

				inst := loadProcessInstance(tc.Context, dataStore, "<handler-key>", "<instance>")
				gomega.Expect(inst).To(gomega.Equal(expect))
			})

			ginkgo.It("does not block if the context is canceled", func() {
				// This test ensures that the implementation returns
				// immediately, either with a context.Canceled error, or with
				// the correct result.

				expect := persistence.ProcessInstance{
					HandlerKey: "<handler-key>",
					InstanceID: "<instance>",
					Packet: marshalkit.Packet{
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

				md, err := dataStore.LoadProcessInstance(ctx, "<handler-key>", "<instance>")
				if err != nil {
					gomega.Expect(err).To(gomega.Equal(context.Canceled))
				} else {
					gomega.Expect(md).To(gomega.Equal(expect))
				}
			})
		})
	})
}

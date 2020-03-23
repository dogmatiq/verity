package providertest

import (
	"context"

	"github.com/dogmatiq/configkit"
	configkitfixtures "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

func declareDataStoreTests(
	ctx *context.Context,
	in *In,
	out *Out,
) {
	ginkgo.Describe("type DataStore (interface)", func() {
		var dataStore persistence.DataStore

		app := configkit.FromApplication(&dogmafixtures.Application{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("<app-name>", "<app-key>")

				c.RegisterIntegration(&dogmafixtures.IntegrationMessageHandler{
					ConfigureFunc: func(c dogma.IntegrationConfigurer) {
						c.Identity("<int-name>", "<int-key>")

						c.ConsumesCommandType(dogmafixtures.MessageC{})

						c.ProducesEventType(dogmafixtures.MessageE{})
						c.ProducesEventType(dogmafixtures.MessageF{})
						c.ProducesEventType(dogmafixtures.MessageG{})
					},
				})
			},
		})

		ginkgo.BeforeEach(func() {
			var err error
			dataStore, err = out.Provider.Open(*ctx, app, in.Marshaler)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.AfterEach(func() {
			if dataStore != nil {
				dataStore.Close()
			}
		})

		ginkgo.Describe("func EventStream()", func() {
			ginkgo.It("returns a non-nil event stream", func() {
				s := dataStore.EventStream()
				gomega.Expect(s).ToNot(gomega.BeNil())
			})

			ginkgo.It("returns a stream that advertises the correct types", func() {
				s := dataStore.EventStream()

				types, err := s.MessageTypes(*ctx)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				expect := message.NewTypeSet(
					configkitfixtures.MessageEType,
					configkitfixtures.MessageFType,
					configkitfixtures.MessageGType,
				)

				gomega.Expect(
					message.IsEqualSetT(
						types,
						expect,
					),
				).To(gomega.BeTrue())
			})
		})

		// ginkgo.Describe("func MessageQueue()", func() {
		// 	ginkgo.It("returns a non-nil message queue", func() {
		// 		q := dataStore.MessageQueue()
		// 		gomega.Expect(q).ToNot(gomega.BeNil())
		// 	})
		// })

		// ginkgo.Describe("func OffsetRepository()", func() {
		// 	ginkgo.It("returns a non-nil offset repository", func() {
		// 		r := dataStore.OffsetRepository()
		// 		gomega.Expect(r).ToNot(gomega.BeNil())
		// 	})
		// })
	})
}

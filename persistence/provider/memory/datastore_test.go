package memory_test

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/persistence"
	. "github.com/dogmatiq/infix/persistence/provider/memory"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type dataStore", func() {
	var dataStore persistence.DataStore

	BeforeEach(func() {
		provider := &Provider{}

		cfg := configkit.FromApplication(&Application{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("<app>", "<app-key>")
				c.RegisterAggregate(&AggregateMessageHandler{
					ConfigureFunc: func(c dogma.AggregateConfigurer) {
						c.Identity("<agg>", "<agg-key>")
						c.ConsumesCommandType(MessageC{})
						c.ProducesEventType(MessageE{})
					},
				})
			},
		})

		var err error
		dataStore, err = provider.Open(
			context.Background(),
			cfg,
			Marshaler,
		)
		Expect(err).ShouldNot(HaveOccurred())
	})

	AfterEach(func() {
		dataStore.Close()
	})

	Describe("func EventStream()", func() {
		It("configures the stream with the expected message types", func() {
			stream, err := dataStore.EventStream(context.Background())
			Expect(err).ShouldNot(HaveOccurred())

			Expect(
				message.IsEqualSetT(
					stream.MessageTypes(),
					message.TypesOf(MessageE{}),
				),
			).To(BeTrue())
		})

		It("returns the same instance on subsequent calls", func() {
			stream1, err := dataStore.EventStream(context.Background())
			Expect(err).ShouldNot(HaveOccurred())

			stream2, err := dataStore.EventStream(context.Background())
			Expect(err).ShouldNot(HaveOccurred())

			Expect(stream1).To(BeIdenticalTo(stream2))
		})
	})
})

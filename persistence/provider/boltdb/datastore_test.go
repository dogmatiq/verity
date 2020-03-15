package boltdb_test

import (
	"context"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/internal/testing/boltdbtest"
	"github.com/dogmatiq/infix/persistence"
	. "github.com/dogmatiq/infix/persistence/provider/boltdb"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.etcd.io/bbolt"
)

var _ = Describe("type dataStore", func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		db        *bbolt.DB
		close     func()
		dataStore persistence.DataStore
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		db, close = boltdbtest.Open()

		provider := &Provider{
			DB: db,
		}

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
			ctx,
			cfg,
			Marshaler,
		)
		Expect(err).ShouldNot(HaveOccurred())
	})

	AfterEach(func() {
		if dataStore != nil {
			dataStore.Close()
		}

		if close != nil {
			close()
		}

		cancel()
	})

	Describe("func EventStream()", func() {
		It("configures the stream with the expected message types", func() {
			stream, err := dataStore.EventStream(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(
				message.IsEqualSetT(
					stream.MessageTypes(),
					message.TypesOf(MessageE{}),
				),
			).To(BeTrue())
		})

		It("returns the same instance on subsequent calls", func() {
			stream1, err := dataStore.EventStream(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			stream2, err := dataStore.EventStream(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(stream1).To(BeIdenticalTo(stream2))
		})
	})
})

// +build cgo

package sql_test

import (
	"context"
	"database/sql"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/internal/testing/sqltest"
	"github.com/dogmatiq/infix/persistence"
	. "github.com/dogmatiq/infix/persistence/provider/sql"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type dataStore", func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		db        *sql.DB
		dataStore persistence.DataStore
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		db = sqltest.Open("sqlite3")

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

		if db != nil {
			db.Close()
		}

		cancel()
	})

	Describe("func EventStream()", func() {
		It("configures the stream with the expected message types", func() {
			stream, err := dataStore.EventStream(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			types, err := stream.MessageTypes(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(
				message.IsEqualSetT(
					types,
					message.TypesOf(MessageE{}),
				),
			).To(BeTrue())
		})
	})
})

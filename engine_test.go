package verity_test

import (
	"context"
	"time"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/verity"
	. "github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/persistence/memorypersistence"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ dogma.CommandExecutor = (*Engine)(nil)

var _ = Describe("type Engine", func() {
	var (
		app            *Application
		deliveryPolicy dogma.ProjectionDeliveryPolicy
	)

	BeforeEach(func() {
		deliveryPolicy = dogma.UnicastProjectionDeliveryPolicy{}

		app = &Application{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("<app-name>", DefaultAppKey)

				c.RegisterAggregate(&AggregateMessageHandler{
					ConfigureFunc: func(c dogma.AggregateConfigurer) {
						c.Identity("<agg-name>", "e4ff048e-79f7-45e2-9f02-3b10d17614c6")
						c.ConsumesCommandType(MessageC{})
						c.ProducesEventType(MessageE{})
					},
				})

				c.RegisterProcess(&ProcessMessageHandler{
					ConfigureFunc: func(c dogma.ProcessConfigurer) {
						c.Identity("<proc-name>", "2ae0b937-e806-4e70-9b23-f36298f68973")
						c.ConsumesEventType(MessageE{})
						c.ProducesCommandType(MessageI{})
					},
				})

				c.RegisterIntegration(&IntegrationMessageHandler{
					ConfigureFunc: func(c dogma.IntegrationConfigurer) {
						c.Identity("<int-name>", "27fb3936-6f88-4873-8c56-e6a1d01f027a")
						c.ConsumesCommandType(MessageI{})
						c.ProducesEventType(MessageJ{})
					},
				})

				c.RegisterProjection(&ProjectionMessageHandler{
					ConfigureFunc: func(c dogma.ProjectionConfigurer) {
						c.Identity("<proj-name>", "b084ea4f-87d1-4001-8c1a-347c29baed35")
						c.ConsumesEventType(MessageE{})
						c.DeliveryPolicy(deliveryPolicy)
					},
				})
			},
		}
	})

	Describe("func New()", func() {
		It("allows the app to be provided as the first parameter", func() {
			Expect(func() {
				New(app)
			}).NotTo(Panic())
		})

		It("allows the app to be provided via the WithApplication() option", func() {
			Expect(func() {
				New(nil, WithApplication(app))
			}).NotTo(Panic())
		})

		It("provides default values for networking", func() {
			Expect(func() {
				New(app, WithNetworking())
			}).NotTo(Panic())
		})

		It("panics if no apps are provided", func() {
			Expect(func() {
				New(nil)
			}).To(Panic())
		})

		It("returns an error if a projection uses an unsupported delivery policy", func() {
			deliveryPolicy = dogma.BroadcastProjectionDeliveryPolicy{}

			Expect(func() {
				New(app)
			}).To(PanicWith("the <proj-name>/b084ea4f-87d1-4001-8c1a-347c29baed35 handler uses the dogma.BroadcastProjectionDeliveryPolicy delivery policy, which is not supported"))
		})
	})

	Describe("func Run()", func() {
		var (
			ctx    context.Context
			cancel context.CancelFunc
		)

		BeforeEach(func() {
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		})

		AfterEach(func() {
			cancel()
		})

		It("returns an error if the context is canceled before calling", func() {
			cancel()

			err := Run(ctx, app)
			Expect(err).To(MatchError(context.Canceled))
		})

		It("returns an error if the context is canceled while running", func() {
			go func() {
				time.Sleep(10 * time.Millisecond)
				cancel()
			}()

			err := Run(
				ctx,
				app,
				WithPersistence(&memorypersistence.Provider{}), // avoid default BoltDB location
			)
			Expect(err).To(MatchError(context.Canceled))
		})
	})
})

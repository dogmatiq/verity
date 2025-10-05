package verity_test

import (
	"context"
	"time"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/verity"
	. "github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/persistence/memorypersistence"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ dogma.CommandExecutor = (*Engine)(nil)

var _ = Describe("type Engine", func() {
	var app *ApplicationStub

	BeforeEach(func() {
		app = &ApplicationStub{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("<app-name>", DefaultAppKey)

				c.Routes(
					dogma.ViaAggregate(&AggregateMessageHandlerStub{
						ConfigureFunc: func(c dogma.AggregateConfigurer) {
							c.Identity("<agg-name>", "e4ff048e-79f7-45e2-9f02-3b10d17614c6")
							c.Routes(
								dogma.HandlesCommand[CommandStub[TypeC]](),
								dogma.RecordsEvent[EventStub[TypeE]](),
							)
						},
					}),

					dogma.ViaProcess(&ProcessMessageHandlerStub{
						ConfigureFunc: func(c dogma.ProcessConfigurer) {
							c.Identity("<proc-name>", "2ae0b937-e806-4e70-9b23-f36298f68973")
							c.Routes(
								dogma.HandlesEvent[EventStub[TypeE]](),
								dogma.ExecutesCommand[CommandStub[TypeI]](),
							)
						},
					}),

					dogma.ViaIntegration(&IntegrationMessageHandlerStub{
						ConfigureFunc: func(c dogma.IntegrationConfigurer) {
							c.Identity("<int-name>", "27fb3936-6f88-4873-8c56-e6a1d01f027a")
							c.Routes(
								dogma.HandlesCommand[CommandStub[TypeI]](),
								dogma.RecordsEvent[EventStub[TypeJ]](),
							)
						},
					}),

					dogma.ViaProjection(&ProjectionMessageHandlerStub{
						ConfigureFunc: func(c dogma.ProjectionConfigurer) {
							c.Identity("<proj-name>", "b084ea4f-87d1-4001-8c1a-347c29baed35")
							c.Routes(
								dogma.HandlesEvent[EventStub[TypeE]](),
							)
						},
					}),

					dogma.ViaProjection(&ProjectionMessageHandlerStub{
						ConfigureFunc: func(c dogma.ProjectionConfigurer) {
							c.Identity("<disabled-proj-name>", "4ad6edf4-78b3-46bc-8321-0e1b834e3808")
							c.Routes(
								dogma.HandlesEvent[EventStub[TypeE]](),
							)
							c.Disable()
						},
					}),
				)
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
	})

	Describe("func Run()", func() {
		var (
			ctx    context.Context
			cancel context.CancelFunc
		)

		BeforeEach(func() {
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			DeferCleanup(cancel)
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
